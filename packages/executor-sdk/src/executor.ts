/**
 * DSH SDK executor: one `@gethen/sdk-driver` client per workspace behind the
 * worker-agent `SessionExecutor` port.
 *
 * A dispatch becomes a DSH SDK session named after its assignment:
 *
 * 1. ensure the workspace's `dsh --profile sdk` client exists and is initialized
 *    with the configured provider/model/cwd and the approval + user-question
 *    capabilities this executor answers;
 * 2. subscribe to `session.event` for that session and forward every event;
 * 3. prompt the session with the Master's assignment prompt (or the whole
 *    envelope when it carries no `prompt` string);
 * 4. `session/wait` until the turn is quiet, then parse the last assistant text
 *    containing a `[GETHEN-REPORT]` block and report it (`waiting` when the
 *    worker produced no report, `failed` on transport or parse errors).
 *
 * DSH's native channels map onto Master escalations:
 * - `approval/request` becomes a blocking `permission` escalation
 *   (`allow <tool>?`, options allow/reject); the resolution's option decides the
 *   DSH outcome (`allowed-once` vs `rejected`).
 * - `user-question/ask` becomes one blocking `decision` escalation per question
 *   (sequentially, because the worker protocol carries one live escalation at a
 *   time); a `answer` resolution maps back to the DSH `answers` batch, while
 *   `escalate`/`drop` map to `{cancelled:true}`.
 *
 * Cancellation: the DSH SDK protocol has no per-turn cancel/interrupt method
 * (only process shutdown), so `ExecutionHandle.cancel` closes the workspace's
 * SDK client — killing every session it hosts — and reports `failed`. See
 * README.md.
 */
import { parseReportText, REPORT_OPEN } from "@gethen/master";
import type {
  ApprovalOutcome,
  ApprovalRequestParams,
  InitializeOptions,
  InitializeResult,
  SessionEventNotification,
  SessionPromptResult,
  StartDshSdkOptions,
  UserQuestionRequest,
  UserQuestionResult,
} from "@gethen/sdk-driver";
import { DshSdkClient } from "@gethen/sdk-driver";
import type {
  ExecutionHandle,
  ExecutorEscalation,
  ExecutorEvent,
  ExecutorReport,
  ExecutorSink,
  ExecutorTask,
  SessionExecutor,
} from "@gethen/worker-agent";

/** The `ResolutionBody` the worker protocol hands to `sendResolution`. */
export type ExecutorResolution = Parameters<ExecutionHandle["sendResolution"]>[0];

/** The subset of `@gethen/sdk-driver`'s client the executor drives. */
export interface SdkClientLike {
  /** True when a factory supplied an already-started client. */
  readonly running?: boolean;
  start?(options: StartDshSdkOptions): unknown;
  initialize(options: InitializeOptions): Promise<InitializeResult>;
  prompt(sessionId: string, text: string): Promise<SessionPromptResult>;
  wait(sessionId: string, timeoutMs?: number): Promise<void>;
  onEvent(listener: (notification: SessionEventNotification) => void): () => void;
  onApprovalRequest(
    handler: (params: ApprovalRequestParams) => ApprovalOutcome | Promise<ApprovalOutcome>,
  ): void;
  onUserQuestion(
    handler: (request: UserQuestionRequest) => UserQuestionResult | Promise<UserQuestionResult>,
  ): void;
  close(): void;
}

export interface SdkExecutorOptions {
  /** Command used to spawn the runtime; defaults to `GETHEN_DSH`/`dsh`. */
  dshCommand?: string;
  /** Runtime arguments; defaults to `["--profile", "sdk"]`. */
  dshArgs?: string[];
  provider: string;
  model: string;
  /** Session cwd per workspace; defaults to the workspace string itself. */
  cwdFor?: (workspace: string) => string;
  /** Injection seam for tests or a shared client pool. */
  clientFactory?: () => SdkClientLike;
  /** JSON-RPC request timeout handed to the client; long waits may raise it. */
  requestTimeoutMs?: number;
  /** Injectable clock (used for the assignment-deadline guard and any timestamps). */
  now?: () => Date;
}

/** A `SessionExecutor` that can also release its pooled runtime clients. */
export interface SdkExecutor extends SessionExecutor {
  /** Close every pooled SDK client and stop accepting work. */
  close(): Promise<void>;
}

/** Per-session routing table attached to one workspace client. */
interface SessionBinding {
  event(notification: SessionEventNotification): void;
  approval(params: ApprovalRequestParams): Promise<ApprovalOutcome>;
  question(request: UserQuestionRequest): Promise<UserQuestionResult>;
}

interface WorkspaceClient {
  workspace: string;
  client: SdkClientLike;
  sessions: Map<string, SessionBinding>;
  detach: () => void;
}

/** How an execution settled: it reports exactly once. */
interface ExecutionState {
  task: ExecutorTask;
  sink: ExecutorSink;
  workspaceClient: WorkspaceClient | undefined;
  settled: boolean;
  cancelled: boolean;
  /** Last assistant text that contained a report block. */
  reportText: string | undefined;
  /** One escalation is live at a time; later ones queue on `escalationChain`. */
  pending: PendingEscalation | undefined;
  escalationChain: Promise<void>;
  escalationSeq: number;
}

interface PendingEscalation {
  resolve(resolution: ExecutorResolution): void;
  reject(error: Error): void;
}

export class SdkExecutorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SdkExecutorError";
  }
}

/**
 * Create the SDK-backed SessionExecutor. One SDK runtime is kept per workspace
 * (initialized once); each dispatch runs in its own session id.
 */
export function createSdkExecutor(options: SdkExecutorOptions): SdkExecutor {
  return new SdkExecutorImpl(options);
}

class SdkExecutorImpl implements SdkExecutor {
  private readonly options: SdkExecutorOptions;
  private readonly cwdFor: (workspace: string) => string;
  private readonly clientFactory: () => SdkClientLike;
  private readonly now: () => Date;
  private readonly clients = new Map<string, WorkspaceClient>();
  private readonly creating = new Map<string, Promise<WorkspaceClient>>();
  private closed = false;

  constructor(options: SdkExecutorOptions) {
    this.options = options;
    this.cwdFor = options.cwdFor ?? ((workspace) => workspace);
    this.clientFactory = options.clientFactory ?? (() => new DshSdkClient());
    this.now = options.now ?? (() => new Date());
  }

  async start(task: ExecutorTask, sink: ExecutorSink): Promise<ExecutionHandle> {
    const state: ExecutionState = {
      task,
      sink,
      workspaceClient: undefined,
      settled: false,
      cancelled: false,
      reportText: undefined,
      pending: undefined,
      escalationChain: Promise.resolve(),
      escalationSeq: 0,
    };
    const handle: ExecutionHandle = {
      sendResolution: async (resolution: ExecutorResolution): Promise<void> => {
        const pending = state.pending;
        if (pending === undefined) return; // Duplicate or late resolution.
        pending.resolve(resolution);
      },
      cancel: async (reason?: string): Promise<void> => {
        this.cancelExecution(state, reason);
      },
    };

    if (this.guardStaleTask(state)) return handle;
    if (this.closed) {
      this.finish(state, {
        kind: "failed",
        summary: "executor is closed",
      });
      return handle;
    }

    let workspaceClient: WorkspaceClient;
    try {
      workspaceClient = await this.ensureClient(task.workspace);
    } catch (error) {
      this.finish(state, {
        kind: "failed",
        summary: `failed to start DSH SDK runtime: ${messageOf(error)}`,
      });
      return handle;
    }
    state.workspaceClient = workspaceClient;
    if (state.cancelled || state.settled) return handle;

    workspaceClient.sessions.set(task.assignmentId, this.bindingFor(state));
    try {
      await workspaceClient.client.prompt(task.assignmentId, promptFor(task));
    } catch (error) {
      workspaceClient.sessions.delete(task.assignmentId);
      if (!state.cancelled) {
        this.finish(state, {
          kind: "failed",
          summary: `failed to prompt DSH SDK session: ${messageOf(error)}`,
        });
      }
      return handle;
    }
    if (state.cancelled || state.settled) return handle;

    void this.run(state, workspaceClient);
    return handle;
  }

  /** Close all pooled clients; running executions are left to their agent cancel. */
  async close(): Promise<void> {
    this.closed = true;
    const clients = [...this.clients.values()];
    this.clients.clear();
    for (const workspaceClient of clients) {
      workspaceClient.detach();
      workspaceClient.sessions.clear();
      try {
        workspaceClient.client.close();
      } catch {
        // Closing is best effort; the daemon is going away.
      }
    }
  }

  private guardStaleTask(state: ExecutionState): boolean {
    const { deadlineAt } = state.task;
    if (deadlineAt === undefined) return false;
    if (Date.parse(deadlineAt) > this.now().getTime()) return false;
    this.finish(state, {
      kind: "failed",
      summary: "assignment deadline had already passed",
    });
    return true;
  }

  private async ensureClient(workspace: string): Promise<WorkspaceClient> {
    if (this.closed) throw new SdkExecutorError("executor is closed");
    const ready = this.clients.get(workspace);
    if (ready !== undefined) return ready;
    const inFlight = this.creating.get(workspace);
    if (inFlight !== undefined) return inFlight;

    const creation = this.createClient(workspace).then(
      (workspaceClient) => {
        this.creating.delete(workspace);
        if (this.closed) {
          workspaceClient.detach();
          workspaceClient.client.close();
          throw new SdkExecutorError("executor closed while starting");
        }
        this.clients.set(workspace, workspaceClient);
        return workspaceClient;
      },
      (error: unknown) => {
        this.creating.delete(workspace);
        throw error;
      },
    );
    this.creating.set(workspace, creation);
    return creation;
  }

  private async createClient(workspace: string): Promise<WorkspaceClient> {
    const cwd = this.cwdFor(workspace);
    const client = this.clientFactory();
    if (client.running !== true) {
      const startOptions: StartDshSdkOptions = { cwd };
      if (this.options.dshCommand !== undefined) startOptions.command = this.options.dshCommand;
      if (this.options.dshArgs !== undefined) startOptions.args = [...this.options.dshArgs];
      if (this.options.requestTimeoutMs !== undefined) {
        startOptions.requestTimeoutMs = this.options.requestTimeoutMs;
      }
      client.start?.(startOptions);
    }
    const sessions = new Map<string, SessionBinding>();
    const detachEvent = client.onEvent((notification) => {
      sessions.get(notification.sessionId)?.event(notification);
    });
    client.onApprovalRequest((params) => {
      const binding = sessions.get(params.sessionId);
      return binding === undefined ? "cancelled" : binding.approval(params);
    });
    client.onUserQuestion((request) => {
      const binding = sessions.get(request.sessionId);
      return binding === undefined ? { cancelled: true } : binding.question(request);
    });
    const initialize: InitializeOptions = {
      provider: this.options.provider,
      model: this.options.model,
      cwd,
      approvalRequests: true,
      userQuestions: true,
    };
    try {
      await client.initialize(initialize);
    } catch (error) {
      detachEvent();
      try {
        client.close();
      } catch {
        // ignore; the initialize failure is the actionable error
      }
      throw error;
    }
    return { workspace, client, sessions, detach: detachEvent };
  }

  private bindingFor(state: ExecutionState): SessionBinding {
    return {
      event: (notification) => {
        const event = notification.event as unknown as ExecutorEvent;
        state.sink.event(event);
        const text = assistantTextOf(event);
        if (text?.includes(REPORT_OPEN)) state.reportText = text;
      },
      approval: (params) => this.askApproval(state, params),
      question: (request) => this.askQuestion(state, request),
    };
  }

  private async run(state: ExecutionState, workspaceClient: WorkspaceClient): Promise<void> {
    try {
      await workspaceClient.client.wait(state.task.assignmentId);
    } catch (error) {
      if (!state.cancelled) {
        this.finish(state, {
          kind: "failed",
          summary: `DSH SDK wait failed: ${messageOf(error)}`,
        });
      }
      this.release(state);
      return;
    }
    if (state.settled || state.cancelled) {
      this.release(state);
      return;
    }
    this.finish(state, this.reportFor(state));
    this.release(state);
  }

  private reportFor(state: ExecutionState): ExecutorReport {
    const text = state.reportText;
    if (text === undefined) {
      return { kind: "waiting", summary: "worker produced no report" };
    }
    let report: ReturnType<typeof parseReportText>;
    try {
      report = parseReportText(text);
    } catch (error) {
      return { kind: "failed", summary: `worker report is invalid: ${messageOf(error)}` };
    }
    if (report === undefined) {
      return { kind: "waiting", summary: "worker produced no report" };
    }
    const result: ExecutorReport = { kind: report.kind, summary: report.summary };
    if (report.question !== undefined) result.question = report.question;
    return result;
  }

  private async askApproval(
    state: ExecutionState,
    params: ApprovalRequestParams,
  ): Promise<ApprovalOutcome> {
    const escalation: ExecutorEscalation = {
      escalationId: `${state.task.assignmentId}:approval:${params.callId ?? `${state.escalationSeq}`}`,
      kind: "permission",
      question: `allow ${params.toolName}?`,
      options: [
        { id: "allow", label: "Allow once" },
        { id: "reject", label: "Reject" },
      ],
    };
    state.escalationSeq += 1;
    try {
      const resolution = await this.requestEscalation(state, escalation);
      return resolution.kind === "answer" && resolution.option === "allow"
        ? "allowed-once"
        : "rejected";
    } catch {
      return "rejected";
    }
  }

  private async askQuestion(
    state: ExecutionState,
    request: UserQuestionRequest,
  ): Promise<UserQuestionResult> {
    const answers: { id: string; selected: string[]; custom?: string }[] = [];
    for (let index = 0; index < request.questions.length; index += 1) {
      const question = request.questions[index];
      if (question === undefined) continue;
      const options = (question.options ?? []).map((option, optionIndex) => ({
        id: `q${optionIndex}`,
        label: option.label,
      }));
      const suffix =
        request.questions.length > 1 ? ` (${index + 1}/${request.questions.length})` : "";
      let resolution: ExecutorResolution;
      try {
        resolution = await this.requestEscalation(state, {
          escalationId: `${state.task.assignmentId}:question:${question.id}`,
          kind: "decision",
          question: `${question.question}${suffix}`,
          options,
        });
      } catch {
        return { cancelled: true };
      }
      if (resolution.kind !== "answer") return { cancelled: true };

      const matched = options.find(
        (option) =>
          option.id === resolution.option ||
          (option.label === resolution.option && option.id !== ""),
      );
      if (matched !== undefined) {
        answers.push({ id: question.id, selected: [matched.label] });
      } else if (resolution.text.length > 0) {
        answers.push({ id: question.id, selected: [], custom: resolution.text });
      } else if (options[0] !== undefined) {
        const fallbackOption = options[0];
        answers.push({ id: question.id, selected: [fallbackOption.label] });
      } else {
        answers.push({ id: question.id, selected: [], custom: "" });
      }
    }
    return { answers };
  }

  /** Emit one escalation and wait for its resolution, serializing escalations. */
  private requestEscalation(
    state: ExecutionState,
    escalation: ExecutorEscalation,
  ): Promise<ExecutorResolution> {
    if (state.pending === undefined) {
      const run = this.emitEscalation(state, escalation);
      state.escalationChain = run.then(
        () => undefined,
        () => undefined,
      );
      return run;
    }
    const run = state.escalationChain.then(() => this.emitEscalation(state, escalation));
    state.escalationChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private emitEscalation(
    state: ExecutionState,
    escalation: ExecutorEscalation,
  ): Promise<ExecutorResolution> {
    if (state.cancelled || state.settled) {
      return Promise.reject(new SdkExecutorError("execution is no longer running"));
    }
    return new Promise<ExecutorResolution>((resolvePromise, rejectPromise) => {
      let pending: PendingEscalation;
      const detach = (): void => {
        if (state.pending === pending) state.pending = undefined;
      };
      pending = {
        resolve: (resolution) => {
          detach();
          resolvePromise(resolution);
        },
        reject: (error) => {
          detach();
          rejectPromise(error);
        },
      };
      state.pending = pending;
      try {
        state.sink.escalation(escalation);
      } catch (error) {
        detach();
        rejectPromise(toError(error));
      }
    });
  }

  private cancelExecution(state: ExecutionState, reason?: string): void {
    if (state.settled) return;
    state.cancelled = true;
    state.pending?.reject(new SdkExecutorError("execution cancelled"));
    const workspaceClient = state.workspaceClient;
    if (workspaceClient !== undefined) this.closeWorkspaceClient(workspaceClient);
    this.finish(state, {
      kind: "failed",
      summary: reason === undefined ? "cancelled" : `cancelled: ${reason}`,
    });
    this.release(state);
  }

  private closeWorkspaceClient(workspaceClient: WorkspaceClient): void {
    if (this.clients.get(workspaceClient.workspace) === workspaceClient) {
      this.clients.delete(workspaceClient.workspace);
    }
    workspaceClient.detach();
    workspaceClient.sessions.clear();
    try {
      workspaceClient.client.close();
    } catch {
      // Best effort: cancellation reports the failure regardless.
    }
  }

  private finish(state: ExecutionState, report: ExecutorReport): void {
    if (state.settled) return;
    state.settled = true;
    state.sink.report(report);
  }

  private release(state: ExecutionState): void {
    state.workspaceClient?.sessions.delete(state.task.assignmentId);
  }
}

/** The prompt sent to the session: the envelope's `prompt`, else the envelope. */
function promptFor(task: ExecutorTask): string {
  const prompt = task.envelope.prompt;
  return typeof prompt === "string" ? prompt : JSON.stringify(task.envelope);
}

/** Assistant text carried by a DSH `assistant/message` session event. */
function assistantTextOf(event: ExecutorEvent): string | undefined {
  if (event.type !== "assistant/message") return undefined;
  const data = asRecord(event.data);
  const message = asRecord(data?.message);
  const content = message?.content;
  if (!Array.isArray(content)) return undefined;
  const parts: string[] = [];
  for (const block of content) {
    const record = asRecord(block);
    if (record?.type === "text" && typeof record.text === "string") {
      parts.push(record.text);
    }
  }
  return parts.length === 0 ? undefined : parts.join("");
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

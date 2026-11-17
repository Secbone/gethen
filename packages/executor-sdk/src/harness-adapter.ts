/**
 * DSH as a `HarnessExecutor`: one `@gethen/sdk-driver` client per workspace.
 *
 * This is the package's only execution implementation. It implements the
 * harness-agnostic port from `@gethen/harness` directly — no worker-protocol
 * types, no worker-agent `SessionExecutor`, no transport:
 *
 * 1. a `HarnessTask` becomes a DSH SDK session whose id is the assignment id;
 *    the workspace's `dsh --profile sdk` client is started once and initialized
 *    with `provider`/`model`/`cwd` plus `approvalRequests` and `userQuestions`;
 * 2. every `session.event` payload (assistant text, tool results, tool inputs,
 *    any string field) is scanned for a complete `[GETHEN-REPORT]` block (last
 *    block wins) and a `[GETHEN-ESCALATION]` block (worker-side compat tool);
 * 3. `session/wait` ends the turn; the report is parsed with
 *    `@gethen/master`'s `parseReportText` and narrowed to the port's three
 *    states (`ready`/`failed`/`waiting`);
 * 4. DSH's native channels map onto blocking `HarnessEscalation`s:
 *    `approval/request` → `permission` (`allow <tool>?`), `user-question/ask` →
 *    one `decision` per question, and a `[GETHEN-ESCALATION]` tool result → the
 *    escalation it carries. `HarnessExecution.sendResolution` answers them;
 * 5. errors (initialize, prompt, wait, report parse, resolution delivery) become
 *    a single `HarnessReport{kind:"failed"}`; `start` never rejects;
 * 6. `cancel` keeps the DSH limitation: the SDK protocol has no per-turn
 *    cancel, so it closes the workspace runtime (all its sessions) and reports
 *    `failed`.
 */
import type {
  HarnessEscalation,
  HarnessEvent,
  HarnessExecution,
  HarnessExecutor,
  HarnessReport,
  HarnessResolution,
  HarnessSink,
  HarnessTask,
  JsonObject,
} from "@gethen/harness";
import { buildResolutionText, parseReportText, REPORT_CLOSE, REPORT_OPEN } from "@gethen/master";
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

/** Worker-side compat markers emitted by the `gethen_escalate` tool result. */
const ESCALATION_OPEN = "[GETHEN-ESCALATION]";
const ESCALATION_CLOSE = "[/GETHEN-ESCALATION]";
/** A single execution answers at most this many tool-result escalations. */
const MAX_TOOL_ESCALATIONS = 5;

/** Options accepted by the DSH harness executor. */
export interface SdkHarnessExecutorOptions {
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
  /** Injectable clock (assignment-deadline guard and event timestamp fallback). */
  now?: () => Date;
}

/** A `HarnessExecutor` that can also release its pooled DSH runtimes. */
export interface SdkHarnessExecutor extends HarnessExecutor {
  /** Close every pooled SDK client and stop accepting work. */
  close(): Promise<void>;
}

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

/** A consumer-side failure with a stable code. */
export class SdkHarnessError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SdkHarnessError";
  }
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

/** How one execution settled: it reports exactly once. */
interface ExecutionState {
  task: HarnessTask;
  sink: HarnessSink;
  workspaceClient: WorkspaceClient | undefined;
  settled: boolean;
  cancelled: boolean;
  /** Last event string that contained a complete report block. */
  reportText: string | undefined;
  /** Last complete escalation block found in an event string, not yet answered. */
  toolEscalation: HarnessEscalation | undefined;
  /** Tool-result escalations answered so far. */
  rounds: number;
  /** One escalation is live at a time; later ones queue on `escalationChain`. */
  pending: PendingEscalation | undefined;
  escalationChain: Promise<void>;
  escalationSeq: number;
}

interface PendingEscalation {
  resolve(resolution: HarnessResolution): void;
  reject(error: Error): void;
}

/**
 * Create the DSH-backed `HarnessExecutor`. One runtime is kept per workspace;
 * each `start` runs one DSH session named after its assignment.
 */
export function createSdkHarnessExecutor(options: SdkHarnessExecutorOptions): SdkHarnessExecutor {
  return new DshHarnessExecutor(options);
}

class DshHarnessExecutor implements SdkHarnessExecutor {
  private readonly options: SdkHarnessExecutorOptions;
  private readonly cwdFor: (workspace: string) => string;
  private readonly clientFactory: () => SdkClientLike;
  private readonly now: () => Date;
  private readonly clients = new Map<string, WorkspaceClient>();
  private readonly creating = new Map<string, Promise<WorkspaceClient>>();
  private closed = false;

  constructor(options: SdkHarnessExecutorOptions) {
    this.options = options;
    this.cwdFor = options.cwdFor ?? ((workspace) => workspace);
    this.clientFactory = options.clientFactory ?? (() => new DshSdkClient());
    this.now = options.now ?? (() => new Date());
  }

  async start(task: HarnessTask, sink: HarnessSink): Promise<HarnessExecution> {
    const state: ExecutionState = {
      task,
      sink,
      workspaceClient: undefined,
      settled: false,
      cancelled: false,
      reportText: undefined,
      toolEscalation: undefined,
      rounds: 0,
      pending: undefined,
      escalationChain: Promise.resolve(),
      escalationSeq: 0,
    };
    const execution: HarnessExecution = {
      sendResolution: async (resolution: HarnessResolution): Promise<void> => {
        const pending = state.pending;
        if (pending === undefined) return; // Duplicate or late resolution.
        pending.resolve(resolution);
      },
      cancel: async (reason?: string): Promise<void> => {
        this.cancelExecution(state, reason);
      },
    };

    if (this.guardStaleTask(state)) return execution;
    if (this.closed) {
      this.finish(state, { kind: "failed", summary: "executor is closed" });
      return execution;
    }

    let workspaceClient: WorkspaceClient;
    try {
      workspaceClient = await this.ensureClient(task.workspace);
    } catch (error) {
      this.finish(state, {
        kind: "failed",
        summary: `failed to start DSH SDK runtime: ${messageOf(error)}`,
      });
      return execution;
    }
    state.workspaceClient = workspaceClient;
    if (state.cancelled || state.settled) return execution;

    workspaceClient.sessions.set(task.assignmentId, this.bindingFor(state));
    try {
      await workspaceClient.client.prompt(task.assignmentId, task.prompt);
    } catch (error) {
      workspaceClient.sessions.delete(task.assignmentId);
      if (!state.cancelled) {
        this.finish(state, {
          kind: "failed",
          summary: `failed to prompt DSH SDK session: ${messageOf(error)}`,
        });
      }
      return execution;
    }
    if (state.cancelled || state.settled) return execution;

    void this.run(state, workspaceClient);
    return execution;
  }

  /** Close all pooled clients; running executions are left to their consumer. */
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
    this.finish(state, { kind: "failed", summary: "assignment deadline had already passed" });
    return true;
  }

  private async ensureClient(workspace: string): Promise<WorkspaceClient> {
    if (this.closed) throw new SdkHarnessError("executor is closed");
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
          throw new SdkHarnessError("executor closed while starting");
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
        if (state.settled) return;
        state.sink.event(mapSessionEvent(notification.event, this.now()));
        scanEvent(state, notification.event);
      },
      approval: (params) => this.askApproval(state, params),
      question: (request) => this.askQuestion(state, request),
    };
  }

  private async run(state: ExecutionState, workspaceClient: WorkspaceClient): Promise<void> {
    for (;;) {
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

      // A report always wins over a pending tool-result escalation in the same
      // batch: the worker finished, whatever else it printed.
      if (state.reportText !== undefined) {
        this.finish(state, this.reportFor(state));
        this.release(state);
        return;
      }

      const escalation = state.toolEscalation;
      if (escalation === undefined) {
        this.finish(state, this.reportFor(state));
        this.release(state);
        return;
      }
      state.toolEscalation = undefined;
      state.rounds += 1;
      if (state.rounds > MAX_TOOL_ESCALATIONS) {
        this.finish(state, {
          kind: "failed",
          summary: `worker escalated more than ${MAX_TOOL_ESCALATIONS} times`,
        });
        this.release(state);
        return;
      }

      let resolution: HarnessResolution;
      try {
        resolution = await this.requestEscalation(
          state,
          escalationForAssignment(state.task.assignmentId, escalation),
        );
      } catch {
        this.release(state);
        return; // Cancelled: the failed report was already emitted.
      }
      if (state.settled || state.cancelled) {
        this.release(state);
        return;
      }

      try {
        await workspaceClient.client.prompt(
          state.task.assignmentId,
          resolutionPromptFor(escalation, resolution),
        );
      } catch (error) {
        if (!state.cancelled) {
          this.finish(state, {
            kind: "failed",
            summary: `failed to deliver the resolution to the DSH SDK session: ${messageOf(error)}`,
          });
        }
        this.release(state);
        return;
      }
      if (state.settled || state.cancelled) {
        this.release(state);
        return;
      }
    }
  }

  private reportFor(state: ExecutionState): HarnessReport {
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
    const mapped: HarnessReport = {
      kind: harnessReportKind(report.kind),
      summary: report.summary,
    };
    if (report.question !== undefined) mapped.question = report.question;
    return mapped;
  }

  private async askApproval(
    state: ExecutionState,
    params: ApprovalRequestParams,
  ): Promise<ApprovalOutcome> {
    state.escalationSeq += 1;
    const escalation: HarnessEscalation = {
      id: `${state.task.assignmentId}:approval:${params.callId ?? `${state.escalationSeq}`}`,
      kind: "permission",
      question: `allow ${params.toolName}?`,
      options: [
        { id: "allow", label: "Allow once" },
        { id: "reject", label: "Reject" },
      ],
      blocking: true,
    };
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
      let resolution: HarnessResolution;
      try {
        resolution = await this.requestEscalation(state, {
          id: `${state.task.assignmentId}:question:${question.id}`,
          kind: "decision",
          question: `${question.question}${suffix}`,
          options,
          blocking: true,
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
      } else if (resolution.text !== undefined && resolution.text.length > 0) {
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
    escalation: HarnessEscalation,
  ): Promise<HarnessResolution> {
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
    escalation: HarnessEscalation,
  ): Promise<HarnessResolution> {
    if (state.cancelled || state.settled) {
      return Promise.reject(new SdkHarnessError("execution is no longer running"));
    }
    return new Promise<HarnessResolution>((resolvePromise, rejectPromise) => {
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
    state.pending?.reject(new SdkHarnessError("execution cancelled"));
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

  private finish(state: ExecutionState, report: HarnessReport): void {
    if (state.settled) return;
    state.settled = true;
    state.sink.report(report);
  }

  private release(state: ExecutionState): void {
    state.workspaceClient?.sessions.delete(state.task.assignmentId);
  }
}

// ---------------------------------------------------------------- pure helpers

/** Narrow a worker report kind to the harness port's three states. */
export function harnessReportKind(kind: string): HarnessReport["kind"] {
  switch (kind) {
    case "ready":
    case "dropped":
      return "ready";
    case "failed":
      return "failed";
    default:
      // waiting / blocked / progress and anything unknown are not terminal.
      return "waiting";
  }
}

/**
 * Map one `session.event` payload to a `HarnessEvent`: `type` from the event,
 * RFC 3339 `at` from its epoch-ms `time` (or `at`), and the event `data` object
 * (or the whole payload when it has none).
 */
export function mapSessionEvent(event: unknown, now: Date): HarnessEvent {
  const record = asRecord(event) ?? {};
  const rawType = record.type;
  const type = typeof rawType === "string" && rawType.length > 0 ? rawType : "event";
  const at = eventTimestamp(record) ?? now.toISOString();
  const data = asRecord(record.data);
  return { type, at, data: (data ?? record) as unknown as JsonObject };
}

/** The prompt sent back into the session for a tool-result escalation answer. */
function resolutionPromptFor(escalation: HarnessEscalation, resolution: HarnessResolution): string {
  const text = resolution.text ?? "";
  const fallback =
    text.length > 0
      ? text
      : resolution.kind === "answer"
        ? "Proceed with the selected option."
        : "A person must decide this; report blocked and end the turn.";
  return buildResolutionText({
    id: `resolution-${escalation.id}`,
    text: fallback,
    basis: ["policy"],
    ...(resolution.option === undefined ? {} : { option: resolution.option }),
  });
}

/** Ensure a tool-result escalation id is unique within its assignment. */
function escalationForAssignment(
  assignmentId: string,
  escalation: HarnessEscalation,
): HarnessEscalation {
  if (!escalation.id.startsWith("tool-escalation-")) return escalation;
  return { ...escalation, id: `${assignmentId}:${escalation.id}` };
}

/**
 * Scan one session-event payload for report and escalation blocks. Only
 * individual string values are considered, so a block split across events (or
 * fields) never gets glued together; a complete marker pair is required, which
 * keeps prose that merely mentions a marker from matching.
 */
function scanEvent(state: ExecutionState, event: unknown): void {
  for (const text of collectStrings(event)) {
    if (text.includes(REPORT_OPEN) && text.includes(REPORT_CLOSE)) {
      state.reportText = text;
      continue;
    }
    if (text.includes(ESCALATION_OPEN) && text.includes(ESCALATION_CLOSE)) {
      const escalation = parseToolEscalation(text);
      if (escalation !== undefined) state.toolEscalation = escalation;
    }
  }
}

/** Every string value in a JSON-ish payload, depth first. */
function collectStrings(value: unknown, out: string[] = []): string[] {
  if (typeof value === "string") {
    out.push(value);
  } else if (Array.isArray(value)) {
    for (const item of value) collectStrings(item, out);
  } else if (typeof value === "object" && value !== null) {
    for (const item of Object.values(value)) collectStrings(item, out);
  }
  return out;
}

/** Decode `[GETHEN-ESCALATION] ... [/GETHEN-ESCALATION]`; undefined when unusable. */
function parseToolEscalation(text: string): HarnessEscalation | undefined {
  const start = text.indexOf(ESCALATION_OPEN);
  const end = text.indexOf(ESCALATION_CLOSE, start + ESCALATION_OPEN.length);
  if (start === -1 || end === -1) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text.slice(start + ESCALATION_OPEN.length, end).trim());
  } catch {
    return undefined;
  }
  const record = asRecord(parsed);
  if (record === undefined) return undefined;
  const question = record.question;
  if (typeof question !== "string" || question.length === 0) return undefined;
  const kind = record.kind;
  if (!isEscalationKind(kind)) return undefined;
  const options: { id: string; label: string }[] = [];
  if (Array.isArray(record.options)) {
    for (const entry of record.options) {
      const option = asRecord(entry);
      if (option === undefined) continue;
      const { id, label } = option;
      if (typeof id === "string" && typeof label === "string") options.push({ id, label });
    }
  }
  const escalation: HarnessEscalation = {
    id:
      typeof record.id === "string" && record.id.length > 0
        ? record.id
        : `tool-escalation-${question.slice(0, 24)}`,
    kind,
    question,
    options,
    blocking: true,
  };
  if (typeof record.defaultOption === "string" && record.defaultOption.length > 0) {
    escalation.defaultOption = record.defaultOption;
  }
  return escalation;
}

function isEscalationKind(value: unknown): value is HarnessEscalation["kind"] {
  return (
    value === "decision" ||
    value === "blocked" ||
    value === "permission" ||
    value === "scope" ||
    value === "conflict"
  );
}

function eventTimestamp(record: Record<string, unknown>): string | undefined {
  const time = record.time;
  if (typeof time === "number" && Number.isFinite(time)) {
    return new Date(time).toISOString();
  }
  const at = record.at;
  if (typeof at === "string" && at.length > 0) return at;
  return undefined;
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

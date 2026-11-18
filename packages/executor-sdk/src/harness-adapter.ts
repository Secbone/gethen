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
 *    any string field) is scanned for a complete `[GETHEN-REPORT]` block (the
 *    last syntactically valid block wins) and a `[GETHEN-ESCALATION]` block;
 * 3. completion is **event-driven**: the session's `turn/end` event ends the
 *    turn, with a `session.status → idle` event as a fallback; `session/wait`
 *    is only a background fallback for a broken event stream (no total timeout);
 * 4. a silence watchdog (`idleTimeoutMs`, default 5 min) fails the execution
 *    when no event arrives for too long; a task `deadlineAt` also fails it;
 * 5. DSH's native channels map onto blocking `HarnessEscalation`s:
 *    `approval/request` → `permission`, `user-question/ask` → one `decision` per
 *    question, and a `[GETHEN-ESCALATION]` tool result → the escalation it
 *    carries. `HarnessExecution.sendResolution` answers them;
 * 6. every event updates a live `HarnessTaskState` (`state()`); `steer(text)`
 *    queues more input; `interrupt()` rejects pending escalation, steers a stop,
 *    waits a grace period and then terminates the runtime; `cancel(reason)`
 *    performs that interrupt and reports `failed`;
 * 7. session selection: `session.mode: "new"` (default) names the DSH session
 *    after the assignment; `"reuse"` resumes `sessionId` or the session behind a
 *    registry `key` (created on first use), never silently starting a new one on
 *    a failed resume. `sessions()` exposes the in-memory session directory;
 * 8. the runtime's stderr and lifecycle milestones (`start`/`end`/`watchdog`/
 *    `deadline`/`cancel`) are forwarded as bounded `HarnessEvent{type:"log"}`
 *    events, so the Master sees the whole worker process;
 * 9. `isolation: "process"` gives each assignment its own runtime (terminated on
 *    completion/cancel); `"shared"` (default) reuses the per-workspace pool and
 *    only closes that client on a hard stop. DSH has no native per-turn
 *    interrupt, so a stop means closing (shared) or killing (dedicated) the
 *    runtime.
 */

import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { DshSessionStore, type SessionSummary } from "@gethen/dsh-sessions";
import type {
  HarnessEscalation,
  HarnessEvent,
  HarnessExecution,
  HarnessExecutor,
  HarnessReport,
  HarnessResolution,
  HarnessSessionEvent,
  HarnessSessionHistoryOptions,
  HarnessSessionInfo,
  HarnessSink,
  HarnessTask,
  HarnessTaskPhase,
  HarnessTaskState,
  JsonObject,
  SessionSpec,
} from "@gethen/harness";
import { buildResolutionText, parseReportText, REPORT_CLOSE, REPORT_OPEN } from "@gethen/master";
import type {
  ApprovalOutcome,
  ApprovalRequestParams,
  InitializeOptions,
  InitializeResult,
  SessionEventNotification,
  SessionPromptResult,
  SessionStatusNotification,
  StartDshSdkOptions,
  UserQuestionRequest,
  UserQuestionResult,
  WaitOptions,
} from "@gethen/sdk-driver";
import { DshSdkClient } from "@gethen/sdk-driver";

/** Worker-side compat markers emitted by the `gethen_escalate` tool result. */
const ESCALATION_OPEN = "[GETHEN-ESCALATION]";
const ESCALATION_CLOSE = "[/GETHEN-ESCALATION]";
/** The assignment prompt itself documents the report markers; never treat it as a report. */
const ASSIGNMENT_OPEN = "[GETHEN-ASSIGNMENT]";
/** A single execution answers at most this many tool-result escalations. */
const MAX_TOOL_ESCALATIONS = 5;
/** Silence before the watchdog fails an execution. */
export const DEFAULT_IDLE_TIMEOUT_MS = 300_000;
/** Time a cancel/interrupt lets a steered stop take effect before terminating. */
export const DEFAULT_INTERRUPT_GRACE_MS = 1_000;
/** Hard cap for one execution; a long-but-live task must still release eventually. */
export const DEFAULT_MAX_EXECUTION_MS = 60 * 60 * 1000;
/** Open-turn sessionswithout activity for this long are reported as stale. */
export const DEFAULT_STALE_SESSION_MS = 10 * 60 * 1000;
/** Session-history tail read when salvaging a report after silence. */
const SALVAGE_HISTORY_LIMIT = 500;
/** Bound for the salvage read; a wedged store must not delay the failure report. */
const SALVAGE_TIMEOUT_MS = 2_000;
/** Steering message sent before a hard interrupt. */
export const STOP_STEER_TEXT =
  "Stop the current work immediately and summarize what you have done so far.";
/** How much assistant text is kept in the live state. */
const MAX_LAST_MESSAGE_CHARS = 500;
/** Default cap for one forwarded log text (stderr/lifecycle). */
export const DEFAULT_MAX_LOG_CHARS = 8_000;
/** Marker appended when a log text was truncated. */
export const LOG_TRUNCATION_SUFFIX = "…[truncated]";
/** Default cap for `sessions()` entries. */
export const DEFAULT_SESSION_LIST_LIMIT = 100;

/** Process isolation model: reuse the workspace pool, or spawn per assignment. */
export type HarnessIsolation = "shared" | "process";

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
  /** JSON-RPC request timeout for short calls (initialize/prompt/wait fallback). */
  requestTimeoutMs?: number;
  /** Silence before the execution fails; `0` disables the watchdog. Default 300000. */
  idleTimeoutMs?: number;
  /** Grace period after the stop steer before the runtime is terminated. Default 1000. */
  interruptGraceMs?: number;
  /**
   * Hard cap for one execution. 0 disables. A task that keeps emitting events
   * but never finishes is failed and its session released instead of hanging
   * forever. Default 60 minutes.
   */
  maxExecutionMs?: number;
  /** A store session with an open turn and no activity for this long is stale. */
  staleSessionMs?: number;
  /** `"shared"` (default) pools one runtime per workspace; `"process"` isolates each. */
  isolation?: HarnessIsolation;
  /** Cap for one forwarded log text; longer text is truncated. Default 8000; `0` disables. */
  maxLogChars?: number;
  /** DSH session store used to enrich the directory and read history. */
  sessionStore?: DshSessionStore;
  /** Cap for `sessions()` entries (newest activity first). Default 100. */
  sessionListLimit?: number;
  /**
   * JSON file that persists the `key -> sessionId` registry across executor
   * restarts. Absent keeps the registry in memory only (tests, short-lived).
   */
  sessionKeysFile?: string;
  /** Injectable clock (deadline scheduling, event timestamp fallback, state). */
  now?: () => Date;
}

/** A `HarnessExecutor` that can also release its pooled DSH runtimes. */
export interface SdkHarnessExecutor extends HarnessExecutor {
  /** Close every pooled SDK client and stop accepting work. */
  close(): Promise<void>;
  /** Session directory (registry merged with the DSH store). */
  sessions(): HarnessSessionInfo[];
  /** One session's event history from the DSH store (empty when unknown/unreadable). */
  sessionHistory(
    sessionId: string,
    options?: HarnessSessionHistoryOptions,
  ): Promise<HarnessSessionEvent[]>;
}

/** The subset of `@gethen/sdk-driver`'s client the executor drives. */
export interface SdkClientLike {
  /** True when a factory supplied an already-started client. */
  readonly running?: boolean;
  start?(options: StartDshSdkOptions): unknown;
  initialize(options: InitializeOptions): Promise<InitializeResult>;
  prompt(sessionId: string, text: string): Promise<SessionPromptResult>;
  resume(sessionId: string): Promise<void>;
  wait(sessionId: string, options?: WaitOptions): Promise<void>;
  onEvent(listener: (notification: SessionEventNotification) => void): () => void;
  onStatus(listener: (notification: SessionStatusNotification) => void): () => void;
  onApprovalRequest(
    handler: (params: ApprovalRequestParams) => ApprovalOutcome | Promise<ApprovalOutcome>,
  ): void;
  onUserQuestion(
    handler: (request: UserQuestionRequest) => UserQuestionResult | Promise<UserQuestionResult>,
  ): void;
  /** Subscribe to raw stderr text; returns an unsubscribe. Optional. */
  onStderr?(listener: (text: string) => void): () => void;
  /** SIGKILL the child immediately; falls back to `close()` when absent. */
  terminate?(): void;
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
  status(notification: SessionStatusNotification): void;
  approval(params: ApprovalRequestParams): Promise<ApprovalOutcome>;
  question(request: UserQuestionRequest): Promise<UserQuestionResult>;
}

interface WorkspaceClient {
  workspace: string;
  client: SdkClientLike;
  sessions: Map<string, SessionBinding>;
  detach: () => void;
}

/** One entry of the executor's in-memory session registry. */
interface SessionRecord {
  sessionId: string;
  key?: string;
  workspace: string;
  lastUsedAt: string;
  activeAssignmentId?: string;
  /** Runtime client this session is currently loaded in (for resume skipping). */
  client?: SdkClientLike;
}

/** Resolved session for one task, before the runtime is acquired. */
interface SessionPlan {
  sessionId: string;
  resume: boolean;
  key?: string;
}

/** How one execution settled: it reports exactly once. */
interface ExecutionState {
  task: HarnessTask;
  sink: HarnessSink;
  workspaceClient: WorkspaceClient | undefined;
  /** True when the client belongs to this assignment (`isolation: "process"`). */
  dedicated: boolean;
  settled: boolean;
  cancelled: boolean;
  /** Complete `[GETHEN-REPORT]` blocks in event order; the last valid one wins. */
  reportTexts: string[];
  /** Last complete escalation block found in an event string, not yet answered. */
  toolEscalation: HarnessEscalation | undefined;
  /** Tool-result escalations answered so far. */
  rounds: number;
  /** One escalation is live at a time; later ones queue on `escalationChain`. */
  pending: PendingEscalation | undefined;
  /** A blocking escalation is in flight; turn-end/idle must not report yet. */
  awaitingResolution: boolean;
  escalationChain: Promise<void>;
  escalationSeq: number;
  // ---- session
  sessionId: string;
  sessionKey?: string;
  /** Wall clock this execution was claimed; older history is a previous turn. */
  startedAt: string;
  // ---- live state
  phase: HarnessTaskPhase;
  step: number;
  lastTool: { name: string; startedAt: string } | undefined;
  /** A tool call started and has not produced its result yet. */
  toolInFlight: boolean;
  lastEventAt: string;
  lastMessage: string | undefined;
  seq: number;
  // ---- lifecycle
  /** Unsubscribes this execution's stderr listener (shared clients may have several). */
  detachStderr: (() => void) | undefined;
  abort: AbortController;
  watchdogTimer: NodeJS.Timeout | undefined;
  deadlineTimer: NodeJS.Timeout | undefined;
  maxDurationTimer: NodeJS.Timeout | undefined;
  interruptPromise: Promise<void> | undefined;
}

interface PendingEscalation {
  resolve(resolution: HarnessResolution): void;
  reject(error: Error): void;
}

/**
 * Create the DSH-backed `HarnessExecutor`. In `shared` isolation one runtime is
 * kept per workspace; in `process` isolation each assignment gets its own.
 */
export function createSdkHarnessExecutor(options: SdkHarnessExecutorOptions): SdkHarnessExecutor {
  return new DshHarnessExecutor(options);
}

class DshHarnessExecutor implements SdkHarnessExecutor {
  private readonly options: SdkHarnessExecutorOptions;
  private readonly cwdFor: (workspace: string) => string;
  private readonly clientFactory: () => SdkClientLike;
  private readonly now: () => Date;
  private readonly isolation: HarnessIsolation;
  private readonly idleTimeoutMs: number;
  private readonly interruptGraceMs: number;
  private readonly maxLogChars: number;
  private readonly maxExecutionMs: number;
  private readonly staleSessionMs: number;
  private readonly clients = new Map<string, WorkspaceClient>();
  private readonly creating = new Map<string, Promise<WorkspaceClient>>();
  private readonly sessionRecords = new Map<string, SessionRecord>();
  private readonly sessionKeys: Map<string, string>;
  private readonly sessionKeysFile: string | undefined;
  private readonly sessionStore: DshSessionStore;
  private readonly sessionListLimit: number;
  private sessionCache: SessionSummary[] = [];
  private closed = false;

  constructor(options: SdkHarnessExecutorOptions) {
    this.options = options;
    this.cwdFor = options.cwdFor ?? ((workspace) => workspace);
    this.clientFactory = options.clientFactory ?? (() => new DshSdkClient());
    this.now = options.now ?? (() => new Date());
    this.isolation = options.isolation ?? "shared";
    this.idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS;
    this.interruptGraceMs = options.interruptGraceMs ?? DEFAULT_INTERRUPT_GRACE_MS;
    this.maxLogChars = Math.max(0, options.maxLogChars ?? DEFAULT_MAX_LOG_CHARS);
    this.maxExecutionMs = Math.max(0, options.maxExecutionMs ?? DEFAULT_MAX_EXECUTION_MS);
    this.staleSessionMs = Math.max(0, options.staleSessionMs ?? DEFAULT_STALE_SESSION_MS);
    this.sessionStore = options.sessionStore ?? new DshSessionStore();
    this.sessionListLimit = Math.max(1, options.sessionListLimit ?? DEFAULT_SESSION_LIST_LIMIT);
    this.sessionKeysFile = options.sessionKeysFile;
    this.sessionKeys = loadSessionKeys(options.sessionKeysFile);
  }

  async start(task: HarnessTask, sink: HarnessSink): Promise<HarnessExecution> {
    const state: ExecutionState = {
      task,
      sink,
      workspaceClient: undefined,
      dedicated: this.isolation === "process",
      settled: false,
      cancelled: false,
      reportTexts: [],
      toolEscalation: undefined,
      rounds: 0,
      pending: undefined,
      awaitingResolution: false,
      escalationChain: Promise.resolve(),
      escalationSeq: 0,
      sessionId: task.assignmentId,
      startedAt: this.now().toISOString(),
      phase: "starting",
      step: 0,
      lastTool: undefined,
      toolInFlight: false,
      lastEventAt: this.now().toISOString(),
      lastMessage: undefined,
      seq: 0,
      abort: new AbortController(),
      watchdogTimer: undefined,
      deadlineTimer: undefined,
      maxDurationTimer: undefined,
      interruptPromise: undefined,
      detachStderr: undefined,
    };

    const execution: HarnessExecution = {
      sendResolution: async (resolution: HarnessResolution): Promise<void> => {
        const pending = state.pending;
        if (pending === undefined) return; // Duplicate or late resolution.
        pending.resolve(resolution);
      },
      cancel: async (reason?: string): Promise<void> => {
        await this.cancelExecution(state, reason);
      },
      state: (): HarnessTaskState => this.snapshot(state),
      steer: async (text: string): Promise<void> => {
        await this.steer(state, text);
      },
      interrupt: async (): Promise<void> => {
        await this.interruptExecution(state);
      },
    };

    if (this.guardStaleTask(state)) return execution;
    if (this.closed) {
      this.finish(state, { kind: "failed", summary: "executor is closed" });
      return execution;
    }

    let plan: SessionPlan;
    try {
      plan = this.planSession(task);
      this.claimSession(task, plan);
      state.sessionId = plan.sessionId;
      if (plan.key !== undefined) state.sessionKey = plan.key;
    } catch (error) {
      this.finish(state, { kind: "failed", summary: messageOf(error) });
      return execution;
    }

    let workspaceClient: WorkspaceClient;
    try {
      workspaceClient =
        this.isolation === "process"
          ? await this.createDedicatedClient(task.workspace)
          : await this.ensureClient(task.workspace);
    } catch (error) {
      this.finish(state, {
        kind: "failed",
        summary: `failed to start DSH SDK runtime: ${messageOf(error)}`,
      });
      return execution;
    }
    state.workspaceClient = workspaceClient;
    if (state.cancelled || state.settled) {
      this.disposeDedicated(state);
      return execution;
    }

    const onStderr = workspaceClient.client.onStderr;
    if (onStderr !== undefined) {
      state.detachStderr = onStderr.call(workspaceClient.client, (text) => {
        this.onClientStderr(state, text);
      });
    }

    workspaceClient.sessions.set(plan.sessionId, this.bindingFor(state));
    const record = this.sessionRecords.get(plan.sessionId);
    // A session already loaded in this pooled runtime cannot be resumed again
    // (DSH rejects resume on an open session); just prompt it with the new turn.
    const alreadyOpen = plan.resume && record?.client === workspaceClient.client;
    if (record !== undefined) record.client = workspaceClient.client;
    if (alreadyOpen) plan = { ...plan, resume: false };
    if (plan.resume) {
      try {
        await workspaceClient.client.resume(plan.sessionId);
      } catch (error) {
        workspaceClient.sessions.delete(plan.sessionId);
        if (!state.cancelled) {
          this.finish(state, {
            kind: "failed",
            summary: `failed to resume DSH SDK session: ${messageOf(error)}`,
          });
        }
        this.disposeDedicated(state);
        return execution;
      }
    }
    try {
      await workspaceClient.client.prompt(plan.sessionId, task.prompt);
    } catch (error) {
      workspaceClient.sessions.delete(plan.sessionId);
      if (!state.cancelled) {
        this.finish(state, {
          kind: "failed",
          summary: `failed to prompt DSH SDK session: ${messageOf(error)}`,
        });
      }
      this.disposeDedicated(state);
      return execution;
    }
    if (state.cancelled || state.settled) {
      this.disposeDedicated(state);
      return execution;
    }

    state.phase = "thinking";
    state.lastEventAt = this.now().toISOString();
    this.emitLog(state, {
      phase: "start",
      text: `assignment ${task.assignmentId} started in session ${plan.sessionId}`,
    });
    await this.refreshSessionCache();
    this.armTimers(state);
    this.startWaitFallback(state, workspaceClient);
    return execution;
  }

  /** Close all pooled clients; process-isolated runtimes clean themselves up. */
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

  // ------------------------------------------------------------ live state

  private snapshot(state: ExecutionState): HarnessTaskState {
    const snapshot: HarnessTaskState = {
      assignmentId: state.task.assignmentId,
      phase: state.phase,
      step: state.step,
      lastEventAt: state.lastEventAt,
      seq: state.seq,
    };
    if (state.lastTool !== undefined) snapshot.lastTool = { ...state.lastTool };
    if (state.lastMessage !== undefined) snapshot.lastMessage = state.lastMessage;
    snapshot.sessionId = state.sessionId;
    return snapshot;
  }

  // ---------------------------------------------------------------- sessions

  /** Resolve the task's session spec into a session id (and whether to resume). */
  private planSession(task: HarnessTask): SessionPlan {
    const spec: SessionSpec | undefined = task.session;
    if (spec === undefined || spec.mode === "new") {
      if (spec?.sessionId !== undefined) {
        throw new SdkHarnessError('session mode "new" must not name a sessionId');
      }
      if (spec?.key !== undefined) {
        requireSessionKey(spec.key);
        return { sessionId: task.assignmentId, resume: false, key: spec.key };
      }
      return { sessionId: task.assignmentId, resume: false };
    }
    if (spec.mode !== "reuse") {
      throw new SdkHarnessError('session mode must be "new" or "reuse"');
    }
    if (spec.sessionId !== undefined) {
      requireSessionKey(spec.sessionId, 'session "sessionId"');
      return {
        sessionId: spec.sessionId,
        resume: true,
        ...(spec.key === undefined ? {} : { key: spec.key }),
      };
    }
    if (spec.key !== undefined) {
      requireSessionKey(spec.key);
      const mapped = this.sessionKeys.get(spec.key);
      if (mapped === undefined) {
        throw new SdkHarnessError(
          `reuse session key "${spec.key}" is unknown; create it with session.mode "new" and the same key first`,
        );
      }
      return { sessionId: mapped, resume: true, key: spec.key };
    }
    throw new SdkHarnessError('reuse session needs "sessionId" or "key"');
  }

  /** Reserve the session (and key) for this assignment; rejects a busy session. */
  private claimSession(task: HarnessTask, plan: SessionPlan): void {
    const now = this.now().toISOString();
    const existing = this.sessionRecords.get(plan.sessionId);
    if (existing?.activeAssignmentId !== undefined) {
      throw new SdkHarnessError(
        `session "${plan.sessionId}" is already running assignment "${existing.activeAssignmentId}"`,
      );
    }
    if (plan.key !== undefined) {
      const mapped = this.sessionKeys.get(plan.key);
      if (mapped !== undefined && mapped !== plan.sessionId) {
        const mappedRecord = this.sessionRecords.get(mapped);
        if (mappedRecord?.activeAssignmentId !== undefined) {
          throw new SdkHarnessError(
            `session key "${plan.key}" is already running assignment "${mappedRecord.activeAssignmentId}"`,
          );
        }
        if (mappedRecord !== undefined) delete mappedRecord.key;
      }
    }

    const record: SessionRecord = existing ?? {
      sessionId: plan.sessionId,
      workspace: task.workspace,
      lastUsedAt: now,
    };
    record.workspace = task.workspace;
    record.lastUsedAt = now;
    record.activeAssignmentId = task.assignmentId;
    if (plan.key !== undefined) {
      record.key = plan.key;
      this.sessionKeys.set(plan.key, plan.sessionId);
      persistSessionKeys(this.sessionKeysFile, this.sessionKeys);
    }
    this.sessionRecords.set(plan.sessionId, record);
  }

  /** Mark the session idle again; the record stays for later reuse. */
  private releaseSession(state: ExecutionState): void {
    const record = this.sessionRecords.get(state.sessionId);
    if (record === undefined) return;
    if (record.activeAssignmentId === state.task.assignmentId) {
      delete record.activeAssignmentId;
    }
    record.lastUsedAt = this.now().toISOString();
  }

  /** Best-effort refresh of the DSH session-store cache (failures degrade). */
  private async refreshSessionCache(): Promise<void> {
    try {
      this.sessionCache = await this.sessionStore.list({ limit: this.sessionListLimit });
    } catch {
      this.sessionCache = [];
    }
  }

  /**
   * Session directory: DSH store summaries enriched with this executor's
   * registry (key, active assignment, last use), newest activity first. Store
   * failures degrade to registry-only. The store is refreshed in the background;
   * `start` awaits one refresh, `finish` kicks one off.
   */
  sessions(): HarnessSessionInfo[] {
    void this.refreshSessionCache();
    const byId = new Map<string, HarnessSessionInfo>();
    for (const summary of this.sessionCache) {
      const info: HarnessSessionInfo = {
        sessionId: summary.sessionId,
        workspace: summary.config.cwd,
        lastUsedAt: summary.lastActivityAt ?? summary.config.createdAt,
        config: summary.config,
        state: summary.state,
        hasLog: summary.hasLog,
      };
      if (summary.lastActivityAt !== undefined) info.lastActivityAt = summary.lastActivityAt;
      // An open turn with no live registry entry and no recent activity is a
      // leftover from a crashed/wedged runtime, not a running assignment.
      if (summary.state === "running" && this.staleSessionMs > 0) {
        const last = summary.lastActivityAt ?? summary.config.createdAt;
        if (this.now().getTime() - Date.parse(last) > this.staleSessionMs) info.state = "stale";
      }
      if (summary.eventCount !== undefined) info.eventCount = summary.eventCount;
      if (summary.tokenUsage !== undefined) info.tokenUsage = summary.tokenUsage;
      byId.set(summary.sessionId, info);
    }
    for (const record of this.sessionRecords.values()) {
      const info: HarnessSessionInfo = byId.get(record.sessionId) ?? {
        sessionId: record.sessionId,
        workspace: record.workspace,
        lastUsedAt: record.lastUsedAt,
      };
      info.workspace = record.workspace;
      info.lastUsedAt = record.lastUsedAt;
      if (record.key !== undefined) info.key = record.key;
      if (record.activeAssignmentId !== undefined) {
        info.activeAssignmentId = record.activeAssignmentId;
        info.state = "running";
      }
      byId.set(record.sessionId, info);
    }
    return [...byId.values()]
      .sort((left, right) => activityKey(right).localeCompare(activityKey(left)))
      .slice(0, this.sessionListLimit);
  }

  /** Read one session's history; unknown sessions and read errors return `[]`. */
  async sessionHistory(
    sessionId: string,
    options: HarnessSessionHistoryOptions = {},
  ): Promise<HarnessSessionEvent[]> {
    try {
      return await this.sessionStore.history(sessionId, options);
    } catch {
      return [];
    }
  }

  private applyEventToState(state: ExecutionState, event: unknown): void {
    const record = asRecord(event);
    if (record === undefined) return;
    const type = typeof record.type === "string" ? record.type : "event";
    state.seq += 1;
    state.lastEventAt = eventTimestamp(record) ?? this.now().toISOString();
    const data = asRecord(record.data) ?? {};

    switch (type) {
      case "step/start": {
        state.phase = "thinking";
        const step = data.step;
        if (typeof step === "number" && Number.isInteger(step) && step > 0) state.step = step;
        return;
      }
      case "tool/call": {
        state.phase = "tool";
        const name = typeof data.name === "string" && data.name.length > 0 ? data.name : "tool";
        state.lastTool = { name, startedAt: state.lastEventAt };
        state.toolInFlight = true;
        return;
      }
      case "tool/result":
        state.phase = "thinking";
        state.toolInFlight = false;
        return;
      case "assistant/message": {
        state.phase = "thinking";
        const message = assistantMessageText(data);
        if (message !== undefined) {
          state.lastMessage = message.slice(0, MAX_LAST_MESSAGE_CHARS);
        }
        return;
      }
      case "turn/start":
        state.phase = "thinking";
        return;
      case "turn/end":
        state.phase = "done";
        state.toolInFlight = false;
        return;
      default:
        return;
    }
  }

  // -------------------------------------------------------------- lifecycle

  private guardStaleTask(state: ExecutionState): boolean {
    const { deadlineAt } = state.task;
    if (deadlineAt === undefined) return false;
    if (Date.parse(deadlineAt) > this.now().getTime()) return false;
    this.finish(state, { kind: "failed", summary: "assignment deadline had already passed" });
    return true;
  }

  private armTimers(state: ExecutionState): void {
    this.resetWatchdog(state);
    this.armMaxDuration(state);
    const { deadlineAt } = state.task;
    if (deadlineAt === undefined) return;
    const remaining = Date.parse(deadlineAt) - this.now().getTime();
    if (!Number.isFinite(remaining) || remaining <= 0) return;
    state.deadlineTimer = setTimeout(() => {
      if (state.settled) return;
      this.emitLog(state, { phase: "deadline", text: `deadline reached at ${deadlineAt}` });
      void this.failWithSalvage(state, "assignment deadline passed");
    }, remaining);
    state.deadlineTimer.unref?.();
  }

  /** One absolute cap per execution; emitting events forever must not pin a session. */
  private armMaxDuration(state: ExecutionState): void {
    if (state.maxDurationTimer !== undefined) clearTimeout(state.maxDurationTimer);
    state.maxDurationTimer = undefined;
    if (this.maxExecutionMs <= 0) return;
    state.maxDurationTimer = setTimeout(() => {
      if (state.settled) return;
      this.emitLog(state, {
        phase: "timeout",
        text: `execution exceeded ${this.maxExecutionMs}ms and is being released`,
      });
      void this.failWithSalvage(
        state,
        `assignment exceeded the ${this.maxExecutionMs}ms execution limit`,
      );
    }, this.maxExecutionMs);
    state.maxDurationTimer.unref?.();
  }

  private resetWatchdog(state: ExecutionState): void {
    if (state.watchdogTimer !== undefined) clearTimeout(state.watchdogTimer);
    state.watchdogTimer = undefined;
    if (this.idleTimeoutMs <= 0) return;
    state.watchdogTimer = setTimeout(() => {
      if (state.settled) return;
      // A running tool (a long `pnpm check`, build, migration, ...) produces no
      // session events until it returns. Treat that as activity and re-arm the
      // silence window instead of killing a healthy task; the hard
      // `maxExecutionMs` cap still bounds a tool that never returns.
      if (state.toolInFlight) {
        this.resetWatchdog(state);
        return;
      }
      this.emitLog(state, {
        phase: "watchdog",
        text: `no session events for ${this.idleTimeoutMs}ms`,
      });
      void this.failWithSalvage(state, `no session events for ${this.idleTimeoutMs}ms`);
    }, this.idleTimeoutMs);
    state.watchdogTimer.unref?.();
  }

  /** Stop the silence watchdog without touching the deadline timer. */
  private pauseWatchdog(state: ExecutionState): void {
    if (state.watchdogTimer !== undefined) {
      clearTimeout(state.watchdogTimer);
      state.watchdogTimer = undefined;
    }
  }

  private clearLifecycleTimers(state: ExecutionState): void {
    if (state.watchdogTimer !== undefined) {
      clearTimeout(state.watchdogTimer);
      state.watchdogTimer = undefined;
    }
    if (state.deadlineTimer !== undefined) {
      clearTimeout(state.deadlineTimer);
      state.deadlineTimer = undefined;
    }
    if (state.maxDurationTimer !== undefined) {
      clearTimeout(state.maxDurationTimer);
      state.maxDurationTimer = undefined;
    }
  }

  /**
   * Settle a silent/wedged execution. The final report may already be in the
   * session log even when the event stream stopped (a finished turn whose last
   * event was lost), so salvage it before falling back to failure.
   */
  private async failWithSalvage(state: ExecutionState, summary: string): Promise<void> {
    const report = (await this.salvageReport(state)) ?? { kind: "failed", summary };
    if (state.settled) return;
    this.finish(state, report);
    this.hardStop(state);
  }

  private async salvageReport(state: ExecutionState): Promise<HarnessReport | undefined> {
    const since = Date.parse(state.startedAt);
    const history = await withTimeout(
      this.sessionStore.history(state.sessionId, { limit: SALVAGE_HISTORY_LIMIT }),
      SALVAGE_TIMEOUT_MS,
    );
    if (history === undefined) return undefined;
    for (const event of history) {
      const at = Date.parse(event.at);
      if (Number.isFinite(since) && Number.isFinite(at) && at < since) continue;
      scanEvent(state, event);
    }
    if (state.reportTexts.length === 0) return undefined;
    const report = this.reportFor(state);
    return report.kind === "waiting" ? undefined : report;
  }

  private finish(state: ExecutionState, report: HarnessReport): void {
    if (state.settled) return;
    state.settled = true;
    state.phase = report.kind === "failed" ? "failed" : "done";
    state.lastEventAt = this.now().toISOString();
    this.clearLifecycleTimers(state);
    state.abort.abort();
    this.detachStderr(state);
    state.workspaceClient?.sessions.delete(state.sessionId);
    this.releaseSession(state);
    this.emitLog(state, { phase: "end", text: `${report.kind}: ${report.summary}` });
    this.disposeDedicated(state);
    state.sink.report(report);
    void this.refreshSessionCache();
  }

  /** Reject escalations, steer a stop, wait the grace period, then hard-stop. */
  private interruptExecution(state: ExecutionState): Promise<void> {
    if (state.interruptPromise !== undefined) return state.interruptPromise;
    state.interruptPromise = this.runInterrupt(state);
    return state.interruptPromise;
  }

  private async runInterrupt(state: ExecutionState): Promise<void> {
    if (state.settled) return;
    state.cancelled = true;
    this.emitLog(state, {
      phase: "cancel",
      text: "execution interrupted; steering a stop before terminating",
    });
    state.pending?.reject(new SdkHarnessError("execution interrupted"));
    state.abort.abort();
    const workspaceClient = state.workspaceClient;
    if (workspaceClient !== undefined) {
      try {
        await workspaceClient.client.prompt(state.task.assignmentId, STOP_STEER_TEXT);
      } catch {
        // Best effort: the runtime may already be gone.
      }
    }
    if (this.interruptGraceMs > 0) await delay(this.interruptGraceMs);
    if (state.settled) return;
    this.hardStop(state);
  }

  private async cancelExecution(state: ExecutionState, reason?: string): Promise<void> {
    if (state.settled) return;
    await this.interruptExecution(state);
    this.finish(state, {
      kind: "failed",
      summary: reason === undefined ? "cancelled" : `cancelled: ${reason}`,
    });
  }

  /** Terminate (process isolation) or close the shared runtime, best effort. */
  private hardStop(state: ExecutionState): void {
    const workspaceClient = state.workspaceClient;
    if (workspaceClient === undefined) return;
    if (state.dedicated) {
      this.disposeDedicated(state);
      return;
    }
    this.closeWorkspaceClient(workspaceClient);
    state.workspaceClient = undefined;
  }

  private async steer(state: ExecutionState, text: string): Promise<void> {
    if (state.settled || state.cancelled) return;
    const workspaceClient = state.workspaceClient;
    if (workspaceClient === undefined) return;
    try {
      await workspaceClient.client.prompt(state.task.assignmentId, text);
    } catch {
      // Steering is best effort; the execution continues with its current turn.
    }
  }

  // ------------------------------------------------------------- log events

  private onClientStderr(state: ExecutionState, text: string): void {
    if (state.settled) return;
    this.emitLog(state, { stream: "stderr", text });
  }

  /** Emit one bounded `log` event; it does not touch the live state or `seq`. */
  private emitLog(
    state: ExecutionState,
    data: { phase?: string; stream?: "stderr"; text?: string },
  ): void {
    const record: JsonObject = { ...data };
    if (typeof data.text === "string" && this.maxLogChars > 0) {
      record.text =
        data.text.length > this.maxLogChars
          ? `${data.text.slice(0, this.maxLogChars)}${LOG_TRUNCATION_SUFFIX}`
          : data.text;
    }
    state.sink.event({ type: "log", at: this.now().toISOString(), data: record });
  }

  private detachStderr(state: ExecutionState): void {
    const detach = state.detachStderr;
    state.detachStderr = undefined;
    if (detach === undefined) return;
    try {
      detach();
    } catch {
      // The runtime may already be gone.
    }
  }

  // ---------------------------------------------------------------- clients

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
    this.launch(client, cwd);
    const workspaceClient = this.wire(workspace, client);
    try {
      await this.initialize(client, cwd);
    } catch (error) {
      workspaceClient.detach();
      this.disposeClient(client);
      throw error;
    }
    return workspaceClient;
  }

  private async createDedicatedClient(workspace: string): Promise<WorkspaceClient> {
    const cwd = this.cwdFor(workspace);
    const client = this.clientFactory();
    this.launch(client, cwd);
    const workspaceClient = this.wire(workspace, client);
    try {
      await this.initialize(client, cwd);
    } catch (error) {
      workspaceClient.detach();
      this.disposeClient(client);
      throw error;
    }
    return workspaceClient;
  }

  private launch(client: SdkClientLike, cwd: string): void {
    if (client.running === true) return;
    const startOptions: StartDshSdkOptions = { cwd };
    if (this.options.dshCommand !== undefined) startOptions.command = this.options.dshCommand;
    if (this.options.dshArgs !== undefined) startOptions.args = [...this.options.dshArgs];
    if (this.options.requestTimeoutMs !== undefined) {
      startOptions.requestTimeoutMs = this.options.requestTimeoutMs;
    }
    client.start?.(startOptions);
  }

  private async initialize(client: SdkClientLike, cwd: string): Promise<void> {
    const initialize: InitializeOptions = {
      provider: this.options.provider,
      model: this.options.model,
      cwd,
      approvalRequests: true,
      userQuestions: true,
    };
    await client.initialize(initialize);
  }

  private wire(workspace: string, client: SdkClientLike): WorkspaceClient {
    const sessions = new Map<string, SessionBinding>();
    const detachEvent = client.onEvent((notification) => {
      sessions.get(notification.sessionId)?.event(notification);
    });
    const detachStatus = client.onStatus((notification) => {
      sessions.get(notification.sessionId)?.status(notification);
    });
    client.onApprovalRequest((params) => {
      const binding = sessions.get(params.sessionId);
      return binding === undefined ? "cancelled" : binding.approval(params);
    });
    client.onUserQuestion((request) => {
      const binding = sessions.get(request.sessionId);
      return binding === undefined ? { cancelled: true } : binding.question(request);
    });
    return {
      workspace,
      client,
      sessions,
      detach: () => {
        detachEvent();
        detachStatus();
      },
    };
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
      // Best effort: the caller reports the failure regardless.
    }
  }

  private disposeDedicated(state: ExecutionState): void {
    if (!state.dedicated) return;
    const workspaceClient = state.workspaceClient;
    if (workspaceClient === undefined) return;
    state.workspaceClient = undefined;
    workspaceClient.detach();
    workspaceClient.sessions.clear();
    this.disposeClient(workspaceClient.client);
  }

  private disposeClient(client: SdkClientLike): void {
    try {
      if (client.terminate !== undefined) client.terminate();
      else client.close();
    } catch {
      // Best effort.
    }
  }

  // ---------------------------------------------------------------- events

  private bindingFor(state: ExecutionState): SessionBinding {
    return {
      event: (notification) => this.onSessionEvent(state, notification),
      status: (notification) => this.onSessionStatus(state, notification),
      approval: (params) => this.askApproval(state, params),
      question: (request) => this.askQuestion(state, request),
    };
  }

  private onSessionEvent(state: ExecutionState, notification: SessionEventNotification): void {
    if (state.settled) return;
    state.sink.event(mapSessionEvent(notification.event, this.now()));
    scanEvent(state, notification.event);
    this.applyEventToState(state, notification.event);
    this.resetWatchdog(state);
    if (eventTypeOf(notification.event) === "turn/end") this.onTurnEnd(state);
  }

  private onSessionStatus(state: ExecutionState, notification: SessionStatusNotification): void {
    if (state.settled || notification.status !== "idle") return;
    // An idle without any observed event is not evidence of a finished turn.
    if (state.seq === 0) return;
    this.onTurnEnd(state);
  }

  /** Turn completion: report, answer a pending tool-result escalation, or wait. */
  private onTurnEnd(state: ExecutionState): void {
    if (state.settled || state.cancelled) return;
    // A trailing turn/end or idle status can arrive while the blocking ask is
    // still waiting for an answer; finishing here would orphan the answer.
    if (state.awaitingResolution || state.pending !== undefined) return;
    if (state.reportTexts.length > 0) {
      this.finish(state, this.reportFor(state));
      return;
    }
    const escalation = state.toolEscalation;
    if (escalation !== undefined) {
      void this.answerToolEscalation(state, escalation);
      return;
    }
    this.finish(state, this.reportFor(state));
  }

  private async answerToolEscalation(
    state: ExecutionState,
    escalation: HarnessEscalation,
  ): Promise<void> {
    if (state.settled || state.cancelled) return;
    if (state.toolEscalation === escalation) state.toolEscalation = undefined;
    state.rounds += 1;
    if (state.rounds > MAX_TOOL_ESCALATIONS) {
      this.finish(state, {
        kind: "failed",
        summary: `worker escalated more than ${MAX_TOOL_ESCALATIONS} times`,
      });
      this.hardStop(state);
      return;
    }

    let resolution: HarnessResolution;
    try {
      resolution = await this.requestEscalation(
        state,
        escalationForAssignment(state.task.assignmentId, escalation),
      );
    } catch {
      return; // Interrupted/cancelled: the cancel path owns the report.
    }
    if (state.settled || state.cancelled) return;

    const workspaceClient = state.workspaceClient;
    if (workspaceClient === undefined) return;
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
        this.hardStop(state);
      }
      return;
    }
    if (state.settled || state.cancelled) return;
    this.resetWatchdog(state);
    this.startWaitFallback(state, workspaceClient);
  }

  /** Background wait: a fallback for a broken event stream, never a timeout. */
  private startWaitFallback(state: ExecutionState, workspaceClient: WorkspaceClient): void {
    const signal = state.abort.signal;
    void (async () => {
      try {
        await workspaceClient.client.wait(state.sessionId, {
          timeoutMs: 0,
          signal,
        });
      } catch (error) {
        if (state.settled || state.cancelled || signal.aborted) return;
        this.finish(state, {
          kind: "failed",
          summary: `DSH SDK wait failed: ${messageOf(error)}`,
        });
        return;
      }
      if (state.settled || state.cancelled) return;
      this.onTurnEnd(state);
    })();
  }

  // -------------------------------------------------------------- escalations

  private reportFor(state: ExecutionState): HarnessReport {
    if (state.reportTexts.length === 0) {
      return { kind: "failed", summary: "worker ended without a report" };
    }
    // Models sometimes echo a hand-written report block after the tool result;
    // prefer the last block that actually parses instead of failing the whole
    // assignment on a malformed echo.
    let lastError: unknown;
    let parsed: ReturnType<typeof parseReportText>;
    for (let index = state.reportTexts.length - 1; index >= 0; index -= 1) {
      const text = state.reportTexts[index] as string;
      try {
        parsed = parseReportText(text);
      } catch (error) {
        lastError = error;
        try {
          parsed = parseReportText(repairReportBlock(text));
        } catch {
          continue;
        }
      }
      if (parsed !== undefined) break;
    }
    const report: ReturnType<typeof parseReportText> = parsed;
    if (report === undefined) {
      if (lastError !== undefined) {
        return { kind: "failed", summary: `worker report is invalid: ${messageOf(lastError)}` };
      }
      return { kind: "failed", summary: "worker ended without a report" };
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
    // Waiting for a person/Master answer produces no session events by design;
    // the silence watchdog must not kill the execution while it is parked.
    this.pauseWatchdog(state);
    state.awaitingResolution = true;
    const start = (): Promise<HarnessResolution> => {
      const run = this.emitEscalation(state, escalation);
      return run.finally(() => {
        state.awaitingResolution = false;
        if (!state.settled && !state.cancelled) this.resetWatchdog(state);
      });
    };
    if (state.pending === undefined) {
      const run = start();
      state.escalationChain = run.then(
        () => undefined,
        () => undefined,
      );
      return run;
    }
    const run = state.escalationChain.then(start);
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
 * Repair a report block whose JSON string values contain raw control characters
 * (models occasionally emit literal newlines inside a JSON string). Only
 * characters inside strings are escaped; structural whitespace is untouched.
 */
export function repairReportBlock(text: string): string {
  const open = text.indexOf(REPORT_OPEN);
  const close = text.lastIndexOf(REPORT_CLOSE);
  if (open < 0 || close < 0 || close <= open) return text;
  const body = text.slice(open + REPORT_OPEN.length, close);
  let out = "";
  let inString = false;
  let escaped = false;
  for (const char of body) {
    if (escaped) {
      out += char;
      escaped = false;
      continue;
    }
    if (char === "\\") {
      out += char;
      escaped = true;
      continue;
    }
    if (char === '"') {
      inString = !inString;
      out += char;
      continue;
    }
    if (inString) {
      if (char === "\n") out += "\\n";
      else if (char === "\r") out += "\\r";
      else if (char === "\t") out += "\\t";
      else if (char < " ") out += `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`;
      else out += char;
      continue;
    }
    out += char;
  }
  return `${text.slice(0, open + REPORT_OPEN.length)}${out}${text.slice(close)}`;
}

/**
 * Scan one session-event payload for report and escalation blocks. Only
 * individual string values are considered, so a block split across events (or
 * fields) never gets glued together; a complete marker pair is required, which
 * keeps prose that merely mentions a marker from matching.
 */
function scanEvent(state: ExecutionState, event: unknown, depth = 0): void {
  for (const text of collectStrings(event)) {
    // The rendered assignment prompt contains the report/escalation protocol
    // examples; scanning them would turn the instructions into a bogus report.
    if (text.includes(ASSIGNMENT_OPEN)) continue;
    if (text.includes(REPORT_OPEN) && text.includes(REPORT_CLOSE)) {
      state.reportTexts.push(text);
    }
    if (text.includes(ESCALATION_OPEN) && text.includes(ESCALATION_CLOSE)) {
      const escalation = parseToolEscalation(text);
      if (escalation !== undefined) state.toolEscalation = escalation;
    }
    // DSH tool results are often themselves JSON text (e.g. `{"text":"..."}`
    // returned by the worker tools). Decode one or two layers and scan again,
    // otherwise a report block inside an encoded tool result is missed.
    if (depth < 3 && text.length < 1_000_000) {
      const trimmed = text.trimStart();
      if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
        try {
          scanEvent(state, JSON.parse(text), depth + 1);
        } catch {
          // not JSON; the surrounding loop already handled plain markers
        }
      }
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

/** Text blocks joined from an `assistant/message` event's message. */
function assistantMessageText(data: Record<string, unknown>): string | undefined {
  const message = asRecord(data.message);
  const content = message?.content;
  if (!Array.isArray(content)) return undefined;
  const parts: string[] = [];
  for (const block of content) {
    const record = asRecord(block);
    if (record?.type === "text" && typeof record.text === "string") parts.push(record.text);
  }
  return parts.length > 0 ? parts.join("") : undefined;
}

function eventTypeOf(event: unknown): string | undefined {
  const record = asRecord(event);
  return typeof record?.type === "string" ? record.type : undefined;
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

/** Resolve `promise` or `undefined` after `milliseconds` (never rejects). */
async function withTimeout<T>(promise: Promise<T>, milliseconds: number): Promise<T | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timed = new Promise<undefined>((resolve) => {
      timer = setTimeout(() => resolve(undefined), milliseconds);
      timer.unref?.();
    });
    return await Promise.race([promise, timed]);
  } catch {
    return undefined;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function activityKey(info: HarnessSessionInfo): string {
  return info.lastActivityAt ?? info.lastUsedAt;
}

/** Load the persisted `key -> sessionId` registry; a missing/corrupt file is empty. */
function loadSessionKeys(file: string | undefined): Map<string, string> {
  const keys = new Map<string, string>();
  if (file === undefined) return keys;
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(file, "utf8"));
  } catch {
    return keys;
  }
  const record = asRecord(parsed);
  if (record === undefined) return keys;
  for (const [key, sessionId] of Object.entries(record)) {
    if (typeof sessionId === "string" && sessionId.length > 0) keys.set(key, sessionId);
  }
  return keys;
}

/**
 * Persist the registry atomically (temp + rename). A write failure must not
 * fail the assignment: the in-memory map still serves this process, and the
 * next successful claim rewrites the file.
 */
function persistSessionKeys(file: string | undefined, keys: Map<string, string>): void {
  if (file === undefined) return;
  const temp = `${file}.tmp`;
  try {
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(temp, `${JSON.stringify(Object.fromEntries(keys), null, 2)}\n`, "utf8");
    renameSync(temp, file);
  } catch {
    // Best effort; see the doc comment.
  }
}

function requireSessionKey(value: string, label = 'session "key"'): void {
  if (value.length === 0) throw new SdkHarnessError(`${label} must be a non-empty string`);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

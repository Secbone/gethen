/**
 * Harness-agnostic executor port for Gethen workers.
 *
 * A mailbox consumer (C3) hands one `HarnessTask` to a `HarnessExecutor` and
 * receives events, blocking escalations and a final report through a
 * `HarnessSink`. DSH (via `@gethen/executor-sdk`) is only the first
 * implementation; a pi/opencode/codex adapter can implement the same port
 * without touching the mailbox, the transport or the master.
 *
 * Boundaries:
 * - no IO, no clock, no Node builtins, no DSH/`@deepseek-ai` imports;
 * - no Gethen domain types: this package defines its own JSON vocabulary, so a
 *   harness implementation never imports `@gethen/master`, DSH or a transport;
 * - everything here is a value or a type: functions are pure and only validate
 *   or interpret the data the mailbox already received.
 *
 * Lifecycle:
 * 1. `start(task, sink)` begins one execution and resolves with its handle;
 * 2. the execution streams zero or more `sink.event` calls;
 * 3. it may ask one blocking `sink.escalation` at a time and wait for
 *    `execution.sendResolution(resolution)`;
 * 4. it finishes with exactly one `sink.report`; `ready` and `failed` are
 *    terminal, `waiting` means the mailbox consumer keeps the execution open
 *    (for example while a person answers a question);
 * 5. `execution.cancel(reason?)` asks the execution to stop; the implementation
 *    is expected to report `failed` (never both a cancel report and a normal one);
 * 6. all three observation/control methods on the execution are optional:
 *    `state()` exposes a live `HarnessTaskState`, `steer(text)` queues more input
 *    into the running turn, and `interrupt()` asks for a hard stop without
 *    reporting (the consumer decides what the report is).
 */
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
/** An opaque JSON object carried by tasks, events and reports. */
export type JsonObject = { [key: string]: JsonValue };

/** Why an execution is asking; mirrors the worker protocol's escalation kinds. */
export type HarnessEscalationKind = "decision" | "permission" | "conflict" | "scope" | "blocked";
/** How the Master/human answered an escalation. */
export type HarnessResolutionKind = "answer" | "escalate" | "drop";
/** The only report kinds the port understands. */
export type HarnessReportKind = "ready" | "failed" | "waiting";

export const HARNESS_ESCALATION_KINDS: readonly HarnessEscalationKind[] = [
  "decision",
  "permission",
  "conflict",
  "scope",
  "blocked",
];

export const HARNESS_RESOLUTION_KINDS: readonly HarnessResolutionKind[] = [
  "answer",
  "escalate",
  "drop",
];

export const HARNESS_REPORT_KINDS: readonly HarnessReportKind[] = ["ready", "failed", "waiting"];

/** Reports after which the execution is over (`waiting` is not terminal). */
export const TERMINAL_REPORT_KINDS: readonly HarnessReportKind[] = ["ready", "failed"];

/** Whether a task starts a fresh session or attaches to an existing one. */
export type SessionMode = "new" | "reuse";

/**
 * How the harness should pick the session for one assignment:
 * - mode `"new"` (default): a fresh session named after the assignment;
 * - mode `"reuse"` + `sessionId`: resume that persisted session before prompting;
 * - mode `"reuse"` + `key`: a harness-owned registry maps the key to a session,
 *   creating it on first use and resuming it afterwards.
 * A reuse mode that can resolve no session must fail rather than silently
 * starting a new one.
 */
export interface SessionSpec {
  mode: SessionMode;
  /** Explicit persisted session to resume. */
  sessionId?: string;
  /** Registry key, stable across assignments. */
  key?: string;
}

/** One assignment handed to a harness. */
export interface HarnessTask {
  assignmentId: string;
  ticketId: string;
  /** Absolute workspace the harness runs in. */
  workspace: string;
  /** Fully rendered prompt the harness starts from. */
  prompt: string;
  /** The Master's assignment envelope, opaque to the port. */
  envelope?: JsonObject;
  model?: string;
  /** RFC 3339 UTC deadline; `parseHarnessDeadline` converts it to epoch ms. */
  deadlineAt?: string;
  /** Caller-owned metadata (pack digest, origin, ...), opaque to the port. */
  metadata?: JsonObject;
  /** Session selection; omitted means a fresh session per assignment. */
  session?: SessionSpec;
}

/** One observation streamed by an execution (message/tool/usage/... are open). */
export interface HarnessEvent {
  type: string;
  /** RFC 3339 UTC timestamp. */
  at: string;
  data: JsonObject;
}

export interface HarnessEscalationOption {
  id: string;
  label: string;
}

/** A blocking question an execution needs answered before it can go on. */
export interface HarnessEscalation {
  id: string;
  kind: HarnessEscalationKind;
  question: string;
  options?: HarnessEscalationOption[];
  /** Safe option for reversible questions; must name one of `options`. */
  defaultOption?: string;
  blocking: true;
}

/** The answer to one escalation. */
export interface HarnessResolution {
  kind: HarnessResolutionKind;
  /** Selected option id when the resolution answers with a choice. */
  option?: string;
  /** Human-readable answer, when one was supplied. */
  text?: string;
}

/** The final (or pending) state one execution reports. */
export interface HarnessReport {
  kind: HarnessReportKind;
  summary: string;
  /** One concrete question for a person, when the report needs one. */
  question?: string;
  /** Harness-reported usage, opaque to the port. */
  usage?: JsonObject;
}

/** What a harness execution is doing right now. */
export type HarnessTaskPhase = "starting" | "thinking" | "tool" | "waiting" | "done" | "failed";

export const HARNESS_TASK_PHASES: readonly HarnessTaskPhase[] = [
  "starting",
  "thinking",
  "tool",
  "waiting",
  "done",
  "failed",
];

/** The tool a harness is currently executing, when one is running. */
export interface HarnessToolState {
  name: string;
  /** RFC 3339 UTC timestamp of the tool call. */
  startedAt: string;
}

/**
 * A live snapshot of one execution. Every field except the optional ones is
 * present from the first `start`; `seq` and `lastEventAt` advance with the
 * harness event stream, and `phase` becomes terminal (`done`/`failed`) once the
 * execution reports.
 */
export interface HarnessTaskState {
  assignmentId: string;
  phase: HarnessTaskPhase;
  /** 1-based step within the current turn (`0` before the first step). */
  step: number;
  lastTool?: HarnessToolState;
  /** RFC 3339 UTC timestamp of the most recent observation. */
  lastEventAt: string;
  /** Truncated assistant text from the most recent assistant message. */
  lastMessage?: string;
  /** Monotonic count of observations received by the consumer. */
  seq: number;
  /** Session the execution is attached to. */
  sessionId?: string;
}

/** Where a running execution delivers its observations. */
export interface HarnessSink {
  event(event: HarnessEvent): void;
  escalation(escalation: HarnessEscalation): void;
  report(report: HarnessReport): void;
}

/** The consumer's control surface for one running execution. */
export interface HarnessExecution {
  /** Answer the pending escalation. The mailbox consumer dedupes by id. */
  sendResolution(resolution: HarnessResolution): Promise<void>;
  /** Stop the execution; the executor is expected to report `failed`. */
  cancel(reason?: string): Promise<void>;
  /** Live execution state, when the harness can report one. */
  state?(): HarnessTaskState;
  /** Queue more input into the running execution; best effort. */
  steer?(text: string): Promise<void>;
  /** Ask for a hard stop without settling the execution (no report is written). */
  interrupt?(): Promise<void>;
}

/** One entry of a harness session directory (for `sessions()`). */
export interface HarnessSessionInfo {
  sessionId: string;
  /** Registry key that selected this session, when one was used. */
  key?: string;
  workspace: string;
  /** RFC 3339 UTC timestamp of the last claim/release. */
  lastUsedAt: string;
  /** Assignment currently running in this session, when one is. */
  activeAssignmentId?: string;
  /** Persisted configuration, when the harness can read it. */
  config?: {
    cwd: string;
    createdAt: string;
    title?: string;
    model?: { provider: string; model: string };
    sandboxMode?: string;
    approvalPolicy?: string;
  };
  /** Last known persisted state (`unknown` when no store record exists). */
  state?: "running" | "idle" | "stale" | "unknown";
  /** RFC 3339 timestamp of the last stored activity. */
  lastActivityAt?: string;
  /** Last persisted event sequence (cheap estimate). */
  eventCount?: number;
  tokenUsage?: JsonObject;
  /** Whether a session log exists on disk. */
  hasLog?: boolean;
}

/** One normalized event from a session history. */
export interface HarnessSessionEvent {
  seq: number;
  type: string;
  at: string;
  data?: JsonValue;
}

export interface HarnessSessionHistoryOptions {
  /** Default 200. Without `afterSeq` returns the newest `limit` events. */
  limit?: number;
  /** Return events with `seq > afterSeq`. */
  afterSeq?: number;
}

/** Starts (and owns) one execution per task. */
export interface HarnessExecutor {
  start(task: HarnessTask, sink: HarnessSink): Promise<HarnessExecution>;
  /** Snapshot of the sessions the harness knows (for session listings). */
  sessions?(): HarnessSessionInfo[];
  /** One session's normalized event history (newest tail or forward page). */
  sessionHistory?(
    sessionId: string,
    options?: HarnessSessionHistoryOptions,
  ): Promise<HarnessSessionEvent[]>;
}

/** What a harness can run; the mailbox/router may use it for placement. */
export interface HarnessCapabilities {
  models?: string[];
  tools?: string[];
  workspaceRoots?: string[];
}

// ---------------------------------------------------------------- validation

export type HarnessValidationCode =
  | "invalid_report"
  | "invalid_report_kind"
  | "missing_summary"
  | "invalid_question"
  | "invalid_usage"
  | "invalid_escalation"
  | "invalid_escalation_kind"
  | "missing_escalation_id"
  | "missing_question"
  | "invalid_options"
  | "invalid_default_option"
  | "not_blocking"
  | "invalid_resolution"
  | "invalid_resolution_kind"
  | "invalid_option"
  | "invalid_text"
  | "invalid_deadline";

export interface HarnessValidationSuccess<T> {
  ok: true;
  value: T;
}

export interface HarnessValidationFailure {
  ok: false;
  code: HarnessValidationCode;
  message: string;
}

export type HarnessValidation<T> = HarnessValidationSuccess<T> | HarnessValidationFailure;

/** Thrown by the throwing helpers (`parseHarnessDeadline`). */
export class HarnessValidationError extends Error {
  readonly code: HarnessValidationCode;

  constructor(code: HarnessValidationCode, message: string) {
    super(message);
    this.name = "HarnessValidationError";
    this.code = code;
  }
}

export function isHarnessReportKind(value: unknown): value is HarnessReportKind {
  return value === "ready" || value === "failed" || value === "waiting";
}

export function isHarnessEscalationKind(value: unknown): value is HarnessEscalationKind {
  return (HARNESS_ESCALATION_KINDS as readonly unknown[]).includes(value);
}

export function isHarnessResolutionKind(value: unknown): value is HarnessResolutionKind {
  return (HARNESS_RESOLUTION_KINDS as readonly unknown[]).includes(value);
}

/** True when a report kind ends the execution (`ready` or `failed`). */
export function hasTerminalReport(kind: HarnessReportKind): boolean {
  return kind === "ready" || kind === "failed";
}

/** True when an execution phase is over (`done` or `failed`). */
export function isTerminalPhase(phase: HarnessTaskPhase): boolean {
  return phase === "done" || phase === "failed";
}

/** Runtime guard for phases read from outside the type system. */
export function isHarnessTaskPhase(value: unknown): value is HarnessTaskPhase {
  return (HARNESS_TASK_PHASES as readonly unknown[]).includes(value);
}

/**
 * Validate an untrusted `HarnessReport` (for example one parsed from a harness
 * transcript) and return the normalized value.
 */
export function validateHarnessReport(report: unknown): HarnessValidation<HarnessReport> {
  if (!isRecord(report)) return failure("invalid_report", "report must be a JSON object");
  const kind = report.kind;
  if (!isHarnessReportKind(kind)) {
    return failure(
      "invalid_report_kind",
      `report "kind" must be one of: ${HARNESS_REPORT_KINDS.join(", ")}`,
    );
  }
  const summary = report.summary;
  if (typeof summary !== "string" || summary.trim().length === 0) {
    return failure("missing_summary", 'report "summary" must be a non-empty string');
  }

  const value: HarnessReport = { kind, summary };
  const question = report.question;
  if (question !== undefined) {
    if (typeof question !== "string") {
      return failure("invalid_question", 'report "question" must be a string when present');
    }
    if (question.length > 0) value.question = question;
  }
  const usage = report.usage;
  if (usage !== undefined) {
    if (!isRecord(usage)) {
      return failure("invalid_usage", 'report "usage" must be a JSON object when present');
    }
    value.usage = usage as JsonObject;
  }
  return { ok: true, value };
}

/**
 * Validate a `HarnessEscalation` before it is forwarded to a person or router:
 * ids and questions must be non-empty, options unique, and a `defaultOption`
 * must name one of the offered options.
 */
export function validateHarnessEscalation(
  escalation: unknown,
): HarnessValidation<HarnessEscalation> {
  if (!isRecord(escalation)) {
    return failure("invalid_escalation", "escalation must be a JSON object");
  }
  const id = escalation.id;
  if (typeof id !== "string" || id.length === 0) {
    return failure("missing_escalation_id", 'escalation "id" must be a non-empty string');
  }
  const kind = escalation.kind;
  if (!isHarnessEscalationKind(kind)) {
    return failure(
      "invalid_escalation_kind",
      `escalation "kind" must be one of: ${HARNESS_ESCALATION_KINDS.join(", ")}`,
    );
  }
  const question = escalation.question;
  if (typeof question !== "string" || question.trim().length === 0) {
    return failure("missing_question", 'escalation "question" must be a non-empty string');
  }
  if (escalation.blocking !== true) {
    return failure("not_blocking", 'escalation "blocking" must be true');
  }

  const options: HarnessEscalationOption[] = [];
  const rawOptions = escalation.options;
  if (rawOptions !== undefined) {
    if (!Array.isArray(rawOptions)) {
      return failure("invalid_options", 'escalation "options" must be an array when present');
    }
    const seen = new Set<string>();
    for (let index = 0; index < rawOptions.length; index += 1) {
      const option: unknown = rawOptions[index];
      if (!isRecord(option)) {
        return failure("invalid_options", `escalation option ${index} must be an object`);
      }
      const optionId = option.id;
      const label = option.label;
      if (typeof optionId !== "string" || optionId.length === 0) {
        return failure("invalid_options", `escalation option ${index} "id" must be non-empty`);
      }
      if (typeof label !== "string" || label.length === 0) {
        return failure("invalid_options", `escalation option ${index} "label" must be non-empty`);
      }
      if (seen.has(optionId)) {
        return failure("invalid_options", `escalation option id "${optionId}" is duplicated`);
      }
      seen.add(optionId);
      options.push({ id: optionId, label });
    }
  }

  const value: HarnessEscalation = { id, kind, question, blocking: true };
  if (rawOptions !== undefined) value.options = options;
  const defaultOption = escalation.defaultOption;
  if (defaultOption !== undefined) {
    if (typeof defaultOption !== "string" || defaultOption.length === 0) {
      return failure("invalid_default_option", 'escalation "defaultOption" must be non-empty');
    }
    if (!options.some((option) => option.id === defaultOption)) {
      return failure(
        "invalid_default_option",
        `escalation "defaultOption" must name one of the provided options`,
      );
    }
    value.defaultOption = defaultOption;
  }
  return { ok: true, value };
}

/** Validate a `HarnessResolution` from the mailbox before handing it back. */
export function validateHarnessResolution(
  resolution: unknown,
): HarnessValidation<HarnessResolution> {
  if (!isRecord(resolution)) {
    return failure("invalid_resolution", "resolution must be a JSON object");
  }
  const kind = resolution.kind;
  if (!isHarnessResolutionKind(kind)) {
    return failure(
      "invalid_resolution_kind",
      `resolution "kind" must be one of: ${HARNESS_RESOLUTION_KINDS.join(", ")}`,
    );
  }
  const value: HarnessResolution = { kind };
  const option = resolution.option;
  if (option !== undefined) {
    if (typeof option !== "string" || option.length === 0) {
      return failure("invalid_option", 'resolution "option" must be a non-empty string');
    }
    value.option = option;
  }
  const text = resolution.text;
  if (text !== undefined) {
    if (typeof text !== "string") {
      return failure("invalid_text", 'resolution "text" must be a string when present');
    }
    value.text = text;
  }
  return { ok: true, value };
}

/**
 * Parse an RFC 3339 deadline into epoch milliseconds. `undefined` means "no
 * deadline"; an unparseable string throws, so a bad deadline never silently
 * becomes "runs forever".
 */
export function parseHarnessDeadline(deadlineAt: string | undefined): number | undefined {
  if (deadlineAt === undefined) return undefined;
  const milliseconds = Date.parse(deadlineAt);
  if (Number.isNaN(milliseconds)) {
    throw new HarnessValidationError(
      "invalid_deadline",
      `deadlineAt ${JSON.stringify(deadlineAt)} is not a valid RFC 3339 timestamp`,
    );
  }
  return milliseconds;
}

// ------------------------------------------------------------------ internals

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function failure(code: HarnessValidationCode, message: string): HarnessValidationFailure {
  return { ok: false, code, message };
}

/**
 * Worker-side L2 mailbox consumer.
 *
 * The agent has no transport and no harness: it pulls `gethen.task.*` envelopes
 * from its own mailbox (`mailbox:<workerId>`), starts one `HarnessExecutor`
 * execution per assignment, and turns that execution's observations into
 * mailbox messages. DSH is only one possible harness behind the `HarnessExecutor`
 * port; nothing here imports DSH, a legacy frame protocol or a transport.
 *
 * Message flow (application types are plain strings; see `MESSAGE_TYPES`):
 *
 * - in  `gethen.task.assign`  → validate, map to `HarnessTask`, `harness.start`;
 * - out `gethen.task.progress` → sink.event (at-most-once, optional throttle);
 * - out `gethen.task.ask`      → sink.escalation, correlationId = assign id;
 * - in  `gethen.task.answer`   → validate resolution, `execution.sendResolution`;
 * - in  `gethen.task.status`   → reply `gethen.task.status.result` with the live
 *   `execution.state()` (or a synthesized minimal state) and ack;
 * - in  `gethen.task.steer`    → `execution.steer(text)` when the harness has one;
 * - in  `gethen.task.cancel`   → `execution.interrupt()` when available, else
 *   `execution.cancel(reason)` (the harness reports `failed`; the consumer does
 *   not invent a second result and never cancels one execution twice);
 * - out `gethen.task.result`   → sink.report, then ack the assign message.
 *
 * Delivery policy:
 * - the assign is acked only after its result was sent; a failed send keeps the
 *   result in an in-memory outbox and the assign message leased for a retry;
 * - a harness start failure nacks the assign for a limited number of attempts;
 *   once attempts reach the limit the consumer reports `failed` and acks;
 * - the lease of a running assignment is renewed while the execution runs,
 *   including while it waits for an answer to a blocking escalation;
 * - at most `quota` assignments run at once; extra assigns are requeued;
 * - `stop()` stops polling, gives running executions a bounded grace period,
 *   cancels what is left, flushes the outbox and resolves.
 */
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
  HarnessTaskState,
  JsonObject,
  SessionSpec,
} from "@gethen/harness";
import {
  parseHarnessDeadline,
  validateHarnessReport,
  validateHarnessResolution,
} from "@gethen/harness";
import {
  type Envelope,
  newEnvelope,
  type ReceivedMessage,
  validateEnvelope,
} from "@gethen/mailbox";

/** Application message types carried by the mailbox envelope. */
export const MESSAGE_TYPES = {
  assign: "gethen.task.assign",
  accept: "gethen.task.accept",
  progress: "gethen.task.progress",
  ask: "gethen.task.ask",
  answer: "gethen.task.answer",
  result: "gethen.task.result",
  status: "gethen.task.status",
  statusResult: "gethen.task.status.result",
  steer: "gethen.task.steer",
  cancel: "gethen.task.cancel",
  sessionList: "gethen.session.list",
  sessionListResult: "gethen.session.list.result",
  sessionHistory: "gethen.session.history",
  sessionHistoryResult: "gethen.session.history.result",
} as const;

export type MessageType = (typeof MESSAGE_TYPES)[keyof typeof MESSAGE_TYPES];

/** Mailbox name a worker consumes: `mailbox:<workerId>`. */
export function agentMailboxName(workerId: string): string {
  return `mailbox:${workerId}`;
}

// ------------------------------------------------------------------- payloads

/** Untrusted `gethen.task.assign` payload. */
export interface TaskAssignPayload {
  assignmentId?: string;
  ticketId?: string;
  workspace?: string;
  prompt?: string;
  envelope?: JsonObject;
  model?: string;
  deadlineAt?: string;
  metadata?: JsonObject;
  /** Untrusted `{mode,sessionId?,key?}` session selection. */
  session?: unknown;
}

export interface TaskProgressPayload {
  assignmentId: string;
  /** Monotonic event counter for this execution (heartbeats reuse the last). */
  seq?: number;
  /** Optional: periodic heartbeats carry state without a fresh event. */
  event?: HarnessEvent;
  /** Live state snapshot when the harness exposes `execution.state()`. */
  state?: HarnessTaskState;
  /** Session the execution runs in, when known. */
  sessionId?: string;
}

export interface TaskAskPayload {
  assignmentId: string;
  escalationId: string;
  kind: HarnessEscalation["kind"];
  question: string;
  options?: NonNullable<HarnessEscalation["options"]>;
  defaultOption?: string;
}

export interface TaskAnswerPayload {
  assignmentId?: string;
  escalationId?: string;
  resolution: HarnessResolution;
}

export interface TaskResultPayload {
  assignmentId: string;
  kind: HarnessReport["kind"];
  summary: string;
  question?: string;
  usage?: JsonObject;
  /** Session the assignment ran in, when the harness exposed one. */
  sessionId?: string;
}

export interface TaskStatusPayload {
  assignmentId?: string;
}

export interface TaskStatusResultPayload {
  assignmentId: string;
  state: HarnessTaskState | null;
  at: string;
  /** Session the execution runs in, when known. */
  sessionId?: string;
}

export interface TaskSessionListPayload {
  sessions: HarnessSessionInfo[];
  at: string;
}

export interface TaskSessionHistoryResultPayload {
  sessionId: string;
  events: HarnessSessionEvent[];
  at: string;
}

export interface TaskSteerPayload {
  assignmentId?: string;
  text?: string;
}

export interface TaskCancelPayload {
  assignmentId?: string;
  reason?: string;
}

// ---------------------------------------------------------------- validation

export type AssignValidationCode =
  | "invalid_envelope"
  | "wrong_type"
  | "invalid_payload"
  | "missing_assignment_id"
  | "missing_ticket_id"
  | "missing_workspace"
  | "missing_prompt"
  | "invalid_envelope_object"
  | "invalid_model"
  | "invalid_metadata"
  | "invalid_deadline"
  | "invalid_session";

export interface AssignValidationFailure {
  ok: false;
  code: AssignValidationCode;
  message: string;
}

export interface AssignValidationSuccess {
  ok: true;
  task: HarnessTask;
}

export type AssignValidation = AssignValidationSuccess | AssignValidationFailure;

/**
 * Validate one `gethen.task.assign` envelope and map it to a `HarnessTask`.
 * Pure: the mailbox consumer uses it, and tests/other consumers can too.
 */
export function assignToHarnessTask(envelope: Envelope): AssignValidation {
  const envelopeValidation = validateEnvelope(envelope);
  if (!envelopeValidation.valid) {
    return failure("invalid_envelope", envelopeValidation.errors.join("; "));
  }
  if (envelope.type !== MESSAGE_TYPES.assign) {
    return failure("wrong_type", `expected "${MESSAGE_TYPES.assign}", got "${envelope.type}"`);
  }
  const data = asRecord(envelope.data);
  if (data === undefined) return failure("invalid_payload", "assign data must be a JSON object");

  const assignmentId = stringValue(data.assignmentId) ?? stringValue(envelope.subject);
  if (assignmentId === undefined) {
    return failure("missing_assignment_id", 'assign "assignmentId" must be a non-empty string');
  }
  const ticketId = stringValue(data.ticketId);
  if (ticketId === undefined) {
    return failure("missing_ticket_id", 'assign "ticketId" must be a non-empty string');
  }
  const workspace = stringValue(data.workspace);
  if (workspace === undefined) {
    return failure("missing_workspace", 'assign "workspace" must be a non-empty string');
  }
  const prompt = stringValue(data.prompt);
  if (prompt === undefined) {
    return failure("missing_prompt", 'assign "prompt" must be a non-empty string');
  }

  const task: HarnessTask = {
    assignmentId,
    ticketId,
    workspace,
    prompt,
  };

  if (data.envelope !== undefined) {
    const assignmentEnvelope = asRecord(data.envelope);
    if (assignmentEnvelope === undefined) {
      return failure("invalid_envelope_object", 'assign "envelope" must be a JSON object');
    }
    task.envelope = assignmentEnvelope as JsonObject;
  }
  const model = data.model;
  if (model !== undefined) {
    if (typeof model !== "string" || model.length === 0) {
      return failure("invalid_model", 'assign "model" must be a non-empty string');
    }
    task.model = model;
  }
  if (data.metadata !== undefined) {
    const metadata = asRecord(data.metadata);
    if (metadata === undefined) {
      return failure("invalid_metadata", 'assign "metadata" must be a JSON object');
    }
    task.metadata = metadata as JsonObject;
  }
  const deadlineAt = stringValue(data.deadlineAt) ?? envelope.deadline;
  if (deadlineAt !== undefined) {
    try {
      parseHarnessDeadline(deadlineAt);
    } catch (error) {
      return failure("invalid_deadline", error instanceof Error ? error.message : String(error));
    }
    task.deadlineAt = deadlineAt;
  }
  const session = parseAssignSession(data.session);
  if (!session.ok) return failure("invalid_session", session.message);
  if (session.value !== undefined) task.session = session.value;
  return { ok: true, task };
}

// -------------------------------------------------------------------- mailbox

/** A value or a promise, so sync stores and remote links share one surface. */
export type MaybePromise<T> = T | Promise<T>;

/**
 * The mailbox operations the consumer drives, with the destination mailbox as an
 * explicit argument. `MailboxClient` from `@gethen/mailbox` satisfies this
 * directly; a bound remote client (like `MailboxClientLink` from
 * `@gethen/mailbox-link`) uses `BoundMailboxLike` below and is adapted with
 * `toMailboxLike`, which keeps this package free of a link/transport dependency.
 */
export interface MailboxLike {
  receive(
    mailbox: string,
    options?: { limit?: number; visibilityTimeoutMs?: number },
  ): MaybePromise<ReceivedMessage[]>;
  ack(mailbox: string, ids: string[], options?: { dedupeWindowMs?: number }): MaybePromise<number>;
  nack(
    mailbox: string,
    ids: string[],
    options: { action: "requeue" | "dead"; delayMs?: number; maxAttempts?: number },
  ): MaybePromise<number>;
  renewLeases(mailbox: string, ids: string[], visibilityTimeoutMs?: number): MaybePromise<number>;
  send(envelope: Envelope): MaybePromise<{ mailbox: string; duplicate: boolean }>;
}

/**
 * The bound mailbox surface of a remote link (`MailboxClientLink` structurally):
 * the client already knows its mailbox, so the calls carry no mailbox argument.
 */
export interface BoundMailboxLike {
  readonly mailbox: string;
  receive(options?: { limit?: number; timeoutMs?: number }): MaybePromise<ReceivedMessage[]>;
  ack(ids: string[]): MaybePromise<number>;
  nack(
    ids: string[],
    options?: { action?: "requeue" | "dead"; delayMs?: number },
  ): MaybePromise<number>;
  renewLeases(ids: string[], visibilityTimeoutMs?: number): MaybePromise<number>;
  send(envelope: Envelope): MaybePromise<{ mailbox: string; duplicate: boolean }>;
}

/** Anything `createMailboxAgent` accepts: a plain client or a bound link client. */
export type MailboxSource = MailboxLike | BoundMailboxLike;

/** True when the source is a bound client (`readonly mailbox: string`). */
export function isBoundMailbox(source: MailboxSource): source is BoundMailboxLike {
  return typeof (source as { mailbox?: unknown }).mailbox === "string";
}

/** Normalize a bound link client to the mailbox-explicit surface. */
export function toMailboxLike(source: MailboxSource): MailboxLike {
  if (!isBoundMailbox(source)) return source;
  const bound = source;
  return {
    receive: (_mailbox, options) =>
      bound.receive(
        options === undefined
          ? {}
          : {
              ...(options.limit === undefined ? {} : { limit: options.limit }),
            },
      ),
    ack: (_mailbox, ids) => bound.ack(ids),
    nack: (_mailbox, ids, options) =>
      bound.nack(ids, {
        action: options.action,
        ...(options.delayMs === undefined ? {} : { delayMs: options.delayMs }),
      }),
    renewLeases: (_mailbox, ids, visibilityTimeoutMs) =>
      bound.renewLeases(ids, visibilityTimeoutMs),
    send: (envelope) => bound.send(envelope),
  };
}

/** Injection point for a push-based link: it owns leasing and hands messages in. */
export type MailboxListener = (
  mailbox: string,
  onMessage: (message: ReceivedMessage) => Promise<void>,
) => () => void;

export type MailboxAgentErrorPhase =
  | "receive"
  | "assign"
  | "answer"
  | "status"
  | "steer"
  | "cancel"
  | "session"
  | "harness"
  | "progress"
  | "escalation"
  | "result"
  | "outgoing"
  | "lease"
  | "stop";

/** Context handed to `onError`; the agent keeps running unless `stop()` is called. */
export interface MailboxAgentErrorContext {
  phase: MailboxAgentErrorPhase;
  assignmentId?: string;
  envelope?: Envelope;
}

export interface MailboxAgentOptions {
  workerId: string;
  /** A `MailboxClient`, a bound link client, or any `MailboxLike`. */
  mailbox: MailboxSource;
  harness: HarnessExecutor;
  /** Maximum concurrent executions; `0` pauses new assignments. Default 1. */
  quota?: number;
  /** Mailbox to consume; defaults to `mailbox:<workerId>`. */
  mailboxName?: string;
  /** Fallback destination when an assign carries no `replyTo`. Default `mailbox:master`. */
  masterMailbox?: string;
  /** Poll interval used by `start()`. Default 50 ms. */
  pollIntervalMs?: number;
  /** Maximum messages per receive; defaults to the quota (at least 1). */
  batchSize?: number;
  /** Lease duration requested on receive. Default 30 s. */
  leaseMs?: number;
  /** Lease renewal period while an execution runs; `0` disables. Default 10 s. */
  leaseRenewIntervalMs?: number;
  /** Drop progress events closer than this to the previous one; `0` sends every event. */
  progressThrottleMs?: number;
  /** Send a progress heartbeat this often while an execution runs (0 disables). */
  progressHeartbeatMs?: number;
  /** Requeue delay after a failed start / send. Default 1 s. */
  retryDelayMs?: number;
  /** Deliveries before an assign is treated as poisoned. Default 5. */
  maxAttempts?: number;
  /** How long the settled-assignment id stays deduplicated. Default 5 min. */
  dedupeWindowMs?: number;
  /** Grace period `stop()` gives running executions before cancelling them. Default 5 s. */
  stopTimeoutMs?: number;
  /** Clock used for envelope timestamps, throttling and dedupe windows. */
  now?: () => Date;
  /** Push-based delivery; when set, `start()` does not poll. */
  listen?: MailboxListener;
  onError?: (error: unknown, context: MailboxAgentErrorContext) => void;
}

/** The consumer's public surface. */
export interface MailboxAgent {
  readonly workerId: string;
  readonly mailboxName: string;
  /** Assignments currently running (or waiting for an answer). */
  readonly active: number;
  /** Start polling (or listening) and processing messages. Idempotent. */
  start(): void;
  /** Stop intake, drain/cancel executions, flush the outbox. Idempotent. */
  stop(): Promise<void>;
  /** Receive and handle one batch; useful for tests and externally driven loops. */
  runOnce(): Promise<number>;
  /** Retry queued outgoing envelopes; resolves with how many were delivered. */
  flush(): Promise<number>;
}

/** A consumer-side failure with a stable code (`ASSIGN_INVALID`, ...). */
export class MailboxAgentError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "MailboxAgentError";
    this.code = code;
  }
}

const DEFAULTS = {
  quota: 1,
  masterMailbox: "mailbox:master",
  pollIntervalMs: 50,
  leaseMs: 30_000,
  leaseRenewIntervalMs: 10_000,
  retryDelayMs: 1_000,
  maxAttempts: 5,
  dedupeWindowMs: 5 * 60_000,
  stopTimeoutMs: 5_000,
  progressHeartbeatMs: 15_000,
} as const;

interface Outgoing {
  envelope: Envelope;
  onDelivered?: () => MaybePromise<void>;
  onFailed?: (error: unknown) => void;
}

interface ExecutionEntry {
  task: HarnessTask;
  message: ReceivedMessage;
  assignEnvelope: Envelope;
  sink: HarnessSink;
  execution?: HarnessExecution;
  settled: boolean;
  cancelled: boolean;
  cancelRequested: boolean;
  /** The interrupt/cancel control call was issued; never issue it twice. */
  cancelApplied: boolean;
  cancelReason?: string;
  lastState?: HarnessTaskState;
  lastPhase?: HarnessTaskState["phase"];
  lastSessionId?: string;
  lastEventAt?: string;
  eventSeq: number;
  pendingEscalation?: HarnessEscalation;
  askEnvelopeId?: string;
  queuedResolutions: HarnessResolution[];
  leaseTimer?: NodeJS.Timeout;
  lastProgressAt?: number;
  heartbeatTimer?: NodeJS.Timeout;
  done: Promise<void>;
  resolveDone: () => void;
}

/** Create a mailbox consumer for one worker. */
export function createMailboxAgent(options: MailboxAgentOptions): MailboxAgent {
  return new MailboxAgentImpl(options);
}

class MailboxAgentImpl implements MailboxAgent {
  readonly workerId: string;
  readonly mailboxName: string;

  private readonly mailbox: MailboxLike;
  private readonly harness: HarnessExecutor;
  private readonly quota: number;
  private readonly masterMailbox: string;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly leaseMs: number;
  private readonly leaseRenewIntervalMs: number;
  private readonly progressThrottleMs: number;
  private readonly progressHeartbeatMs: number;
  private readonly retryDelayMs: number;
  private readonly maxAttempts: number;
  private readonly dedupeWindowMs: number;
  private readonly stopTimeoutMs: number;
  private readonly now: () => Date;
  private readonly listen: MailboxListener | undefined;
  private readonly onError:
    | ((error: unknown, context: MailboxAgentErrorContext) => void)
    | undefined;

  private readonly executions = new Map<string, ExecutionEntry>();
  private readonly completed = new Map<string, number>();
  private readonly outbox: Outgoing[] = [];
  private timer: NodeJS.Timeout | undefined;
  private unsubscribe: (() => void) | undefined;
  private started = false;
  private stopping = false;
  private stoppingPromise: Promise<void> | undefined;
  private flushing: Promise<number> | undefined;
  private ticking = false;

  constructor(options: MailboxAgentOptions) {
    this.workerId = options.workerId;
    this.mailbox = toMailboxLike(options.mailbox);
    this.harness = options.harness;
    this.quota = Math.max(0, options.quota ?? DEFAULTS.quota);
    this.mailboxName = options.mailboxName ?? agentMailboxName(options.workerId);
    this.masterMailbox = options.masterMailbox ?? DEFAULTS.masterMailbox;
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULTS.pollIntervalMs;
    this.batchSize = Math.max(1, options.batchSize ?? Math.max(1, this.quota));
    this.leaseMs = options.leaseMs ?? DEFAULTS.leaseMs;
    this.leaseRenewIntervalMs = options.leaseRenewIntervalMs ?? DEFAULTS.leaseRenewIntervalMs;
    this.progressThrottleMs = options.progressThrottleMs ?? 0;
    this.progressHeartbeatMs = options.progressHeartbeatMs ?? DEFAULTS.progressHeartbeatMs;
    this.retryDelayMs = options.retryDelayMs ?? DEFAULTS.retryDelayMs;
    this.maxAttempts = Math.max(1, options.maxAttempts ?? DEFAULTS.maxAttempts);
    this.dedupeWindowMs = options.dedupeWindowMs ?? DEFAULTS.dedupeWindowMs;
    this.stopTimeoutMs = options.stopTimeoutMs ?? DEFAULTS.stopTimeoutMs;
    this.now = options.now ?? (() => new Date());
    this.listen = options.listen;
    this.onError = options.onError;
  }

  get active(): number {
    return this.executions.size;
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.stopping = false;
    if (this.listen !== undefined) {
      this.unsubscribe = this.listen(this.mailboxName, (message) => this.handlePushed(message));
      return;
    }
    this.timer = setInterval(() => {
      void this.tick();
    }, this.pollIntervalMs);
    this.timer.unref?.();
  }

  stop(): Promise<void> {
    if (this.stoppingPromise !== undefined) return this.stoppingPromise;
    this.stopping = true;
    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    this.stoppingPromise = this.drainAndCancel();
    return this.stoppingPromise;
  }

  async runOnce(): Promise<number> {
    if (this.stopping) return 0;
    await this.flush();
    if (this.quota === 0) return 0;
    const capacity = Math.max(0, this.quota - this.executions.size);
    // At capacity we still receive control messages (answer/cancel) for the
    // running executions; extra assigns are requeued by `handleAssign`.
    const limit = capacity === 0 ? this.batchSize : Math.max(1, Math.min(this.batchSize, capacity));
    let messages: ReceivedMessage[];
    try {
      messages = await this.mailbox.receive(this.mailboxName, {
        limit,
        visibilityTimeoutMs: this.leaseMs,
      });
    } catch (error) {
      this.reportError(error, { phase: "receive" });
      return 0;
    }

    let handled = 0;
    for (const message of messages) {
      try {
        await this.handleMessage(message);
        handled += 1;
      } catch (error) {
        this.reportError(error, { phase: "receive", envelope: message.envelope });
        await this.nack(message.envelope.id, "requeue");
      }
      await this.flush();
    }
    return handled;
  }

  async flush(): Promise<number> {
    // Single-flight: concurrent callers share one drain loop. Without this a
    // second flush could observe the same head twice and shift the *next*
    // queued envelope after the first one was already sent (dropping it).
    if (this.flushing !== undefined) return this.flushing;
    const running = this.drainOutbox().finally(() => {
      this.flushing = undefined;
    });
    this.flushing = running;
    return running;
  }

  private async drainOutbox(): Promise<number> {
    let delivered = 0;
    while (this.outbox.length > 0) {
      const item = this.outbox[0];
      if (item === undefined) break;
      try {
        await this.mailbox.send(item.envelope);
      } catch (error) {
        this.reportError(error, { phase: "outgoing" });
        item.onFailed?.(error);
        return delivered;
      }
      this.outbox.shift();
      delivered += 1;
      await item.onDelivered?.();
    }
    return delivered;
  }

  // ------------------------------------------------------------- intake

  private async tick(): Promise<void> {
    if (this.ticking || this.stopping) return;
    this.ticking = true;
    try {
      await this.runOnce();
    } catch (error) {
      this.reportError(error, { phase: "receive" });
    } finally {
      this.ticking = false;
    }
  }

  private async handlePushed(message: ReceivedMessage): Promise<void> {
    if (this.stopping) return;
    try {
      await this.handleMessage(message);
      await this.flush();
    } catch (error) {
      this.reportError(error, { phase: "receive", envelope: message.envelope });
      await this.nack(message.envelope.id, "requeue");
    }
  }

  private async handleMessage(message: ReceivedMessage): Promise<void> {
    switch (message.envelope.type) {
      case MESSAGE_TYPES.assign:
        return this.handleAssign(message);
      case MESSAGE_TYPES.answer:
        return this.handleAnswer(message);
      case MESSAGE_TYPES.status:
        return this.handleStatus(message);
      case MESSAGE_TYPES.steer:
        return this.handleSteer(message);
      case MESSAGE_TYPES.sessionList:
        return this.handleSessionList(message);
      case MESSAGE_TYPES.sessionHistory:
        return this.handleSessionHistory(message);
      case MESSAGE_TYPES.cancel:
        return this.handleCancel(message);
      default:
        // Result/progress/ask/unknown types on a worker mailbox are not ours.
        await this.ack(message.envelope.id);
        return;
    }
  }

  private async handleAssign(message: ReceivedMessage): Promise<void> {
    const envelope = message.envelope;
    const validation = assignToHarnessTask(envelope);
    if (!validation.ok) {
      this.reportError(new MailboxAgentError(validation.code, validation.message), {
        phase: "assign",
        envelope,
      });
      await this.nack(envelope.id, "dead");
      return;
    }
    const { task } = validation;

    if (this.isDuplicate(task.assignmentId)) {
      await this.ack(envelope.id);
      return;
    }
    if (this.executions.size >= this.quota) {
      await this.nack(envelope.id, "requeue");
      return;
    }

    const entry = this.createEntry(message, task);
    this.executions.set(task.assignmentId, entry);
    this.startLeaseRenewal(entry);

    try {
      const execution = await this.harness.start(task, entry.sink);
      entry.execution = execution;
      this.startProgressHeartbeat(entry);
      if (entry.cancelled) {
        try {
          await this.applyCancel(entry, entry.cancelReason);
        } catch (error) {
          this.reportError(error, {
            phase: "cancel",
            assignmentId: task.assignmentId,
            envelope,
          });
        }
        return;
      }
      for (const resolution of entry.queuedResolutions.splice(0)) {
        await execution.sendResolution(resolution);
      }
    } catch (error) {
      this.stopProgressHeartbeat(entry);
      this.stopLeaseRenewal(entry);
      this.executions.delete(task.assignmentId);
      this.reportError(error, {
        phase: "harness",
        assignmentId: task.assignmentId,
        envelope,
      });
      if (message.attempts >= this.maxAttempts) {
        this.finishExecution(entry, {
          kind: "failed",
          summary: `harness failed to start: ${messageOf(error)}`,
        });
      } else {
        await this.nack(envelope.id, "requeue");
        this.resolveEntry(entry);
      }
    }
  }

  private async handleAnswer(message: ReceivedMessage): Promise<void> {
    const envelope = message.envelope;
    const data = asRecord(envelope.data) ?? {};
    const assignmentId = stringValue(data.assignmentId) ?? stringValue(envelope.subject);
    const entry = this.findExecution(assignmentId, envelope.correlationId);
    if (entry === undefined) {
      await this.ack(envelope.id);
      return;
    }

    const resolution = validateHarnessResolution(data.resolution);
    if (!resolution.ok) {
      this.reportError(new MailboxAgentError(resolution.code, resolution.message), {
        phase: "answer",
        assignmentId: entry.task.assignmentId,
        envelope,
      });
      await this.ack(envelope.id);
      return;
    }

    const pending = entry.pendingEscalation;
    if (pending === undefined) {
      // A stale or unsolicited answer; the execution has nothing waiting.
      await this.ack(envelope.id);
      return;
    }
    const escalationId = stringValue(data.escalationId);
    if (escalationId !== undefined && escalationId !== pending.id) {
      await this.ack(envelope.id);
      return;
    }
    entry.pendingEscalation = undefined;

    if (entry.execution === undefined) {
      entry.queuedResolutions.push(resolution.value);
      await this.ack(envelope.id);
      return;
    }
    try {
      await entry.execution.sendResolution(resolution.value);
      await this.ack(envelope.id);
    } catch (error) {
      this.reportError(error, {
        phase: "answer",
        assignmentId: entry.task.assignmentId,
        envelope,
      });
      entry.pendingEscalation = pending;
      await this.nack(envelope.id, "requeue");
    }
  }

  private async handleCancel(message: ReceivedMessage): Promise<void> {
    const envelope = message.envelope;
    const data = asRecord(envelope.data) ?? {};
    const assignmentId = stringValue(data.assignmentId) ?? stringValue(envelope.subject);
    const entry = this.findExecution(assignmentId, envelope.correlationId);
    if (entry === undefined || entry.settled) {
      await this.ack(envelope.id);
      return;
    }
    if (entry.cancelled) {
      // One cancel per execution: a duplicate request is acked without a second
      // interrupt/cancel call.
      await this.ack(envelope.id);
      return;
    }

    const reason = stringValue(data.reason);
    entry.cancelled = true;
    entry.cancelRequested = true;
    if (reason !== undefined) entry.cancelReason = reason;
    if (entry.execution === undefined) {
      // The assign is still starting; handleAssign applies the cancel as soon as
      // the harness returns its execution handle.
      await this.ack(envelope.id);
      return;
    }
    try {
      await this.applyCancel(entry, reason);
      await this.ack(envelope.id);
    } catch (error) {
      // Allow a redelivered cancel to retry the control call.
      entry.cancelled = false;
      this.reportError(error, {
        phase: "cancel",
        assignmentId: entry.task.assignmentId,
        envelope,
      });
      await this.nack(envelope.id, "requeue");
    }
  }

  /**
   * Cancellation always goes through `cancel(reason)`: the harness contract
   * makes `cancel` responsible for stopping the execution *and* emitting the
   * terminal report. `interrupt` alone may stop the runtime without a report,
   * which would leave the assignment open until the bridge timeout.
   */
  private async applyCancel(entry: ExecutionEntry, reason?: string): Promise<void> {
    if (entry.cancelApplied) return;
    const execution = entry.execution;
    if (execution === undefined) return;
    await execution.cancel(reason);
    entry.cancelApplied = true;
  }

  private async handleStatus(message: ReceivedMessage): Promise<void> {
    const envelope = message.envelope;
    try {
      const data = asRecord(envelope.data) ?? {};
      const requestedId = stringValue(data.assignmentId) ?? stringValue(envelope.subject);
      const entry = this.findExecution(requestedId, envelope.correlationId);
      const assignmentId = entry?.task.assignmentId ?? requestedId ?? "";
      const at = this.now().toISOString();
      const state = entry === undefined ? null : this.snapshotState(entry);
      const sessionId =
        entry === undefined ? undefined : this.rememberSessionId(entry, state?.sessionId);
      const reply = newEnvelope({
        source: `agent:${this.workerId}`,
        type: MESSAGE_TYPES.statusResult,
        to: envelope.replyTo ?? this.masterMailbox,
        ...(assignmentId === "" ? {} : { subject: assignmentId }),
        time: at,
        correlationId: envelope.id,
        causationId: envelope.id,
        data: {
          assignmentId,
          state,
          at,
          ...(sessionId === undefined ? {} : { sessionId }),
        } satisfies TaskStatusResultPayload,
      });
      await this.mailbox.send(reply);
      await this.ack(envelope.id);
    } catch (error) {
      this.reportError(error, { phase: "status", envelope });
      await this.nack(envelope.id, "requeue");
    }
  }

  /**
   * Answers `gethen.session.list` with the harness session directory. A harness
   * without `sessions()` reports an empty list rather than an error, so the
   * master can treat "no capability" and "no sessions" the same way.
   */
  private async handleSessionList(message: ReceivedMessage): Promise<void> {
    const envelope = message.envelope;
    try {
      const at = this.now().toISOString();
      const sessions = this.harness.sessions?.() ?? [];
      const reply = newEnvelope({
        source: `agent:${this.workerId}`,
        type: MESSAGE_TYPES.sessionListResult,
        to: envelope.replyTo ?? this.masterMailbox,
        time: at,
        correlationId: envelope.id,
        causationId: envelope.id,
        data: {
          sessions: sessions.map((session) => ({ ...session })),
          at,
        } satisfies TaskSessionListPayload,
      });
      await this.mailbox.send(reply);
      await this.ack(envelope.id);
    } catch (error) {
      this.reportError(error, { phase: "session", envelope });
      await this.nack(envelope.id, "requeue");
    }
  }

  /**
   * Answers `gethen.session.history`. A request without `sessionId` (or with a
   * bad `limit`/`afterSeq`) is a caller bug: report it and ack, never retry a
   * poison control message. A harness without `sessionHistory()` reports an
   * empty event list.
   */
  private async handleSessionHistory(message: ReceivedMessage): Promise<void> {
    const envelope = message.envelope;
    try {
      const request = parseSessionHistoryRequest(asRecord(envelope.data) ?? {});
      if (!request.ok) {
        this.reportError(new MailboxAgentError("SESSION_HISTORY_INVALID", request.message), {
          phase: "session",
          envelope,
        });
        await this.ack(envelope.id);
        return;
      }

      const at = this.now().toISOString();
      let events: HarnessSessionEvent[] = [];
      if (this.harness.sessionHistory !== undefined) {
        const options: HarnessSessionHistoryOptions = {};
        if (request.limit !== undefined) options.limit = request.limit;
        if (request.afterSeq !== undefined) options.afterSeq = request.afterSeq;
        events = await this.harness.sessionHistory(request.sessionId, options);
      }
      const reply = newEnvelope({
        source: `agent:${this.workerId}`,
        type: MESSAGE_TYPES.sessionHistoryResult,
        to: envelope.replyTo ?? this.masterMailbox,
        time: at,
        correlationId: envelope.id,
        causationId: envelope.id,
        data: {
          sessionId: request.sessionId,
          events: events.map((event) => ({ ...event })),
          at,
        } satisfies TaskSessionHistoryResultPayload,
      });
      await this.mailbox.send(reply);
      await this.ack(envelope.id);
    } catch (error) {
      this.reportError(error, { phase: "session", envelope });
      await this.nack(envelope.id, "requeue");
    }
  }

  /** Session id from the live state, falling back to the last one observed. */
  private sessionIdOf(entry: ExecutionEntry): string | undefined {
    const live = entry.execution?.state?.();
    return this.rememberSessionId(entry, live?.sessionId);
  }

  /** Keeps the last known session id so it survives states that omit it. */
  private rememberSessionId(
    entry: ExecutionEntry,
    ...candidates: Array<string | undefined>
  ): string | undefined {
    for (const candidate of candidates) {
      if (candidate !== undefined) {
        entry.lastSessionId = candidate;
        return candidate;
      }
    }
    return entry.lastSessionId;
  }

  private async handleSteer(message: ReceivedMessage): Promise<void> {
    const envelope = message.envelope;
    const data = asRecord(envelope.data) ?? {};
    const assignmentId = stringValue(data.assignmentId) ?? stringValue(envelope.subject);
    const entry = this.findExecution(assignmentId, envelope.correlationId);
    if (entry === undefined || entry.settled) {
      this.reportError(
        new MailboxAgentError(
          "STEER_UNKNOWN_ASSIGNMENT",
          `no running execution for assignment ${assignmentId ?? "<unknown>"}`,
        ),
        { phase: "steer", ...(assignmentId === undefined ? {} : { assignmentId }), envelope },
      );
      await this.ack(envelope.id);
      return;
    }

    const text = stringValue(data.text);
    const execution = entry.execution;
    if (execution === undefined || execution.steer === undefined) {
      this.reportError(
        new MailboxAgentError(
          "STEER_UNSUPPORTED",
          `assignment ${entry.task.assignmentId}: harness has no steer capability`,
        ),
        { phase: "steer", assignmentId: entry.task.assignmentId, envelope },
      );
      await this.ack(envelope.id);
      return;
    }
    if (text === undefined) {
      this.reportError(
        new MailboxAgentError(
          "STEER_INVALID",
          `assignment ${entry.task.assignmentId}: steer text must be a non-empty string`,
        ),
        { phase: "steer", assignmentId: entry.task.assignmentId, envelope },
      );
      await this.ack(envelope.id);
      return;
    }

    try {
      await execution.steer(text);
      await this.ack(envelope.id);
    } catch (error) {
      this.reportError(error, {
        phase: "steer",
        assignmentId: entry.task.assignmentId,
        envelope,
      });
      await this.nack(envelope.id, "requeue");
    }
  }

  /** Live state from the harness, else the last one, else a minimal synthesis. */
  private snapshotState(entry: ExecutionEntry): HarnessTaskState {
    const live = entry.execution?.state?.();
    if (live !== undefined) {
      entry.lastState = live;
      this.rememberSessionId(entry, live.sessionId);
      return live;
    }
    if (entry.lastState !== undefined) return entry.lastState;
    return {
      assignmentId: entry.task.assignmentId,
      phase: entry.eventSeq > 0 ? "thinking" : "starting",
      step: 0,
      lastEventAt: entry.lastEventAt ?? entry.assignEnvelope.time,
      seq: entry.eventSeq,
    };
  }

  // ------------------------------------------------------------- harness

  private createEntry(message: ReceivedMessage, task: HarnessTask): ExecutionEntry {
    let resolveDone: () => void = () => undefined;
    const done = new Promise<void>((resolve) => {
      resolveDone = resolve;
    });
    const entry: ExecutionEntry = {
      task,
      message,
      assignEnvelope: message.envelope,
      sink: {
        event: (event) => this.onEvent(entry, event),
        escalation: (escalation) => this.onEscalation(entry, escalation),
        report: (report) => this.onReport(entry, report),
      },
      settled: false,
      cancelled: false,
      cancelRequested: false,
      cancelApplied: false,
      eventSeq: 0,
      queuedResolutions: [],
      done,
      resolveDone,
    };
    return entry;
  }

  private onEvent(entry: ExecutionEntry, event: HarnessEvent): void {
    if (entry.settled) return;
    entry.eventSeq += 1;
    entry.lastEventAt = event.at;

    const previousSessionId = entry.lastState?.sessionId;
    const state = entry.execution?.state?.();
    const phaseChanged =
      state !== undefined && (entry.lastPhase === undefined || entry.lastPhase !== state.phase);
    if (state !== undefined) {
      entry.lastState = state;
      entry.lastPhase = state.phase;
    }
    const sessionId = this.rememberSessionId(entry, state?.sessionId, previousSessionId);

    if (this.progressThrottleMs > 0) {
      const timestamp = this.now().getTime();
      // A phase change is worth a message even inside the throttle window: the
      // master's live view should not wait for the next allowed tick to show
      // "tool" or "waiting".
      if (
        !phaseChanged &&
        entry.lastProgressAt !== undefined &&
        timestamp - entry.lastProgressAt < this.progressThrottleMs
      ) {
        return;
      }
      entry.lastProgressAt = timestamp;
    }

    const payload: TaskProgressPayload = {
      assignmentId: entry.task.assignmentId,
      seq: entry.eventSeq,
      event,
    };
    if (state !== undefined) payload.state = state;
    if (sessionId !== undefined) payload.sessionId = sessionId;
    this.queue(entry, MESSAGE_TYPES.progress, payload);
    void this.flush();
  }

  private onEscalation(entry: ExecutionEntry, escalation: HarnessEscalation): void {
    if (entry.settled) return;
    if (entry.pendingEscalation !== undefined) {
      this.reportError(
        new MailboxAgentError(
          "ESCALATION_BUSY",
          "a blocking escalation is already waiting for its answer",
        ),
        { phase: "escalation", assignmentId: entry.task.assignmentId },
      );
      return;
    }
    entry.pendingEscalation = escalation;
    const payload: TaskAskPayload = {
      assignmentId: entry.task.assignmentId,
      escalationId: escalation.id,
      kind: escalation.kind,
      question: escalation.question,
    };
    if (escalation.options !== undefined) payload.options = escalation.options;
    if (escalation.defaultOption !== undefined) payload.defaultOption = escalation.defaultOption;
    this.queue(entry, MESSAGE_TYPES.ask, payload, this.mailboxName);
    void this.flush();
  }

  private onReport(entry: ExecutionEntry, report: HarnessReport): void {
    if (entry.settled) return;
    this.finishExecution(entry, report);
  }

  private finishExecution(entry: ExecutionEntry, report: HarnessReport): void {
    if (entry.settled) return;
    entry.settled = true;
    this.stopProgressHeartbeat(entry);
    this.stopLeaseRenewal(entry);

    const validation = validateHarnessReport(report);
    let normalized: HarnessReport;
    if (validation.ok) {
      normalized = validation.value;
    } else {
      this.reportError(new MailboxAgentError(validation.code, validation.message), {
        phase: "result",
        assignmentId: entry.task.assignmentId,
      });
      normalized = { kind: "failed", summary: `invalid harness report: ${validation.message}` };
    }

    const payload: TaskResultPayload = {
      assignmentId: entry.task.assignmentId,
      kind: normalized.kind,
      summary: normalized.summary,
    };
    if (normalized.question !== undefined) payload.question = normalized.question;
    if (normalized.usage !== undefined) payload.usage = normalized.usage;
    const sessionId = this.sessionIdOf(entry);
    if (sessionId !== undefined) payload.sessionId = sessionId;
    this.queue(entry, MESSAGE_TYPES.result, payload, undefined, async () => {
      await this.ack(entry.assignEnvelope.id);
      this.completeEntry(entry);
    });
    void this.flush();
  }

  // ------------------------------------------------------------- outgoing

  private queue(
    entry: ExecutionEntry,
    type: MessageType,
    data: unknown,
    replyTo?: string,
    onDelivered?: () => MaybePromise<void>,
  ): void {
    const envelope = newEnvelope({
      source: `agent:${this.workerId}`,
      type,
      to: entry.assignEnvelope.replyTo ?? this.masterMailbox,
      subject: entry.task.assignmentId,
      time: this.now().toISOString(),
      correlationId: entry.assignEnvelope.id,
      causationId: entry.assignEnvelope.id,
      data,
      ...(entry.assignEnvelope.thread === undefined ? {} : { thread: entry.assignEnvelope.thread }),
      ...(replyTo === undefined ? {} : { replyTo }),
    });
    this.outbox.push({ envelope, ...(onDelivered === undefined ? {} : { onDelivered }) });
    if (type === MESSAGE_TYPES.ask) entry.askEnvelopeId = envelope.id;
  }

  private async ack(id: string): Promise<void> {
    try {
      await this.mailbox.ack(this.mailboxName, [id]);
    } catch (error) {
      this.reportError(error, { phase: "receive" });
    }
  }

  private async nack(id: string, action: "requeue" | "dead"): Promise<void> {
    try {
      await this.mailbox.nack(this.mailboxName, [id], {
        action,
        delayMs: this.retryDelayMs,
        maxAttempts: this.maxAttempts,
      });
    } catch (error) {
      this.reportError(error, { phase: "receive" });
    }
  }

  private completeEntry(entry: ExecutionEntry): void {
    this.stopProgressHeartbeat(entry);
    this.stopLeaseRenewal(entry);
    this.executions.delete(entry.task.assignmentId);
    this.completed.set(entry.task.assignmentId, this.now().getTime() + this.dedupeWindowMs);
    this.resolveEntry(entry);
  }

  private resolveEntry(entry: ExecutionEntry): void {
    entry.resolveDone();
  }

  private findExecution(
    assignmentId: string | undefined,
    correlationId: string | undefined,
  ): ExecutionEntry | undefined {
    // An explicit assignment id is authoritative: an answer/ cancellation that
    // names another assignment must never be applied to this execution, even
    // when its correlation id happens to match one of our envelopes.
    if (assignmentId !== undefined) return this.executions.get(assignmentId);
    if (correlationId === undefined) return undefined;
    for (const entry of this.executions.values()) {
      if (entry.assignEnvelope.id === correlationId) return entry;
      if (entry.askEnvelopeId === correlationId) return entry;
    }
    return undefined;
  }

  private isDuplicate(assignmentId: string): boolean {
    if (this.executions.has(assignmentId)) return true;
    const expiresAt = this.completed.get(assignmentId);
    if (expiresAt === undefined) return false;
    if (expiresAt > this.now().getTime()) return true;
    this.completed.delete(assignmentId);
    return false;
  }

  // --------------------------------------------------------------- leases

  private startProgressHeartbeat(entry: ExecutionEntry): void {
    if (this.progressHeartbeatMs <= 0 || entry.heartbeatTimer !== undefined) return;
    const timer = setInterval(() => {
      if (entry.settled) {
        this.stopProgressHeartbeat(entry);
        return;
      }
      const state = entry.execution?.state?.() ?? entry.lastState;
      const sessionId = this.rememberSessionId(entry, state?.sessionId);
      const payload: TaskProgressPayload = {
        assignmentId: entry.task.assignmentId,
        seq: entry.eventSeq,
      };
      if (state !== undefined) payload.state = state;
      if (sessionId !== undefined) payload.sessionId = sessionId;
      this.queue(entry, MESSAGE_TYPES.progress, payload);
      void this.flush();
    }, this.progressHeartbeatMs);
    timer.unref();
    entry.heartbeatTimer = timer;
  }

  private stopProgressHeartbeat(entry: ExecutionEntry): void {
    if (entry.heartbeatTimer === undefined) return;
    clearInterval(entry.heartbeatTimer);
    entry.heartbeatTimer = undefined;
  }

  private startLeaseRenewal(entry: ExecutionEntry): void {
    if (this.leaseRenewIntervalMs <= 0) return;
    entry.leaseTimer = setInterval(() => {
      if (entry.settled) return;
      void Promise.resolve(
        this.mailbox.renewLeases(this.mailboxName, [entry.assignEnvelope.id], this.leaseMs),
      ).catch((error: unknown) => {
        this.reportError(error, { phase: "lease", assignmentId: entry.task.assignmentId });
      });
    }, this.leaseRenewIntervalMs);
    entry.leaseTimer.unref?.();
  }

  private stopLeaseRenewal(entry: ExecutionEntry): void {
    if (entry.leaseTimer === undefined) return;
    clearInterval(entry.leaseTimer);
    entry.leaseTimer = undefined;
  }

  // ---------------------------------------------------------------- stop

  private async drainAndCancel(): Promise<void> {
    const running = (): ExecutionEntry[] =>
      [...this.executions.values()].filter((entry) => !entry.settled);

    if (running().length > 0) {
      await raceWithTimeout(Promise.all(running().map((entry) => entry.done)), this.stopTimeoutMs);
    }
    const remaining = running();
    if (remaining.length > 0) {
      this.reportError(
        new MailboxAgentError(
          "STOPPING",
          `${remaining.length} execution(s) still running after the stop grace period`,
        ),
        { phase: "stop" },
      );
      for (const entry of remaining) {
        if (entry.execution === undefined) continue;
        try {
          await entry.execution.cancel("agent stopped");
        } catch (error) {
          this.reportError(error, { phase: "stop", assignmentId: entry.task.assignmentId });
        }
      }
      await raceWithTimeout(Promise.all(remaining.map((entry) => entry.done)), this.stopTimeoutMs);
    }
    await this.flush();
  }

  private reportError(error: unknown, context: MailboxAgentErrorContext): void {
    this.onError?.(error, context);
  }
}

// -------------------------------------------------------------------- helpers

type SessionHistoryRequest =
  | { ok: true; sessionId: string; limit?: number; afterSeq?: number }
  | { ok: false; message: string };

/** Validates the untrusted `gethen.session.history` payload before it reaches a harness. */
function parseSessionHistoryRequest(data: Record<string, unknown>): SessionHistoryRequest {
  const rawSessionId = data.sessionId;
  const sessionId = typeof rawSessionId === "string" ? rawSessionId.trim() : "";
  if (sessionId.length === 0) {
    return sessionFailure('session history "sessionId" must be a non-empty string');
  }
  const request: SessionHistoryRequest = { ok: true, sessionId };
  const limit = data.limit;
  if (limit !== undefined) {
    if (typeof limit !== "number" || !Number.isInteger(limit) || limit <= 0) {
      return sessionFailure('session history "limit" must be a positive integer when present');
    }
    request.limit = limit;
  }
  const afterSeq = data.afterSeq;
  if (afterSeq !== undefined) {
    if (typeof afterSeq !== "number" || !Number.isInteger(afterSeq) || afterSeq < 0) {
      return sessionFailure(
        'session history "afterSeq" must be a non-negative integer when present',
      );
    }
    request.afterSeq = afterSeq;
  }
  return request;
}

type SessionParse = { ok: true; value: SessionSpec | undefined } | { ok: false; message: string };

/**
 * Validates the untrusted `assign.session` selector with the same rules the
 * master applies: `new` carries no ids, `reuse` carries exactly one of
 * `sessionId` / `key`. Anything else invalidates the whole assign.
 */
function parseAssignSession(value: unknown): SessionParse {
  if (value === undefined) return { ok: true, value: undefined };
  const record = asRecord(value);
  if (record === undefined) return sessionFailure('assign "session" must be a JSON object');
  const mode = record.mode;
  if (mode !== "new" && mode !== "reuse") {
    return sessionFailure('assign session "mode" must be "new" or "reuse"');
  }
  const sessionId = record.sessionId;
  if (sessionId !== undefined && (typeof sessionId !== "string" || sessionId.length === 0)) {
    return sessionFailure('assign session "sessionId" must be a non-empty string when present');
  }
  const key = record.key;
  if (key !== undefined && (typeof key !== "string" || key.length === 0)) {
    return sessionFailure('assign session "key" must be a non-empty string when present');
  }
  if (mode === "new") {
    if (sessionId !== undefined || key !== undefined) {
      return sessionFailure('assign session mode "new" must not carry sessionId or key');
    }
    return { ok: true, value: { mode } };
  }
  if ((sessionId === undefined) === (key === undefined)) {
    return sessionFailure(
      'assign session mode "reuse" requires exactly one of "sessionId" or "key"',
    );
  }
  const spec: SessionSpec = { mode };
  if (sessionId !== undefined) spec.sessionId = sessionId;
  if (key !== undefined) spec.key = key;
  return { ok: true, value: spec };
}

function sessionFailure(message: string): { ok: false; message: string } {
  return { ok: false, message };
}

function failure(code: AssignValidationCode, message: string): AssignValidationFailure {
  return { ok: false, code, message };
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function raceWithTimeout(promise: Promise<unknown>, timeoutMs: number): Promise<void> {
  return new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, timeoutMs);
    timer.unref?.();
    void promise.then(
      () => {
        clearTimeout(timer);
        resolve();
      },
      () => {
        clearTimeout(timer);
        resolve();
      },
    );
  });
}

/**
 * Master-side `LaunchPort` over the L2 mailbox.
 *
 * This replaces the removed `@gethen/bridge-ws` adapter: the Master engine still only
 * sees `LaunchPort.launch(request)`; the adapter turns it into one
 * `gethen.task.assign` envelope, then waits for the correlated
 * `gethen.task.result` on its own mailbox. Blocking `gethen.task.ask` envelopes
 * are answered with `gethen.task.answer` (or parked for a person when no
 * resolver is configured), mirroring the WS escalation semantics.
 *
 * Message flow (correlation is the assignment id, which is also the assign
 * envelope id):
 *
 * - out `gethen.task.assign`  → `mailbox:<workerId>`, `replyTo` master mailbox;
 * - in  `gethen.task.progress` → receipt count + live TaskState + one cached
 *   TaskEvent per `data.event` (bounded ring per assignment, queryable through
 *   `taskEvents()`/`taskEventCount()` for the Master/CLI timeline);
 * - in  `gethen.task.ask`      → escalation record + resolver → answer;
 * - out `gethen.task.answer`   → the ask's `replyTo` (worker mailbox);
 * - in  `gethen.task.result`   → report + receipt (LaunchResult), final state,
 *   plus a synthetic `gethen.result` event in the stream; after a parked
 *   launch already resolved it is delivered through `onLateResult`;
 * - out `gethen.task.answer`   → a person answering a parked escalation
 *   (`answer()`), keeping the pending entry until the late result arrives;
 * - out `gethen.task.status`   → request/response liveness probe (status.result);
 * - out `gethen.task.steer` / `gethen.task.cancel` → live control messages;
 * - out `gethen.session.list` / `gethen.session.history` → worker mailbox
 *   request/reply probes for the UI (results matched by correlationId).
 *
 * The adapter owns no transport and no harness: it only needs a
 * `MailboxClient`-shaped object, so Master and Worker can share a mailbox store
 * (memory/sqlite) or reach it through `@gethen/mailbox-link`.
 */

import {
  type EnqueueResult,
  type Envelope,
  newEnvelope,
  type ReceivedMessage,
} from "@gethen/mailbox";
import type {
  AssignmentReceipt,
  EscalationKindLike,
  EscalationOptionLike,
  EscalationRecord,
  EscalationResolutionInput,
  LaunchPort,
  LaunchRequest,
  LaunchResult,
  Report,
  ReportKind,
} from "@gethen/master";
import { LAUNCH_TIMEOUT_CODE } from "@gethen/master";

/** Application message types carried by the mailbox (mirrors `@gethen/agent`). */
export const MESSAGE_TYPES = {
  assign: "gethen.task.assign",
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

/** Default time a dispatched assignment may take before the launch fails. */
export const DEFAULT_MAILBOX_LAUNCH_TIMEOUT_MS = 10 * 60 * 1000;
/** Default mailbox name prefix; `<prefix><workerId>` and `<prefix>master`. */
export const DEFAULT_MAILBOX_PREFIX = "mailbox:";
/** Name of the Master's own mailbox (under the configured prefix). */
export const MASTER_MAILBOX = "master";
const DEFAULT_POLL_INTERVAL_MS = 50;
const DEFAULT_BATCH_SIZE = 20;
const DEFAULT_LEASE_MS = 30_000;
/** Default how long `requestStatus` waits for a `status.result`. */
export const DEFAULT_STATUS_TIMEOUT_MS = 5_000;
/** Default per-assignment event ring size. */
export const DEFAULT_MAX_TASK_EVENTS = 1_000;
/** Default byte cap for one cached event (larger events are truncated). */
export const DEFAULT_MAX_EVENT_BYTES = 65_536;
/** Default page size of `taskEvents()`. */
export const DEFAULT_TASK_EVENT_LIMIT = 200;

/** Phases a Worker may report in a progress state. */
export type TaskPhase = "starting" | "thinking" | "tool" | "waiting" | "done" | "failed";

export const TASK_PHASES: readonly TaskPhase[] = [
  "starting",
  "thinking",
  "tool",
  "waiting",
  "done",
  "failed",
];

/** The last tool a Worker reported in a live state. */
export interface TaskToolState {
  name: string;
  /** RFC 3339 timestamp the tool started at. */
  startedAt: string;
}

/**
 * Live assignment state reported by a Worker in `gethen.task.progress` (the
 * Master also retains the last state after a result). `updatedAt` is
 * Master-side bookkeeping: the bridge fills it when it receives the update.
 */
export interface TaskState {
  phase: TaskPhase;
  step: number;
  lastTool?: TaskToolState;
  /** RFC 3339 timestamp of the last event the Worker observed. */
  lastEventAt: string;
  lastMessage?: string;
  seq: number;
  updatedAt?: string;
}

/** Minimal JSON vocabulary for opaque event payloads (no harness/DSH import). */
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

/**
 * One cached event from an assignment's progress stream (plus the synthetic
 * `gethen.result` event appended when the assignment reports).
 */
export interface TaskEvent {
  /** Worker-reported sequence number, or a bridge-assigned counter. */
  seq: number;
  /** Event type, e.g. `message`, `tool.start`, `tool.result`, `usage`, `gethen.result`. */
  type: string;
  /** RFC 3339 event timestamp. */
  at: string;
  /** Opaque event payload; replaced with `{preview}` when the event is too large. */
  data?: JsonObject;
  /** True when the event exceeded `maxEventBytes` and `data` is a preview. */
  truncated?: boolean;
}

export interface TaskEventQuery {
  /** Page size; defaults to {@link DEFAULT_TASK_EVENT_LIMIT}. */
  limit?: number;
  /** Only events with `seq > afterSeq` (forward pagination; no tail semantics). */
  afterSeq?: number;
  /** Only events of this type. */
  type?: string;
}

/**
 * One harness session directory entry (`gethen.session.list` reply). Local JSON
 * wire type: the bridge never imports the harness package.
 */
export interface HarnessSessionSummary {
  sessionId: string;
  /** Registry key that selected this session, when one was used. */
  key?: string;
  workspace?: string;
  /** RFC 3339 UTC timestamp of the last claim/release. */
  lastUsedAt?: string;
  /** Assignment currently running in this session, when one is. */
  activeAssignmentId?: string;
  /** Persisted configuration, when the worker can read it. */
  config?: JsonObject;
  /** Last known persisted state. */
  state?: "running" | "idle" | "stale" | "unknown";
  /** RFC 3339 timestamp of the last stored activity. */
  lastActivityAt?: string;
  /** Last persisted event sequence (cheap estimate). */
  eventCount?: number;
  tokenUsage?: JsonObject;
  /** Whether a session log exists on disk. */
  hasLog?: boolean;
}

/** One normalized event from a session history (`gethen.session.history` reply). */
export interface HarnessSessionEvent {
  seq: number;
  type: string;
  at: string;
  data?: JsonValue;
}

/** The snapshot `sessionHistory()` returns. */
export interface HarnessSessionHistory {
  sessionId: string;
  events: HarnessSessionEvent[];
  /** RFC 3339 timestamp the worker replied at, when provided. */
  at?: string;
}

export interface SessionListOptions {
  /** How long to wait for the reply; defaults to `maxSessionListTimeoutMs`. */
  timeoutMs?: number;
}

export interface SessionHistoryOptions {
  /** Forwarded to the worker (default there is 200). */
  limit?: number;
  /** Return events with `seq > afterSeq` (forwarded to the worker). */
  afterSeq?: number;
  /** How long to wait for the reply; defaults to `maxSessionListTimeoutMs`. */
  timeoutMs?: number;
}

const REPORT_KINDS: readonly ReportKind[] = [
  "progress",
  "waiting",
  "ready",
  "blocked",
  "failed",
  "dropped",
];

const ESCALATION_KINDS: readonly EscalationKindLike[] = [
  "decision",
  "blocked",
  "permission",
  "scope",
  "conflict",
];

/** Stable error codes a failed mailbox launch rejects with. */
export type MailboxLaunchErrorCode =
  | "MAILBOX_LAUNCH_TIMEOUT"
  | "MAILBOX_DISPATCH_FAILED"
  | "MAILBOX_RESOLUTION_FAILED"
  | "MAILBOX_CLOSED"
  | "MAILBOX_STATUS_TIMEOUT"
  | "MAILBOX_SEND_FAILED"
  | "MAILBOX_UNKNOWN_ASSIGNMENT"
  | "MAILBOX_NO_PARKED_ESCALATION"
  | "MAILBOX_INVALID_RESOLUTION";

/** The same codes as a runtime list, for callers that classify failures. */
export const MAILBOX_LAUNCH_ERROR_CODES: readonly MailboxLaunchErrorCode[] = [
  "MAILBOX_LAUNCH_TIMEOUT",
  "MAILBOX_DISPATCH_FAILED",
  "MAILBOX_RESOLUTION_FAILED",
  "MAILBOX_CLOSED",
  "MAILBOX_STATUS_TIMEOUT",
  "MAILBOX_SEND_FAILED",
  "MAILBOX_UNKNOWN_ASSIGNMENT",
  "MAILBOX_NO_PARKED_ESCALATION",
  "MAILBOX_INVALID_RESOLUTION",
];

/** A launch failure; `message` is prefixed with `code` for log-grep-ability. */
export class MailboxLaunchError extends Error {
  readonly code: MailboxLaunchErrorCode;

  constructor(code: MailboxLaunchErrorCode, message: string) {
    super(`${code}: ${message}`);
    this.name = "MailboxLaunchError";
    this.code = code;
  }
}

/**
 * The slice of `@gethen/mailbox`'s `MailboxClient` this adapter needs.
 * `MailboxClient` satisfies it; tests (and mailbox links) can pass a fake.
 */
export interface MailboxClientLike {
  send(envelope: Envelope): EnqueueResult;
  receive(
    mailbox: string,
    options?: { limit?: number; visibilityTimeoutMs?: number },
  ): ReceivedMessage[];
  ack(mailbox: string, ids: string[], options?: { dedupeWindowMs?: number }): number;
  nack(
    mailbox: string,
    ids: string[],
    options: { action: "requeue" | "dead"; delayMs?: number; maxAttempts?: number },
  ): number;
  readonly pollIntervalMs?: number;
}

/** A result produced after the original launch had already returned `waiting`. */
export interface LateLaunchResult {
  request: LaunchRequest;
  report: Report;
  receipt: AssignmentReceipt;
  escalations?: EscalationRecord[];
}

export interface MailboxLaunchOptions {
  /** Shared mailbox client (memory/sqlite store or a mailbox link). */
  client: MailboxClientLike;
  /** Worker whose mailbox receives assignments: `<prefix><workerId>`. */
  workerId: string;
  /** Answers a worker escalation; absent parks it for a person. */
  resolveEscalation?: (record: EscalationRecord) => Promise<EscalationResolutionInput | undefined>;
  /** Observer fired when an `ask` is parsed (status `"waiting"`). */
  onEscalation?: (record: EscalationRecord) => void;
  /** Observer fired when an escalation is resolved or parked. */
  onResolution?: (record: EscalationRecord) => void;
  /** Observer fired whenever a progress/result update changes a TaskState. */
  onTaskState?: (assignmentId: string, state: TaskState) => void;
  /**
   * Observer fired when a result arrives after the launch promise already
   * settled with a parked `waiting` report (answered escalation resumed).
   */
  onLateResult?: (input: LateLaunchResult) => void;
  /** Per-launch timeout; defaults to {@link DEFAULT_MAILBOX_LAUNCH_TIMEOUT_MS}. */
  timeoutMs?: number;
  now?: () => Date;
  /** Mailbox prefix for both sides; defaults to {@link DEFAULT_MAILBOX_PREFIX}. */
  mailboxPrefix?: string;
  /** Poll interval for the Master mailbox; defaults to the client's or 50 ms. */
  pollIntervalMs?: number;
  /** Visibility timeout requested when receiving; default 30 s. */
  leaseMs?: number;
  /** Messages per receive; default 20. */
  batchSize?: number;
  /** Start the Master-mailbox poll loop immediately (default true). */
  autoStart?: boolean;
  /** Per-assignment event ring size; defaults to {@link DEFAULT_MAX_TASK_EVENTS}. */
  maxTaskEvents?: number;
  /** Byte cap for one cached event; defaults to {@link DEFAULT_MAX_EVENT_BYTES}. */
  maxEventBytes?: number;
  /** Default timeout for session list/history requests; defaults to 5 s. */
  maxSessionListTimeoutMs?: number;
}

export interface MailboxLaunchBridge {
  /** The port to hand to `MasterService`. */
  readonly launchPort: LaunchPort;
  /** Mailbox the worker consumes, e.g. `mailbox:worker-1`. */
  readonly workerMailbox: string;
  /** Mailbox this adapter polls for ask/result/progress, e.g. `mailbox:master`. */
  readonly masterMailbox: string;
  /** Process one received envelope; the subscription callback and the test hook. */
  handleEnvelope(envelope: Envelope): void;
  /** Poll the Master mailbox once; returns how many envelopes were processed. */
  pollOnce(): number;
  /** Last known state for one assignment (retained after the result). */
  taskState(assignmentId: string): TaskState | undefined;
  /** Snapshot of every known assignment state. */
  taskStates(): Record<string, TaskState>;
  /**
   * Ask the Worker for its live state (`gethen.task.status` -> correlated
   * `status.result`). Rejects with `MAILBOX_STATUS_TIMEOUT` when unanswered.
   */
  requestStatus(assignmentId: string, options?: { timeoutMs?: number }): Promise<TaskState | null>;
  /** Send `gethen.task.steer` to a running assignment. */
  steer(assignmentId: string, text: string): Promise<void>;
  /** Send `gethen.task.cancel`; the launch keeps waiting for its `result`. */
  cancel(assignmentId: string, reason?: string): Promise<void>;
  /**
   * Answer the escalation that parked a launch for a person and send
   * `gethen.task.answer` to the Worker. The launch promise already resolved
   * with a `waiting` report; the resumed Worker's eventual result is delivered
   * through `onLateResult`.
   */
  answer(
    assignmentId: string,
    resolution: EscalationResolutionInput,
    options?: { escalationId?: string },
  ): EscalationRecord;
  /**
   * Cached events for one assignment, oldest first. Without `afterSeq` the
   * newest `limit` events are returned (tail); with `afterSeq` a forward page
   * of the events after that sequence is returned.
   */
  taskEvents(assignmentId: string, query?: TaskEventQuery): TaskEvent[];
  /** Number of events currently buffered for one assignment. */
  taskEventCount(assignmentId: string): number;
  /** Events evicted by the ring buffer for one assignment. */
  taskEventDropped(assignmentId: string): number;
  /**
   * Ask the Worker for its harness session directory
   * (`gethen.session.list` -> correlated `session.list.result`).
   */
  listSessions(options?: SessionListOptions): Promise<HarnessSessionSummary[]>;
  /**
   * Ask the Worker for one session's event history
   * (`gethen.session.history` -> correlated `session.history.result`).
   */
  sessionHistory(
    sessionId: string,
    options?: SessionHistoryOptions,
  ): Promise<HarnessSessionHistory>;
  /** Stop polling and reject in-flight launches with `MAILBOX_CLOSED`. */
  close(): void;
}

interface PendingLaunch {
  readonly promise: Promise<LaunchResult>;
  readonly request: LaunchRequest;
  readonly escalations: EscalationRecord[];
  readonly seenEscalations: Set<string>;
  events: number;
  /** The launch promise has settled (waiting) but the Worker may still report. */
  launchSettled: boolean;
  settled: boolean;
  timer: ReturnType<typeof setTimeout> | undefined;
  /** The escalation currently parked for a person, when there is one. */
  parkedEscalation?: EscalationRecord;
  /** The Worker's ask envelope; answers route back to its replyTo. */
  parkedAsk?: Envelope;
  resolve(result: LaunchResult): void;
  reject(error: Error): void;
}

interface PendingStatusRequest {
  readonly assignmentId: string;
  readonly resolve: (state: TaskState | null) => void;
  readonly reject: (error: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

interface PendingSessionRequest {
  readonly resolve: (data: Record<string, unknown> | undefined) => void;
  readonly reject: (error: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

interface TaskEventBuffer {
  events: TaskEvent[];
  /** Events evicted by the ring (maxTaskEvents). */
  dropped: number;
  /** Next sequence number used when a worker omits `data.seq`. */
  nextSeq: number;
}

/**
 * Build the mailbox `LaunchPort`. The poll loop watches only the Master's
 * mailbox; each pending launch is keyed by its assignment id.
 */
export function createMailboxLaunchPort(options: MailboxLaunchOptions): MailboxLaunchBridge {
  const client = options.client;
  const now = options.now ?? (() => new Date());
  const nowIso = (): string => now().toISOString();
  const prefix = options.mailboxPrefix ?? DEFAULT_MAILBOX_PREFIX;
  const workerMailbox = `${prefix}${options.workerId}`;
  const masterMailbox = `${prefix}${MASTER_MAILBOX}`;
  const timeoutMs = positiveTimeout(options.timeoutMs ?? DEFAULT_MAILBOX_LAUNCH_TIMEOUT_MS);
  const pollIntervalMs =
    options.pollIntervalMs ?? client.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
  const leaseMs = options.leaseMs ?? DEFAULT_LEASE_MS;
  const batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
  const maxTaskEvents = positiveInteger(
    options.maxTaskEvents ?? DEFAULT_MAX_TASK_EVENTS,
    "maxTaskEvents",
  );
  const maxEventBytes = positiveInteger(
    options.maxEventBytes ?? DEFAULT_MAX_EVENT_BYTES,
    "maxEventBytes",
  );
  const pendingByAssignment = new Map<string, PendingLaunch>();

  /**
   * (Re)arm the inactivity timer for one launch. `timeoutMs` is a *silence*
   * window: progress/ask traffic keeps a long task alive, while a genuinely
   * stuck worker still fails. A fixed total-duration timeout used to kill
   * legitimate 10-minute tasks.
   *
   * A timeout rejects the launch but keeps the pending entry: the task was
   * dispatched and may still report, so its late result is delivered through
   * `onLateResult` instead of being dropped.
   */
  function armLaunchTimer(pending: PendingLaunch): void {
    if (pending.timer !== undefined) clearTimeout(pending.timer);
    pending.timer = setTimeout(() => {
      pending.timer = undefined;
      pending.launchSettled = true;
      pending.reject(
        new MailboxLaunchError(
          LAUNCH_TIMEOUT_CODE,
          `assignment ${pending.request.assignment.id} produced no activity within ${timeoutMs}ms`,
        ),
      );
    }, timeoutMs);
    pending.timer.unref();
  }
  /** Live/recent assignment states, keyed by assignment id (retained after result). */
  const states = new Map<string, TaskState>();
  /** In-flight `gethen.task.status` requests, keyed by request envelope id. */
  const statusRequests = new Map<string, PendingStatusRequest>();
  /** Per-assignment event ring buffers (retained after the result). */
  const eventStreams = new Map<string, TaskEventBuffer>();
  /** In-flight session list/history requests, keyed by request envelope id. */
  const sessionRequests = new Map<string, PendingSessionRequest>();
  const maxSessionListTimeoutMs = positiveTimeout(
    options.maxSessionListTimeoutMs ?? DEFAULT_STATUS_TIMEOUT_MS,
    "maxSessionListTimeoutMs",
  );
  let timer: ReturnType<typeof setInterval> | undefined;
  let closed = false;

  function notify(callback: (() => void) | undefined): void {
    // Observers (audit/persistence hooks) are best-effort; they must not break
    // message handling or turn into unhandled rejections.
    try {
      callback?.();
    } catch {
      // ignored on purpose
    }
  }

  function publishTaskState(assignmentId: string, state: TaskState): void {
    states.set(assignmentId, state);
    notify(() => options.onTaskState?.(assignmentId, state));
  }

  /** Retain the final `done`/`failed` state when the result arrives. */
  function finalizeTaskState(pending: PendingLaunch, report: Report): void {
    const assignmentId = pending.request.assignment.id;
    const at = nowIso();
    const previous = states.get(assignmentId);
    const phase: TaskPhase = report.kind === "failed" ? "failed" : "done";
    const state: TaskState =
      previous === undefined
        ? {
            phase,
            step: 0,
            lastEventAt: report.at,
            lastMessage: report.summary,
            seq: 0,
            updatedAt: at,
          }
        : {
            ...previous,
            phase,
            lastEventAt: report.at,
            lastMessage: report.summary,
            updatedAt: at,
          };
    publishTaskState(assignmentId, state);
  }

  function eventBufferFor(assignmentId: string): TaskEventBuffer {
    const existing = eventStreams.get(assignmentId);
    if (existing !== undefined) return existing;
    const buffer: TaskEventBuffer = { events: [], dropped: 0, nextSeq: 1 };
    eventStreams.set(assignmentId, buffer);
    return buffer;
  }

  /**
   * Append one event to the assignment ring. Explicit worker `seq` values are
   * de-duplicated; missing seqs use a monotonic bridge counter. The ring drops
   * the oldest events past `maxTaskEvents` and counts them as dropped.
   */
  function appendTaskEvent(
    assignmentId: string,
    input: { type: string; at: string; data?: JsonObject },
    explicitSeq?: number,
  ): TaskEvent {
    const buffer = eventBufferFor(assignmentId);
    let seq: number;
    if (explicitSeq === undefined) {
      seq = buffer.nextSeq;
      buffer.nextSeq = seq + 1;
    } else {
      const existing = buffer.events.find((event) => event.seq === explicitSeq);
      if (existing !== undefined) return existing;
      seq = explicitSeq;
      if (seq >= buffer.nextSeq) buffer.nextSeq = seq + 1;
    }
    const event = buildTaskEvent(seq, input, maxEventBytes);
    buffer.events.push(event);
    while (buffer.events.length > maxTaskEvents) {
      buffer.events.shift();
      buffer.dropped += 1;
    }
    return event;
  }

  function finish(pending: PendingLaunch): void {
    pending.settled = true;
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
      pending.timer = undefined;
    }
    pendingByAssignment.delete(pending.request.assignment.id);
  }

  function resolveWith(pending: PendingLaunch, result: LaunchResult): void {
    if (pending.settled) return;
    finish(pending);
    pending.resolve(result);
  }

  function rejectWith(pending: PendingLaunch, error: Error): void {
    if (pending.settled) return;
    finish(pending);
    pending.reject(error);
  }

  function createPending(request: LaunchRequest): PendingLaunch {
    let resolvePromise: (result: LaunchResult) => void = () => {};
    let rejectPromise: (error: Error) => void = () => {};
    const promise = new Promise<LaunchResult>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    });
    return {
      promise,
      request,
      escalations: [],
      seenEscalations: new Set(),
      events: 0,
      launchSettled: false,
      settled: false,
      timer: undefined,
      resolve: resolvePromise,
      reject: rejectPromise,
    };
  }

  function receiptFor(pending: PendingLaunch, report: Report): AssignmentReceipt {
    return {
      assignment: pending.request.assignment.id,
      ticket: pending.request.ticket.id,
      worker: options.workerId,
      workspace: pending.request.assignment.workspace,
      packDigest: pending.request.packDigest,
      at: report.at,
      outcome: report.kind === "failed" ? "failed" : "completed",
      summary: report.summary,
      events: pending.events,
    };
  }

  function resultFor(pending: PendingLaunch, report: Report): LaunchResult {
    const result: LaunchResult = { report, receipt: receiptFor(pending, report) };
    if (pending.escalations.length > 0) result.escalations = [...pending.escalations];
    return result;
  }

  /** Mark one escalation parked (clearing the silence timer) for later answer. */
  function markParked(pending: PendingLaunch, record: EscalationRecord, ask?: Envelope): void {
    record.status = "parked";
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
      pending.timer = undefined;
    }
    pending.parkedEscalation = record;
    if (ask !== undefined) pending.parkedAsk = ask;
    notify(() => options.onResolution?.(record));
  }

  /**
   * Park a launch for a person. The launch promise resolves once with the
   * `waiting` report, but the pending entry is kept so `answer()` and a later
   * Worker result can still be handled.
   */
  function park(pending: PendingLaunch, record: EscalationRecord, ask?: Envelope): void {
    markParked(pending, record, ask);
    if (pending.launchSettled) return;
    pending.launchSettled = true;
    const at = nowIso();
    const report: Report = {
      kind: "waiting",
      summary: `waiting for a person: ${record.question}`,
      question: record.question,
      at,
    };
    pending.resolve(resultFor(pending, report));
  }

  function handleResult(pending: PendingLaunch, envelope: Envelope): void {
    const data = asRecord(envelope.data);
    const parsed = parseTaskResult(data, nowIso());
    const report: Report = parsed.ok
      ? parsed.report
      : {
          kind: "failed",
          summary: `invalid task result: ${parsed.reasons.join("; ")}`,
          at: nowIso(),
        };
    appendTaskEvent(pending.request.assignment.id, {
      type: "gethen.result",
      at: report.at,
      data: { kind: report.kind, summary: report.summary },
    });
    finalizeTaskState(pending, report);
    const result = resultFor(pending, report);
    if (pending.launchSettled) {
      notify(() =>
        options.onLateResult?.({
          request: pending.request,
          report,
          receipt: result.receipt,
          ...(result.escalations === undefined ? {} : { escalations: result.escalations }),
        }),
      );
      finish(pending);
      return;
    }
    resolveWith(pending, result);
  }

  function handleProgress(pending: PendingLaunch, data: Record<string, unknown> | undefined): void {
    // A parked launch must not be killed by the silence timer: the Worker is
    // blocked on a person, and late progress after the answer must not re-arm.
    if (!pending.launchSettled) armLaunchTimer(pending);
    pending.events += 1;
    const assignmentId = pending.request.assignment.id;

    // Older workers send text/event progress without a state; heartbeats send
    // a state without a fresh event. Both are tolerated independently.
    const rawEvent = data?.event;
    if (rawEvent !== undefined && rawEvent !== null) {
      const parsed = parseTaskEvent(rawEvent);
      if (parsed !== undefined) {
        appendTaskEvent(assignmentId, parsed, integerValue(data?.seq));
      }
    }

    const rawState = data?.state;
    if (rawState === undefined || rawState === null || !isRecord(rawState)) return;
    const state = parseTaskState(rawState, nowIso());
    if (state === undefined) return;
    publishTaskState(assignmentId, state);
  }

  function handleSessionResult(envelope: Envelope): void {
    const data = asRecord(envelope.data);
    const requestId = stringValue(envelope.correlationId) ?? stringValue(data?.requestId);
    if (requestId === undefined) return;
    const pending = sessionRequests.get(requestId);
    if (pending === undefined) return;
    sessionRequests.delete(requestId);
    clearTimeout(pending.timer);
    pending.resolve(data);
  }

  function handleStatusResult(envelope: Envelope): void {
    const data = asRecord(envelope.data);
    const requestId =
      stringValue(envelope.correlationId) ??
      stringValue(data?.requestId) ??
      stringValue(data?.statusRequestId);
    if (requestId === undefined) return;
    const pending = statusRequests.get(requestId);
    if (pending === undefined) return;
    statusRequests.delete(requestId);
    clearTimeout(pending.timer);
    pending.resolve(parseStatusResult(data, nowIso()));
  }

  function handleAsk(pending: PendingLaunch, envelope: Envelope): void {
    // A launch that already timed out keeps its entry only to receive the late
    // result/ask; do not restart the silence timer for it.
    if (!pending.launchSettled) armLaunchTimer(pending);
    const data = asRecord(envelope.data);
    if (data === undefined) return;
    const escalationId = stringValue(data.escalationId);
    const kind = stringValue(data.kind);
    const question = stringValue(data.question);
    if (escalationId === undefined || kind === undefined || question === undefined) return;
    if (!isEscalationKind(kind) || pending.seenEscalations.has(escalationId)) return;

    const parsedOptions = parseOptions(data.options);
    if (parsedOptions === undefined) return;
    const defaultOption = optionalStringValue(data.defaultOption);
    pending.seenEscalations.add(escalationId);

    const record: EscalationRecord = {
      id: escalationId,
      ticket: pending.request.ticket.id,
      assignment: pending.request.assignment.id,
      // The mailbox protocol carries no session id; the worker id is the stable
      // identity a person-facing panel can show.
      session: options.workerId,
      kind,
      question,
      options: parsedOptions,
      ...(defaultOption === undefined ? {} : { defaultOption }),
      status: "waiting",
      at: nowIso(),
    };
    pending.escalations.push(record);
    notify(() => options.onEscalation?.(record));

    // A launch that already returned `waiting` parks any later ask for a
    // person instead of resolving the (already settled) launch again.
    if (pending.launchSettled) {
      park(pending, record, envelope);
      return;
    }
    if (options.resolveEscalation === undefined) {
      park(pending, record, envelope);
      return;
    }
    void resolveAsk(pending, record, envelope);
  }

  async function resolveAsk(
    pending: PendingLaunch,
    record: EscalationRecord,
    ask: Envelope,
  ): Promise<void> {
    const resolver = options.resolveEscalation;
    let resolution: EscalationResolutionInput | undefined;
    try {
      resolution = resolver === undefined ? undefined : await resolver(record);
    } catch {
      // A failing resolver cannot answer: fall back to parking for a person.
      resolution = undefined;
    }
    if (pending.settled) return;
    if (resolution === undefined) {
      park(pending, record, ask);
      return;
    }

    const at = nowIso();
    record.status = resolution.kind === "answer" ? "resolved" : "parked";
    record.resolution = {
      kind: resolution.kind,
      text: resolution.text,
      ...(resolution.option === undefined ? {} : { option: resolution.option }),
      at,
    };
    notify(() => options.onResolution?.(record));

    try {
      client.send(answerEnvelope(pending, record, resolution, ask, nowIso()));
    } catch (error) {
      rejectWith(
        pending,
        new MailboxLaunchError(
          "MAILBOX_RESOLUTION_FAILED",
          `assignment ${pending.request.assignment.id}: ${messageOf(error)}`,
        ),
      );
    }
  }

  async function launch(request: LaunchRequest): Promise<LaunchResult> {
    const assignmentId = request.assignment.id;
    const pending = createPending(request);
    pendingByAssignment.set(assignmentId, pending);
    armLaunchTimer(pending);

    try {
      client.send(assignEnvelope(request));
    } catch (error) {
      rejectWith(
        pending,
        new MailboxLaunchError(
          "MAILBOX_DISPATCH_FAILED",
          `assignment ${assignmentId}: ${messageOf(error)}`,
        ),
      );
    }
    return pending.promise;
  }

  function handleEnvelope(envelope: Envelope): void {
    if (closed) return;
    const data = asRecord(envelope.data);
    switch (envelope.type) {
      case MESSAGE_TYPES.result: {
        const assignmentId = assignmentIdOf(data, envelope);
        const pending =
          assignmentId === undefined ? undefined : pendingByAssignment.get(assignmentId);
        if (pending !== undefined) handleResult(pending, envelope);
        return;
      }
      case MESSAGE_TYPES.progress: {
        const assignmentId = assignmentIdOf(data, envelope);
        const pending =
          assignmentId === undefined ? undefined : pendingByAssignment.get(assignmentId);
        if (pending !== undefined) handleProgress(pending, data);
        return;
      }
      case MESSAGE_TYPES.ask: {
        const assignmentId = assignmentIdOf(data, envelope);
        const pending =
          assignmentId === undefined ? undefined : pendingByAssignment.get(assignmentId);
        if (pending !== undefined) handleAsk(pending, envelope);
        return;
      }
      case MESSAGE_TYPES.statusResult: {
        handleStatusResult(envelope);
        return;
      }
      case MESSAGE_TYPES.sessionListResult:
      case MESSAGE_TYPES.sessionHistoryResult: {
        handleSessionResult(envelope);
        return;
      }
      default:
        // accept/cancel/agent messages/unknown types carry no launch state here.
        return;
    }
  }

  function pollOnce(): number {
    if (closed) return 0;
    let messages: ReceivedMessage[];
    try {
      messages = client.receive(masterMailbox, {
        limit: batchSize,
        visibilityTimeoutMs: leaseMs,
      });
    } catch {
      // A store/link hiccup is retried on the next tick; do not kill the loop.
      return 0;
    }
    let handled = 0;
    for (const message of messages) {
      try {
        handleEnvelope(message.envelope);
        handled += 1;
      } catch {
        // Malformed envelopes are dropped (acked) rather than redelivered forever.
      } finally {
        try {
          client.ack(masterMailbox, [message.envelope.id]);
        } catch {
          // The lease will expire and redeliver if the ack failed.
        }
      }
    }
    return handled;
  }

  function start(): void {
    if (timer !== undefined || closed) return;
    timer = setInterval(() => {
      pollOnce();
    }, pollIntervalMs);
    timer.unref();
  }

  function close(): void {
    if (closed) return;
    closed = true;
    if (timer !== undefined) {
      clearInterval(timer);
      timer = undefined;
    }
    for (const pending of [...pendingByAssignment.values()]) {
      if (pending.launchSettled) {
        // The launch promise already resolved with `waiting`; a parked entry
        // is just bookkeeping, so drop it without touching the promise.
        finish(pending);
        continue;
      }
      rejectWith(
        pending,
        new MailboxLaunchError(
          "MAILBOX_CLOSED",
          `assignment ${pending.request.assignment.id} was abandoned because the mailbox bridge closed`,
        ),
      );
    }
    for (const [requestId, pending] of [...statusRequests]) {
      clearTimeout(pending.timer);
      statusRequests.delete(requestId);
      pending.reject(
        new MailboxLaunchError(
          "MAILBOX_CLOSED",
          `assignment ${pending.assignmentId}: status request after the mailbox bridge closed`,
        ),
      );
    }
    for (const [requestId, pending] of [...sessionRequests]) {
      clearTimeout(pending.timer);
      sessionRequests.delete(requestId);
      pending.reject(
        new MailboxLaunchError(
          "MAILBOX_CLOSED",
          `session request ${requestId} after the mailbox bridge closed`,
        ),
      );
    }
  }

  function assignEnvelope(request: LaunchRequest): Envelope {
    return newEnvelope({
      id: request.assignment.id,
      source: "master:router",
      type: MESSAGE_TYPES.assign,
      subject: request.ticket.id,
      time: nowIso(),
      data: {
        assignmentId: request.assignment.id,
        ticketId: request.ticket.id,
        workspace: request.assignment.workspace,
        packDigest: request.packDigest,
        prompt: request.prompt,
        ...(request.assignment.session === undefined
          ? {}
          : { session: request.assignment.session }),
      },
      to: workerMailbox,
      correlationId: request.assignment.id,
      replyTo: masterMailbox,
    });
  }

  function answerEnvelope(
    pending: PendingLaunch,
    record: EscalationRecord,
    resolution: EscalationResolutionInput,
    ask: Envelope | undefined,
    at: string,
  ): Envelope {
    return newEnvelope({
      source: "master:router",
      type: MESSAGE_TYPES.answer,
      subject: pending.request.assignment.id,
      time: at,
      data: {
        assignmentId: pending.request.assignment.id,
        escalationId: record.id,
        resolution: {
          kind: resolution.kind,
          ...(resolution.option === undefined ? {} : { option: resolution.option }),
          text: resolution.text,
        },
      },
      to: ask?.replyTo ?? workerMailbox,
      correlationId: pending.request.assignment.id,
      ...(ask === undefined ? {} : { causationId: ask.id }),
    });
  }

  function taskState(assignmentId: string): TaskState | undefined {
    return states.get(assignmentId);
  }

  function taskStates(): Record<string, TaskState> {
    return Object.fromEntries(states);
  }

  function taskEvents(assignmentId: string, query: TaskEventQuery = {}): TaskEvent[] {
    const buffer = eventStreams.get(assignmentId);
    if (buffer === undefined) return [];
    const limit = positiveIntegerOr(query.limit, DEFAULT_TASK_EVENT_LIMIT);
    const afterSeq = query.afterSeq;
    let filtered = buffer.events;
    if (afterSeq !== undefined && Number.isFinite(afterSeq)) {
      filtered = filtered.filter((event) => event.seq > afterSeq);
    }
    if (query.type !== undefined) {
      filtered = filtered.filter((event) => event.type === query.type);
    }
    // No cursor means "tail": the newest `limit` events. A cursor means a
    // forward page after that sequence, which is what pagination needs.
    const start = afterSeq === undefined ? Math.max(0, filtered.length - limit) : 0;
    return filtered.slice(start, start + limit).map(cloneTaskEvent);
  }

  function taskEventCount(assignmentId: string): number {
    return eventStreams.get(assignmentId)?.events.length ?? 0;
  }

  function taskEventDropped(assignmentId: string): number {
    return eventStreams.get(assignmentId)?.dropped ?? 0;
  }

  /**
   * Send one session request and resolve its correlated reply. The request
   * envelope id is the requestId the Worker must echo as `correlationId`.
   */
  function requestSessionReply<T>(
    type: string,
    data: Record<string, unknown>,
    requestedTimeoutMs: number | undefined,
    select: (payload: Record<string, unknown> | undefined) => T,
  ): Promise<T> {
    if (closed) {
      return Promise.reject(
        new MailboxLaunchError("MAILBOX_CLOSED", `${type} request after the mailbox bridge closed`),
      );
    }
    const requestTimeoutMs = positiveTimeout(requestedTimeoutMs ?? maxSessionListTimeoutMs);
    const request = newEnvelope({
      source: "master:router",
      type,
      time: nowIso(),
      data,
      to: workerMailbox,
      replyTo: masterMailbox,
    });
    const requestId = request.id;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        sessionRequests.delete(requestId);
        reject(
          new MailboxLaunchError(
            "MAILBOX_STATUS_TIMEOUT",
            `${type} request ${requestId} timed out after ${requestTimeoutMs}ms`,
          ),
        );
      }, requestTimeoutMs);
      timer.unref();
      sessionRequests.set(requestId, {
        resolve: (payload) => resolve(select(payload)),
        reject,
        timer,
      });
      try {
        client.send(request);
      } catch (error) {
        sessionRequests.delete(requestId);
        clearTimeout(timer);
        reject(
          new MailboxLaunchError(
            "MAILBOX_SEND_FAILED",
            `${type} request failed: ${messageOf(error)}`,
          ),
        );
      }
    });
  }

  async function listSessions(
    sessionOptions: SessionListOptions = {},
  ): Promise<HarnessSessionSummary[]> {
    return requestSessionReply(MESSAGE_TYPES.sessionList, {}, sessionOptions.timeoutMs, (payload) =>
      parseSessionList(payload),
    );
  }

  async function sessionHistory(
    sessionId: string,
    sessionOptions: SessionHistoryOptions = {},
  ): Promise<HarnessSessionHistory> {
    const wanted = typeof sessionId === "string" ? sessionId.trim() : "";
    if (wanted.length === 0) {
      return Promise.reject(new Error("bridge-mailbox: sessionId must be a non-empty string"));
    }
    const data: Record<string, unknown> = { sessionId: wanted };
    if (sessionOptions.limit !== undefined) {
      data.limit = positiveInteger(sessionOptions.limit, "limit");
    }
    if (sessionOptions.afterSeq !== undefined) {
      data.afterSeq = nonNegativeInteger(sessionOptions.afterSeq, "afterSeq");
    }
    return requestSessionReply(
      MESSAGE_TYPES.sessionHistory,
      data,
      sessionOptions.timeoutMs,
      (payload) => parseSessionHistory(payload, wanted),
    );
  }

  function requestStatus(
    assignmentId: string,
    requestOptions: { timeoutMs?: number } = {},
  ): Promise<TaskState | null> {
    if (closed) {
      return Promise.reject(
        new MailboxLaunchError(
          "MAILBOX_CLOSED",
          `assignment ${assignmentId}: status request after the mailbox bridge closed`,
        ),
      );
    }
    const requestTimeoutMs = positiveTimeout(requestOptions.timeoutMs ?? DEFAULT_STATUS_TIMEOUT_MS);
    return new Promise<TaskState | null>((resolve, reject) => {
      const request = newEnvelope({
        source: "master:router",
        type: MESSAGE_TYPES.status,
        subject: assignmentId,
        time: nowIso(),
        data: { assignmentId },
        to: workerMailbox,
        correlationId: assignmentId,
        replyTo: masterMailbox,
      });
      const requestId = request.id;
      const timer = setTimeout(() => {
        statusRequests.delete(requestId);
        reject(
          new MailboxLaunchError(
            "MAILBOX_STATUS_TIMEOUT",
            `assignment ${assignmentId} status request ${requestId} timed out after ${requestTimeoutMs}ms`,
          ),
        );
      }, requestTimeoutMs);
      timer.unref();
      statusRequests.set(requestId, { assignmentId, resolve, reject, timer });
      try {
        client.send(request);
      } catch (error) {
        statusRequests.delete(requestId);
        clearTimeout(timer);
        reject(
          new MailboxLaunchError(
            "MAILBOX_SEND_FAILED",
            `assignment ${assignmentId} status request failed: ${messageOf(error)}`,
          ),
        );
      }
    });
  }

  async function steer(assignmentId: string, text: string): Promise<void> {
    if (closed) {
      throw new MailboxLaunchError(
        "MAILBOX_CLOSED",
        `assignment ${assignmentId}: steer after the mailbox bridge closed`,
      );
    }
    if (typeof text !== "string" || text.trim().length === 0) {
      throw new Error("bridge-mailbox: steer text must be a non-empty string");
    }
    try {
      client.send(
        newEnvelope({
          source: "master:router",
          type: MESSAGE_TYPES.steer,
          subject: assignmentId,
          time: nowIso(),
          data: { assignmentId, text },
          to: workerMailbox,
          correlationId: assignmentId,
        }),
      );
    } catch (error) {
      throw new MailboxLaunchError(
        "MAILBOX_SEND_FAILED",
        `assignment ${assignmentId} steer failed: ${messageOf(error)}`,
      );
    }
  }

  async function cancel(assignmentId: string, reason?: string): Promise<void> {
    if (closed) {
      throw new MailboxLaunchError(
        "MAILBOX_CLOSED",
        `assignment ${assignmentId}: cancel after the mailbox bridge closed`,
      );
    }
    try {
      client.send(
        newEnvelope({
          source: "master:router",
          type: MESSAGE_TYPES.cancel,
          subject: assignmentId,
          time: nowIso(),
          data: {
            assignmentId,
            ...(reason === undefined ? {} : { reason }),
          },
          to: workerMailbox,
          correlationId: assignmentId,
        }),
      );
    } catch (error) {
      throw new MailboxLaunchError(
        "MAILBOX_SEND_FAILED",
        `assignment ${assignmentId} cancel failed: ${messageOf(error)}`,
      );
    }
  }

  /**
   * Answer the escalation that parked an assignment for a person. The launch
   * promise is already settled, so this only delivers the answer and updates
   * the record; the Worker's eventual result arrives through `onLateResult`.
   */
  function answer(
    assignmentId: string,
    resolution: EscalationResolutionInput,
    answerOptions: { escalationId?: string } = {},
  ): EscalationRecord {
    if (closed) {
      throw new MailboxLaunchError(
        "MAILBOX_CLOSED",
        `assignment ${assignmentId}: answer after the mailbox bridge closed`,
      );
    }
    const pending = pendingByAssignment.get(assignmentId);
    if (pending === undefined) {
      throw new MailboxLaunchError(
        "MAILBOX_UNKNOWN_ASSIGNMENT",
        `no pending assignment ${assignmentId} to answer`,
      );
    }
    const parked = pending.parkedEscalation;
    if (parked === undefined) {
      throw new MailboxLaunchError(
        "MAILBOX_NO_PARKED_ESCALATION",
        `assignment ${assignmentId} has no parked escalation`,
      );
    }
    const wanted = answerOptions.escalationId;
    if (wanted !== undefined && wanted !== parked.id) {
      throw new MailboxLaunchError(
        "MAILBOX_NO_PARKED_ESCALATION",
        `assignment ${assignmentId} has no parked escalation ${wanted} (parked: ${parked.id})`,
      );
    }
    validateResolutionInput(resolution);

    const at = nowIso();
    try {
      client.send(answerEnvelope(pending, parked, resolution, pending.parkedAsk, at));
    } catch (error) {
      throw new MailboxLaunchError(
        "MAILBOX_SEND_FAILED",
        `assignment ${assignmentId} answer failed: ${messageOf(error)}`,
      );
    }

    parked.status = resolution.kind === "answer" ? "resolved" : "parked";
    parked.resolution = {
      kind: resolution.kind,
      text: resolution.text,
      ...(resolution.option === undefined ? {} : { option: resolution.option }),
      at,
    };
    notify(() => options.onResolution?.(parked));
    delete pending.parkedEscalation;
    delete pending.parkedAsk;
    return parked;
  }

  if (options.autoStart ?? true) start();

  return {
    launchPort: { launch },
    workerMailbox,
    masterMailbox,
    handleEnvelope,
    pollOnce,
    taskState,
    taskStates,
    taskEvents,
    taskEventCount,
    taskEventDropped,
    listSessions,
    sessionHistory,
    requestStatus,
    steer,
    cancel,
    answer,
    close,
  };
}

// ---------------------------------------------------------------- validation

/** Validate a `gethen.session.list.result` payload into session summaries. */
function parseSessionList(data: Record<string, unknown> | undefined): HarnessSessionSummary[] {
  const sessions = data?.sessions;
  if (!Array.isArray(sessions)) return [];
  const result: HarnessSessionSummary[] = [];
  for (const entry of sessions) {
    const summary = parseSessionSummary(entry);
    if (summary !== undefined) result.push(summary);
  }
  return result;
}

function parseSessionSummary(value: unknown): HarnessSessionSummary | undefined {
  const record = asRecord(value);
  if (record === undefined) return undefined;
  const sessionId = stringValue(record.sessionId);
  if (sessionId === undefined) return undefined;

  const summary: HarnessSessionSummary = { sessionId };
  const key = stringValue(record.key);
  if (key !== undefined) summary.key = key;
  const workspace = stringValue(record.workspace);
  if (workspace !== undefined) summary.workspace = workspace;
  const lastUsedAt = stringValue(record.lastUsedAt);
  if (lastUsedAt !== undefined) summary.lastUsedAt = lastUsedAt;
  const activeAssignmentId = stringValue(record.activeAssignmentId);
  if (activeAssignmentId !== undefined) summary.activeAssignmentId = activeAssignmentId;
  const config = asRecord(record.config);
  if (config !== undefined) summary.config = config as unknown as JsonObject;
  const state = record.state;
  if (state === "running" || state === "idle" || state === "stale" || state === "unknown") {
    summary.state = state;
  }
  const lastActivityAt = stringValue(record.lastActivityAt);
  if (lastActivityAt !== undefined) summary.lastActivityAt = lastActivityAt;
  const eventCount = record.eventCount;
  if (typeof eventCount === "number" && Number.isInteger(eventCount) && eventCount >= 0) {
    summary.eventCount = eventCount;
  }
  const tokenUsage = asRecord(record.tokenUsage);
  if (tokenUsage !== undefined) summary.tokenUsage = tokenUsage as unknown as JsonObject;
  if (typeof record.hasLog === "boolean") summary.hasLog = record.hasLog;
  return summary;
}

/** Validate a `gethen.session.history.result` payload into a history snapshot. */
function parseSessionHistory(
  data: Record<string, unknown> | undefined,
  requestedSessionId: string,
): HarnessSessionHistory {
  const sessionId = stringValue(data?.sessionId) ?? requestedSessionId;
  const rawEvents = data?.events;
  const events: HarnessSessionEvent[] = [];
  if (Array.isArray(rawEvents)) {
    for (const entry of rawEvents) {
      const event = parseSessionEvent(entry);
      if (event !== undefined) events.push(event);
    }
  }
  const history: HarnessSessionHistory = { sessionId, events };
  const at = stringValue(data?.at);
  if (at !== undefined) history.at = at;
  return history;
}

function parseSessionEvent(value: unknown): HarnessSessionEvent | undefined {
  const record = asRecord(value);
  if (record === undefined) return undefined;
  const seq = record.seq;
  if (typeof seq !== "number" || !Number.isInteger(seq) || seq < 0) return undefined;
  const type = stringValue(record.type);
  if (type === undefined) return undefined;
  const at = stringValue(record.at);
  if (at === undefined) return undefined;
  const event: HarnessSessionEvent = { seq, type, at };
  if (record.data !== undefined) event.data = record.data as JsonValue;
  return event;
}

/** Validate one `data.event` from a progress envelope. */
function parseTaskEvent(
  value: unknown,
): { type: string; at: string; data?: JsonObject } | undefined {
  const record = asRecord(value);
  if (record === undefined) return undefined;
  const type = stringValue(record.type);
  const at = stringValue(record.at);
  if (type === undefined || at === undefined) return undefined;
  const rawData = record.data;
  if (rawData === undefined) return { type, at };
  const data = asRecord(rawData);
  if (data === undefined) return undefined;
  return { type, at, data: data as unknown as JsonObject };
}

/** Build a cached event, truncating oversized payloads into `{preview}`. */
function buildTaskEvent(
  seq: number,
  input: { type: string; at: string; data?: JsonObject },
  byteLimit: number,
): TaskEvent {
  const event: TaskEvent = {
    seq,
    type: input.type,
    at: input.at,
    ...(input.data === undefined ? {} : { data: input.data }),
  };
  if (utf8Length(safeJson(event)) <= byteLimit) return event;

  const serialized = input.data === undefined ? "" : safeJson(input.data);
  let budget = Math.max(16, Math.floor(byteLimit / 2));
  let preview = truncateText(serialized, budget);
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const candidate: TaskEvent = {
      seq,
      type: input.type,
      at: input.at,
      data: { preview },
      truncated: true,
    };
    if (utf8Length(safeJson(candidate)) <= byteLimit || preview.length === 0) {
      return candidate;
    }
    budget = Math.floor(budget / 2);
    preview = truncateText(serialized, budget);
  }
  return { seq, type: input.type, at: input.at, data: { preview: "" }, truncated: true };
}

function cloneTaskEvent(event: TaskEvent): TaskEvent {
  return {
    ...event,
    ...(event.data === undefined ? {} : { data: { ...event.data } }),
  };
}

function safeJson(value: unknown): string {
  try {
    const json = JSON.stringify(value);
    return json === undefined ? "" : json;
  } catch {
    return "";
  }
}

function utf8Length(text: string): number {
  return new TextEncoder().encode(text).length;
}

/** Truncate a string so its UTF-8 encoding fits in `maxBytes`. */
function truncateText(text: string, maxBytes: number): string {
  const encoder = new TextEncoder();
  if (encoder.encode(text).length <= maxBytes) return text;
  let low = 0;
  let high = text.length;
  while (low < high) {
    const mid = Math.ceil((low + high) / 2);
    if (encoder.encode(text.slice(0, mid)).length <= maxBytes) low = mid;
    else high = mid - 1;
  }
  return text.slice(0, low);
}

function integerValue(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : undefined;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`bridge-mailbox: ${name} must be a positive integer`);
  }
  return value;
}

function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`bridge-mailbox: ${name} must be a non-negative integer`);
  }
  return value;
}

function positiveIntegerOr(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isInteger(value) && value > 0 ? value : fallback;
}

/** Validate one worker-reported live state; `undefined` means malformed/absent. */
function parseTaskState(value: unknown, receivedAt: string): TaskState | undefined {
  const record = asRecord(value);
  if (record === undefined) return undefined;
  const phase = record.phase;
  if (typeof phase !== "string" || !isTaskPhase(phase)) return undefined;
  const step = record.step;
  if (typeof step !== "number" || !Number.isFinite(step) || step < 0) return undefined;
  const seq = record.seq;
  if (typeof seq !== "number" || !Number.isInteger(seq) || seq < 0) return undefined;
  const lastEventAt = stringValue(record.lastEventAt);
  if (lastEventAt === undefined) return undefined;

  const state: TaskState = { phase, step, lastEventAt, seq, updatedAt: receivedAt };
  const lastTool = parseToolState(record.lastTool);
  if (lastTool !== undefined) state.lastTool = lastTool;
  const lastMessage = stringValue(record.lastMessage);
  if (lastMessage !== undefined) state.lastMessage = lastMessage;
  return state;
}

function parseToolState(value: unknown): TaskToolState | undefined {
  const record = asRecord(value);
  if (record === undefined) return undefined;
  const name = stringValue(record.name);
  const startedAt = stringValue(record.startedAt);
  if (name === undefined || startedAt === undefined) return undefined;
  return { name, startedAt };
}

/** `gethen.task.status.result` payload: the state, or null when unavailable. */
function parseStatusResult(
  data: Record<string, unknown> | undefined,
  receivedAt: string,
): TaskState | null {
  if (data === undefined) return null;
  const raw = data.state;
  if (raw === undefined || raw === null) return null;
  return parseTaskState(raw, receivedAt) ?? null;
}

function isTaskPhase(value: string): value is TaskPhase {
  return (TASK_PHASES as readonly string[]).includes(value);
}

/** Validate an escalation answer before sending it to the Worker. */
function validateResolutionInput(resolution: EscalationResolutionInput): void {
  const record = asRecord(resolution);
  if (record === undefined) {
    throw new MailboxLaunchError("MAILBOX_INVALID_RESOLUTION", "resolution must be a JSON object");
  }
  const kind = record.kind;
  if (kind !== "answer" && kind !== "escalate" && kind !== "drop") {
    throw new MailboxLaunchError(
      "MAILBOX_INVALID_RESOLUTION",
      'resolution "kind" must be "answer", "escalate" or "drop"',
    );
  }
  const text = record.text;
  if (typeof text !== "string" || text.trim().length === 0) {
    throw new MailboxLaunchError(
      "MAILBOX_INVALID_RESOLUTION",
      'resolution "text" must be a non-empty string',
    );
  }
  const option = record.option;
  if (option !== undefined && (typeof option !== "string" || option.length === 0)) {
    throw new MailboxLaunchError(
      "MAILBOX_INVALID_RESOLUTION",
      'resolution "option" must be a non-empty string when present',
    );
  }
}

type TaskResultParse = { ok: true; report: Report } | { ok: false; reasons: string[] };

/** Validate a `gethen.task.result` payload (the harness report) into a Report. */
function parseTaskResult(data: Record<string, unknown> | undefined, at: string): TaskResultParse {
  if (data === undefined) return { ok: false, reasons: ["result data must be a JSON object"] };
  const reasons: string[] = [];
  const kind = data.kind;
  if (typeof kind !== "string" || !isReportKind(kind)) {
    reasons.push(`"kind" must be one of: ${REPORT_KINDS.join(", ")}`);
  }
  const summary = data.summary;
  if (typeof summary !== "string" || summary.trim().length === 0) {
    reasons.push('"summary" must be a non-empty string');
  }
  const question = data.question;
  if (question !== undefined && (typeof question !== "string" || question.length === 0)) {
    reasons.push('"question" must be a non-empty string when present');
  }
  if (reasons.length > 0 || typeof kind !== "string" || typeof summary !== "string") {
    return { ok: false, reasons };
  }
  const report: Report = { kind: kind as ReportKind, summary, at };
  if (question !== undefined && typeof question === "string") report.question = question;
  return { ok: true, report };
}

function parseOptions(value: unknown): EscalationOptionLike[] | undefined {
  if (value === undefined) return [];
  if (!Array.isArray(value)) return undefined;
  const options: EscalationOptionLike[] = [];
  for (const entry of value) {
    if (!isRecord(entry)) return undefined;
    const id = entry.id;
    const label = entry.label;
    if (typeof id !== "string" || id.length === 0) return undefined;
    if (typeof label !== "string" || label.length === 0) return undefined;
    options.push({ id, label });
  }
  return options;
}

function assignmentIdOf(
  data: Record<string, unknown> | undefined,
  envelope: Envelope,
): string | undefined {
  const fromData = stringValue(data?.assignmentId);
  if (fromData !== undefined) return fromData;
  return stringValue(envelope.correlationId);
}

function isReportKind(value: string): value is ReportKind {
  return (REPORT_KINDS as readonly string[]).includes(value);
}

function isEscalationKind(value: string): value is EscalationKindLike {
  return (ESCALATION_KINDS as readonly string[]).includes(value);
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function optionalStringValue(value: unknown): string | undefined {
  return stringValue(value);
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return asRecord(value) !== undefined;
}

function positiveTimeout(value: number, name = "timeoutMs"): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`bridge-mailbox: ${name} must be a positive finite number`);
  }
  return value;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

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
 * - in  `gethen.task.progress` → counts events for the eventual receipt;
 * - in  `gethen.task.ask`      → escalation record + resolver → answer;
 * - out `gethen.task.answer`   → the ask's `replyTo` (worker mailbox);
 * - in  `gethen.task.result`   → report + receipt (LaunchResult).
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

/** Application message types carried by the mailbox (mirrors `@gethen/agent`). */
export const MESSAGE_TYPES = {
  assign: "gethen.task.assign",
  progress: "gethen.task.progress",
  ask: "gethen.task.ask",
  answer: "gethen.task.answer",
  result: "gethen.task.result",
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
  | "MAILBOX_CLOSED";

/** The same codes as a runtime list, for callers that classify failures. */
export const MAILBOX_LAUNCH_ERROR_CODES: readonly MailboxLaunchErrorCode[] = [
  "MAILBOX_LAUNCH_TIMEOUT",
  "MAILBOX_DISPATCH_FAILED",
  "MAILBOX_RESOLUTION_FAILED",
  "MAILBOX_CLOSED",
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
  /** Stop polling and reject in-flight launches with `MAILBOX_CLOSED`. */
  close(): void;
}

interface PendingLaunch {
  readonly promise: Promise<LaunchResult>;
  readonly request: LaunchRequest;
  readonly escalations: EscalationRecord[];
  readonly seenEscalations: Set<string>;
  events: number;
  settled: boolean;
  timer: ReturnType<typeof setTimeout> | undefined;
  resolve(result: LaunchResult): void;
  reject(error: Error): void;
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
  const pendingByAssignment = new Map<string, PendingLaunch>();
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

  function park(pending: PendingLaunch, record: EscalationRecord): void {
    record.status = "parked";
    notify(() => options.onResolution?.(record));
    const at = nowIso();
    const report: Report = {
      kind: "waiting",
      summary: `waiting for a person: ${record.question}`,
      question: record.question,
      at,
    };
    resolveWith(pending, resultFor(pending, report));
  }

  function handleResult(pending: PendingLaunch, envelope: Envelope): void {
    const data = asRecord(envelope.data);
    const parsed = parseTaskResult(data, nowIso());
    if (!parsed.ok) {
      resolveWith(
        pending,
        resultFor(pending, {
          kind: "failed",
          summary: `invalid task result: ${parsed.reasons.join("; ")}`,
          at: nowIso(),
        }),
      );
      return;
    }
    resolveWith(pending, resultFor(pending, parsed.report));
  }

  function handleProgress(pending: PendingLaunch): void {
    pending.events += 1;
  }

  function handleAsk(pending: PendingLaunch, envelope: Envelope): void {
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

    if (options.resolveEscalation === undefined) {
      park(pending, record);
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
      park(pending, record);
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
    pending.timer = setTimeout(() => {
      rejectWith(
        pending,
        new MailboxLaunchError(
          "MAILBOX_LAUNCH_TIMEOUT",
          `assignment ${assignmentId} produced no result within ${timeoutMs}ms`,
        ),
      );
    }, timeoutMs);
    // A pending launch must not keep the process alive on its own.
    pending.timer.unref();

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
        if (pending !== undefined) handleProgress(pending);
        return;
      }
      case MESSAGE_TYPES.ask: {
        const assignmentId = assignmentIdOf(data, envelope);
        const pending =
          assignmentId === undefined ? undefined : pendingByAssignment.get(assignmentId);
        if (pending !== undefined) handleAsk(pending, envelope);
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
      rejectWith(
        pending,
        new MailboxLaunchError(
          "MAILBOX_CLOSED",
          `assignment ${pending.request.assignment.id} was abandoned because the mailbox bridge closed`,
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
    ask: Envelope,
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
      to: ask.replyTo ?? workerMailbox,
      correlationId: pending.request.assignment.id,
      causationId: ask.id,
    });
  }

  if (options.autoStart ?? true) start();

  return {
    launchPort: { launch },
    workerMailbox,
    masterMailbox,
    handleEnvelope,
    pollOnce,
    close,
  };
}

// ---------------------------------------------------------------- validation

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

function positiveTimeout(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error("bridge-mailbox: timeoutMs must be a positive finite number");
  }
  return value;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

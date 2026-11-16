/**
 * CloudEvents-style envelope used by the L2 mailbox layer.
 *
 * The envelope carries both CloudEvents attributes (`specversion`, `id`,
 * `source`, `type`, `time`, `datacontenttype`, `data`) and Gethen routing /
 * threading extensions (`to`, `replyTo`, `correlationId`, `causationId`,
 * `thread`, `deadline`, `priority`, `ttlMs`). No transport or DSH types here.
 */
import { randomUUID } from "node:crypto";

export const SPEC_VERSION = "1.0" as const;
export const DEFAULT_DATACONTENTTYPE = "application/json" as const;

export interface Envelope<T = unknown> {
  /** CloudEvents spec version; always "1.0". */
  specversion: "1.0";
  /** Stable idempotency key for this message. */
  id: string;
  /** Producer identity, e.g. "agent:router-1" or "worker:build-1". */
  source: string;
  /** Application message type, e.g. "gethen.task.assign". */
  type: string;
  /** Ticket / assignment / task id this message is about. */
  subject?: string;
  /** RFC 3339 UTC timestamp. */
  time: string;
  datacontenttype: "application/json";
  data: T;
  /** Conversation / task thread. */
  thread?: string;
  /** Links a reply to its request. */
  correlationId?: string;
  /** The envelope that caused this one. */
  causationId?: string;
  /** Destination mailbox, e.g. "agent:worker-1", "topic:builds", "mailbox:human". */
  to?: string;
  /** Mailbox the reply should be delivered to. */
  replyTo?: string;
  /** RFC 3339 deadline; after it the message is no longer useful. */
  deadline?: string;
  /** 0 (lowest) .. 9 (highest); defaults to 0 when omitted. */
  priority?: number;
  /** Relative retention in milliseconds; must be positive. */
  ttlMs?: number;
}

/** Input of `newEnvelope`; missing `id` / `time` are generated. */
export interface NewEnvelopeInput<T = unknown> {
  id?: string;
  source: string;
  type: string;
  subject?: string;
  time?: string;
  data: T;
  datacontenttype?: "application/json";
  thread?: string;
  correlationId?: string;
  causationId?: string;
  to?: string;
  replyTo?: string;
  deadline?: string;
  priority?: number;
  ttlMs?: number;
}

export interface EnvelopeValidation {
  valid: boolean;
  errors: string[];
}

/** Thrown by `newEnvelope` / `assertValidEnvelope` for malformed input. */
export class EnvelopeError extends Error {
  readonly errors: string[];

  constructor(errors: string[]) {
    super(`invalid envelope: ${errors.join("; ")}`);
    this.name = "EnvelopeError";
    this.errors = errors;
  }
}

/**
 * Builds a validated envelope. `specversion` and `datacontenttype` are fixed,
 * `id` defaults to a UUID and `time` defaults to now; all other optional
 * attributes are copied only when provided.
 */
export function newEnvelope<T>(input: NewEnvelopeInput<T>): Envelope<T> {
  const envelope: Envelope<T> = {
    specversion: SPEC_VERSION,
    id: input.id ?? randomUUID(),
    source: input.source,
    type: input.type,
    time: input.time ?? new Date().toISOString(),
    datacontenttype: input.datacontenttype ?? DEFAULT_DATACONTENTTYPE,
    data: input.data,
  };
  if (input.subject !== undefined) envelope.subject = input.subject;
  if (input.thread !== undefined) envelope.thread = input.thread;
  if (input.correlationId !== undefined) envelope.correlationId = input.correlationId;
  if (input.causationId !== undefined) envelope.causationId = input.causationId;
  if (input.to !== undefined) envelope.to = input.to;
  if (input.replyTo !== undefined) envelope.replyTo = input.replyTo;
  if (input.deadline !== undefined) envelope.deadline = input.deadline;
  if (input.priority !== undefined) envelope.priority = input.priority;
  if (input.ttlMs !== undefined) envelope.ttlMs = input.ttlMs;
  assertValidEnvelope(envelope);
  return envelope;
}

/** Collects every validation problem instead of failing on the first one. */
export function validateEnvelope(value: unknown): EnvelopeValidation {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return { valid: false, errors: ["envelope must be an object"] };
  }
  const record = value as Record<string, unknown>;
  const errors: string[] = [];

  if (record.specversion !== SPEC_VERSION) errors.push(`specversion must be "${SPEC_VERSION}"`);
  if (!isNonEmptyString(record.id)) errors.push("id must be a non-empty string");
  if (!isNonEmptyString(record.source)) errors.push("source must be a non-empty string");
  if (!isNonEmptyString(record.type)) errors.push("type must be a non-empty string");
  if (!("data" in record)) errors.push("data is required");
  if (record.datacontenttype !== DEFAULT_DATACONTENTTYPE) {
    errors.push(`datacontenttype must be "${DEFAULT_DATACONTENTTYPE}"`);
  }
  if (!isRfc3339Timestamp(record.time)) errors.push("time must be an RFC 3339 timestamp");

  for (const field of ["subject", "thread", "correlationId", "causationId", "to", "replyTo"]) {
    const fieldValue = record[field];
    if (fieldValue !== undefined && typeof fieldValue !== "string") {
      errors.push(`${field} must be a string when present`);
    }
  }
  const deadline = record.deadline;
  if (deadline !== undefined && !isRfc3339Timestamp(deadline)) {
    errors.push("deadline must be an RFC 3339 timestamp when present");
  }
  const priority = record.priority;
  if (priority !== undefined) {
    if (
      typeof priority !== "number" ||
      !Number.isInteger(priority) ||
      priority < 0 ||
      priority > 9
    ) {
      errors.push("priority must be an integer between 0 and 9");
    }
  }
  const ttlMs = record.ttlMs;
  if (ttlMs !== undefined && (typeof ttlMs !== "number" || !Number.isFinite(ttlMs) || ttlMs <= 0)) {
    errors.push("ttlMs must be a positive finite number");
  }

  return { valid: errors.length === 0, errors };
}

export function isEnvelope(value: unknown): value is Envelope {
  return validateEnvelope(value).valid;
}

export function assertValidEnvelope(value: unknown): asserts value is Envelope {
  const { valid, errors } = validateEnvelope(value);
  if (!valid) throw new EnvelopeError(errors);
}

/**
 * RFC 3339 with a mandatory timezone. Parsing is delegated to `Date.parse`,
 * which also rejects impossible calendar dates for the ISO form.
 */
export function isRfc3339Timestamp(value: unknown): value is string {
  if (typeof value !== "string") return false;
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) {
    return false;
  }
  return Number.isFinite(Date.parse(value));
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim() !== "";
}

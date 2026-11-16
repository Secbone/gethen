/**
 * Wire protocol for `@gethen/mailbox-link`: L2 mailbox operations tunnelled
 * over one L1 {@link TransportSession}.
 *
 * Every frame is one JSON object `{v:1, id, op, payload?}`. A request and its
 * response share the request `id` (the response uses `op:"payload"` for success
 * and `op:"error"` for failure); `deliver` frames are unsolicited server pushes
 * with their own fresh id. `payload` is op-specific JSON and carries CloudEvents
 * {@link Envelope}s for `send`, `reply` and `deliver`.
 *
 * Validation never throws: decoders return a {@link ProtocolResult}. This
 * module knows transport and mailbox types but nothing about workers, DSH or
 * task semantics.
 */
import { type Envelope, isRfc3339Timestamp, validateEnvelope } from "@gethen/mailbox";
import type { TransportMessage, TransportPayload } from "@gethen/transport";

export const MAILBOX_LINK_VERSION = 1 as const;

export const MAILBOX_LINK_OPS = [
  "hello",
  "subscribe",
  "deliver",
  "payload",
  "ack",
  "nack",
  "renew",
  "send",
  "reply",
  "error",
  "ping",
] as const;

export type MailboxLinkOp = (typeof MAILBOX_LINK_OPS)[number];

/** Stable failure codes; server wire errors and client-local errors share them. */
export type MailboxLinkErrorCode =
  | "invalid_frame"
  | "unsupported_version"
  | "unknown_op"
  | "invalid_payload"
  | "invalid_envelope"
  | "not_allowed"
  | "too_many_requests"
  | "internal"
  | "timeout"
  | "disconnected"
  | "closed";

export class MailboxLinkError extends Error {
  readonly code: MailboxLinkErrorCode;

  constructor(code: MailboxLinkErrorCode, message: string, options: { cause?: unknown } = {}) {
    super(
      `${code}: ${message}`,
      options.cause === undefined ? undefined : { cause: options.cause },
    );
    this.name = "MailboxLinkError";
    this.code = code;
  }
}

export interface MailboxLinkFrame {
  v: typeof MAILBOX_LINK_VERSION;
  /** Frame id; also the correlation id for a request/response pair. */
  id: string;
  op: MailboxLinkOp;
  payload?: unknown;
}

export type ProtocolResult<T> =
  | { ok: true; value: T }
  | { ok: false; code: MailboxLinkErrorCode; message: string };

// ---------------------------------------------------------------------------
// Op payloads / results
// ---------------------------------------------------------------------------

export interface HelloPayload {
  protocol: number;
  /** Mailbox the client intends to subscribe to (informational). */
  mailbox?: string;
}

export interface HelloResult {
  protocol: typeof MAILBOX_LINK_VERSION;
  serverTime: string;
  mailbox?: string;
}

export interface SubscribePayload {
  mailbox: string;
  /** Durable subscription name (informational for the remote link). */
  name?: string;
  batchSize?: number;
  visibilityTimeoutMs?: number;
}

export interface SubscribeResult {
  mailbox: string;
  subscription: string;
  batchSize: number;
  visibilityTimeoutMs: number;
}

export interface WireDeliveredMessage {
  envelope: Envelope;
  attempts: number;
  leaseUntil: string;
}

export interface DeliverPayload {
  mailbox: string;
  messages: WireDeliveredMessage[];
}

export interface AckPayload {
  mailbox: string;
  ids: string[];
}

export interface AckResult {
  acked: number;
}

export interface NackPayload {
  mailbox: string;
  ids: string[];
  action?: "requeue" | "dead";
  delayMs?: number;
}

export interface NackResult {
  nacked: number;
}

export interface RenewPayload {
  mailbox: string;
  ids: string[];
  visibilityTimeoutMs?: number;
}

export interface RenewResult {
  renewed: number;
}

export interface SendPayload {
  envelope: Envelope;
}

export interface ReplyPayload {
  request: Envelope;
  envelope: Envelope;
}

/** Result of `send` / `reply`: where the envelope was routed. */
export interface RouteResult {
  mailbox: string;
  duplicate: boolean;
}

export interface ErrorPayload {
  code: MailboxLinkErrorCode;
  message: string;
}

export interface PingPayload {
  at?: string;
}

export interface PingResult {
  at: string;
}

// ---------------------------------------------------------------------------
// Encode / decode
// ---------------------------------------------------------------------------

export function createMailboxFrame(
  op: MailboxLinkOp,
  id: string,
  payload?: unknown,
): MailboxLinkFrame {
  return payload === undefined
    ? { v: MAILBOX_LINK_VERSION, id, op }
    : { v: MAILBOX_LINK_VERSION, id, op, payload };
}

export function encodeMailboxFrame(frame: MailboxLinkFrame): string {
  return JSON.stringify(frame);
}

/** Wrap a frame as an L1 text message (`application/json`). */
export function toTransportMessage(
  frame: MailboxLinkFrame,
  at: string = new Date().toISOString(),
): TransportMessage {
  return { id: frame.id, payload: encodeMailboxFrame(frame), contentType: "application/json", at };
}

export function decodeMailboxFrame(payload: TransportPayload): ProtocolResult<MailboxLinkFrame> {
  let value: unknown;
  if (typeof payload === "string") {
    try {
      value = JSON.parse(payload);
    } catch {
      return fail("invalid_frame", "frame payload is not valid JSON");
    }
  } else {
    try {
      value = JSON.parse(new TextDecoder().decode(payload));
    } catch {
      return fail("invalid_frame", "frame payload is not valid JSON");
    }
  }
  return parseMailboxFrame(value);
}

/** Validate the envelope of a frame (not its op payload). */
export function parseMailboxFrame(value: unknown): ProtocolResult<MailboxLinkFrame> {
  if (!isRecord(value)) return fail("invalid_frame", "frame must be a JSON object");
  if (value.v !== MAILBOX_LINK_VERSION) {
    return fail("unsupported_version", `frame "v" must be ${MAILBOX_LINK_VERSION}`);
  }
  if (typeof value.id !== "string" || value.id.length === 0) {
    return fail("invalid_frame", 'frame "id" must be a non-empty string');
  }
  if (typeof value.op !== "string" || !isMailboxLinkOp(value.op)) {
    return fail("unknown_op", `unknown mailbox link op "${String(value.op)}"`);
  }
  return ok({
    v: MAILBOX_LINK_VERSION,
    id: value.id,
    op: value.op,
    ...("payload" in value ? { payload: value.payload } : {}),
  });
}

// ---------------------------------------------------------------------------
// Op payload parsers
// ---------------------------------------------------------------------------

export function parseHelloPayload(value: unknown): ProtocolResult<HelloPayload> {
  const object = parseRecord(value, "hello payload");
  if (!object.ok) return object;
  const protocol = object.value.protocol;
  if (protocol !== MAILBOX_LINK_VERSION) {
    return fail("unsupported_version", `hello "protocol" must be ${MAILBOX_LINK_VERSION}`);
  }
  const mailbox = optionalStringField(object.value, "mailbox", "hello.mailbox");
  if (!mailbox.ok) return mailbox;
  return ok({
    protocol: MAILBOX_LINK_VERSION,
    ...(mailbox.value === undefined ? {} : { mailbox: mailbox.value }),
  });
}

export function parseSubscribePayload(value: unknown): ProtocolResult<SubscribePayload> {
  const object = parseRecord(value, "subscribe payload");
  if (!object.ok) return object;
  const mailbox = requiredStringField(object.value, "mailbox", "subscribe.mailbox");
  if (!mailbox.ok) return mailbox;
  const name = optionalStringField(object.value, "name", "subscribe.name");
  if (!name.ok) return name;
  const batchSize = optionalPositiveInteger(object.value, "batchSize", "subscribe.batchSize");
  if (!batchSize.ok) return batchSize;
  const visibilityTimeoutMs = optionalPositiveNumber(
    object.value,
    "visibilityTimeoutMs",
    "subscribe.visibilityTimeoutMs",
  );
  if (!visibilityTimeoutMs.ok) return visibilityTimeoutMs;
  return ok({
    mailbox: mailbox.value,
    ...(name.value === undefined ? {} : { name: name.value }),
    ...(batchSize.value === undefined ? {} : { batchSize: batchSize.value }),
    ...(visibilityTimeoutMs.value === undefined
      ? {}
      : { visibilityTimeoutMs: visibilityTimeoutMs.value }),
  });
}

export function parseDeliverPayload(value: unknown): ProtocolResult<DeliverPayload> {
  const object = parseRecord(value, "deliver payload");
  if (!object.ok) return object;
  const mailbox = requiredStringField(object.value, "mailbox", "deliver.mailbox");
  if (!mailbox.ok) return mailbox;
  const rawMessages = object.value.messages;
  if (!Array.isArray(rawMessages) || rawMessages.length === 0) {
    return fail("invalid_payload", "deliver.messages must be a non-empty array");
  }
  const messages: WireDeliveredMessage[] = [];
  for (const [index, raw] of rawMessages.entries()) {
    const message = parseWireMessage(raw, `deliver.messages[${index}]`);
    if (!message.ok) return message;
    messages.push(message.value);
  }
  return ok({ mailbox: mailbox.value, messages });
}

export function parseAckPayload(value: unknown): ProtocolResult<AckPayload> {
  const object = parseRecord(value, "ack payload");
  if (!object.ok) return object;
  const mailbox = requiredStringField(object.value, "mailbox", "ack.mailbox");
  if (!mailbox.ok) return mailbox;
  const ids = requiredIdArray(object.value, "ids", "ack.ids");
  if (!ids.ok) return ids;
  return ok({ mailbox: mailbox.value, ids: ids.value });
}

export function parseNackPayload(value: unknown): ProtocolResult<NackPayload> {
  const base = parseAckPayload(value);
  if (!base.ok) return base;
  const object = parseRecord(value, "nack payload");
  if (!object.ok) return object;
  const action = object.value.action;
  if (action !== undefined && action !== "requeue" && action !== "dead") {
    return fail("invalid_payload", 'nack.action must be "requeue" or "dead"');
  }
  const delayMs = optionalNonNegativeNumber(object.value, "delayMs", "nack.delayMs");
  if (!delayMs.ok) return delayMs;
  return ok({
    mailbox: base.value.mailbox,
    ids: base.value.ids,
    ...(action === undefined ? {} : { action }),
    ...(delayMs.value === undefined ? {} : { delayMs: delayMs.value }),
  });
}

export function parseRenewPayload(value: unknown): ProtocolResult<RenewPayload> {
  const base = parseAckPayload(value);
  if (!base.ok) return base;
  const object = parseRecord(value, "renew payload");
  if (!object.ok) return object;
  const visibilityTimeoutMs = optionalPositiveNumber(
    object.value,
    "visibilityTimeoutMs",
    "renew.visibilityTimeoutMs",
  );
  if (!visibilityTimeoutMs.ok) return visibilityTimeoutMs;
  return ok({
    mailbox: base.value.mailbox,
    ids: base.value.ids,
    ...(visibilityTimeoutMs.value === undefined
      ? {}
      : { visibilityTimeoutMs: visibilityTimeoutMs.value }),
  });
}

export function parseSendPayload(value: unknown): ProtocolResult<SendPayload> {
  const object = parseRecord(value, "send payload");
  if (!object.ok) return object;
  const envelope = parseEnvelopeValue(object.value.envelope, "send.envelope");
  if (!envelope.ok) return envelope;
  return ok({ envelope: envelope.value });
}

export function parseReplyPayload(value: unknown): ProtocolResult<ReplyPayload> {
  const object = parseRecord(value, "reply payload");
  if (!object.ok) return object;
  const request = parseEnvelopeValue(object.value.request, "reply.request");
  if (!request.ok) return request;
  const envelope = parseEnvelopeValue(object.value.envelope, "reply.envelope");
  if (!envelope.ok) return envelope;
  return ok({ request: request.value, envelope: envelope.value });
}

export function parseErrorPayload(value: unknown): ProtocolResult<ErrorPayload> {
  const object = parseRecord(value, "error payload");
  if (!object.ok) return object;
  const code = requiredStringField(object.value, "code", "error.code");
  if (!code.ok) return code;
  const message = requiredStringField(object.value, "message", "error.message");
  if (!message.ok) return message;
  return ok({ code: code.value as MailboxLinkErrorCode, message: message.value });
}

export function parsePingPayload(value: unknown): ProtocolResult<PingPayload> {
  if (value === undefined) return ok({});
  const object = parseRecord(value, "ping payload");
  if (!object.ok) return object;
  const at = object.value.at;
  if (at !== undefined && !isRfc3339Timestamp(at)) {
    return fail("invalid_payload", "ping.at must be an RFC 3339 timestamp");
  }
  return ok(at === undefined ? {} : { at });
}

// ---------------------------------------------------------------------------
// Typed response parsers used by the client
// ---------------------------------------------------------------------------

export function parseHelloResult(value: unknown): ProtocolResult<HelloResult> {
  const object = parseRecord(value, "hello result");
  if (!object.ok) return object;
  if (object.value.protocol !== MAILBOX_LINK_VERSION) {
    return fail("invalid_payload", `hello result "protocol" must be ${MAILBOX_LINK_VERSION}`);
  }
  const serverTime = object.value.serverTime;
  if (!isRfc3339Timestamp(serverTime)) {
    return fail("invalid_payload", "hello result serverTime must be an RFC 3339 timestamp");
  }
  const mailbox = optionalStringField(object.value, "mailbox", "hello result mailbox");
  if (!mailbox.ok) return mailbox;
  return ok({
    protocol: MAILBOX_LINK_VERSION,
    serverTime,
    ...(mailbox.value === undefined ? {} : { mailbox: mailbox.value }),
  });
}

export function parseSubscribeResult(value: unknown): ProtocolResult<SubscribeResult> {
  const object = parseRecord(value, "subscribe result");
  if (!object.ok) return object;
  const mailbox = requiredStringField(object.value, "mailbox", "subscribe result mailbox");
  if (!mailbox.ok) return mailbox;
  const subscription = requiredStringField(
    object.value,
    "subscription",
    "subscribe result subscription",
  );
  if (!subscription.ok) return subscription;
  const batchSize = optionalPositiveInteger(
    object.value,
    "batchSize",
    "subscribe result batchSize",
  );
  if (!batchSize.ok) return batchSize;
  const visibilityTimeoutMs = optionalPositiveNumber(
    object.value,
    "visibilityTimeoutMs",
    "subscribe result visibilityTimeoutMs",
  );
  if (!visibilityTimeoutMs.ok) return visibilityTimeoutMs;
  return ok({
    mailbox: mailbox.value,
    subscription: subscription.value,
    batchSize: batchSize.value ?? 0,
    visibilityTimeoutMs: visibilityTimeoutMs.value ?? 0,
  });
}

export function parseCountResult(
  value: unknown,
  field: "acked" | "nacked" | "renewed",
): ProtocolResult<number> {
  const object = parseRecord(value, `${field} result`);
  if (!object.ok) return object;
  const count = object.value[field];
  if (typeof count !== "number" || !Number.isInteger(count) || count < 0) {
    return fail("invalid_payload", `${field} result "${field}" must be a non-negative integer`);
  }
  return ok(count);
}

export function parseRouteResult(value: unknown): ProtocolResult<RouteResult> {
  const object = parseRecord(value, "route result");
  if (!object.ok) return object;
  const mailbox = requiredStringField(object.value, "mailbox", "route result mailbox");
  if (!mailbox.ok) return mailbox;
  if (typeof object.value.duplicate !== "boolean") {
    return fail("invalid_payload", 'route result "duplicate" must be a boolean');
  }
  return ok({ mailbox: mailbox.value, duplicate: object.value.duplicate });
}

export function parsePingResult(value: unknown): ProtocolResult<PingResult> {
  const object = parseRecord(value, "ping result");
  if (!object.ok) return object;
  const at = object.value.at;
  if (!isRfc3339Timestamp(at)) {
    return fail("invalid_payload", "ping result at must be an RFC 3339 timestamp");
  }
  return ok({ at });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseWireMessage(value: unknown, path: string): ProtocolResult<WireDeliveredMessage> {
  const object = parseRecord(value, path);
  if (!object.ok) return object;
  const envelope = parseEnvelopeValue(object.value.envelope, `${path}.envelope`);
  if (!envelope.ok) return envelope;
  const attempts = object.value.attempts;
  if (typeof attempts !== "number" || !Number.isInteger(attempts) || attempts < 1) {
    return fail("invalid_payload", `${path}.attempts must be a positive integer`);
  }
  const leaseUntil = object.value.leaseUntil;
  if (!isRfc3339Timestamp(leaseUntil)) {
    return fail("invalid_payload", `${path}.leaseUntil must be an RFC 3339 timestamp`);
  }
  return ok({ envelope: envelope.value, attempts, leaseUntil });
}

function parseEnvelopeValue(value: unknown, path: string): ProtocolResult<Envelope> {
  if (value === undefined) return fail("invalid_envelope", `${path} is required`);
  const validation = validateEnvelope(value);
  if (!validation.valid) {
    return fail("invalid_envelope", `${path}: ${validation.errors.join("; ")}`);
  }
  return ok(value as Envelope);
}

function parseRecord(value: unknown, path: string): ProtocolResult<Record<string, unknown>> {
  if (!isRecord(value)) return fail("invalid_payload", `${path} must be a JSON object`);
  return ok(value);
}

function requiredStringField(
  record: Record<string, unknown>,
  field: string,
  path: string,
): ProtocolResult<string> {
  const value = record[field];
  if (typeof value !== "string" || value.trim() === "") {
    return fail("invalid_payload", `${path} must be a non-empty string`);
  }
  return ok(value);
}

function optionalStringField(
  record: Record<string, unknown>,
  field: string,
  path: string,
): ProtocolResult<string | undefined> {
  const value = record[field];
  if (value === undefined) return ok(undefined);
  if (typeof value !== "string" || value.trim() === "") {
    return fail("invalid_payload", `${path} must be a non-empty string when present`);
  }
  return ok(value);
}

function requiredIdArray(
  record: Record<string, unknown>,
  field: string,
  path: string,
): ProtocolResult<string[]> {
  const value = record[field];
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    value.some((item) => typeof item !== "string" || item.length === 0)
  ) {
    return fail("invalid_payload", `${path} must be a non-empty array of non-empty strings`);
  }
  return ok([...value]);
}

function optionalPositiveInteger(
  record: Record<string, unknown>,
  field: string,
  path: string,
): ProtocolResult<number | undefined> {
  const value = record[field];
  if (value === undefined) return ok(undefined);
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    return fail("invalid_payload", `${path} must be a positive integer when present`);
  }
  return ok(value);
}

function optionalPositiveNumber(
  record: Record<string, unknown>,
  field: string,
  path: string,
): ProtocolResult<number | undefined> {
  const value = record[field];
  if (value === undefined) return ok(undefined);
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return fail("invalid_payload", `${path} must be a positive number when present`);
  }
  return ok(value);
}

function optionalNonNegativeNumber(
  record: Record<string, unknown>,
  field: string,
  path: string,
): ProtocolResult<number | undefined> {
  const value = record[field];
  if (value === undefined) return ok(undefined);
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return fail("invalid_payload", `${path} must be a non-negative number when present`);
  }
  return ok(value);
}

function isMailboxLinkOp(value: string): value is MailboxLinkOp {
  return (MAILBOX_LINK_OPS as readonly string[]).includes(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function ok<T>(value: T): ProtocolResult<T> {
  return { ok: true, value };
}

export function fail(
  code: MailboxLinkErrorCode,
  message: string,
): { ok: false; code: MailboxLinkErrorCode; message: string } {
  return { ok: false, code, message };
}

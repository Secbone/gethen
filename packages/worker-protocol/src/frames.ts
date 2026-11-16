/**
 * `gethen.worker/v1` WebSocket frame types, pure validation and encoding.
 *
 * Every frame is one JSON object with `v: 1` and a discriminating `type`. This
 * module is shared by the Master endpoint and the Worker agent: it must stay
 * pure (no IO, no Node builtins, no other Gethen imports) so both sides can use
 * it in any runtime.
 *
 * Validation policy:
 * - `parseDownstreamFrame` / `parseUpstreamFrame` never throw; they return a
 *   `Result` with a stable error code.
 * - Unknown extra fields are tolerated and dropped from the returned frame;
 *   required fields, field types, version and size are strict.
 * - `envelope`, event payloads, capabilities, usage and control state are
 *   opaque JSON objects (`JsonObject`) so this package never depends on the
 *   Master's domain types.
 */
import { type ControlOp, parseControlOp } from "./control";

export const PROTOCOL_VERSION = 1;
export const MAX_FRAME_BYTES = 262_144;
export const PROTOCOL_NAME = "gethen.worker.v1";

/** Any JSON value; the wire format is JSON by definition. */
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
/** A JSON object with opaque contents (envelopes, event payloads, ...). */
export type JsonObject = { [key: string]: JsonValue };

/** Stable failure codes for parse/validate results. */
export type ProtocolErrorCode =
  | "invalid_frame"
  | "unsupported_version"
  | "unknown_type"
  | "invalid_field"
  | "frame_too_large"
  | "unknown_control_op"
  | "runtime_not_allowed"
  | "path_outside_root";

export interface ProtocolSuccess<T> {
  ok: true;
  value: T;
}

export interface ProtocolFailure {
  ok: false;
  code: ProtocolErrorCode;
  message: string;
}

/** `{ok:true,value}` or `{ok:false,code,message}`; parsing never throws. */
export type Result<T> = ProtocolSuccess<T> | ProtocolFailure;

export const DOWNSTREAM_FRAME_TYPES = [
  "hello_ok",
  "dispatch",
  "resolution",
  "cancel",
  "control",
] as const;

export const UPSTREAM_FRAME_TYPES = [
  "hello",
  "heartbeat",
  "accepted",
  "event",
  "escalation",
  "report",
  "control_result",
  "error",
] as const;

export type DownstreamFrameType = (typeof DOWNSTREAM_FRAME_TYPES)[number];
export type UpstreamFrameType = (typeof UPSTREAM_FRAME_TYPES)[number];

export type ReportKind = "progress" | "waiting" | "ready" | "blocked" | "failed" | "dropped";
export type EscalationKind = "decision" | "blocked" | "permission" | "scope" | "conflict";
export type ResolutionKind = "answer" | "escalate" | "drop";

export const REPORT_KINDS: readonly ReportKind[] = [
  "progress",
  "waiting",
  "ready",
  "blocked",
  "failed",
  "dropped",
];

export const ESCALATION_KINDS: readonly EscalationKind[] = [
  "decision",
  "blocked",
  "permission",
  "scope",
  "conflict",
];

export const RESOLUTION_KINDS: readonly ResolutionKind[] = ["answer", "escalate", "drop"];

/** Every frame carries the protocol version and a literal `type`. */
interface FrameEnvelope {
  v: typeof PROTOCOL_VERSION;
}

// ---------------------------------------------------------------------------
// Downstream frames (Master -> Worker)
// ---------------------------------------------------------------------------

export interface HelloOkFrame extends FrameEnvelope {
  type: "hello_ok";
  workerId: string;
  serverTime: string;
  heartbeatMs: number;
}

export interface DispatchFrame extends FrameEnvelope {
  type: "dispatch";
  assignmentId: string;
  ticketId: string;
  workspace: string;
  packDigest: string;
  /** The assignment envelope, opaque to the transport. */
  envelope: JsonObject;
  model?: string;
  deadlineAt?: string;
}

export interface ResolutionFrame extends FrameEnvelope {
  type: "resolution";
  assignmentId: string;
  escalationId: string;
  resolution: ResolutionBody;
}

export interface ResolutionBody {
  kind: ResolutionKind;
  option?: string;
  text: string;
}

export interface CancelFrame extends FrameEnvelope {
  type: "cancel";
  assignmentId: string;
  reason?: string;
}

export interface ControlFrame extends FrameEnvelope {
  type: "control";
  operationId: string;
  op: ControlOp;
}

export type DownFrame = HelloOkFrame | DispatchFrame | ResolutionFrame | CancelFrame | ControlFrame;

// ---------------------------------------------------------------------------
// Upstream frames (Worker -> Master)
// ---------------------------------------------------------------------------

export interface LoadSnapshot {
  running: number;
  quota: number;
}

export interface EscalationOption {
  id: string;
  label: string;
}

export interface HelloFrame extends FrameEnvelope {
  type: "hello";
  workerId: string;
  /** Mirrors `v`; both must be `PROTOCOL_VERSION`. */
  protocol: typeof PROTOCOL_VERSION;
  machine: string;
  platform: string;
  runtimes: string[];
  workspaces: string[];
  /** Opaque capability object; the Master advertises it in manifests. */
  capabilities: JsonObject;
  load: LoadSnapshot;
  models: string[];
  token?: string;
}

export interface HeartbeatFrame extends FrameEnvelope {
  type: "heartbeat";
  at: string;
  load: LoadSnapshot;
}

export interface AcceptedFrame extends FrameEnvelope {
  type: "accepted";
  assignmentId: string;
}

export interface EventFrame extends FrameEnvelope {
  type: "event";
  assignmentId: string;
  seq: number;
  at: string;
  /** Opaque event payload; must at least carry a string `type`. */
  event: JsonObject;
}

export interface EscalationFrame extends FrameEnvelope {
  type: "escalation";
  assignmentId: string;
  escalationId: string;
  kind: EscalationKind;
  question: string;
  options: EscalationOption[];
  defaultOption?: string;
  /** A blocking escalation always ends the Worker's turn. */
  blocking: true;
}

export interface ReportReceipt {
  packDigest: string;
  events: number;
}

export interface ReportFrame extends FrameEnvelope {
  type: "report";
  assignmentId: string;
  kind: ReportKind;
  summary: string;
  question?: string;
  usage?: JsonObject;
  receipt: ReportReceipt;
}

export interface ControlResultFrame extends FrameEnvelope {
  type: "control_result";
  operationId: string;
  ok: boolean;
  state?: JsonObject;
  error?: string;
}

export interface ErrorFrame extends FrameEnvelope {
  type: "error";
  code: string;
  message: string;
  assignmentId?: string;
}

export type UpFrame =
  | HelloFrame
  | HeartbeatFrame
  | AcceptedFrame
  | EventFrame
  | EscalationFrame
  | ReportFrame
  | ControlResultFrame
  | ErrorFrame;

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/**
 * Validate a Master->Worker frame. Accepts either a raw JSON string (as it
 * comes off the socket) or an already-parsed value; never throws.
 */
export function parseDownstreamFrame(value: unknown): Result<DownFrame> {
  const envelope = validateEnvelope(value, DOWNSTREAM_FRAME_TYPES);
  if (!envelope.ok) return envelope;
  const { record, type } = envelope.value;
  try {
    switch (type) {
      case "hello_ok":
        return ok(parseHelloOk(record));
      case "dispatch":
        return ok(parseDispatch(record));
      case "resolution":
        return ok(parseResolution(record));
      case "cancel":
        return ok(parseCancel(record));
      case "control":
        return ok(parseControl(record));
      default:
        return fail("unknown_type", `unknown downstream frame type "${type}"`);
    }
  } catch (error) {
    return fromThrown(error);
  }
}

/**
 * Validate a Worker->Master frame. Accepts either a raw JSON string or an
 * already-parsed value; never throws.
 */
export function parseUpstreamFrame(value: unknown): Result<UpFrame> {
  const envelope = validateEnvelope(value, UPSTREAM_FRAME_TYPES);
  if (!envelope.ok) return envelope;
  const { record, type } = envelope.value;
  try {
    switch (type) {
      case "hello":
        return ok(parseHello(record));
      case "heartbeat":
        return ok(parseHeartbeat(record));
      case "accepted":
        return ok(parseAccepted(record));
      case "event":
        return ok(parseEvent(record));
      case "escalation":
        return ok(parseEscalation(record));
      case "report":
        return ok(parseReport(record));
      case "control_result":
        return ok(parseControlResult(record));
      case "error":
        return ok(parseError(record));
      default:
        return fail("unknown_type", `unknown upstream frame type "${type}"`);
    }
  } catch (error) {
    return fromThrown(error);
  }
}

/**
 * Serialize one frame and enforce `MAX_FRAME_BYTES`. Throws on frames that are
 * too large or not JSON-serializable: the caller built them, so a bad frame is
 * a programmer error rather than remote input.
 */
export function encodeFrame(frame: DownFrame | UpFrame): string {
  const json = JSON.stringify(frame);
  if (json === undefined) {
    throw new Error(`${PROTOCOL_NAME}: frame is not JSON-serializable`);
  }
  const bytes = utf8Bytes(json);
  if (bytes > MAX_FRAME_BYTES) {
    throw new Error(
      `${PROTOCOL_NAME}: encoded frame is ${bytes} bytes, over the ${MAX_FRAME_BYTES}-byte limit`,
    );
  }
  return json;
}

// ---------------------------------------------------------------------------
// Shared envelope / size checks
// ---------------------------------------------------------------------------

interface Envelope {
  record: Record<string, unknown>;
  type: string;
}

function validateEnvelope(value: unknown, allowed: readonly string[]): Result<Envelope> {
  let record: Record<string, unknown>;
  let bytes: number | undefined;

  if (typeof value === "string") {
    // A raw WebSocket text frame: size-check the exact wire bytes first.
    bytes = utf8Bytes(value);
    if (bytes > MAX_FRAME_BYTES) {
      return fail(
        "frame_too_large",
        `frame is ${bytes} bytes, over the ${MAX_FRAME_BYTES}-byte limit`,
      );
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(value);
    } catch {
      return fail("invalid_frame", "frame must be valid JSON");
    }
    if (!isRecord(parsed)) return fail("invalid_frame", "frame must be a JSON object");
    record = parsed;
  } else {
    if (!isRecord(value)) return fail("invalid_frame", "frame must be a JSON object");
    record = value;
    bytes = byteLength(value);
    if (bytes === undefined) return fail("invalid_frame", "frame must be JSON-serializable");
    if (bytes > MAX_FRAME_BYTES) {
      return fail(
        "frame_too_large",
        `frame is ${bytes} bytes, over the ${MAX_FRAME_BYTES}-byte limit`,
      );
    }
  }

  if (record.v !== PROTOCOL_VERSION) {
    return fail("unsupported_version", `frame "v" must be ${PROTOCOL_VERSION}`);
  }
  const type = record.type;
  if (typeof type !== "string" || !allowed.includes(type)) {
    return fail("unknown_type", `frame "type" must be one of: ${allowed.join(", ")}`);
  }
  return ok({ record, type });
}

function byteLength(value: unknown): number | undefined {
  try {
    const json = JSON.stringify(value);
    return json === undefined ? undefined : utf8Bytes(json);
  } catch {
    return undefined;
  }
}

function utf8Bytes(text: string): number {
  return new TextEncoder().encode(text).length;
}

// ---------------------------------------------------------------------------
// Downstream frame shapes
// ---------------------------------------------------------------------------

function parseHelloOk(record: Record<string, unknown>): HelloOkFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "hello_ok",
    workerId: fieldString(record, "workerId", "hello_ok.workerId"),
    serverTime: fieldString(record, "serverTime", "hello_ok.serverTime"),
    heartbeatMs: fieldPositiveNumber(record, "heartbeatMs", "hello_ok.heartbeatMs"),
  };
}

function parseDispatch(record: Record<string, unknown>): DispatchFrame {
  const frame: DispatchFrame = {
    v: PROTOCOL_VERSION,
    type: "dispatch",
    assignmentId: fieldString(record, "assignmentId", "dispatch.assignmentId"),
    ticketId: fieldString(record, "ticketId", "dispatch.ticketId"),
    workspace: fieldString(record, "workspace", "dispatch.workspace"),
    packDigest: fieldString(record, "packDigest", "dispatch.packDigest"),
    envelope: fieldObject(record, "envelope", "dispatch.envelope"),
  };
  const model = optionalString(record, "model", "dispatch.model");
  if (model !== undefined) frame.model = model;
  const deadlineAt = optionalString(record, "deadlineAt", "dispatch.deadlineAt");
  if (deadlineAt !== undefined) frame.deadlineAt = deadlineAt;
  return frame;
}

function parseResolution(record: Record<string, unknown>): ResolutionFrame {
  const body = fieldObject(record, "resolution", "resolution.resolution");
  const kind = fieldString(body, "kind", "resolution.resolution.kind");
  if (!isResolutionKind(kind)) {
    throw invalid(`resolution "kind" must be one of: ${RESOLUTION_KINDS.join(", ")}`);
  }
  const parsed: ResolutionBody = {
    kind,
    text: fieldString(body, "text", "resolution.resolution.text"),
  };
  const option = optionalString(body, "option", "resolution.resolution.option");
  if (option !== undefined) parsed.option = option;
  return {
    v: PROTOCOL_VERSION,
    type: "resolution",
    assignmentId: fieldString(record, "assignmentId", "resolution.assignmentId"),
    escalationId: fieldString(record, "escalationId", "resolution.escalationId"),
    resolution: parsed,
  };
}

function parseCancel(record: Record<string, unknown>): CancelFrame {
  const frame: CancelFrame = {
    v: PROTOCOL_VERSION,
    type: "cancel",
    assignmentId: fieldString(record, "assignmentId", "cancel.assignmentId"),
  };
  const reason = optionalString(record, "reason", "cancel.reason");
  if (reason !== undefined) frame.reason = reason;
  return frame;
}

function parseControl(record: Record<string, unknown>): ControlFrame {
  const operationId = fieldString(record, "operationId", "control.operationId");
  const parsed = parseControlOp(record.op);
  if (!parsed.ok) throw new FrameError(parsed.code, parsed.message);
  return { v: PROTOCOL_VERSION, type: "control", operationId, op: parsed.value };
}

// ---------------------------------------------------------------------------
// Upstream frame shapes
// ---------------------------------------------------------------------------

function parseHello(record: Record<string, unknown>): HelloFrame {
  const protocol = fieldNumber(record, "protocol", "hello.protocol");
  if (protocol !== PROTOCOL_VERSION) {
    throw new FrameError("unsupported_version", `hello "protocol" must be ${PROTOCOL_VERSION}`);
  }
  const frame: HelloFrame = {
    v: PROTOCOL_VERSION,
    type: "hello",
    workerId: fieldString(record, "workerId", "hello.workerId"),
    protocol: PROTOCOL_VERSION,
    machine: fieldString(record, "machine", "hello.machine"),
    platform: fieldString(record, "platform", "hello.platform"),
    runtimes: fieldStringArray(record, "runtimes", "hello.runtimes"),
    workspaces: fieldStringArray(record, "workspaces", "hello.workspaces"),
    capabilities: fieldObject(record, "capabilities", "hello.capabilities"),
    load: fieldLoad(record, "hello.load"),
    models: fieldStringArray(record, "models", "hello.models"),
  };
  const token = optionalString(record, "token", "hello.token");
  if (token !== undefined) frame.token = token;
  return frame;
}

function parseHeartbeat(record: Record<string, unknown>): HeartbeatFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "heartbeat",
    at: fieldString(record, "at", "heartbeat.at"),
    load: fieldLoad(record, "heartbeat.load"),
  };
}

function parseAccepted(record: Record<string, unknown>): AcceptedFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "accepted",
    assignmentId: fieldString(record, "assignmentId", "accepted.assignmentId"),
  };
}

function parseEvent(record: Record<string, unknown>): EventFrame {
  const event = fieldObject(record, "event", "event.event");
  // The event type is an open set (message/tool.start/tool.result/usage today);
  // an event that carries no type at all is malformed.
  fieldString(event, "type", "event.event.type");
  return {
    v: PROTOCOL_VERSION,
    type: "event",
    assignmentId: fieldString(record, "assignmentId", "event.assignmentId"),
    seq: fieldNonNegativeInteger(record, "seq", "event.seq"),
    at: fieldString(record, "at", "event.at"),
    event,
  };
}

function parseEscalation(record: Record<string, unknown>): EscalationFrame {
  const kind = fieldString(record, "kind", "escalation.kind");
  if (!isEscalationKind(kind)) {
    throw invalid(`escalation "kind" must be one of: ${ESCALATION_KINDS.join(", ")}`);
  }
  if (record.blocking !== true) {
    throw invalid('escalation "blocking" must be true');
  }
  const frame: EscalationFrame = {
    v: PROTOCOL_VERSION,
    type: "escalation",
    assignmentId: fieldString(record, "assignmentId", "escalation.assignmentId"),
    escalationId: fieldString(record, "escalationId", "escalation.escalationId"),
    kind,
    question: fieldString(record, "question", "escalation.question"),
    options: fieldOptions(record, "options", "escalation.options"),
    blocking: true,
  };
  const defaultOption = optionalString(record, "defaultOption", "escalation.defaultOption");
  if (defaultOption !== undefined) frame.defaultOption = defaultOption;
  return frame;
}

function parseReport(record: Record<string, unknown>): ReportFrame {
  const kind = fieldString(record, "kind", "report.kind");
  if (!isReportKind(kind)) {
    throw invalid(`report "kind" must be one of: ${REPORT_KINDS.join(", ")}`);
  }
  const receipt = fieldObject(record, "receipt", "report.receipt");
  const frame: ReportFrame = {
    v: PROTOCOL_VERSION,
    type: "report",
    assignmentId: fieldString(record, "assignmentId", "report.assignmentId"),
    kind,
    summary: fieldString(record, "summary", "report.summary"),
    receipt: {
      packDigest: fieldString(receipt, "packDigest", "report.receipt.packDigest"),
      events: fieldNonNegativeInteger(receipt, "events", "report.receipt.events"),
    },
  };
  const question = optionalString(record, "question", "report.question");
  if (question !== undefined) frame.question = question;
  const usage = optionalObject(record, "usage", "report.usage");
  if (usage !== undefined) frame.usage = usage;
  return frame;
}

function parseControlResult(record: Record<string, unknown>): ControlResultFrame {
  const okValue = record.ok;
  if (typeof okValue !== "boolean") throw invalid('control_result "ok" must be a boolean');
  const frame: ControlResultFrame = {
    v: PROTOCOL_VERSION,
    type: "control_result",
    operationId: fieldString(record, "operationId", "control_result.operationId"),
    ok: okValue,
  };
  const state = optionalObject(record, "state", "control_result.state");
  if (state !== undefined) frame.state = state;
  const error = optionalString(record, "error", "control_result.error");
  if (error !== undefined) frame.error = error;
  return frame;
}

function parseError(record: Record<string, unknown>): ErrorFrame {
  const frame: ErrorFrame = {
    v: PROTOCOL_VERSION,
    type: "error",
    code: fieldString(record, "code", "error.code"),
    message: fieldString(record, "message", "error.message"),
  };
  const assignmentId = optionalString(record, "assignmentId", "error.assignmentId");
  if (assignmentId !== undefined) frame.assignmentId = assignmentId;
  return frame;
}

// ---------------------------------------------------------------------------
// Field helpers (throw FrameError; the public parsers catch them)
// ---------------------------------------------------------------------------

class FrameError extends Error {
  readonly code: ProtocolErrorCode;

  constructor(code: ProtocolErrorCode, message: string) {
    super(message);
    this.name = "FrameError";
    this.code = code;
  }
}

function fromThrown(error: unknown): Result<never> {
  if (error instanceof FrameError) return fail(error.code, error.message);
  return fail("invalid_frame", `frame could not be validated: ${messageOf(error)}`);
}

function invalid(message: string): never {
  throw new FrameError("invalid_field", message);
}

function ok<T>(value: T): Result<T> {
  return { ok: true, value };
}

function fail(code: ProtocolErrorCode, message: string): Result<never> {
  return { ok: false, code, message };
}

function fieldString(record: Record<string, unknown>, field: string, path: string): string {
  const value = record[field];
  if (typeof value !== "string" || value.length === 0) {
    throw invalid(`${path} must be a non-empty string`);
  }
  return value;
}

function optionalString(
  record: Record<string, unknown>,
  field: string,
  path: string,
): string | undefined {
  const value = record[field];
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.length === 0) {
    throw invalid(`${path} must be a non-empty string when present`);
  }
  return value;
}

function fieldNumber(record: Record<string, unknown>, field: string, path: string): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw invalid(`${path} must be a finite number`);
  }
  return value;
}

function fieldPositiveNumber(record: Record<string, unknown>, field: string, path: string): number {
  const value = fieldNumber(record, field, path);
  if (value <= 0) throw invalid(`${path} must be positive`);
  return value;
}

function fieldNonNegativeInteger(
  record: Record<string, unknown>,
  field: string,
  path: string,
): number {
  const value = fieldNumber(record, field, path);
  if (!Number.isInteger(value) || value < 0) {
    throw invalid(`${path} must be a non-negative integer`);
  }
  return value;
}

function fieldObject(record: Record<string, unknown>, field: string, path: string): JsonObject {
  const value = record[field];
  if (!isRecord(value)) throw invalid(`${path} must be a JSON object`);
  return value as unknown as JsonObject;
}

function optionalObject(
  record: Record<string, unknown>,
  field: string,
  path: string,
): JsonObject | undefined {
  const value = record[field];
  if (value === undefined) return undefined;
  if (!isRecord(value)) throw invalid(`${path} must be a JSON object when present`);
  return value as unknown as JsonObject;
}

function fieldStringArray(record: Record<string, unknown>, field: string, path: string): string[] {
  const value = record[field];
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw invalid(`${path} must be an array of strings`);
  }
  return [...(value as string[])];
}

function fieldOptions(
  record: Record<string, unknown>,
  field: string,
  path: string,
): EscalationOption[] {
  const value = record[field];
  if (!Array.isArray(value)) throw invalid(`${path} must be an array`);
  return value.map((option, index) => {
    if (!isRecord(option)) throw invalid(`${path}[${index}] must be a JSON object`);
    return {
      id: fieldString(option, "id", `${path}[${index}].id`),
      label: fieldString(option, "label", `${path}[${index}].label`),
    };
  });
}

function fieldLoad(record: Record<string, unknown>, path: string): LoadSnapshot {
  const value = record.load;
  if (!isRecord(value)) throw invalid(`${path} must be a JSON object`);
  return {
    running: fieldNonNegativeInteger(value, "running", `${path}.running`),
    quota: fieldNonNegativeInteger(value, "quota", `${path}.quota`),
  };
}

function isReportKind(value: string): value is ReportKind {
  return (REPORT_KINDS as readonly string[]).includes(value);
}

function isEscalationKind(value: string): value is EscalationKind {
  return (ESCALATION_KINDS as readonly string[]).includes(value);
}

function isResolutionKind(value: string): value is ResolutionKind {
  return (RESOLUTION_KINDS as readonly string[]).includes(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Session-facing envelope protocol between the Gethen Master and a Worker.
 *
 * The marker formats mirror `packages/master/src/prompts.ts` exactly:
 *
 * - `[GETHEN-ASSIGNMENT]` ... `[/GETHEN-ASSIGNMENT]` — Master -> Worker task.
 * - `[GETHEN-REPORT]` ... `[/GETHEN-REPORT]` — Worker -> Master result.
 * - `[GETHEN-ESCALATION]` ... `[/GETHEN-ESCALATION]` — Worker -> Master question.
 * - `[GETHEN-RESOLUTION]` ... `[/GETHEN-RESOLUTION]` — Master -> Worker answer.
 *
 * Worker flow: the first message of an Assignment is the assignment prompt.
 * Parse it with `parseAssignmentPrompt`, materialize its capability pack with
 * `materializePack`, do the work, then answer once with `buildReportText`. When
 * a person must decide something, send `buildEscalationText` and end the turn;
 * the Master answers with a resolution block read by `parseResolutionText`.
 *
 * Parsers return `undefined` when the marker is absent, but throw when a marker
 * exists and its body is malformed: a truncated or invented block must never be
 * mistaken for a valid envelope. Pure module: no IO, no clock.
 */
import type { Escalation, Resolution } from "@gethen/worker-bridge";

export const ASSIGNMENT_OPEN = "[GETHEN-ASSIGNMENT]";
export const ASSIGNMENT_CLOSE = "[/GETHEN-ASSIGNMENT]";
export const REPORT_OPEN = "[GETHEN-REPORT]";
export const REPORT_CLOSE = "[/GETHEN-REPORT]";
export const ESCALATION_OPEN = "[GETHEN-ESCALATION]";
export const ESCALATION_CLOSE = "[/GETHEN-ESCALATION]";
export const RESOLUTION_OPEN = "[GETHEN-RESOLUTION]";
export const RESOLUTION_CLOSE = "[/GETHEN-RESOLUTION]";

export const ASSIGNMENT_PROTO = "gethen/assignment/v1";
export const REPORT_PROTO = "gethen/report/v1";
export const RESOLUTION_PROTO = "gethen/resolution/v1";

/** Report kinds a Worker may use; mirrors `ReportKind` in the Master model. */
export type ReportKindLike = "progress" | "waiting" | "ready" | "blocked" | "failed" | "dropped";

export const REPORT_KINDS: readonly ReportKindLike[] = [
  "progress",
  "waiting",
  "ready",
  "blocked",
  "failed",
  "dropped",
];

/** The report contract the Master states inside an assignment envelope. */
export interface AssignmentReportContractLike {
  proto: string;
  marker: string;
  closeMarker: string;
  kinds: string[];
}

/** The subset of the Master's assignment envelope a Worker needs. */
export interface AssignmentEnvelope {
  proto: typeof ASSIGNMENT_PROTO;
  ticket: { id: string };
  assignment: { id: string; workspace: string; worker: string };
  workspace: string;
  packDigest: string;
  objective: string;
  report?: AssignmentReportContractLike;
}

/** The report body the Master parses out of a transcript. */
export interface ReportLike {
  kind: ReportKindLike;
  summary: string;
  question?: string;
  at: string;
}

/**
 * The subset of a bridge `Escalation` a Worker fills in. The session id is
 * optional because the Worker learns it from its own session, not the envelope.
 */
export type EscalationLike = Omit<Escalation, "session"> & { session?: string };

/** The answer variant of the bridge's `Resolution`. */
type ResolutionAnswer = Extract<Resolution, { kind: "answer" }>;

/**
 * The subset of a bridge `Resolution` a Worker acts on. The Master's
 * `gethen/resolution/v1` block carries `id`, `option?`, `text` and `basis`;
 * a bridge-shaped answer also carries `escalation`, `confidence` and `at`,
 * which are preserved when present.
 */
export interface ResolutionLike {
  id: ResolutionAnswer["id"];
  text: ResolutionAnswer["text"];
  option?: string;
  basis?: string[];
  escalation?: ResolutionAnswer["escalation"];
  confidence?: ResolutionAnswer["confidence"];
  at?: ResolutionAnswer["at"];
}

/**
 * Parse the first `[GETHEN-ASSIGNMENT] ... [/GETHEN-ASSIGNMENT]` block of an
 * Assignment's prompt. Returns `undefined` when there is no assignment marker;
 * a block that exists but is unclosed, invalid JSON, has the wrong proto, or
 * misses a required field throws.
 */
export function parseAssignmentPrompt(text: string): AssignmentEnvelope | undefined {
  const body = sliceBlock(text, ASSIGNMENT_OPEN, ASSIGNMENT_CLOSE, "assignment");
  if (body === undefined) return undefined;

  const value = parseJsonObject(body, "assignment");
  if (value.proto !== ASSIGNMENT_PROTO) {
    throw new Error(`assignment "proto" must be "${ASSIGNMENT_PROTO}"`);
  }

  const ticket = requireRecord(value, "ticket");
  const assignment = requireRecord(value, "assignment");
  const envelope: AssignmentEnvelope = {
    proto: ASSIGNMENT_PROTO,
    ticket: { id: requireString(ticket, "id", "ticket.id") },
    assignment: {
      id: requireString(assignment, "id", "assignment.id"),
      workspace: requireString(assignment, "workspace", "assignment.workspace", true),
      worker: requireString(assignment, "worker", "assignment.worker", true),
    },
    workspace: requireString(value, "workspace", "workspace", true),
    packDigest: requireString(value, "packDigest", "packDigest", true),
    objective: requireString(value, "objective", "objective"),
  };
  if (value.report !== undefined) envelope.report = parseReportContract(value.report);
  return envelope;
}

/**
 * Render one Worker report block. The JSON body mirrors the fields the Master's
 * `parseReportText` accepts (`kind`, `summary`, optional `question`, `at`); an
 * empty question is omitted.
 */
export function buildReportText(report: ReportLike): string {
  if (!isReportKind(report.kind)) {
    throw new Error(`report "kind" must be one of: ${REPORT_KINDS.join(", ")}`);
  }
  requireNonEmpty(report.summary, "report summary");
  requireNonEmpty(report.at, 'report "at"');

  const body: Record<string, unknown> = { kind: report.kind, summary: report.summary };
  if (report.question !== undefined && report.question.length > 0) body.question = report.question;
  body.at = report.at;
  return markerBlock(REPORT_OPEN, REPORT_CLOSE, body);
}

/**
 * Render one Worker escalation block. A blocking escalation ends the turn: the
 * Master answers with a resolution block or escalates to a person.
 */
export function buildEscalationText(escalation: EscalationLike): string {
  if (!isRecord(escalation)) throw new Error("escalation must be an object");
  requireNonEmpty(escalation.question, "escalation question");
  if (!Array.isArray(escalation.options)) {
    throw new Error('escalation "options" must be an array');
  }
  return markerBlock(ESCALATION_OPEN, ESCALATION_CLOSE, escalation);
}

/**
 * Parse the first `[GETHEN-RESOLUTION] ... [/GETHEN-RESOLUTION]` block of a
 * Master message. Returns `undefined` when there is no resolution marker; a
 * malformed block throws. `proto` is checked when present so both the Master's
 * envelope and a bridge-shaped answer are accepted.
 */
export function parseResolutionText(text: string): ResolutionLike | undefined {
  const body = sliceBlock(text, RESOLUTION_OPEN, RESOLUTION_CLOSE, "resolution");
  if (body === undefined) return undefined;

  const value = parseJsonObject(body, "resolution");
  if (value.proto !== undefined && value.proto !== RESOLUTION_PROTO) {
    throw new Error(`resolution "proto" must be "${RESOLUTION_PROTO}"`);
  }

  const resolution: ResolutionLike = {
    id: requireString(value, "id", "resolution.id"),
    text: requireString(value, "text", "resolution.text"),
  };
  const option = optionalString(value, "option", "resolution.option");
  if (option !== undefined) resolution.option = option;
  if (value.basis !== undefined)
    resolution.basis = requireStringArray(value.basis, "resolution.basis");
  const escalation = optionalString(value, "escalation", "resolution.escalation");
  if (escalation !== undefined) resolution.escalation = escalation;
  if (value.confidence !== undefined) {
    if (typeof value.confidence !== "number" || Number.isNaN(value.confidence)) {
      throw new Error('resolution "confidence" must be a number when present');
    }
    resolution.confidence = value.confidence;
  }
  const at = optionalString(value, "at", "resolution.at");
  if (at !== undefined) resolution.at = at;
  return resolution;
}

function parseReportContract(value: unknown): AssignmentReportContractLike {
  const report = asRecord(value, 'assignment "report"');
  return {
    proto: requireString(report, "proto", "report.proto"),
    marker: requireString(report, "marker", "report.marker"),
    closeMarker: requireString(report, "closeMarker", "report.closeMarker"),
    kinds: requireStringArray(report.kinds, "report.kinds"),
  };
}

function markerBlock(open: string, close: string, body: unknown): string {
  return [open, JSON.stringify(body, null, 2), close].join("\n");
}

/** Slice a marker-delimited body; `undefined` when the open marker is absent. */
function sliceBlock(text: string, open: string, close: string, label: string): string | undefined {
  const start = text.indexOf(open);
  if (start === -1) return undefined;
  const bodyStart = start + open.length;
  const end = text.indexOf(close, bodyStart);
  if (end === -1) {
    throw new Error(`${label} block is missing its closing marker ${close}`);
  }
  return text.slice(bodyStart, end).trim();
}

function parseJsonObject(body: string, label: string): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch (error) {
    throw new Error(`${label} block is not valid JSON: ${messageOf(error)}`, { cause: error });
  }
  return asRecord(parsed, `${label} block`);
}

function asRecord(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`${label} must contain a JSON object`);
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireRecord(record: Record<string, unknown>, field: string): Record<string, unknown> {
  return asRecord(record[field], `assignment "${field}"`);
}

function requireString(
  record: Record<string, unknown>,
  field: string,
  path: string,
  allowEmpty = false,
): string {
  const value = record[field];
  if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
    throw new Error(`${path} must be a ${allowEmpty ? "" : "non-empty "}string`);
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
    throw new Error(`${path} must be a non-empty string when present`);
  }
  return value;
}

function requireStringArray(value: unknown, path: string): string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw new Error(`${path} must be an array of strings`);
  }
  return [...value] as string[];
}

function requireNonEmpty(value: string, path: string): void {
  if (value.trim().length === 0) throw new Error(`${path} must be a non-empty string`);
}

function isReportKind(value: string): value is ReportKindLike {
  return (REPORT_KINDS as readonly string[]).includes(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Session-facing prompt envelopes. The control plane talks to a Worker through
 * three marker-delimited blocks (`gethen/*` protos); the JSON body is the
 * contract and the surrounding prose gives the Worker its role and next step.
 *
 * - `[GETHEN-ASSIGNMENT]` ... `[/GETHEN-ASSIGNMENT]` — Master -> Worker task.
 * - `[GETHEN-REPORT]` ... `[/GETHEN-REPORT]` — Worker -> Master result; parsed
 *   here so a Worker transcript can become a domain `Report`.
 * - `[GETHEN-RESOLUTION]` ... `[/GETHEN-RESOLUTION]` — Master -> Worker answer
 *   to an Attention question. P1 only emits it; parsing arrives with a later
 *   phase.
 *
 * Pure module: no IO, no clock, no DSH imports.
 */
import type { Assignment, Report, ReportKind, Ticket } from "./model";

export const ASSIGNMENT_OPEN = "[GETHEN-ASSIGNMENT]";
export const ASSIGNMENT_CLOSE = "[/GETHEN-ASSIGNMENT]";
export const REPORT_OPEN = "[GETHEN-REPORT]";
export const REPORT_CLOSE = "[/GETHEN-REPORT]";
export const RESOLUTION_OPEN = "[GETHEN-RESOLUTION]";
export const RESOLUTION_CLOSE = "[/GETHEN-RESOLUTION]";

export const ASSIGNMENT_PROTO = "gethen/assignment/v1";
export const REPORT_PROTO = "gethen/report/v1";
export const RESOLUTION_PROTO = "gethen/resolution/v1";

/** The report kinds a Worker may use; mirrors `ReportKind` in model.ts. */
export const REPORT_KINDS: readonly ReportKind[] = [
  "progress",
  "waiting",
  "ready",
  "blocked",
  "failed",
  "dropped",
];

/** Fields the Worker must put in a report block. */
export interface ReportContractFields {
  /** One of `report.kinds`. */
  kind: string;
  /** One-paragraph result the person can read on its own. */
  summary: string;
  /** One concrete question for a person, when the Report needs one. */
  question?: string;
  /** RFC 3339 UTC timestamp of the report. */
  at: string;
}

/** How a Worker is expected to answer this Assignment. */
export interface AssignmentReportContract {
  proto: typeof REPORT_PROTO;
  marker: typeof REPORT_OPEN;
  closeMarker: typeof REPORT_CLOSE;
  kinds: readonly ReportKind[];
  fields: ReportContractFields;
  /** Free-form override supplied by the caller (defaults to the standard text). */
  format: string;
}

/** JSON body of an assignment prompt. */
export interface AssignmentEnvelope {
  proto: typeof ASSIGNMENT_PROTO;
  ticket: Ticket;
  assignment: Assignment;
  workspace: string;
  packDigest: string;
  objective: string;
  report: AssignmentReportContract;
}

/** Inputs to `buildAssignmentPrompt`; mirrors the launch effect payload. */
export interface AssignmentPromptInput {
  ticket: Ticket;
  assignment: Assignment;
  packDigest: string;
  objective: string;
  reportFormat?: string;
}

/** JSON body of a resolution prompt. */
export interface ResolutionEnvelope {
  proto: typeof RESOLUTION_PROTO;
  id: string;
  option?: string;
  text: string;
  basis: string[];
}

/** Inputs to `buildResolutionText`. */
export interface ResolutionInput {
  id: string;
  option?: string;
  text: string;
  basis: string[];
}

const DEFAULT_REPORT_FORMAT =
  "Reply with one [GETHEN-REPORT] ... [/GETHEN-REPORT] block whose JSON body has " +
  '"kind" (one of report.kinds), "summary", an optional "question" for a person, ' +
  'and "at" (RFC 3339 UTC). Report "ready" when the work is complete, "blocked" ' +
  'with exactly one question when a person must decide, and "failed" when you ' +
  "cannot continue.";

const ASSIGNMENT_INSTRUCTIONS =
  "You are the Gethen Worker for this Assignment. Work only inside the workspace " +
  "named above, complete the objective, then report exactly once using the report " +
  "contract in the envelope (you may emit the block yourself or call gethen_report). " +
  "If a decision can change scope, spend money, touch production data, or is not " +
  "covered by the objective, call gethen_escalate when that tool is available, then " +
  "end your turn with the returned block unchanged and wait for the resolution " +
  "message before continuing. Do not edit or echo the envelope itself.";

/**
 * Render the task sent to a Worker session: a marker-delimited JSON envelope
 * starting with `[GETHEN-ASSIGNMENT]`, followed by a short instruction telling
 * the Worker to finish the objective and answer with a report block.
 */
export function buildAssignmentPrompt(input: AssignmentPromptInput): string {
  const { ticket, assignment, packDigest, objective, reportFormat } = input;
  const envelope: AssignmentEnvelope = {
    proto: ASSIGNMENT_PROTO,
    ticket: snapshotTicket(ticket),
    assignment: { ...assignment },
    workspace: assignment.workspace,
    packDigest,
    objective,
    report: {
      proto: REPORT_PROTO,
      marker: REPORT_OPEN,
      closeMarker: REPORT_CLOSE,
      kinds: [...REPORT_KINDS],
      fields: {
        kind: "one of report.kinds",
        summary: "string",
        question: "string (optional; one concrete question for a person)",
        at: "RFC 3339 UTC timestamp",
      },
      format: reportFormat ?? DEFAULT_REPORT_FORMAT,
    },
  };
  return [
    ASSIGNMENT_OPEN,
    JSON.stringify(envelope, null, 2),
    ASSIGNMENT_CLOSE,
    "",
    ASSIGNMENT_INSTRUCTIONS,
  ].join("\n");
}

/**
 * Parse the first `[GETHEN-REPORT] ... [/GETHEN-REPORT]` block in a Worker
 * transcript. Returns `undefined` when there is no report block; a block that
 * exists but is malformed (bad JSON, unknown `kind`, missing `summary`/`at`)
 * throws, so a truncated or invented report never silently becomes a Report.
 */
export function parseReportText(text: string): Report | undefined {
  const open = text.indexOf(REPORT_OPEN);
  if (open === -1) return undefined;

  const bodyStart = open + REPORT_OPEN.length;
  const close = text.indexOf(REPORT_CLOSE, bodyStart);
  if (close === -1) {
    throw new Error(`report block is missing its closing marker ${REPORT_CLOSE}`);
  }

  const raw = text.slice(bodyStart, close).trim();
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`report block is not valid JSON: ${messageOf(error)}`, { cause: error });
  }
  if (!isRecord(parsed)) {
    throw new Error("report block must contain a JSON object");
  }

  const { kind, summary, question, at } = parsed;
  if (typeof kind !== "string" || !isReportKind(kind)) {
    throw new Error(`report "kind" must be one of: ${REPORT_KINDS.join(", ")}`);
  }
  if (typeof summary !== "string") {
    throw new Error('report is missing required field "summary"');
  }
  if (typeof at !== "string" || at.length === 0) {
    throw new Error('report is missing required field "at"');
  }
  if (question !== undefined && typeof question !== "string") {
    throw new Error('report field "question" must be a string when present');
  }

  const report: Report = { kind, summary, at };
  if (question !== undefined && question.length > 0) report.question = question;
  return report;
}

/**
 * Render the Master's answer to an Attention question. P1 only produces the
 * text; a later phase parses a Worker's follow-up instead.
 */
export function buildResolutionText(input: ResolutionInput): string {
  const envelope: ResolutionEnvelope = {
    proto: RESOLUTION_PROTO,
    id: input.id,
    ...(input.option === undefined ? {} : { option: input.option }),
    text: input.text,
    basis: [...input.basis],
  };
  return [RESOLUTION_OPEN, JSON.stringify(envelope, null, 2), RESOLUTION_CLOSE].join("\n");
}

function snapshotTicket(ticket: Ticket): Ticket {
  return {
    ...ticket,
    threads: [...ticket.threads],
    subscriptions: [...ticket.subscriptions],
    channels: [...ticket.channels],
    inputs: [...ticket.inputs],
    assignments: [...ticket.assignments],
  };
}

function isReportKind(value: string): value is ReportKind {
  return (REPORT_KINDS as readonly string[]).includes(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Worker-role DSH entry (P3b): two tools that let a Worker session talk to the
 * Master through the marker protocol instead of writing free-form prose.
 *
 * - `gethen_escalate` renders a `[GETHEN-ESCALATION]` block. The Worker must
 *   echo it verbatim as its final message and end the turn; the Master parses
 *   the transcript, resolves the question or parks it for a person, and sends a
 *   `[GETHEN-RESOLUTION]` back to the same session.
 * - `gethen_report` renders a `[GETHEN-REPORT]` block, the only way an
 *   Assignment finishes.
 *
 * This module imports no DSH package: the narrow `WorkerDshContext` below is
 * the whole integration surface. Envelope rendering is owned by
 * `@gethen/worker`; this file only adds the tool contract and input validation.
 */
import { randomUUID } from "node:crypto";
import {
  buildEscalationText,
  buildReportText,
  type EscalationLike,
  REPORT_KINDS,
  type ReportKindLike,
  type ReportLike,
} from "@gethen/worker";

/** The narrow DSH surface the Worker entry uses; no DSH package is imported. */
export interface WorkerToolDefinition {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
  output: {
    schema: Record<string, unknown>;
    render(args: unknown, value: unknown): Array<{ type: "text"; text: string }>;
  };
  execute(args: unknown, exec: unknown): Promise<unknown>;
}

export interface WorkerDshContext {
  tools: { register(definition: WorkerToolDefinition): () => void };
  logger?: { error?(message: string): void };
}

export interface WorkerConfig {
  /** Advertised worker name; defaults to `workerName`. */
  worker?: string;
}

export const workerName = "gethen-worker";

/** Escalation kinds the Master accepts (mirrors `@gethen/worker-bridge`). */
const ESCALATION_KINDS = ["decision", "blocked", "permission", "scope", "conflict"] as const;
type EscalationKind = (typeof ESCALATION_KINDS)[number];

interface EscalationOption {
  id: string;
  label: string;
}

/**
 * The Worker -> Master escalation body (docs/design/p3b-escalation-research.md
 * §2). It deliberately omits the ticket/assignment/session fields of the
 * bridge `Escalation`: the Master knows those from the session that produced
 * the transcript.
 */
interface EscalationEnvelope {
  id: string;
  kind: EscalationKind;
  question: string;
  options: EscalationOption[];
  defaultOption?: string;
  blocking: true;
  at: string;
}

const ESCALATE_INSTRUCTION =
  "Reply with this block verbatim as your final message and end the turn.";

function text(value: unknown): Array<{ type: "text"; text: string }> {
  return [{ type: "text", text: JSON.stringify(value) }];
}

function objectSchema(
  properties: Record<string, unknown>,
  required: string[],
): Record<string, unknown> {
  return { type: "object", properties, required, additionalProperties: false };
}

function requireText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value;
}

function parseKind(value: unknown): EscalationKind {
  const kind = value ?? "decision";
  if (typeof kind !== "string" || !(ESCALATION_KINDS as readonly string[]).includes(kind)) {
    throw new Error(`escalation "kind" must be one of: ${ESCALATION_KINDS.join(", ")}`);
  }
  return kind as EscalationKind;
}

function parseOptions(value: unknown): EscalationOption[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throw new Error('escalation "options" must be an array');
  const options = value.map((item, index) => {
    if (typeof item !== "object" || item === null || Array.isArray(item)) {
      throw new Error(`escalation option ${index} must be an object`);
    }
    const record = item as { id?: unknown; label?: unknown };
    return {
      id: requireText(record.id, `escalation option ${index} "id"`),
      label: requireText(record.label, `escalation option ${index} "label"`),
    };
  });
  const ids = new Set(options.map((option) => option.id));
  if (ids.size !== options.length) throw new Error("escalation option ids must be unique");
  return options;
}

function parseDefaultOption(value: unknown, options: EscalationOption[]): string | undefined {
  if (value === undefined) return undefined;
  const defaultOption = requireText(value, 'escalation "defaultOption"');
  if (!options.some((option) => option.id === defaultOption)) {
    throw new Error('escalation "defaultOption" must name one of the provided options');
  }
  return defaultOption;
}

function isReportKind(value: unknown): value is ReportKindLike {
  return typeof value === "string" && (REPORT_KINDS as readonly string[]).includes(value);
}

/**
 * Mount the Worker-role tools. The Worker talks to the Master only through the
 * rendered blocks; the plugin holds no state and needs no service.
 */
export function applyWorker(ctx: WorkerDshContext, config: WorkerConfig = {}): void {
  const worker = config.worker ?? workerName;

  ctx.tools.register({
    name: "gethen_escalate",
    description:
      "Ask the Master for a decision or permission when the task cannot proceed; " +
      `blocks this turn until the Master answers (worker: ${worker}).`,
    parameters: {
      question: {
        type: "string",
        required: true,
        description: "One concrete question for the Master or a person.",
      },
      kind: {
        type: "string",
        required: false,
        enum: [...ESCALATION_KINDS],
        description: 'Why the question is asked; defaults to "decision".',
      },
      options: {
        type: "array",
        required: false,
        description: "Concrete choices; id is the value a resolution may select.",
        items: {
          type: "object",
          properties: { id: { type: "string" }, label: { type: "string" } },
          required: ["id", "label"],
          additionalProperties: false,
        },
      },
      defaultOption: {
        type: "string",
        required: false,
        description: "Safe option id the Master may choose for reversible questions.",
      },
    },
    output: {
      schema: objectSchema({ text: { type: "string" }, instruction: { type: "string" } }, [
        "text",
        "instruction",
      ]),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      const args = raw as {
        question?: unknown;
        kind?: unknown;
        options?: unknown;
        defaultOption?: unknown;
      };
      const question = requireText(args.question, 'escalation "question"');
      const options = parseOptions(args.options);
      const defaultOption = parseDefaultOption(args.defaultOption, options);
      const envelope: EscalationEnvelope = {
        id: `escalation-${randomUUID()}`,
        kind: parseKind(args.kind),
        question,
        options,
        ...(defaultOption === undefined ? {} : { defaultOption }),
        blocking: true,
        at: new Date().toISOString(),
      };
      // `buildEscalationText` types its input as the full bridge `Escalation`,
      // but serializes the object verbatim; the Worker only owns this subset.
      const block = buildEscalationText(envelope as unknown as EscalationLike);
      return { text: block, instruction: ESCALATE_INSTRUCTION };
    },
  });

  ctx.tools.register({
    name: "gethen_report",
    description:
      "Report the current Assignment state to the Master; a final ready/blocked/failed/" +
      `waiting/dropped report ends the turn (worker: ${worker}).`,
    parameters: {
      kind: {
        type: "string",
        required: true,
        enum: [...REPORT_KINDS],
        description: "Report kind; mirrors the Master's ReportKind.",
      },
      summary: {
        type: "string",
        required: true,
        description: "One-paragraph summary of what happened.",
      },
      question: {
        type: "string",
        required: false,
        description: "One concrete question when a person must decide.",
      },
    },
    output: {
      schema: objectSchema({ text: { type: "string" } }, ["text"]),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      const args = raw as { kind?: unknown; summary?: unknown; question?: unknown };
      if (!isReportKind(args.kind)) {
        throw new Error(`report "kind" must be one of: ${REPORT_KINDS.join(", ")}`);
      }
      const report: ReportLike = {
        kind: args.kind,
        summary: requireText(args.summary, 'report "summary"'),
        at: new Date().toISOString(),
      };
      if (typeof args.question === "string" && args.question.trim().length > 0) {
        report.question = args.question;
      }
      return { text: buildReportText(report) };
    },
  });
}

// Cordis plugin surface: lets a Worker profile mount this file directly as a
// bundle entry (name/inject/apply are what the profile loader expects).
export const name = workerName;
export const inject = ["tools"];

export function apply(ctx: WorkerDshContext, config: WorkerConfig = {}): void {
  applyWorker(ctx, config);
}

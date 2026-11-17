/**
 * `gethen_ticket` tool: acknowledge, close or archive one Ticket.
 *
 * The lifecycle rules and persistence live in `ControlPlane`; this file only
 * validates tool arguments and names the actions the master exposes.
 */
import type { ControlPlane, ControlTicketAction } from "@gethen/master-runtime";
import type { DshContext, DshToolDefinition } from "./index";

export const TICKET_ACTIONS: readonly (ControlTicketAction | "answer")[] = [
  "ack",
  "close",
  "archive",
  "answer",
];

const CLOSE_OUTCOMES = ["completed", "closed", "merged", "dropped"] as const;
type CloseOutcome = (typeof CLOSE_OUTCOMES)[number];

/** Register `gethen_ticket` against the shared control surface. */
export function registerTicketTool(
  ctx: DshContext,
  control: Pick<ControlPlane, "acknowledge" | "close" | "archive" | "answer">,
): void {
  const definition: DshToolDefinition = {
    name: "gethen_ticket",
    description:
      "Move one Ticket through its lifecycle: ack (mark the current needs-action state " +
      "seen), close (terminal; default outcome completed), archive (hide a closed Ticket " +
      "from the default overview) or answer (resolve the escalation that parked a Worker " +
      "and deliver the text to it). Closing is idempotent for the same outcome.",
    parameters: {
      action: {
        type: "string",
        required: true,
        enum: [...TICKET_ACTIONS],
        description: "ack | close | archive",
      },
      ticketId: {
        type: "string",
        required: true,
        description: "Ticket id from gethen_overview or gethen_status.",
      },
      outcome: {
        type: "string",
        required: false,
        enum: [...CLOSE_OUTCOMES],
        description: "close only; defaults to completed.",
      },
      reason: {
        type: "string",
        required: false,
        description: "close only; forwarded to a best-effort Worker cancel.",
      },
      text: {
        type: "string",
        required: false,
        description: "answer only; the person's answer, delivered to the parked Worker.",
      },
      option: {
        type: "string",
        required: false,
        description: "answer only; one of the escalation's declared option ids.",
      },
      escalationId: {
        type: "string",
        required: false,
        description: "answer only; disambiguates multiple escalations on one Ticket.",
      },
    },
    output: {
      schema: objectSchema(
        {
          action: { type: "string", enum: [...TICKET_ACTIONS] },
          ticket: { type: "object" },
          escalation: { type: "object" },
          delivered: { type: "boolean" },
          assignmentId: { type: "string" },
        },
        ["action", "ticket"],
      ),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      if (typeof raw !== "object" || raw === null) {
        throw new Error("gethen_ticket requires an argument object");
      }
      const args = raw as {
        action?: unknown;
        ticketId?: unknown;
        outcome?: unknown;
        reason?: unknown;
        text?: unknown;
        option?: unknown;
        escalationId?: unknown;
      };
      const action = parseAction(args.action);
      const ticketId = requireText(args.ticketId, 'gethen_ticket requires a non-empty "ticketId"');
      if (action === "answer") {
        const text = requireText(args.text, 'gethen_ticket "text" is required for answer');
        const option = optionalText(args.option, 'gethen_ticket "option"');
        const escalationId = optionalText(args.escalationId, 'gethen_ticket "escalationId"');
        return {
          action,
          ...(await control.answer({
            ticketId,
            text,
            ...(option === undefined ? {} : { option }),
            ...(escalationId === undefined ? {} : { escalationId }),
          })),
        };
      }
      if (action === "ack") {
        return { action, ticket: control.acknowledge(ticketId) };
      }
      if (action === "archive") {
        return { action, ticket: control.archive(ticketId) };
      }
      const outcome = optionalOutcome(args.outcome);
      const reason = optionalReason(args.reason);
      return {
        action,
        ticket: await control.close({
          ticketId,
          ...(outcome === undefined ? {} : { outcome }),
          ...(reason === undefined ? {} : { reason }),
        }),
      };
    },
  };
  ctx.tools.register(definition);
}

function text(value: unknown): Array<{ type: "text"; text: string }> {
  return [{ type: "text", text: JSON.stringify(value) }];
}

function objectSchema(
  properties: Record<string, unknown>,
  required: string[],
): Record<string, unknown> {
  return { type: "object", properties, required, additionalProperties: false };
}

function parseAction(value: unknown): ControlTicketAction | "answer" {
  if (typeof value !== "string" || !(TICKET_ACTIONS as readonly string[]).includes(value)) {
    throw new Error(`gethen_ticket "action" must be one of: ${TICKET_ACTIONS.join(", ")}`);
  }
  return value as ControlTicketAction | "answer";
}

function requireText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value;
}

function optionalOutcome(value: unknown): CloseOutcome | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !(CLOSE_OUTCOMES as readonly string[]).includes(value)) {
    throw new Error(`gethen_ticket "outcome" must be one of: ${CLOSE_OUTCOMES.join(", ")}`);
  }
  return value as CloseOutcome;
}

function optionalReason(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  return requireText(value, 'gethen_ticket "reason"');
}

function optionalText(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined;
  return requireText(value, label);
}

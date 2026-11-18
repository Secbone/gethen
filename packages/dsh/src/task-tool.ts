/**
 * `gethen_task` tool: inspect, observe and steer one live Worker assignment.
 *
 * Argument validation lives here because tool errors should name the missing
 * field; execution lives in `ControlPlane.task`, shared with the HTTP routes so
 * both surfaces answer and fail identically.
 */
import type { ControlPlane, ControlTaskRequest } from "@gethen/master-runtime";
import type { DshContext, DshToolDefinition } from "./index";

export const TASK_ACTIONS = [
  "status",
  "steer",
  "cancel",
  "logs",
  "sessions",
  "history",
  "answer",
] as const;
export type TaskAction = (typeof TASK_ACTIONS)[number];

/** Register `gethen_task` against the shared control surface. */
export function registerTaskTool(ctx: DshContext, control: Pick<ControlPlane, "task">): void {
  const definition: DshToolDefinition = {
    name: "gethen_task",
    description:
      "Inspect, observe or control one live Worker assignment via the mailbox runtime: " +
      "action=status (cached state first, then ask the Worker) | steer (send a text " +
      "instruction) | cancel (ask the Worker to stop) | logs (cached task events) | " +
      "sessions (Worker session directory) | history (one session's persisted events) | " +
      "answer (answer a parked escalation and let its late result update the ticket). " +
      "Not available with the fake launch.",
    parameters: {
      action: {
        type: "string",
        required: true,
        enum: [...TASK_ACTIONS],
        description: "status | steer | cancel | logs | sessions | history | answer",
      },
      assignmentId: {
        type: "string",
        required: false,
        description:
          "Assignment id reported by gethen_status/gethen_overview; required for " +
          "status/steer/cancel/logs.",
      },
      sessionId: {
        type: "string",
        required: false,
        description: "Session id; required for action=history.",
      },
      text: {
        type: "string",
        required: false,
        description: "Steer instruction; required for action=steer.",
      },
      reason: {
        type: "string",
        required: false,
        description: "Optional cancel reason; used for action=cancel.",
      },
      timeoutMs: {
        type: "number",
        required: false,
        description: "How long a Worker round-trip (status/sessions/history) may take.",
      },
      limit: {
        type: "number",
        required: false,
        description: "Page size for action=logs/history.",
      },
      afterSeq: {
        type: "number",
        required: false,
        description: "Only events with seq > afterSeq (logs/history).",
      },
      type: {
        type: "string",
        required: false,
        description: "Only events of this type (action=logs).",
      },
      kind: {
        type: "string",
        required: false,
        enum: ["answer", "escalate", "drop"],
        description: "Escalation resolution kind for action=answer (default answer).",
      },
      option: {
        type: "string",
        required: false,
        description: "One of the escalation's declared option ids (action=answer).",
      },
      escalationId: {
        type: "string",
        required: false,
        description: "Target a specific escalation (action=answer).",
      },
    },
    output: {
      schema: objectSchema(
        {
          action: { type: "string", enum: [...TASK_ACTIONS] },
          assignmentId: { type: "string" },
          state: { type: "object", description: "Live TaskState; null when the Worker has none." },
          cached: { type: "boolean" },
          message: { type: "string" },
          text: { type: "string" },
          steered: { type: "boolean" },
          cancelled: { type: "boolean" },
          reason: { type: "string" },
          events: { type: "array", items: { type: "object" } },
          count: { type: "number" },
          dropped: { type: "number" },
          sessions: { type: "array", items: { type: "object" } },
          sessionId: { type: "string" },
          at: { type: "string" },
          escalation: { type: "object", description: "Resolved escalation record." },
          scheduling: {
            type: "object",
            description: "Live queued/running/reused/queue-timeout counters (action=status).",
          },
          settlements: {
            type: "object",
            description: "Direct vs status-recovered completion counters (action=status).",
          },
        },
        ["action"],
      ),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      const request = taskRequest(raw);
      return control.task(request);
    },
  };
  ctx.tools.register(definition);
}

// ---------------------------------------------------------------------- helpers

function text(value: unknown): Array<{ type: "text"; text: string }> {
  return [{ type: "text", text: JSON.stringify(value) }];
}

function objectSchema(
  properties: Record<string, unknown>,
  required: string[],
): Record<string, unknown> {
  return { type: "object", properties, required, additionalProperties: false };
}

function parseAction(value: unknown): TaskAction {
  if (typeof value !== "string" || !(TASK_ACTIONS as readonly string[]).includes(value)) {
    throw new Error(`gethen_task "action" must be one of: ${TASK_ACTIONS.join(", ")}`);
  }
  return value as TaskAction;
}

interface RawTaskArgs {
  action?: unknown;
  assignmentId?: unknown;
  sessionId?: unknown;
  text?: unknown;
  reason?: unknown;
  timeoutMs?: unknown;
  limit?: unknown;
  afterSeq?: unknown;
  type?: unknown;
  kind?: unknown;
  option?: unknown;
  escalationId?: unknown;
}

/** Validate tool arguments and turn them into one control request. */
export function taskRequest(raw: unknown): ControlTaskRequest {
  if (typeof raw !== "object" || raw === null) {
    throw new Error("gethen_task requires an argument object");
  }
  const args = raw as RawTaskArgs;
  const action = parseAction(args.action);

  switch (action) {
    case "status": {
      const assignmentId = requireText(
        args.assignmentId,
        'gethen_task status requires "assignmentId"',
      );
      const timeoutMs = optionalTimeout(args.timeoutMs);
      return { action, assignmentId, ...(timeoutMs === undefined ? {} : { timeoutMs }) };
    }
    case "steer": {
      const assignmentId = requireText(
        args.assignmentId,
        'gethen_task steer requires "assignmentId"',
      );
      const text = requireText(args.text, 'gethen_task steer requires a non-empty "text"');
      return { action, assignmentId, text };
    }
    case "cancel": {
      const assignmentId = requireText(
        args.assignmentId,
        'gethen_task cancel requires "assignmentId"',
      );
      const reason = optionalReason(args.reason);
      return { action, assignmentId, ...(reason === undefined ? {} : { reason }) };
    }
    case "logs": {
      const assignmentId = requireText(
        args.assignmentId,
        'gethen_task logs requires "assignmentId"',
      );
      return {
        action,
        assignmentId,
        ...optionalLimit(args.limit, 'gethen_task "limit"'),
        ...optionalAfterSeq(args.afterSeq),
        ...(args.type === undefined ? {} : { type: requireText(args.type, 'gethen_task "type"') }),
      };
    }
    case "sessions": {
      const timeoutMs = optionalTimeout(args.timeoutMs);
      return { action, ...(timeoutMs === undefined ? {} : { timeoutMs }) };
    }
    case "answer": {
      const assignmentId = requireText(
        args.assignmentId,
        'gethen_task answer requires "assignmentId"',
      );
      const text = requireText(args.text, 'gethen_task answer requires a non-empty "text"');
      const kind = optionalKind(args.kind);
      const option = optionalOption(args.option);
      const escalationId = optionalOption(args.escalationId);
      return {
        action,
        assignmentId,
        resolution: {
          kind,
          text,
          ...(option === undefined ? {} : { option }),
        },
        ...(escalationId === undefined ? {} : { escalationId }),
      };
    }
    case "history": {
      const sessionId = requireText(args.sessionId, 'gethen_task history requires "sessionId"');
      const timeoutMs = optionalTimeout(args.timeoutMs);
      return {
        action,
        sessionId,
        ...optionalLimit(args.limit, 'gethen_task "limit"'),
        ...optionalAfterSeq(args.afterSeq),
        ...(timeoutMs === undefined ? {} : { timeoutMs }),
      };
    }
  }
}

function requireText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value;
}

function optionalKind(value: unknown): "answer" | "escalate" | "drop" {
  if (value === undefined) return "answer";
  if (value === "answer" || value === "escalate" || value === "drop") return value;
  throw new Error('gethen_task "kind" must be "answer", "escalate" or "drop"');
}

function optionalOption(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  return requireText(value, 'gethen_task "option"');
}

function optionalReason(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  return requireText(value, 'gethen_task "reason"');
}

/**
 * Tool hosts may JSON-encode numeric arguments as strings (`"2"`); accept both
 * forms, matching `gethen_wait`. A non-numeric string is not a number.
 */
function numeric(value: unknown): number | undefined {
  if (typeof value === "number") return Number.isFinite(value) ? value : undefined;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function optionalTimeout(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  const parsed = numeric(value);
  if (parsed === undefined || parsed <= 0) {
    throw new Error('gethen_task "timeoutMs" must be a positive number');
  }
  return parsed;
}

function optionalLimit(value: unknown, label: string): { limit?: number } {
  if (value === undefined) return {};
  const parsed = numeric(value);
  if (parsed === undefined || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
  return { limit: parsed };
}

function optionalAfterSeq(value: unknown): { afterSeq?: number } {
  if (value === undefined) return {};
  const parsed = numeric(value);
  if (parsed === undefined || !Number.isInteger(parsed) || parsed < 0) {
    throw new Error('gethen_task "afterSeq" must be a non-negative integer');
  }
  return { afterSeq: parsed };
}

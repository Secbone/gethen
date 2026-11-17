/**
 * `gethen_task` tool: inspect and steer one live Worker assignment.
 *
 * The control surface lives in `@gethen/master-runtime` (`runtime.tasks`) and
 * requires the mailbox transport: in fake-launch mode there is no worker to ask
 * or steer, so calls fail with a clear actionable error instead of a raw
 * transport code. The tool is a thin adapter — it validates arguments, prefers
 * the cached live state (`tasks.state`) over a round-trip, then asks the Worker
 * (`tasks.status`) and maps transport failures to human-readable messages.
 */
import {
  MasterRuntimeError,
  type MasterRuntimeTasks,
  type TaskState,
} from "@gethen/master-runtime";
import type { DshContext, DshToolDefinition } from "./index";

export const TASK_ACTIONS = ["status", "steer", "cancel"] as const;
export type TaskAction = (typeof TASK_ACTIONS)[number];

/** A compact list of live tasks for `gethen_status` (bounded, newest first). */
export interface TaskSummary {
  count: number;
  recent: Array<{
    assignmentId: string;
    phase: string;
    step: number;
    lastEventAt: string;
    lastMessage?: string;
  }>;
}

const RECENT_TASKS_LIMIT = 5;

/**
 * Summarize the cached task states for `gethen_status`. Never throws: fake
 * mode (`RUNTIME_COMMS_UNAVAILABLE`) and a closed runtime report no summary.
 */
export function taskSummary(tasks: MasterRuntimeTasks | undefined): TaskSummary | undefined {
  if (tasks === undefined) return undefined;
  let states: Record<string, TaskState>;
  try {
    states = tasks.states();
  } catch {
    return undefined;
  }
  const entries = Object.entries(states);
  entries.sort((left, right) => {
    const leftAt = left[1].updatedAt ?? left[1].lastEventAt;
    const rightAt = right[1].updatedAt ?? right[1].lastEventAt;
    return rightAt.localeCompare(leftAt);
  });
  return {
    count: entries.length,
    recent: entries.slice(0, RECENT_TASKS_LIMIT).map(([assignmentId, state]) => ({
      assignmentId,
      phase: state.phase,
      step: state.step,
      lastEventAt: state.lastEventAt,
      ...(state.lastMessage === undefined ? {} : { lastMessage: state.lastMessage }),
    })),
  };
}

/** Register `gethen_task` against one runtime task control surface. */
export function registerTaskTool(ctx: DshContext, tasks: MasterRuntimeTasks): void {
  const definition: DshToolDefinition = {
    name: "gethen_task",
    description:
      "Inspect or control one live Worker assignment via the mailbox runtime: " +
      "action=status (cached state first, then ask the Worker) | steer (send a text " +
      "instruction) | cancel (ask the Worker to stop). Not available with the fake launch.",
    parameters: {
      action: {
        type: "string",
        required: true,
        enum: [...TASK_ACTIONS],
        description: "status | steer | cancel",
      },
      assignmentId: {
        type: "string",
        required: true,
        description: "Assignment id reported by gethen_status/gethen_overview.",
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
        description: "How long action=status waits for a Worker answer (default 5000).",
      },
    },
    output: {
      schema: objectSchema(
        {
          assignmentId: { type: "string" },
          action: { type: "string", enum: [...TASK_ACTIONS] },
          state: { type: "object", description: "Live TaskState; null when the Worker has none." },
          cached: { type: "boolean" },
          message: { type: "string" },
          text: { type: "string" },
          steered: { type: "boolean" },
          cancelled: { type: "boolean" },
          reason: { type: "string" },
        },
        ["assignmentId", "action"],
      ),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      const args = raw as {
        action?: unknown;
        assignmentId?: unknown;
        text?: unknown;
        reason?: unknown;
        timeoutMs?: unknown;
      };
      const action = parseAction(args.action);
      const assignmentId = requireText(args.assignmentId, 'gethen_task "assignmentId"');

      switch (action) {
        case "status": {
          const timeoutMs = optionalTimeout(args.timeoutMs);
          let cached: TaskState | undefined;
          try {
            cached = tasks.state(assignmentId);
          } catch (error) {
            throw taskFailure(error, assignmentId);
          }
          if (cached !== undefined) {
            return { assignmentId, action, state: cached, cached: true };
          }
          let live: TaskState | null;
          try {
            live = await tasks.status(
              assignmentId,
              timeoutMs === undefined ? undefined : { timeoutMs },
            );
          } catch (error) {
            throw taskFailure(error, assignmentId);
          }
          return {
            assignmentId,
            action,
            state: live,
            cached: false,
            ...(live === null
              ? { message: `no live task state for assignment ${assignmentId}` }
              : {}),
          };
        }
        case "steer": {
          const text = requireText(args.text, 'gethen_task steer requires a non-empty "text"');
          try {
            await tasks.steer(assignmentId, text);
          } catch (error) {
            throw taskFailure(error, assignmentId);
          }
          return { assignmentId, action, text, steered: true };
        }
        case "cancel": {
          const reason = optionalReason(args.reason);
          try {
            await tasks.cancel(assignmentId, reason);
          } catch (error) {
            throw taskFailure(error, assignmentId);
          }
          return {
            assignmentId,
            action,
            cancelled: true,
            ...(reason === undefined ? {} : { reason }),
          };
        }
      }
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

function requireText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value;
}

function optionalReason(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  return requireText(value, 'gethen_task "reason"');
}

function optionalTimeout(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new Error('gethen_task "timeoutMs" must be a positive number');
  }
  return value;
}

/**
 * Translate runtime/transport failures into actionable tool errors. The fake
 * launch has no worker, so `RUNTIME_COMMS_UNAVAILABLE` is a configuration
 * answer rather than a transport bug; a status timeout usually means the
 * assignment is unknown, finished or its Worker is offline.
 */
function taskFailure(error: unknown, assignmentId: string): Error {
  if (error instanceof MasterRuntimeError) {
    if (error.code === "RUNTIME_COMMS_UNAVAILABLE") {
      return new Error(
        "gethen_task: task state/control requires the mailbox runtime; the fake launch " +
          "has no worker to inspect or control (configure ws worker tokens)",
      );
    }
    if (error.code === "RUNTIME_CLOSED") {
      return new Error("gethen_task: the master runtime is closed");
    }
  }
  const code = isRecord(error) ? error.code : undefined;
  if (code === "MAILBOX_STATUS_TIMEOUT") {
    return new Error(
      `gethen_task: assignment ${assignmentId} did not answer a status request in time; ` +
        "it may be unknown, finished, or its Worker may be offline",
    );
  }
  if (code === "MAILBOX_CLOSED") {
    return new Error("gethen_task: the mailbox bridge is closed");
  }
  return error instanceof Error ? error : new Error(String(error));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

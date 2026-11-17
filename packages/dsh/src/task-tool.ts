/**
 * `gethen_task` tool: inspect, observe and steer one live Worker assignment.
 *
 * The control surface lives in `@gethen/master-runtime` (`runtime.tasks`) and
 * requires the mailbox transport: in fake-launch mode there is no worker to ask
 * or steer, so calls fail with a clear actionable error instead of a raw
 * transport code. The tool is a thin adapter — it validates arguments, prefers
 * the cached live state (`tasks.state`) over a round-trip, then asks the Worker
 * (`tasks.status`) and maps transport failures to human-readable messages.
 *
 * Actions:
 * - `status`  cached TaskState, else a Worker round-trip;
 * - `steer`   append an instruction to the running assignment;
 * - `cancel`  ask the Worker to stop;
 * - `logs`    cached task events (`events`/`eventCount`), with filters;
 * - `sessions` the Worker's harness session directory;
 * - `history` one session's persisted event history.
 */
import {
  MasterRuntimeError,
  type MasterRuntimeTasks,
  type TaskEventQuery,
  type TaskState,
} from "@gethen/master-runtime";
import type { DshContext, DshToolDefinition } from "./index";

export const TASK_ACTIONS = ["status", "steer", "cancel", "logs", "sessions", "history"] as const;
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
      "Inspect, observe or control one live Worker assignment via the mailbox runtime: " +
      "action=status (cached state first, then ask the Worker) | steer (send a text " +
      "instruction) | cancel (ask the Worker to stop) | logs (cached task events) | " +
      "sessions (Worker session directory) | history (one session's persisted events). " +
      "Not available with the fake launch.",
    parameters: {
      action: {
        type: "string",
        required: true,
        enum: [...TASK_ACTIONS],
        description: "status | steer | cancel | logs | sessions | history",
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
        },
        ["action"],
      ),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      const args = raw as {
        action?: unknown;
        assignmentId?: unknown;
        sessionId?: unknown;
        text?: unknown;
        reason?: unknown;
        timeoutMs?: unknown;
        limit?: unknown;
        afterSeq?: unknown;
        type?: unknown;
      };
      const action = parseAction(args.action);

      switch (action) {
        case "status": {
          const assignmentId = requireText(
            args.assignmentId,
            'gethen_task status requires "assignmentId"',
          );
          const timeoutMs = optionalTimeout(args.timeoutMs);
          let cached: TaskState | undefined;
          try {
            cached = tasks.state(assignmentId);
          } catch (error) {
            throw taskFailure(
              error,
              `assignment ${assignmentId} did not answer a status request in time; ` +
                "it may be unknown, finished, or its Worker may be offline",
            );
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
            throw taskFailure(
              error,
              `assignment ${assignmentId} did not answer a status request in time; ` +
                "it may be unknown, finished, or its Worker may be offline",
            );
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
          const assignmentId = requireText(
            args.assignmentId,
            'gethen_task steer requires "assignmentId"',
          );
          const text = requireText(args.text, 'gethen_task steer requires a non-empty "text"');
          try {
            await tasks.steer(assignmentId, text);
          } catch (error) {
            throw taskFailure(error, "the Worker did not accept the steer instruction in time");
          }
          return { assignmentId, action, text, steered: true };
        }
        case "cancel": {
          const assignmentId = requireText(
            args.assignmentId,
            'gethen_task cancel requires "assignmentId"',
          );
          const reason = optionalReason(args.reason);
          try {
            await tasks.cancel(assignmentId, reason);
          } catch (error) {
            throw taskFailure(error, "the Worker did not accept the cancel request in time");
          }
          return {
            assignmentId,
            action,
            cancelled: true,
            ...(reason === undefined ? {} : { reason }),
          };
        }
        case "logs": {
          const assignmentId = requireText(
            args.assignmentId,
            'gethen_task logs requires "assignmentId"',
          );
          const query = eventQuery(args);
          try {
            const events = tasks.events(assignmentId, query);
            const count = tasks.eventCount(assignmentId);
            const dropped = tasks.eventDropped(assignmentId);
            return {
              assignmentId,
              action,
              events,
              count,
              ...(dropped === 0 ? {} : { dropped }),
            };
          } catch (error) {
            throw taskFailure(error, `assignment ${assignmentId} has no cached task events`);
          }
        }
        case "sessions": {
          const timeoutMs = optionalTimeout(args.timeoutMs);
          try {
            const sessions = await tasks.sessions(
              timeoutMs === undefined ? undefined : { timeoutMs },
            );
            return { action, sessions };
          } catch (error) {
            throw taskFailure(
              error,
              "the Worker did not answer the session list request in time; it may be offline",
            );
          }
        }
        case "history": {
          const sessionId = requireText(args.sessionId, 'gethen_task history requires "sessionId"');
          const options = {
            ...optionalLimit(args.limit, 'gethen_task "limit"'),
            ...optionalAfterSeq(args.afterSeq),
            ...optionalTimeoutOption(args.timeoutMs),
          };
          try {
            const history = await tasks.sessionHistory(
              sessionId,
              Object.keys(options).length === 0 ? undefined : options,
            );
            return {
              action,
              sessionId: history.sessionId,
              events: history.events,
              ...(history.at === undefined ? {} : { at: history.at }),
            };
          } catch (error) {
            throw taskFailure(
              error,
              `session ${sessionId} did not answer the history request in time; ` +
                "it may be unknown, finished, or its Worker may be offline",
            );
          }
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

function optionalTimeoutOption(value: unknown): { timeoutMs?: number } {
  const timeoutMs = optionalTimeout(value);
  return timeoutMs === undefined ? {} : { timeoutMs };
}

function optionalLimit(value: unknown, label: string): { limit?: number } {
  if (value === undefined) return {};
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
  return { limit: value };
}

function optionalAfterSeq(value: unknown): { afterSeq?: number } {
  if (value === undefined) return {};
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error('gethen_task "afterSeq" must be a non-negative integer');
  }
  return { afterSeq: value };
}

function eventQuery(args: {
  limit?: unknown;
  afterSeq?: unknown;
  type?: unknown;
}): TaskEventQuery | undefined {
  const query: TaskEventQuery = {
    ...optionalLimit(args.limit, 'gethen_task "limit"'),
    ...optionalAfterSeq(args.afterSeq),
    ...(args.type === undefined ? {} : { type: requireText(args.type, 'gethen_task "type"') }),
  };
  return Object.keys(query).length === 0 ? undefined : query;
}

/**
 * Translate runtime/transport failures into actionable tool errors. The fake
 * launch has no worker, so `RUNTIME_COMMS_UNAVAILABLE` is a configuration
 * answer rather than a transport bug; a request timeout usually means the
 * assignment/session is unknown or its Worker is offline.
 */
function taskFailure(error: unknown, timeoutMessage: string): Error {
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
    return new Error(`gethen_task: ${timeoutMessage}`);
  }
  if (code === "MAILBOX_CLOSED") {
    return new Error("gethen_task: the mailbox bridge is closed");
  }
  return error instanceof Error ? error : new Error(String(error));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

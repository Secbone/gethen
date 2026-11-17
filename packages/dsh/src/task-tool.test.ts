import {
  createTaskControl,
  type HarnessSessionSummary,
  MasterRuntimeError,
  type MasterRuntimeTasks,
  type TaskEvent,
  type TaskState,
  taskSummary,
} from "@gethen/master-runtime";
import { describe, expect, it, vi } from "vitest";
import type { DshContext, DshToolDefinition } from "./index";
import { registerTaskTool, TASK_ACTIONS } from "./task-tool";

const STATE: TaskState = {
  phase: "tool",
  step: 3,
  lastTool: { name: "bash", startedAt: "2026-11-17T06:00:00.000Z" },
  lastEventAt: "2026-11-17T06:00:01.000Z",
  lastMessage: "running the test suite",
  seq: 7,
  updatedAt: "2026-11-17T06:00:01.500Z",
};

const EVENTS: TaskEvent[] = [
  { seq: 1, type: "message", at: "2026-11-17T06:00:00.000Z", data: { text: "hi" } },
  { seq: 2, type: "tool.start", at: "2026-11-17T06:00:01.000Z", data: { name: "bash" } },
];

const SESSIONS: HarnessSessionSummary[] = [
  { sessionId: "session-1", key: "k1", state: "idle", eventCount: 4, hasLog: true },
];

function context() {
  const tools: DshToolDefinition[] = [];
  const ctx: DshContext = {
    provide() {
      return () => {};
    },
    tools: {
      register(definition) {
        tools.push(definition);
        return () => {};
      },
    },
  };
  return { ctx, tools };
}

function stubTasks(overrides: Partial<MasterRuntimeTasks> = {}): MasterRuntimeTasks {
  return {
    states: () => ({}),
    state: () => undefined,
    status: async () => null,
    steer: async () => {},
    cancel: async () => {},
    events: () => [],
    eventCount: () => 0,
    eventDropped: () => 0,
    sessions: async () => [],
    sessionHistory: async (sessionId) => ({ sessionId, events: [] }),
    ...overrides,
  };
}

function registered(tasks: MasterRuntimeTasks = stubTasks()): DshToolDefinition {
  const { ctx, tools } = context();
  registerTaskTool(ctx, createTaskControl(tasks));
  const definition = tools[0];
  if (definition === undefined) throw new Error("gethen_task was not registered");
  return definition;
}

async function call(
  tool: DshToolDefinition,
  args: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  return (await tool.execute(args, {})) as Record<string, unknown>;
}

describe("gethen_task tool", () => {
  it("registers the extended schema", () => {
    const tool = registered();
    expect(tool.name).toBe("gethen_task");
    expect(tool.parameters).toMatchObject({
      action: { type: "string", required: true, enum: [...TASK_ACTIONS] },
      assignmentId: { type: "string", required: false },
      sessionId: { type: "string", required: false },
      text: { type: "string", required: false },
      reason: { type: "string", required: false },
      timeoutMs: { type: "number", required: false },
      limit: { type: "number", required: false },
      afterSeq: { type: "number", required: false },
      type: { type: "string", required: false },
    });
    expect([...TASK_ACTIONS]).toEqual(["status", "steer", "cancel", "logs", "sessions", "history"]);
    const schema = tool.output.schema as {
      properties: Record<string, Record<string, unknown>>;
      required: string[];
    };
    expect(Object.keys(schema.properties)).toEqual(
      expect.arrayContaining([
        "action",
        "assignmentId",
        "state",
        "events",
        "count",
        "sessions",
        "sessionId",
      ]),
    );
    expect(schema.required).toEqual(["action"]);
  });

  it("rejects an unknown action and missing per-action identifiers", async () => {
    const tool = registered();
    await expect(call(tool, { action: "pause", assignmentId: "a1" })).rejects.toThrow(/action/);
    await expect(call(tool, { action: "status" })).rejects.toThrow(/assignmentId/);
    await expect(call(tool, { action: "logs" })).rejects.toThrow(/assignmentId/);
    await expect(call(tool, { action: "history" })).rejects.toThrow(/sessionId/);
  });

  it("status prefers the cached live state", async () => {
    const status = vi.fn(async () => STATE);
    const tool = registered(stubTasks({ state: () => STATE, status }));
    await expect(call(tool, { action: "status", assignmentId: "a1" })).resolves.toEqual({
      assignmentId: "a1",
      action: "status",
      state: STATE,
      cached: true,
    });
    expect(status).not.toHaveBeenCalled();
  });

  it("status falls back to a Worker round-trip", async () => {
    const status = vi.fn(async () => STATE);
    const tool = registered(stubTasks({ state: () => undefined, status }));
    await expect(
      call(tool, { action: "status", assignmentId: "a1", timeoutMs: 250 }),
    ).resolves.toEqual({
      assignmentId: "a1",
      action: "status",
      state: STATE,
      cached: false,
    });
    expect(status).toHaveBeenCalledWith("a1", { timeoutMs: 250 });
  });

  it("status reports null when the Worker has no live state", async () => {
    const tool = registered(stubTasks({ status: async () => null }));
    await expect(call(tool, { action: "status", assignmentId: "a1" })).resolves.toEqual({
      assignmentId: "a1",
      action: "status",
      state: null,
      cached: false,
      message: "no live task state for assignment a1",
    });
  });

  it("maps fake-mode, closed and timeout failures to clear errors", async () => {
    const fake = registered(
      stubTasks({
        state: () => {
          throw new MasterRuntimeError("RUNTIME_COMMS_UNAVAILABLE", "no mailbox");
        },
      }),
    );
    await expect(call(fake, { action: "status", assignmentId: "a1" })).rejects.toThrow(
      /requires the mailbox runtime/,
    );

    const closed = registered(
      stubTasks({
        state: () => {
          throw new MasterRuntimeError("RUNTIME_CLOSED", "closed");
        },
      }),
    );
    await expect(call(closed, { action: "status", assignmentId: "a1" })).rejects.toThrow(
      /master runtime is closed/,
    );

    const timeout = registered(
      stubTasks({
        status: async () => {
          throw Object.assign(new Error("timed out"), { code: "MAILBOX_STATUS_TIMEOUT" });
        },
      }),
    );
    await expect(call(timeout, { action: "status", assignmentId: "a1" })).rejects.toThrow(
      /assignment a1 did not answer a status request/,
    );
  });

  it("steer rejects empty text and forwards a valid instruction", async () => {
    const steer = vi.fn(async () => {});
    const tool = registered(stubTasks({ steer }));
    await expect(call(tool, { action: "steer", assignmentId: "a1", text: "  " })).rejects.toThrow(
      /non-empty "text"/,
    );
    await expect(call(tool, { action: "steer", assignmentId: "a1" })).rejects.toThrow(/text/);
    expect(steer).not.toHaveBeenCalled();
    await expect(
      call(tool, { action: "steer", assignmentId: "a1", text: "focus on the failing test" }),
    ).resolves.toEqual({
      assignmentId: "a1",
      action: "steer",
      text: "focus on the failing test",
      steered: true,
    });
    expect(steer).toHaveBeenCalledWith("a1", "focus on the failing test");
  });

  it("cancel forwards the optional reason", async () => {
    const cancel = vi.fn(async () => {});
    const tool = registered(stubTasks({ cancel }));
    await expect(call(tool, { action: "cancel", assignmentId: "a1" })).resolves.toEqual({
      assignmentId: "a1",
      action: "cancel",
      cancelled: true,
    });
    expect(cancel).toHaveBeenLastCalledWith("a1", undefined);
    await expect(
      call(tool, { action: "cancel", assignmentId: "a1", reason: "user asked" }),
    ).resolves.toEqual({
      assignmentId: "a1",
      action: "cancel",
      cancelled: true,
      reason: "user asked",
    });
    expect(cancel).toHaveBeenLastCalledWith("a1", "user asked");
  });

  it("logs returns the cached event page and total count", async () => {
    const events = vi.fn((_id: string, _query?: unknown) => EVENTS);
    const eventCount = vi.fn(() => 9);
    const tool = registered(stubTasks({ events, eventCount, eventDropped: () => 0 }));
    await expect(
      call(tool, {
        action: "logs",
        assignmentId: "a1",
        limit: 2,
        afterSeq: 0,
        type: "tool.start",
      }),
    ).resolves.toEqual({
      assignmentId: "a1",
      action: "logs",
      events: EVENTS,
      count: 9,
    });
    expect(events).toHaveBeenCalledWith("a1", { limit: 2, afterSeq: 0, type: "tool.start" });
    await call(tool, { action: "logs", assignmentId: "a1" });
    expect(events).toHaveBeenLastCalledWith("a1", undefined);
  });

  it("logs surfaces dropped events and validates filters", async () => {
    const tool = registered(stubTasks({ events: () => EVENTS, eventDropped: () => 4 }));
    await expect(call(tool, { action: "logs", assignmentId: "a1" })).resolves.toMatchObject({
      action: "logs",
      dropped: 4,
    });
    await expect(call(tool, { action: "logs", assignmentId: "a1", limit: 0 })).rejects.toThrow(
      /positive integer/,
    );
    await expect(call(tool, { action: "logs", assignmentId: "a1", afterSeq: -1 })).rejects.toThrow(
      /non-negative integer/,
    );
    await expect(call(tool, { action: "logs", assignmentId: "a1", type: "  " })).rejects.toThrow(
      /"type"/,
    );
  });

  it("sessions returns the Worker session directory", async () => {
    const sessions = vi.fn(async (_options?: { timeoutMs?: number }) => SESSIONS);
    const tool = registered(stubTasks({ sessions }));
    await expect(call(tool, { action: "sessions", timeoutMs: 300 })).resolves.toEqual({
      action: "sessions",
      sessions: SESSIONS,
    });
    expect(sessions).toHaveBeenCalledWith({ timeoutMs: 300 });
    await call(tool, { action: "sessions" });
    expect(sessions).toHaveBeenLastCalledWith(undefined);
  });

  it("history returns one session's persisted events", async () => {
    const sessionHistory = vi.fn(async (sessionId: string, _options?: unknown) => ({
      sessionId,
      events: [{ seq: 1, type: "message", at: "2026-11-17T06:00:00.000Z" }],
      at: "2026-11-17T06:00:02.000Z",
    }));
    const tool = registered(stubTasks({ sessionHistory }));
    await expect(
      call(tool, {
        action: "history",
        sessionId: "session-1",
        limit: 50,
        afterSeq: 0,
        timeoutMs: 300,
      }),
    ).resolves.toEqual({
      action: "history",
      sessionId: "session-1",
      events: [{ seq: 1, type: "message", at: "2026-11-17T06:00:00.000Z" }],
      at: "2026-11-17T06:00:02.000Z",
    });
    expect(sessionHistory).toHaveBeenCalledWith("session-1", {
      limit: 50,
      afterSeq: 0,
      timeoutMs: 300,
    });
  });

  it("maps fake-mode and timeout failures for logs/sessions/history", async () => {
    const fake = registered(
      stubTasks({
        events: () => {
          throw new MasterRuntimeError("RUNTIME_COMMS_UNAVAILABLE", "no mailbox");
        },
      }),
    );
    await expect(call(fake, { action: "logs", assignmentId: "a1" })).rejects.toThrow(
      /requires the mailbox runtime/,
    );

    const timeout = registered(
      stubTasks({
        sessions: async () => {
          throw Object.assign(new Error("timeout"), { code: "MAILBOX_STATUS_TIMEOUT" });
        },
        sessionHistory: async () => {
          throw Object.assign(new Error("timeout"), { code: "MAILBOX_STATUS_TIMEOUT" });
        },
      }),
    );
    await expect(call(timeout, { action: "sessions" })).rejects.toThrow(
      /did not answer the session list request/,
    );
    await expect(call(timeout, { action: "history", sessionId: "s1" })).rejects.toThrow(
      /session s1 did not answer the history request/,
    );
  });
});

describe("taskSummary", () => {
  it("summarizes cached states newest-first", () => {
    const summary = taskSummary(
      stubTasks({
        states: () => ({
          older: { ...STATE, updatedAt: "2026-11-17T06:00:00.000Z", lastMessage: "older" },
          newer: { ...STATE, updatedAt: "2026-11-17T06:05:00.000Z", lastMessage: undefined },
        }),
      }),
    );
    expect(summary?.count).toBe(2);
    expect(summary?.recent.map((task) => task.assignmentId)).toEqual(["newer", "older"]);
    expect(summary?.recent[1]).toMatchObject({ assignmentId: "older", phase: "tool", step: 3 });
    expect(summary?.recent[1]?.lastMessage).toBe("older");
  });

  it("degrades to no summary in fake mode or when tasks are absent", () => {
    const fake = stubTasks({
      states: () => {
        throw new MasterRuntimeError("RUNTIME_COMMS_UNAVAILABLE", "no mailbox");
      },
    });
    expect(taskSummary(fake)).toBeUndefined();
    expect(taskSummary(undefined)).toBeUndefined();
  });
});

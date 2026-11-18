import { mkdirSync, mkdtempSync, readFileSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { DshSessionStore } from "@gethen/dsh-sessions";
import type {
  HarnessEscalation,
  HarnessEvent,
  HarnessReport,
  HarnessSink,
  HarnessTask,
  HarnessTaskState,
} from "@gethen/harness";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createSdkHarnessExecutor,
  DEFAULT_IDLE_TIMEOUT_MS,
  DEFAULT_MAX_LOG_CHARS,
  DEFAULT_SESSION_LIST_LIMIT,
  harnessReportKind,
  LOG_TRUNCATION_SUFFIX,
  mapSessionEvent,
  type SdkHarnessExecutor,
  type SdkHarnessExecutorOptions,
  STOP_STEER_TEXT,
} from "./harness-adapter";
import { FakeSdkClient } from "./test-doubles";

const AT = "2026-11-17T00:00:00.000Z";

class RecordingSink implements HarnessSink {
  readonly events: HarnessEvent[] = [];
  readonly escalations: HarnessEscalation[] = [];
  readonly reports: HarnessReport[] = [];

  event(event: HarnessEvent): void {
    this.events.push(event);
  }

  escalation(escalation: HarnessEscalation): void {
    this.escalations.push(escalation);
  }

  report(report: HarnessReport): void {
    this.reports.push(report);
  }
}

const executors: SdkHarnessExecutor[] = [];

afterEach(async () => {
  for (const executor of executors.splice(0)) await executor.close();
});

function setup(options: Partial<SdkHarnessExecutorOptions> = {}) {
  const client = new FakeSdkClient();
  const sink = new RecordingSink();
  const executor = createSdkHarnessExecutor({
    provider: "deepseek",
    model: "deepseek-v4",
    clientFactory: () => client,
    now: () => new Date(AT),
    interruptGraceMs: 0,
    sessionStore: emptySessionStore(),
    ...options,
  });
  executors.push(executor);
  return { client, sink, executor };
}

function task(overrides: Partial<HarnessTask> = {}): HarnessTask {
  return {
    assignmentId: "a1",
    ticketId: "t1",
    workspace: "/work/ws",
    prompt: "do the work",
    ...overrides,
  };
}

function emptySessionStore(): DshSessionStore {
  return new DshSessionStore({ home: mkdtempSync(join(tmpdir(), "gethen-exec-empty-")) });
}

const FIXTURE_CREATED = 1_790_000_000_000;

/**
 * Minimal DSH home fixture: one projcache record plus (optionally) a zstd
 * session log, mirroring `packages/dsh-sessions/src/store.test.ts`.
 */
function dshFixture(
  options: {
    home?: string;
    sessionId?: string;
    cwd?: string;
    openTurn?: boolean;
    withLog?: boolean;
    title?: string;
    atMs?: number;
    extraEvents?: Array<{ type: string; data?: unknown }>;
  } = {},
): { home: string; sessionId: string; store: DshSessionStore } {
  const sessionId = options.sessionId ?? "store-session";
  const cwd = options.cwd ?? "/work/project";
  const home = options.home ?? mkdtempSync(join(tmpdir(), "gethen-exec-sessions-"));
  const atMs = options.atMs ?? FIXTURE_CREATED;
  const projcache = join(home, "storages", "session_projcache", "sessions");
  mkdirSync(projcache, { recursive: true });
  writeFileSync(
    join(projcache, `${sessionId}.json`),
    JSON.stringify({
      version: 7,
      record: {
        identity: { formatVersion: 4, createdAt: atMs, cwd },
        rows: {
          turnBoundary: {
            ver: 2,
            seq: 42,
            val: { openTurnStartSeq: options.openTurn ? 40 : null },
          },
          modelSelection: {
            ver: 3,
            seq: 42,
            val: { lastUsed: { provider: "opencode-go", model: "deepseek-v4.1-flash" } },
          },
          sandboxMode: { ver: 1, seq: 42, val: "workspace-write" },
          approvalState: { ver: 1, seq: 42, val: { policy: "never" } },
          title: { ver: 1, seq: 42, val: options.title ?? "fixture task" },
          tokenUsage: { ver: 1, seq: 42, val: { inputTokens: 10, outputTokens: 20 } },
        },
      },
    }),
  );
  if (options.withLog !== false) {
    const dir = join(home, "sessions", "--work-project--", sessionId);
    mkdirSync(dir, { recursive: true });
    const lines: object[] = [
      { type: "session", version: 4, id: sessionId, createdAt: atMs, cwd },
      { type: "user/message", seq: 0, time: atMs + 1, data: { text: "hello" } },
      { type: "assistant/message", seq: 1, time: atMs + 2, data: { text: "hi" } },
      { type: "tool/call", seq: 2, time: atMs + 3, data: { name: "bash" } },
    ];
    for (const [index, event] of (options.extraEvents ?? []).entries()) {
      lines.push({ ...event, seq: 4 + index, time: atMs + 10 + index });
    }
    lines.push({
      type: "turn/end",
      seq: 3 + (options.extraEvents?.length ?? 0),
      time: atMs + 90,
      data: { turn: 1 },
    });
    writeFileSync(
      join(dir, "session.v4.jsonl.zstd"),
      zstdCompressSync(Buffer.from(lines.map((line) => JSON.stringify(line)).join("\n"))),
    );
  }
  return { home, sessionId, store: new DshSessionStore({ home }) };
}

function reportBlock(body: Record<string, unknown>): string {
  return `[GETHEN-REPORT]\n${JSON.stringify(body)}\n[/GETHEN-REPORT]`;
}

function escalationBlock(body: Record<string, unknown>): string {
  return `[GETHEN-ESCALATION]\n${JSON.stringify(body)}\n[/GETHEN-ESCALATION]`;
}

function toolResultEvent(sessionId: string, text: string) {
  return {
    sessionId,
    event: { type: "tool/result", data: { content: [{ type: "text", text }] } },
  };
}

async function tick(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

function logEvents(sink: RecordingSink): HarnessEvent[] {
  return sink.events.filter((event) => event.type === "log");
}

function logPhases(sink: RecordingSink): unknown[] {
  return logEvents(sink).map((event) => (event.data as { phase?: unknown }).phase);
}

describe("createSdkHarnessExecutor runtime pool", () => {
  it("starts one runtime, initializes it and prompts the assignment session", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);

    expect(execution).toBeDefined();
    expect(client.startCalls).toEqual([{ cwd: "/work/ws" }]);
    expect(client.initializeCalls).toEqual([
      {
        provider: "deepseek",
        model: "deepseek-v4",
        cwd: "/work/ws",
        approvalRequests: true,
        userQuestions: true,
      },
    ]);
    expect(client.prompts).toEqual([{ sessionId: "a1", text: "do the work" }]);
    expect(client.waitCalls).toEqual(["a1"]);
    expect(sink.reports).toEqual([]);
  });

  it("passes the command, args and timeout, and derives the cwd per workspace", async () => {
    const { client, sink, executor } = setup({
      dshCommand: "/opt/dsh",
      dshArgs: ["--profile", "sdk-test"],
      requestTimeoutMs: 1_234,
      cwdFor: (workspace) => `/repos${workspace}`,
    });

    await executor.start(task({ workspace: "/project" }), sink);

    expect(client.startCalls).toEqual([
      {
        cwd: "/repos/project",
        command: "/opt/dsh",
        args: ["--profile", "sdk-test"],
        requestTimeoutMs: 1_234,
      },
    ]);
    expect(client.initializeCalls[0]?.cwd).toBe("/repos/project");
  });

  it("reuses the workspace runtime and releases it on close", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task({ assignmentId: "a1" }), sink);
    await executor.start(task({ assignmentId: "a2" }), sink);

    expect(client.startCalls).toHaveLength(1);
    expect(client.initializeCalls).toHaveLength(1);
    expect(client.prompts.map((prompt) => prompt.sessionId)).toEqual(["a1", "a2"]);

    await executor.close();
    expect(client.closeCount).toBe(1);
  });
});

describe("event-driven completion", () => {
  it("completes on turn/end without waiting for the session/wait reply", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);

    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "done", at: AT }));
    client.emitSessionEvent("a1", "turn/end", { turn: 1, reason: { kind: "completed" } });
    await tick();

    expect(sink.reports).toEqual([{ kind: "ready", summary: "done" }]);
    // `wait` never replied; completion came from the event stream.
    expect(client.closeCount).toBe(0);
  });

  it("does not complete a running turn and ignores early idle status", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);
    expect(execution.state?.().phase).toBe("thinking");

    client.emitSessionEvent("a1", "step/start", { turn: 1, step: 1 });
    client.emitSessionEvent("a1", "tool/call", { turn: 1, step: 1, name: "bash" });
    client.emitStatus({ sessionId: "a1", status: "running" });
    await tick();
    expect(sink.reports).toEqual([]);
    expect(execution.state?.().phase).toBe("tool");

    client.emitSessionEvent("a1", "tool/result", { turn: 1, step: 1 });
    client.emitSessionEvent("a1", "assistant/message", {
      turn: 1,
      step: 1,
      message: { role: "assistant", content: [{ type: "text", text: "finished" }] },
    });
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();

    expect(sink.reports).toEqual([{ kind: "failed", summary: "worker ended without a report" }]);
    expect(execution.state?.().phase).toBe("failed");
  });

  it("falls back to session.status idle only after evidence of a turn", async () => {
    const early = setup();
    await early.executor.start(task(), early.sink);
    early.client.emitStatus({ sessionId: "a1", status: "idle" });
    await tick();
    expect(early.sink.reports).toEqual([]);

    const late = setup();
    await late.executor.start(task(), late.sink);
    late.client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "ok", at: AT }));
    late.client.emitStatus({ sessionId: "a1", status: "idle" });
    await tick();
    expect(late.sink.reports).toEqual([{ kind: "ready", summary: "ok" }]);
  });

  it("completes from the wait fallback when no turn/end event arrives", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);
    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "fallback", at: AT }));
    client.resolveWait("a1");
    await tick();

    expect(sink.reports).toEqual([{ kind: "ready", summary: "fallback" }]);
  });
});

describe("session events and reports", () => {
  it("forwards session events as harness events with RFC 3339 timestamps", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);

    const time = Date.parse(AT);
    client.emitEvent({
      sessionId: "a1",
      event: { type: "tool/start", time, data: { name: "bash" } },
    });
    client.emitEvent({ sessionId: "a1", event: { type: "message", data: { text: "hi" } } });

    expect(sink.events.filter((event) => event.type !== "log")).toEqual([
      { type: "tool/start", at: AT, data: { name: "bash" } },
      { type: "message", at: AT, data: { text: "hi" } },
    ]);
    expect(mapSessionEvent({ type: "message", at: AT }, new Date(AT))).toEqual({
      type: "message",
      at: AT,
      data: { type: "message", at: AT },
    });
  });

  it("prefers the last valid report block across assistant text and tool results", async () => {
    const assistant = setup();
    await assistant.executor.start(task(), assistant.sink);
    assistant.client.emitAssistantText(
      "a1",
      reportBlock({ kind: "ready", summary: "from text", at: AT }),
    );
    assistant.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(assistant.sink.reports).toEqual([{ kind: "ready", summary: "from text" }]);

    const tool = setup();
    await tool.executor.start(task(), tool.sink);
    tool.client.emitEvent(
      toolResultEvent("a1", reportBlock({ kind: "ready", summary: "tool", at: AT })),
    );
    tool.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(tool.sink.reports).toEqual([{ kind: "ready", summary: "tool" }]);

    const last = setup();
    await last.executor.start(task(), last.sink);
    last.client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "first", at: AT }));
    last.client.emitEvent(
      toolResultEvent("a1", reportBlock({ kind: "progress", summary: "second", at: AT })),
    );
    last.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(last.sink.reports).toEqual([{ kind: "waiting", summary: "second" }]);
  });

  it("settles a gethen_report tool result even without a trailing turn/end", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);
    client.emitSessionEvent("a1", "tool/call", {
      turn: 1,
      step: 1,
      callId: "call-report",
      name: "gethen_report",
      arguments: "{}",
    });
    client.emitSessionEvent("a1", "tool/result", {
      turn: 1,
      step: 1,
      message: {
        role: "tool",
        toolCallId: "call-report",
        content: [
          { type: "text", text: reportBlock({ kind: "ready", summary: "reported", at: AT }) },
        ],
      },
    });
    await tick();

    expect(sink.reports).toEqual([{ kind: "ready", summary: "reported" }]);
    expect(execution.state?.()).toMatchObject({
      phase: "done",
      lastTool: { name: "gethen_report" },
    });
  });

  it("decodes JSON-encoded tool results that contain a report block", async () => {
    const encoded = setup();
    await encoded.executor.start(task(), encoded.sink);
    const payload = JSON.stringify({
      text: reportBlock({ kind: "ready", summary: "encoded tool result", at: AT }),
    });
    encoded.client.emitEvent(toolResultEvent("a1", payload));
    encoded.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(encoded.sink.reports).toEqual([{ kind: "ready", summary: "encoded tool result" }]);
  });

  it("ignores report-marker examples inside the assignment prompt", async () => {
    const setupPrompt = setup();
    await setupPrompt.executor.start(task(), setupPrompt.sink);
    setupPrompt.client.emitSessionEvent("a1", "user/message", {
      content: [
        {
          type: "text",
          text:
            '[GETHEN-ASSIGNMENT]\n{"proto":"gethen/assignment/v1"}\n' +
            "[GETHEN-REPORT]\n{not json with a raw control: \u0002\n[/GETHEN-REPORT]",
        },
      ],
    });
    setupPrompt.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(setupPrompt.sink.reports).toEqual([
      { kind: "failed", summary: "worker ended without a report" },
    ]);
  });

  it("fails a turn without a report and on an invalid block or wait", async () => {
    const missing = setup();
    await missing.executor.start(task(), missing.sink);
    missing.client.emitAssistantText("a1", "no report here");
    missing.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(missing.sink.reports).toEqual([
      { kind: "failed", summary: "worker ended without a report" },
    ]);

    const broken = setup();
    await broken.executor.start(task(), broken.sink);
    broken.client.emitAssistantText("a1", "[GETHEN-REPORT]\n{not json\n[/GETHEN-REPORT]");
    broken.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(broken.sink.reports[0]?.kind).toBe("failed");
    expect(broken.sink.reports[0]?.summary).toMatch(/invalid/i);

    const down = setup();
    await down.executor.start(task(), down.sink);
    down.client.rejectWait("a1", new Error("transport down"));
    await tick();
    expect(down.sink.reports).toEqual([
      { kind: "failed", summary: "DSH SDK wait failed: transport down" },
    ]);
  });

  it("narrows blocked to waiting with the question and keeps the other kinds", async () => {
    expect(harnessReportKind("ready")).toBe("ready");
    expect(harnessReportKind("dropped")).toBe("ready");
    expect(harnessReportKind("failed")).toBe("failed");
    expect(harnessReportKind("waiting")).toBe("waiting");
    expect(harnessReportKind("progress")).toBe("waiting");
    expect(harnessReportKind("blocked")).toBe("waiting");
    expect(harnessReportKind("unknown-kind")).toBe("waiting");

    const { client, sink, executor } = setup();
    await executor.start(task(), sink);
    client.emitAssistantText(
      "a1",
      reportBlock({ kind: "blocked", summary: "blocked", question: "which env?", at: AT }),
    );
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(sink.reports).toEqual([{ kind: "waiting", summary: "blocked", question: "which env?" }]);
  });
});

describe("escalations", () => {
  it("maps approval requests to permission escalations and back", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);

    const allowed = client.askApproval({ sessionId: "a1", toolName: "shell" });
    expect(sink.escalations[0]).toMatchObject({
      kind: "permission",
      question: "allow shell?",
      options: [
        { id: "allow", label: "Allow once" },
        { id: "reject", label: "Reject" },
      ],
      blocking: true,
    });
    expect(sink.escalations[0]?.id).toMatch(/^a1:approval:/);
    await execution.sendResolution({ kind: "answer", option: "allow", text: "go ahead" });
    await expect(allowed).resolves.toBe("allowed-once");

    const rejected = client.askApproval({ sessionId: "a1", toolName: "write" });
    await execution.sendResolution({ kind: "answer", option: "reject", text: "no" });
    await expect(rejected).resolves.toBe("rejected");
  });

  it("maps user questions to decision escalations and answers back", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);

    const asked = client.askQuestion({
      requestId: "q-1",
      sessionId: "a1",
      questions: [
        {
          id: "q1",
          question: "Merge or rebase?",
          options: [{ label: "Merge" }, { label: "Rebase" }],
        },
      ],
    });
    expect(sink.escalations[0]).toMatchObject({
      kind: "decision",
      question: "Merge or rebase?",
      options: [
        { id: "q0", label: "Merge" },
        { id: "q1", label: "Rebase" },
      ],
      blocking: true,
    });
    await execution.sendResolution({ kind: "answer", option: "q1", text: "Rebase" });
    await expect(asked).resolves.toEqual({ answers: [{ id: "q1", selected: ["Rebase"] }] });
  });

  it("asks multi-question batches sequentially and maps free text or drops", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);

    const asked = client.askQuestion({
      requestId: "q-2",
      sessionId: "a1",
      questions: [
        { id: "q1", question: "Env?", options: [{ label: "Staging" }, { label: "Prod" }] },
        { id: "q2", question: "Version?" },
      ],
    });
    expect(sink.escalations[0]?.question).toBe("Env? (1/2)");
    await execution.sendResolution({ kind: "answer", option: "q0", text: "Staging" });
    await tick();
    expect(sink.escalations[1]?.question).toBe("Version? (2/2)");
    await execution.sendResolution({ kind: "answer", text: "v2" });

    await expect(asked).resolves.toEqual({
      answers: [
        { id: "q1", selected: ["Staging"] },
        { id: "q2", selected: [], custom: "v2" },
      ],
    });

    const dropped = client.askQuestion({
      requestId: "q-3",
      sessionId: "a1",
      questions: [{ id: "q1", question: "Proceed?", options: [{ label: "Yes" }] }],
    });
    await execution.sendResolution({ kind: "drop", text: "skip" });
    await expect(dropped).resolves.toEqual({ cancelled: true });
  });

  it("answers a tool-result escalation and prompts the resolution back", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);

    client.emitEvent(
      toolResultEvent(
        "a1",
        escalationBlock({
          id: "esc-9",
          kind: "decision",
          question: "Merge or rebase?",
          options: [
            { id: "merge", label: "Merge" },
            { id: "rebase", label: "Rebase" },
          ],
          blocking: true,
        }),
      ),
    );
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();

    expect(sink.escalations[0]).toMatchObject({ id: "esc-9", kind: "decision", blocking: true });

    await execution.sendResolution({ kind: "answer", option: "merge", text: "merge it" });
    await tick();
    expect(client.prompts).toHaveLength(2);
    expect(client.prompts[1]?.text).toContain("[GETHEN-RESOLUTION]");

    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "merged", at: AT }));
    client.emitSessionEvent("a1", "turn/end", { turn: 2 });
    await tick();
    expect(sink.reports).toEqual([{ kind: "ready", summary: "merged" }]);
  });
});

describe("live state, steering and lifecycle timers", () => {
  it("exposes live task state snapshots", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);
    expect(execution.state?.()).toMatchObject({
      assignmentId: "a1",
      phase: "thinking",
      step: 0,
      seq: 0,
    });

    client.emitSessionEvent("a1", "step/start", { turn: 1, step: 2 });
    client.emitSessionEvent("a1", "tool/call", { turn: 1, step: 2, name: "bash" });
    client.emitSessionEvent("a1", "tool/result", { turn: 1, step: 2 });
    client.emitAssistantText("a1", "x".repeat(900));
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });

    const state = execution.state?.() as HarnessTaskState;
    expect(state).toMatchObject({ phase: "failed", step: 2, seq: 5, lastTool: { name: "bash" } });
    expect(state.lastEventAt).toBe(AT);
    expect(state.lastMessage).toHaveLength(500);
    if (state.lastTool !== undefined) state.lastTool.name = "mutated";
    expect(execution.state?.().lastTool?.name).toBe("bash");
  });

  it("steers additional input into the running session", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);

    await execution.steer?.("focus on the failing test");
    expect(client.prompts.at(-1)).toEqual({ sessionId: "a1", text: "focus on the failing test" });

    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    await execution.steer?.("too late");
    expect(client.prompts).toHaveLength(2);
  });

  it("fires the silence watchdog, resets it on events, and enforces the deadline", async () => {
    vi.useFakeTimers();
    try {
      const watchdog = setup({ idleTimeoutMs: 1_000 });
      await watchdog.executor.start(task(), watchdog.sink);
      await vi.advanceTimersByTimeAsync(500);
      watchdog.client.emitSessionEvent("a1", "step/start", { turn: 1, step: 1 });
      await vi.advanceTimersByTimeAsync(500);
      expect(watchdog.sink.reports).toEqual([]);
      await vi.advanceTimersByTimeAsync(500);
      expect(watchdog.sink.reports).toEqual([
        { kind: "failed", summary: "no session events for 1000ms" },
      ]);
      expect(watchdog.client.closeCount).toBe(1);

      const deadline = setup();
      await deadline.executor.start(
        task({ deadlineAt: "2026-11-17T00:00:01.000Z" }),
        deadline.sink,
      );
      await vi.advanceTimersByTimeAsync(1_000);
      expect(deadline.sink.reports).toEqual([
        { kind: "failed", summary: "assignment deadline passed" },
      ]);
      expect(deadline.client.closeCount).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps a silent execution alive while a tool is running", async () => {
    vi.useFakeTimers();
    try {
      const running = setup({ idleTimeoutMs: 1_000 });
      await running.executor.start(task(), running.sink);
      running.client.emitSessionEvent("a1", "tool/call", { turn: 1, step: 1, name: "bash" });
      // A long build/check emits no session events; the silence watchdog must
      // not report failure while the tool is still in flight.
      await vi.advanceTimersByTimeAsync(5_000);
      expect(running.sink.reports).toEqual([]);
      expect(running.client.closeCount).toBe(0);

      running.client.emitSessionEvent("a1", "tool/result", { turn: 1, step: 1 });
      running.client.emitAssistantText(
        "a1",
        reportBlock({ kind: "ready", summary: "checks passed", at: AT }),
      );
      running.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
      await vi.advanceTimersByTimeAsync(0);
      expect(running.sink.reports).toEqual([{ kind: "ready", summary: "checks passed" }]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("salvages the final report from the session log when events stop", async () => {
    const fixture = dshFixture({
      sessionId: "a1",
      atMs: Date.parse(AT),
      extraEvents: [
        {
          type: "assistant/message",
          data: {
            text: reportBlock({ kind: "ready", summary: "pushed the branch", at: AT }),
          },
        },
      ],
    });
    const salvaged = setup({ sessionStore: fixture.store, idleTimeoutMs: 20 });
    await salvaged.executor.start(task({ assignmentId: "a1" }), salvaged.sink);
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(salvaged.sink.reports).toEqual([{ kind: "ready", summary: "pushed the branch" }]);
    expect(salvaged.client.closeCount).toBe(1);
  });

  it("does not finish with an empty report while an escalation is pending", async () => {
    const pending = setup();
    const execution = await pending.executor.start(task(), pending.sink);
    const asked = pending.client.askApproval({ sessionId: "a1", toolName: "shell" });
    pending.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    pending.client.emitStatus({ sessionId: "a1", status: "idle" });
    await tick();
    expect(pending.sink.reports).toEqual([]);
    await execution.sendResolution({ kind: "answer", option: "allow", text: "go" });
    await expect(asked).resolves.toBe("allowed-once");
    await tick();
    expect(pending.sink.reports).toEqual([]);
  });

  it("pauses the silence watchdog while an escalation waits for a person", async () => {
    vi.useFakeTimers();
    try {
      const escalated = setup({ idleTimeoutMs: 1_000 });
      const execution = await escalated.executor.start(task(), escalated.sink);
      const allowed = escalated.client.askApproval({ sessionId: "a1", toolName: "shell" });
      await vi.advanceTimersByTimeAsync(5_000);
      expect(escalated.sink.reports).toEqual([]);
      await execution.sendResolution({ kind: "answer", option: "allow", text: "go" });
      await expect(allowed).resolves.toBe("allowed-once");
      await vi.advanceTimersByTimeAsync(1_500);
      expect(escalated.sink.reports[0]?.summary).toMatch(/no session events/);
    } finally {
      vi.useRealTimers();
    }
  });

  it("fails and releases an execution that hits the hard duration cap", async () => {
    vi.useFakeTimers();
    try {
      const capped = setup({ maxExecutionMs: 1_000, idleTimeoutMs: 60_000 });
      await capped.executor.start(task(), capped.sink);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(capped.sink.reports).toEqual([
        { kind: "failed", summary: "assignment exceeded the 1000ms execution limit" },
      ]);
      expect(capped.client.closeCount).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("marks an open-turn session with no live owner as stale", async () => {
    const fixture = dshFixture({ sessionId: "stale-session", openTurn: true });
    const old = new Date(0);
    for (const path of [
      join(fixture.home, "storages", "session_projcache", "sessions", "stale-session.json"),
      join(fixture.home, "sessions", "--work-project--", "stale-session", "session.v4.jsonl.zstd"),
    ]) {
      utimesSync(path, old, old);
    }
    const stale = setup({ sessionStore: fixture.store, staleSessionMs: 1 });
    await stale.executor.start(task({ assignmentId: "a1" }), stale.sink);
    await new Promise((resolve) => setTimeout(resolve, 5));
    const summary = stale.executor
      .sessions()
      .find((session) => session.sessionId === "stale-session");
    expect(summary?.state).toBe("stale");
  });

  it("defaults the watchdog to five minutes", () => {
    expect(DEFAULT_IDLE_TIMEOUT_MS).toBe(300_000);
    expect(STOP_STEER_TEXT.length).toBeGreaterThan(0);
  });
});

describe("cancellation and process isolation", () => {
  it("interrupts in order: reject escalation, steer stop, then terminate, and reports once", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);

    const approval = client.askApproval({ sessionId: "a1", toolName: "shell" });
    await execution.cancel("superseded by a person");
    await tick();

    await expect(approval).resolves.toBe("rejected");
    const stopIndex = client.actions.indexOf(`prompt:${STOP_STEER_TEXT}`);
    const closeIndex = client.actions.indexOf("close");
    expect(stopIndex).toBeGreaterThanOrEqual(0);
    expect(closeIndex).toBeGreaterThan(stopIndex);
    expect(client.closeCount).toBe(1);
    expect(sink.reports).toEqual([
      { kind: "failed", summary: "cancelled: superseded by a person" },
    ]);

    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "late", at: AT }));
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(sink.reports).toHaveLength(1);
  });

  it("isolates process executions and terminates each dedicated runtime", async () => {
    const clients: FakeSdkClient[] = [];
    const sink = new RecordingSink();
    const executor = createSdkHarnessExecutor({
      provider: "deepseek",
      model: "deepseek-v4",
      isolation: "process",
      clientFactory: () => {
        const client = new FakeSdkClient();
        clients.push(client);
        return client;
      },
      now: () => new Date(AT),
      interruptGraceMs: 0,
      sessionStore: emptySessionStore(),
    });
    executors.push(executor);

    await executor.start(task({ assignmentId: "a1" }), sink);
    await executor.start(task({ assignmentId: "a2" }), sink);
    expect(clients).toHaveLength(2);
    for (const client of clients) expect(client.startCalls).toHaveLength(1);

    const first = clients[0] as FakeSdkClient;
    first.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(first.terminateCount).toBe(1);
    expect(first.closeCount).toBe(0);
    expect(clients[1]?.terminateCount).toBe(0);

    const second = clients[1] as FakeSdkClient;
    second.emitSessionEvent("a2", "turn/end", { turn: 1 });
    await tick();
    expect(second.terminateCount).toBe(1);
    expect(sink.reports).toHaveLength(2);

    await executor.close();
    expect(first.terminateCount).toBe(1);
  });

  it("keeps the shared runtime after a normal completion but closes it on cancel", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task({ assignmentId: "a1" }), sink);
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(sink.reports).toHaveLength(1);
    expect(client.closeCount).toBe(0);

    const execution = await executor.start(task({ assignmentId: "a2" }), sink);
    expect(client.startCalls).toHaveLength(1);
    await execution.cancel("stop");
    expect(client.closeCount).toBe(1);
  });

  it("reports start errors as failed and retries initialization on the next start", async () => {
    const { client, sink, executor } = setup();
    client.failInitialize = new Error("no model route");
    await executor.start(task(), sink);

    expect(sink.reports).toHaveLength(1);
    expect(sink.reports[0]?.kind).toBe("failed");
    expect(sink.reports[0]?.summary).toMatch(/failed to start DSH SDK runtime/);
    expect(client.terminateCount).toBe(1);

    client.failInitialize = undefined;
    await executor.start(task({ assignmentId: "a2" }), sink);
    expect(client.initializeCalls).toHaveLength(2);
    expect(client.prompts.at(-1)).toEqual({ sessionId: "a2", text: "do the work" });

    const prompt = setup();
    prompt.client.failPrompt = new Error("prompt rejected");
    await prompt.executor.start(task(), prompt.sink);
    expect(prompt.sink.reports[0]?.kind).toBe("failed");
    expect(prompt.sink.reports[0]?.summary).toMatch(/failed to prompt DSH SDK session/);
  });

  it("refuses an assignment whose deadline already passed, without spawning", async () => {
    const spawned: FakeSdkClient[] = [];
    const sink = new RecordingSink();
    const executor = createSdkHarnessExecutor({
      provider: "deepseek",
      model: "deepseek-v4",
      clientFactory: () => {
        const client = new FakeSdkClient();
        spawned.push(client);
        return client;
      },
      now: () => new Date(AT),
      sessionStore: emptySessionStore(),
    });
    executors.push(executor);

    await executor.start(task({ deadlineAt: "2026-11-16T00:00:00.000Z" }), sink);

    expect(spawned).toEqual([]);
    expect(sink.reports).toEqual([
      { kind: "failed", summary: "assignment deadline had already passed" },
    ]);
  });
});

describe("stderr and lifecycle log events", () => {
  it("forwards stderr chunks as log events without changing the live state", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);
    const seqBefore = execution.state?.().seq;

    client.emitStderr("diagnostic: provider warning\n");

    const stderrLogs = logEvents(sink).filter(
      (event) => (event.data as { stream?: string }).stream === "stderr",
    );
    expect(stderrLogs).toEqual([
      {
        type: "log",
        at: AT,
        data: { stream: "stderr", text: "diagnostic: provider warning\n" },
      },
    ]);
    expect(execution.state?.().seq).toBe(seqBefore);
  });

  it("truncates long log text to maxLogChars with a marker", async () => {
    expect(DEFAULT_MAX_LOG_CHARS).toBe(8_000);
    const { client, sink, executor } = setup({ maxLogChars: 20 });
    await executor.start(task(), sink);

    client.emitStderr("x".repeat(50));

    const stderr = logEvents(sink).find(
      (event) => (event.data as { stream?: string }).stream === "stderr",
    );
    if (stderr === undefined) throw new Error("expected a stderr log event");
    expect((stderr.data as { text: string }).text).toBe(
      `${"x".repeat(20)}${LOG_TRUNCATION_SUFFIX}`,
    );

    const defaults = setup();
    await defaults.executor.start(task(), defaults.sink);
    defaults.client.emitStderr("y".repeat(DEFAULT_MAX_LOG_CHARS + 5));
    const longLog = logEvents(defaults.sink).find(
      (event) => (event.data as { stream?: string }).stream === "stderr",
    );
    if (longLog === undefined) throw new Error("expected a truncated stderr log event");
    expect((longLog.data as { text: string }).text.endsWith(LOG_TRUNCATION_SUFFIX)).toBe(true);
  });

  it("emits start and end lifecycle logs", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);
    expect(logPhases(sink)).toEqual(["start"]);

    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "done", at: AT }));
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();

    expect(logPhases(sink)).toEqual(["start", "end"]);
    const endLog = logEvents(sink)[1];
    if (endLog === undefined) throw new Error("expected an end log event");
    expect((endLog.data as { text?: string }).text).toMatch(/ready/);
  });

  it("emits watchdog and deadline lifecycle logs", async () => {
    vi.useFakeTimers();
    try {
      const watchdog = setup({ idleTimeoutMs: 500 });
      await watchdog.executor.start(task(), watchdog.sink);
      await vi.advanceTimersByTimeAsync(500);
      expect(logPhases(watchdog.sink)).toEqual(["start", "watchdog", "end"]);
      expect(watchdog.sink.reports[0]?.kind).toBe("failed");

      const deadline = setup();
      await deadline.executor.start(
        task({ deadlineAt: "2026-11-17T00:00:01.000Z" }),
        deadline.sink,
      );
      await vi.advanceTimersByTimeAsync(1_000);
      expect(logPhases(deadline.sink)).toEqual(["start", "deadline", "end"]);
      expect(deadline.sink.reports[0]?.summary).toMatch(/deadline/);
    } finally {
      vi.useRealTimers();
    }
  });

  it("logs cancellation and unsubscribes stderr after finishing", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);
    expect(client.stderrListenerCount).toBe(1);

    await execution.cancel("stop requested");
    await tick();

    expect(logPhases(sink)).toEqual(["start", "cancel", "end"]);
    expect(client.stderrListenerCount).toBe(0);
    const logsBefore = logEvents(sink).length;
    client.emitStderr("late noise");
    expect(logEvents(sink)).toHaveLength(logsBefore);
  });

  it("subscribes and unsubscribes stderr per execution on a shared client", async () => {
    const first = setup();
    const secondSink = new RecordingSink();
    await first.executor.start(task({ assignmentId: "a1" }), first.sink);
    await first.executor.start(task({ assignmentId: "a2" }), secondSink);
    expect(first.client.stderrListenerCount).toBe(2);

    first.client.emitStderr("shared warning");
    expect(
      logEvents(first.sink).filter(
        (event) => (event.data as { stream?: string }).stream === "stderr",
      ),
    ).toHaveLength(1);
    expect(
      logEvents(secondSink).filter(
        (event) => (event.data as { stream?: string }).stream === "stderr",
      ),
    ).toHaveLength(1);

    first.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(first.client.stderrListenerCount).toBe(1);

    first.client.emitSessionEvent("a2", "turn/end", { turn: 2 });
    await tick();
    expect(first.client.stderrListenerCount).toBe(0);
  });

  it("unsubscribes stderr when a process-isolated runtime terminates", async () => {
    const clients: FakeSdkClient[] = [];
    const sink = new RecordingSink();
    const executor = createSdkHarnessExecutor({
      provider: "deepseek",
      model: "deepseek-v4",
      isolation: "process",
      clientFactory: () => {
        const client = new FakeSdkClient();
        clients.push(client);
        return client;
      },
      now: () => new Date(AT),
      interruptGraceMs: 0,
      sessionStore: emptySessionStore(),
    });
    executors.push(executor);

    await executor.start(task(), sink);
    const client = clients[0] as FakeSdkClient;
    expect(client.stderrListenerCount).toBe(1);
    client.emitStderr("dedicated runtime warning");
    expect(logEvents(sink).some((event) => event.type === "log")).toBe(true);

    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(client.stderrListenerCount).toBe(0);
    expect(client.terminateCount).toBe(1);
  });
});

describe("session selection", () => {
  it("starts a new assignment-named session by default and exposes it in the directory", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task({ assignmentId: "a1" }), sink);

    expect(client.resumeCalls).toEqual([]);
    expect(execution.state?.().sessionId).toBe("a1");
    expect(client.prompts[0]?.sessionId).toBe("a1");
    expect(executor.sessions()).toEqual([
      {
        sessionId: "a1",
        workspace: "/work/ws",
        lastUsedAt: AT,
        activeAssignmentId: "a1",
        state: "running",
      },
    ]);

    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(executor.sessions()[0]?.activeAssignmentId).toBeUndefined();

    const snapshot = executor.sessions();
    const firstSnapshot = snapshot[0];
    if (firstSnapshot !== undefined) firstSnapshot.sessionId = "mutated";
    snapshot.push({ sessionId: "extra", workspace: "/x", lastUsedAt: AT });
    expect(executor.sessions()).toHaveLength(1);
    expect(executor.sessions()[0]?.sessionId).toBe("a1");
  });

  it("reuses an explicit sessionId by resuming then prompting it", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(
      task({ assignmentId: "a2", session: { mode: "reuse", sessionId: "s-1" } }),
      sink,
    );

    expect(client.resumeCalls).toEqual(["s-1"]);
    expect(client.prompts).toEqual([{ sessionId: "s-1", text: "do the work" }]);
    expect(execution.state?.().sessionId).toBe("s-1");
    expect(executor.sessions()).toEqual([
      {
        sessionId: "s-1",
        workspace: "/work/ws",
        lastUsedAt: AT,
        activeAssignmentId: "a2",
        state: "running",
      },
    ]);
  });

  it("creates the session behind a new-mode key and resumes it afterwards", async () => {
    const { client, sink, executor } = setup();
    await executor.start(
      task({ assignmentId: "a1", session: { mode: "new", key: "build" } }),
      sink,
    );
    expect(client.resumeCalls).toEqual([]);
    expect(client.prompts[0]?.sessionId).toBe("a1");
    expect(executor.sessions()[0]).toMatchObject({ sessionId: "a1", key: "build" });
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();

    const second = await executor.start(
      task({ assignmentId: "a2", session: { mode: "reuse", key: "build" } }),
      sink,
    );
    // The shared runtime already has the session open; resume would fail there.
    expect(client.resumeCalls).toEqual([]);
    expect(client.prompts.at(-1)).toEqual({ sessionId: "a1", text: "do the work" });
    expect(second.state?.().sessionId).toBe("a1");
    expect(executor.sessions()).toHaveLength(1);
    expect(executor.sessions()[0]).toMatchObject({
      sessionId: "a1",
      key: "build",
      activeAssignmentId: "a2",
    });
  });

  it("persists a new-mode key so a restarted executor can reuse it", async () => {
    const file = join(mkdtempSync(join(tmpdir(), "gethen-exec-keys-")), "state", "keys.json");
    const first = setup({ sessionKeysFile: file });
    await first.executor.start(
      task({ assignmentId: "a1", session: { mode: "new", key: "build" } }),
      first.sink,
    );
    expect(JSON.parse(readFileSync(file, "utf8"))).toEqual({ build: "a1" });
    first.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    await first.executor.close();

    // A fresh process keeps no in-memory state; the registry comes from disk.
    const second = setup({ sessionKeysFile: file });
    await second.executor.start(
      task({ assignmentId: "a2", session: { mode: "reuse", key: "build" } }),
      second.sink,
    );
    expect(second.sink.reports.filter((report) => report.kind === "failed")).toEqual([]);
    expect(second.client.resumeCalls).toEqual(["a1"]);
    expect(second.client.prompts.at(-1)).toEqual({ sessionId: "a1", text: "do the work" });
    expect(second.executor.sessions()[0]).toMatchObject({ sessionId: "a1", key: "build" });
  });

  it("fails a reuse-by-key request when the key was never created", async () => {
    const { sink, executor } = setup();
    await executor.start(
      task({ assignmentId: "a1", session: { mode: "reuse", key: "missing" } }),
      sink,
    );
    expect(sink.reports[0]).toMatchObject({ kind: "failed" });
    expect(sink.reports[0]?.summary).toMatch(/unknown/i);
  });

  it("persists the key -> sessionId registry across executor restarts", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-exec-keys-"));
    const sessionKeysFile = join(dir, "keys.json");
    const first = setup({ sessionKeysFile });
    await first.executor.start(
      task({ assignmentId: "a1", session: { mode: "new", key: "build" } }),
      first.sink,
    );
    first.client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();

    const second = setup({ sessionKeysFile });
    await second.executor.start(
      task({ assignmentId: "a2", session: { mode: "reuse", key: "build" } }),
      second.sink,
    );
    expect(second.client.resumeCalls).toEqual(["a1"]);
    expect(second.client.prompts.at(-1)?.sessionId).toBe("a1");
  });

  it("registers a key chosen by an explicit new-mode task", async () => {
    const { client, sink, executor } = setup();
    await executor.start(
      task({ assignmentId: "a1", session: { mode: "new", key: "review" } }),
      sink,
    );
    expect(executor.sessions()[0]).toMatchObject({ sessionId: "a1", key: "review" });
    client.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();

    await executor.start(
      task({ assignmentId: "a2", session: { mode: "reuse", key: "review" } }),
      sink,
    );
    expect(client.resumeCalls).toEqual([]);
    expect(client.prompts.at(-1)?.sessionId).toBe("a1");
  });

  it("reports a resume failure as failed without prompting or silently starting anew", async () => {
    const { client, sink, executor } = setup();
    client.failResume = new Error("session not found");
    const execution = await executor.start(
      task({ assignmentId: "a2", session: { mode: "reuse", sessionId: "missing" } }),
      sink,
    );

    expect(client.resumeCalls).toEqual(["missing"]);
    expect(client.prompts).toEqual([]);
    expect(execution.state?.().sessionId).toBe("missing");
    expect(sink.reports).toHaveLength(1);
    expect(sink.reports[0]?.kind).toBe("failed");
    expect(sink.reports[0]?.summary).toMatch(/failed to resume DSH SDK session: session not found/);
    expect(executor.sessions()[0]).toMatchObject({ sessionId: "missing" });
    expect(executor.sessions()[0]?.activeAssignmentId).toBeUndefined();
  });

  it("fails invalid reuse specs before spawning a runtime", async () => {
    const noTarget = setup();
    await noTarget.executor.start(task({ session: { mode: "reuse" } }), noTarget.sink);
    expect(noTarget.sink.reports[0]?.summary).toMatch(/needs "sessionId" or "key"/);
    expect(noTarget.client.initializeCalls).toEqual([]);
    expect(noTarget.executor.sessions()).toEqual([]);

    const newWithId = setup();
    await newWithId.executor.start(
      task({ session: { mode: "new", sessionId: "s-1" } }),
      newWithId.sink,
    );
    expect(newWithId.sink.reports[0]?.summary).toMatch(/must not name a sessionId/);
    expect(newWithId.client.initializeCalls).toEqual([]);
  });

  it("rejects a second concurrent task that reuses the same key or session", async () => {
    const { client, sink, executor } = setup();
    await executor.start(
      task({ assignmentId: "a1", session: { mode: "new", key: "shared" } }),
      sink,
    );
    expect(client.initializeCalls).toHaveLength(1);

    const second = new RecordingSink();
    await executor.start(
      task({ assignmentId: "a2", session: { mode: "reuse", key: "shared" } }),
      second,
    );
    expect(second.reports[0]?.summary).toMatch(/already running assignment "a1"/);
    expect(client.initializeCalls).toHaveLength(1);

    const bySession = new RecordingSink();
    await executor.start(
      task({ assignmentId: "a3", session: { mode: "reuse", sessionId: "a1" } }),
      bySession,
    );
    expect(bySession.reports[0]?.summary).toMatch(/already running assignment "a1"/);
    expect(executor.sessions()[0]?.activeAssignmentId).toBe("a1");
  });

  it("resumes on a dedicated runtime under process isolation", async () => {
    const clients: FakeSdkClient[] = [];
    const sink = new RecordingSink();
    const executor = createSdkHarnessExecutor({
      provider: "deepseek",
      model: "deepseek-v4",
      isolation: "process",
      clientFactory: () => {
        const client = new FakeSdkClient();
        clients.push(client);
        return client;
      },
      now: () => new Date(AT),
      interruptGraceMs: 0,
      sessionStore: emptySessionStore(),
    });
    executors.push(executor);

    await executor.start(
      task({ assignmentId: "a1", session: { mode: "new", key: "workspace" } }),
      sink,
    );
    const first = clients[0] as FakeSdkClient;
    expect(first.resumeCalls).toEqual([]);
    expect(first.prompts[0]?.sessionId).toBe("a1");
    first.emitSessionEvent("a1", "turn/end", { turn: 1 });
    await tick();
    expect(first.terminateCount).toBe(1);

    await executor.start(
      task({ assignmentId: "a2", session: { mode: "reuse", key: "workspace" } }),
      sink,
    );
    const second = clients[1] as FakeSdkClient;
    expect(second.resumeCalls).toEqual(["a1"]);
    expect(second.prompts[0]?.sessionId).toBe("a1");
    expect(executor.sessions()[0]).toMatchObject({ sessionId: "a1", key: "workspace" });
  });

  describe("DSH session store integration", () => {
    it("merges store summaries with the executor registry", async () => {
      const fixture = dshFixture({ sessionId: "store-session", title: "stored task" });
      const { sink, executor } = setup({ sessionStore: fixture.store });
      await executor.start(task({ assignmentId: "a-local", workspace: "/work/ws" }), sink);

      const sessions = executor.sessions();
      expect(sessions).toHaveLength(2);
      const stored = sessions.find((info) => info.sessionId === "store-session");
      expect(stored).toMatchObject({
        workspace: "/work/project",
        state: "idle",
        eventCount: 42,
        hasLog: true,
        config: {
          cwd: "/work/project",
          title: "stored task",
          model: { provider: "opencode-go", model: "deepseek-v4.1-flash" },
          sandboxMode: "workspace-write",
          approvalPolicy: "never",
        },
        tokenUsage: { inputTokens: 10, outputTokens: 20 },
      });
      expect(stored?.lastActivityAt).toBeDefined();

      const local = sessions.find((info) => info.sessionId === "a-local");
      expect(local).toMatchObject({
        workspace: "/work/ws",
        activeAssignmentId: "a-local",
        state: "running",
        lastUsedAt: AT,
      });
    });

    it("overrides an active store session to running and reverts after completion", async () => {
      const fixture = dshFixture({ sessionId: "s-active" });
      const { client, sink, executor } = setup({ sessionStore: fixture.store });
      await executor.start(
        task({ assignmentId: "a1", session: { mode: "reuse", sessionId: "s-active" } }),
        sink,
      );
      expect(executor.sessions().find((info) => info.sessionId === "s-active")).toMatchObject({
        state: "running",
        activeAssignmentId: "a1",
      });

      client.emitSessionEvent("s-active", "turn/end", { turn: 1 });
      await tick();
      const idle = executor.sessions().find((info) => info.sessionId === "s-active");
      expect(idle?.activeAssignmentId).toBeUndefined();
      expect(idle?.state).toBe("idle");
    });

    it("carries the registry key and workspace onto a stored session", async () => {
      const fixture = dshFixture({ sessionId: "s-key" });
      const { sink, executor } = setup({ sessionStore: fixture.store });
      await executor.start(
        task({
          assignmentId: "a1",
          session: { mode: "reuse", sessionId: "s-key", key: "review" },
        }),
        sink,
      );

      expect(executor.sessions().find((info) => info.sessionId === "s-key")).toMatchObject({
        key: "review",
        workspace: "/work/ws",
        activeAssignmentId: "a1",
        config: { title: "fixture task" },
      });
    });

    it("sorts by last activity and caps the list at sessionListLimit", async () => {
      const first = dshFixture({ sessionId: "s-1" });
      const second = dshFixture({ home: first.home, sessionId: "s-2" });
      const { sink, executor } = setup({
        sessionStore: second.store,
        sessionListLimit: 1,
      });
      await executor.start(task({ assignmentId: "a-local" }), sink);

      const sessions = executor.sessions();
      expect(DEFAULT_SESSION_LIST_LIMIT).toBe(100);
      expect(sessions).toHaveLength(1);
      expect(sessions[0]?.sessionId).not.toBe("a-local");

      const uncapped = setup({ sessionStore: second.store, sessionListLimit: 10 });
      await uncapped.executor.start(task({ assignmentId: "a-local2" }), uncapped.sink);
      const all = uncapped.executor.sessions();
      expect(all.length).toBeGreaterThan(1);
      const keys = all.map((info) => info.lastActivityAt ?? info.lastUsedAt);
      expect(keys).toEqual([...keys].sort((left, right) => right.localeCompare(left)));
    });

    it("degrades to registry-only when the session store fails", async () => {
      const failing = {
        list: async (): Promise<never> => {
          throw new Error("store unavailable");
        },
        history: async (): Promise<never> => {
          throw new Error("store unavailable");
        },
      } as unknown as DshSessionStore;
      const { sink, executor } = setup({ sessionStore: failing });
      await executor.start(task({ assignmentId: "a1" }), sink);

      expect(executor.sessions()).toEqual([
        {
          sessionId: "a1",
          workspace: "/work/ws",
          lastUsedAt: AT,
          activeAssignmentId: "a1",
          state: "running",
        },
      ]);
      await expect(executor.sessionHistory("whatever")).resolves.toEqual([]);
    });

    it("returns registry-only sessions for a missing DSH home", async () => {
      const { sink, executor } = setup({ sessionStore: emptySessionStore() });
      await executor.start(task({ assignmentId: "a1" }), sink);
      const sessions = executor.sessions();
      expect(sessions).toHaveLength(1);
      expect(sessions[0]).toMatchObject({ sessionId: "a1", activeAssignmentId: "a1" });
      expect(sessions[0]?.config).toBeUndefined();
    });

    it("passes session history through from the store", async () => {
      const fixture = dshFixture({ sessionId: "s-history" });
      const { sink, executor } = setup({ sessionStore: fixture.store });
      await executor.start(task({ assignmentId: "a1" }), sink);

      const tail = await executor.sessionHistory("s-history", { limit: 2 });
      expect(tail.map((event) => event.type)).toEqual(["tool/call", "turn/end"]);
      const forward = await executor.sessionHistory("s-history", { afterSeq: 0, limit: 10 });
      expect(forward.map((event) => event.seq)).toEqual([1, 2, 3]);
      await expect(executor.sessionHistory("missing")).resolves.toEqual([]);
    });
  });
});

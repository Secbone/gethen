import type {
  HarnessEscalation,
  HarnessEvent,
  HarnessReport,
  HarnessSink,
  HarnessTask,
} from "@gethen/harness";
import { afterEach, describe, expect, it } from "vitest";
import {
  createSdkHarnessExecutor,
  harnessReportKind,
  mapSessionEvent,
  type SdkHarnessExecutor,
  type SdkHarnessExecutorOptions,
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

describe("createSdkHarnessExecutor events and reports", () => {
  it("forwards session events as harness events with RFC 3339 timestamps", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);

    const time = Date.parse(AT);
    client.emitEvent({
      sessionId: "a1",
      event: { type: "tool/start", time, data: { name: "bash" } },
    });
    client.emitEvent({ sessionId: "a1", event: { type: "message", data: { text: "hi" } } });

    expect(sink.events).toEqual([
      { type: "tool/start", at: AT, data: { name: "bash" } },
      { type: "message", at: AT, data: { text: "hi" } },
    ]);
    expect(
      mapSessionEvent({ type: "message", at: "2026-11-16T00:00:00.000Z" }, new Date(AT)),
    ).toEqual({
      type: "message",
      at: "2026-11-16T00:00:00.000Z",
      data: { type: "message", at: "2026-11-16T00:00:00.000Z" },
    });
  });

  it("reports a ready report parsed from assistant text", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);

    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "done", at: AT }));
    client.resolveWait("a1");
    await tick();

    expect(sink.reports).toEqual([{ kind: "ready", summary: "done" }]);
  });

  it("reads a report from a gethen_report tool result", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);

    client.emitEvent(
      toolResultEvent("a1", reportBlock({ kind: "ready", summary: "file written", at: AT })),
    );
    client.resolveWait("a1");
    await tick();

    expect(sink.reports).toEqual([{ kind: "ready", summary: "file written" }]);
  });

  it("uses the last complete report block across events", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);

    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "first", at: AT }));
    client.emitEvent(
      toolResultEvent("a1", reportBlock({ kind: "progress", summary: "second", at: AT })),
    );
    client.resolveWait("a1");
    await tick();

    expect(sink.reports).toEqual([{ kind: "waiting", summary: "second" }]);
  });

  it("reports waiting when no report is produced or only a marker is mentioned", async () => {
    const missing = setup();
    await missing.executor.start(task(), missing.sink);
    missing.client.emitAssistantText("a1", "some work without a report");
    missing.client.resolveWait("a1");
    await tick();
    expect(missing.sink.reports).toEqual([
      { kind: "waiting", summary: "worker produced no report" },
    ]);

    const markerOnly = setup();
    await markerOnly.executor.start(task(), markerOnly.sink);
    markerOnly.client.emitAssistantText("a1", "I will send [GETHEN-REPORT] later");
    markerOnly.client.emitAssistantText("a1", "and [GETHEN-REPORT] never closes");
    markerOnly.client.resolveWait("a1");
    await tick();
    expect(markerOnly.sink.reports).toEqual([
      { kind: "waiting", summary: "worker produced no report" },
    ]);
  });

  it("reports failed for an invalid report block or a failed wait", async () => {
    const broken = setup();
    await broken.executor.start(task(), broken.sink);
    broken.client.emitAssistantText("a1", "[GETHEN-REPORT]\n{not json\n[/GETHEN-REPORT]");
    broken.client.resolveWait("a1");
    await tick();
    expect(broken.sink.reports).toHaveLength(1);
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

  it("narrows blocked to waiting with the question and keeps ready/failed terminal", () => {
    expect(harnessReportKind("ready")).toBe("ready");
    expect(harnessReportKind("dropped")).toBe("ready");
    expect(harnessReportKind("failed")).toBe("failed");
    expect(harnessReportKind("waiting")).toBe("waiting");
    expect(harnessReportKind("progress")).toBe("waiting");
    expect(harnessReportKind("blocked")).toBe("waiting");
    expect(harnessReportKind("unknown-kind")).toBe("waiting");
  });

  it("reports a blocked worker report as waiting with its question", async () => {
    const { client, sink, executor } = setup();
    await executor.start(task(), sink);
    client.emitAssistantText(
      "a1",
      reportBlock({ kind: "blocked", summary: "blocked", question: "which env?", at: AT }),
    );
    client.resolveWait("a1");
    await tick();

    expect(sink.reports).toEqual([{ kind: "waiting", summary: "blocked", question: "which env?" }]);
  });
});

describe("createSdkHarnessExecutor escalations", () => {
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

    const escalated = client.askApproval({ sessionId: "a1", toolName: "shell" });
    await execution.sendResolution({ kind: "escalate", text: "a person decides" });
    await expect(escalated).resolves.toBe("rejected");
  });

  it("maps a user question to a decision escalation and back to answers", async () => {
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
    client.resolveWait("a1");
    await tick();

    expect(sink.escalations[0]).toMatchObject({
      id: "esc-9",
      kind: "decision",
      question: "Merge or rebase?",
      options: [
        { id: "merge", label: "Merge" },
        { id: "rebase", label: "Rebase" },
      ],
      blocking: true,
    });

    await execution.sendResolution({ kind: "answer", option: "merge", text: "merge it" });
    await tick();
    expect(client.prompts).toHaveLength(2);
    expect(client.prompts[1]?.text).toContain("[GETHEN-RESOLUTION]");
    expect(client.waitCalls).toHaveLength(2);

    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "merged", at: AT }));
    client.resolveWait("a1");
    await tick();
    expect(sink.reports).toEqual([{ kind: "ready", summary: "merged" }]);
  });
});

describe("createSdkHarnessExecutor failures and cancellation", () => {
  it("cancels by closing the runtime and reporting failed once", async () => {
    const { client, sink, executor } = setup();
    const execution = await executor.start(task(), sink);

    await execution.cancel("superseded by a person");
    await tick();

    expect(client.closeCount).toBe(1);
    expect(sink.reports).toEqual([
      { kind: "failed", summary: "cancelled: superseded by a person" },
    ]);

    client.emitAssistantText("a1", reportBlock({ kind: "ready", summary: "late", at: AT }));
    client.resolveWait("a1");
    await tick();
    expect(sink.reports).toHaveLength(1);
  });

  it("reports start errors as failed and retries initialization on the next start", async () => {
    const { client, sink, executor } = setup();
    client.failInitialize = new Error("no model route");
    await executor.start(task(), sink);

    expect(sink.reports).toHaveLength(1);
    expect(sink.reports[0]?.kind).toBe("failed");
    expect(sink.reports[0]?.summary).toMatch(/failed to start DSH SDK runtime/);
    expect(client.closeCount).toBe(1);

    client.failInitialize = undefined;
    await executor.start(task({ assignmentId: "a2" }), sink);
    expect(client.initializeCalls).toHaveLength(2);
    expect(client.prompts).toEqual([{ sessionId: "a2", text: "do the work" }]);

    const prompt = setup();
    prompt.client.failPrompt = new Error("prompt rejected");
    await prompt.executor.start(task(), prompt.sink);
    expect(prompt.sink.reports).toHaveLength(1);
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
    });
    executors.push(executor);

    await executor.start(task({ deadlineAt: "2026-11-16T00:00:00.000Z" }), sink);

    expect(spawned).toEqual([]);
    expect(sink.reports).toEqual([
      { kind: "failed", summary: "assignment deadline had already passed" },
    ]);
  });
});

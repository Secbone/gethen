import type {
  ExecutorEscalation,
  ExecutorEvent,
  ExecutorReport,
  ExecutorSink,
  ExecutorTask,
} from "@gethen/worker-agent";
import { afterEach, describe, expect, it } from "vitest";
import { createSdkExecutor, type SdkExecutor, type SdkExecutorOptions } from "./executor";
import { FakeSdkClient } from "./test-doubles";

const AT = "2026-11-16T10:00:00.000Z";

class RecordingSink implements ExecutorSink {
  readonly events: ExecutorEvent[] = [];
  readonly escalations: ExecutorEscalation[] = [];
  readonly reports: ExecutorReport[] = [];

  event(event: ExecutorEvent): void {
    this.events.push(event);
  }

  escalation(escalation: ExecutorEscalation): void {
    this.escalations.push(escalation);
  }

  report(report: ExecutorReport): void {
    this.reports.push(report);
  }
}

const running: SdkExecutor[] = [];

afterEach(async () => {
  for (const executor of running.splice(0)) await executor.close();
});

function setup(options: Partial<SdkExecutorOptions> = {}) {
  const client = new FakeSdkClient();
  const sink = new RecordingSink();
  const executor = createSdkExecutor({
    provider: "deepseek",
    model: "deepseek-v4",
    clientFactory: () => client,
    ...options,
  });
  running.push(executor);
  return { client, sink, executor };
}

function task(overrides: Partial<ExecutorTask> = {}): ExecutorTask {
  return {
    assignmentId: "a1",
    ticketId: "t1",
    workspace: "/work/ws",
    packDigest: "sha256:pack",
    envelope: { prompt: "You are the Gethen Worker." },
    ...overrides,
  };
}

function reportBlock(body: Record<string, unknown>): string {
  return `[GETHEN-REPORT]\n${JSON.stringify(body)}\n[/GETHEN-REPORT]`;
}

async function tick(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

describe("createSdkExecutor", () => {
  it("starts one runtime, initializes it and prompts the assignment session", async () => {
    const ctx = setup();
    const handle = await ctx.executor.start(task(), ctx.sink);

    expect(ctx.client.startCalls).toEqual([{ cwd: "/work/ws" }]);
    expect(ctx.client.initializeCalls).toEqual([
      {
        provider: "deepseek",
        model: "deepseek-v4",
        cwd: "/work/ws",
        approvalRequests: true,
        userQuestions: true,
      },
    ]);
    expect(ctx.client.prompts).toEqual([{ sessionId: "a1", text: "You are the Gethen Worker." }]);
    expect(ctx.client.waitCalls).toEqual(["a1"]);
    expect(handle).toBeDefined();
  });

  it("passes the configured command, args and timeout, and derives the cwd", async () => {
    const ctx = setup({
      dshCommand: "/opt/dsh",
      dshArgs: ["--profile", "sdk-test"],
      requestTimeoutMs: 1_234,
      cwdFor: (workspace) => `/repos${workspace}`,
    });

    await ctx.executor.start(task({ workspace: "/project" }), ctx.sink);

    expect(ctx.client.startCalls).toEqual([
      {
        cwd: "/repos/project",
        command: "/opt/dsh",
        args: ["--profile", "sdk-test"],
        requestTimeoutMs: 1_234,
      },
    ]);
    expect(ctx.client.initializeCalls[0]?.cwd).toBe("/repos/project");
  });

  it("reuses the workspace runtime for another assignment and releases it on close", async () => {
    const ctx = setup();
    await ctx.executor.start(task({ assignmentId: "a1" }), ctx.sink);
    await ctx.executor.start(task({ assignmentId: "a2" }), ctx.sink);

    expect(ctx.client.startCalls).toHaveLength(1);
    expect(ctx.client.initializeCalls).toHaveLength(1);
    expect(ctx.client.prompts.map((prompt) => prompt.sessionId)).toEqual(["a1", "a2"]);

    await ctx.executor.close();
    expect(ctx.client.closeCount).toBe(1);
  });

  it("falls back to the JSON envelope when it carries no prompt string", async () => {
    const ctx = setup();
    const envelope = { objective: "fix the bug", ticketId: "t1" };
    await ctx.executor.start(task({ envelope }), ctx.sink);

    expect(ctx.client.prompts[0]?.text).toBe(JSON.stringify(envelope));
  });

  it("forwards only the session's events to the sink", async () => {
    const ctx = setup();
    await ctx.executor.start(task(), ctx.sink);

    ctx.client.emitEvent({ sessionId: "other", event: { type: "message", text: "nope" } });
    ctx.client.emitEvent({ sessionId: "a1", event: { type: "tool.start", name: "shell" } });

    expect(ctx.sink.events).toEqual([{ type: "tool.start", name: "shell" }]);
  });

  it("maps an approval request to a permission escalation and allows on allow", async () => {
    const ctx = setup();
    const handle = await ctx.executor.start(task(), ctx.sink);

    const approval = ctx.client.askApproval({ sessionId: "a1", toolName: "shell" });
    expect(ctx.sink.escalations).toHaveLength(1);
    expect(ctx.sink.escalations[0]).toMatchObject({
      kind: "permission",
      question: "allow shell?",
      options: [
        { id: "allow", label: "Allow once" },
        { id: "reject", label: "Reject" },
      ],
    });
    expect(ctx.sink.escalations[0]?.escalationId).toMatch(/^a1:approval:/);

    await handle.sendResolution({ kind: "answer", option: "allow", text: "go ahead" });
    await expect(approval).resolves.toBe("allowed-once");
  });

  it("rejects an approval on a reject or escalated resolution", async () => {
    const ctx = setup();
    const handle = await ctx.executor.start(task(), ctx.sink);

    const rejected = ctx.client.askApproval({ sessionId: "a1", toolName: "write" });
    await handle.sendResolution({ kind: "answer", option: "reject", text: "no" });
    await expect(rejected).resolves.toBe("rejected");

    const escalated = ctx.client.askApproval({ sessionId: "a1", toolName: "shell" });
    await handle.sendResolution({ kind: "escalate", text: "a person must decide" });
    await expect(escalated).resolves.toBe("rejected");
  });

  it("maps one user question to a decision escalation and back to answers", async () => {
    const ctx = setup();
    const handle = await ctx.executor.start(task(), ctx.sink);

    const asked = ctx.client.askQuestion({
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

    expect(ctx.sink.escalations[0]).toMatchObject({
      kind: "decision",
      question: "Merge or rebase?",
      options: [
        { id: "q0", label: "Merge" },
        { id: "q1", label: "Rebase" },
      ],
    });

    await handle.sendResolution({ kind: "answer", option: "q1", text: "Rebase" });
    await expect(asked).resolves.toEqual({ answers: [{ id: "q1", selected: ["Rebase"] }] });
  });

  it("asks a multi-question batch one question at a time", async () => {
    const ctx = setup();
    const handle = await ctx.executor.start(task(), ctx.sink);

    const asked = ctx.client.askQuestion({
      requestId: "q-2",
      sessionId: "a1",
      questions: [
        { id: "q1", question: "Env?", options: [{ label: "Staging" }, { label: "Prod" }] },
        { id: "q2", question: "Version?", options: [{ label: "v1" }, { label: "v2" }] },
      ],
    });

    expect(ctx.sink.escalations).toHaveLength(1);
    expect(ctx.sink.escalations[0]?.question).toBe("Env? (1/2)");
    await handle.sendResolution({ kind: "answer", option: "q0", text: "Staging" });
    await tick();

    expect(ctx.sink.escalations).toHaveLength(2);
    expect(ctx.sink.escalations[1]?.question).toBe("Version? (2/2)");
    await handle.sendResolution({ kind: "answer", option: "q1", text: "v2" });

    await expect(asked).resolves.toEqual({
      answers: [
        { id: "q1", selected: ["Staging"] },
        { id: "q2", selected: ["v2"] },
      ],
    });
  });

  it("maps a free-text answer to custom text and cancels on a drop resolution", async () => {
    const free = setup();
    const freeHandle = await free.executor.start(task(), free.sink);
    const asked = free.client.askQuestion({
      requestId: "q-3",
      sessionId: "a1",
      questions: [{ id: "q1", question: "Which environment?" }],
    });
    expect(free.sink.escalations[0]?.options).toEqual([]);
    await freeHandle.sendResolution({ kind: "answer", text: "Staging" });
    await expect(asked).resolves.toEqual({
      answers: [{ id: "q1", selected: [], custom: "Staging" }],
    });

    const dropped = setup();
    const dropHandle = await dropped.executor.start(task(), dropped.sink);
    const droppedAsk = dropped.client.askQuestion({
      requestId: "q-4",
      sessionId: "a1",
      questions: [{ id: "q1", question: "Proceed?", options: [{ label: "Yes" }, { label: "No" }] }],
    });
    await dropHandle.sendResolution({ kind: "drop", text: "skip it" });
    await expect(droppedAsk).resolves.toEqual({ cancelled: true });
  });

  it("reports the last parsed worker report after the wait settles", async () => {
    const ctx = setup();
    const handle = await ctx.executor.start(task(), ctx.sink);

    ctx.client.emitAssistantText(
      "a1",
      reportBlock({ kind: "progress", summary: "halfway", at: AT }),
    );
    ctx.client.emitAssistantText(
      "a1",
      reportBlock({ kind: "ready", summary: "finished the work", at: AT }),
    );
    ctx.client.resolveWait("a1");
    await tick();

    expect(ctx.sink.reports).toEqual([{ kind: "ready", summary: "finished the work" }]);
    await handle.cancel();
    expect(ctx.sink.reports).toHaveLength(1);
  });

  it("reports waiting without a report and failed on an invalid report", async () => {
    const clean = setup();
    await clean.executor.start(task(), clean.sink);
    clean.client.emitAssistantText("a1", "here is some work without a report");
    clean.client.resolveWait("a1");
    await tick();
    expect(clean.sink.reports).toEqual([{ kind: "waiting", summary: "worker produced no report" }]);

    const broken = setup();
    await broken.executor.start(task(), broken.sink);
    broken.client.emitAssistantText("a1", "[GETHEN-REPORT]\n{not json\n[/GETHEN-REPORT]");
    broken.client.resolveWait("a1");
    await tick();
    expect(broken.sink.reports).toHaveLength(1);
    expect(broken.sink.reports[0]?.kind).toBe("failed");
    expect(broken.sink.reports[0]?.summary).toMatch(/invalid/i);
  });

  it("reports failed when the wait or the prompt fails", async () => {
    const down = setup();
    await down.executor.start(task(), down.sink);
    down.client.rejectWait("a1", new Error("transport down"));
    await tick();
    expect(down.sink.reports).toEqual([
      { kind: "failed", summary: "DSH SDK wait failed: transport down" },
    ]);

    const refused = setup();
    refused.client.failPrompt = new Error("prompt rejected");
    await refused.executor.start(task(), refused.sink);
    expect(refused.sink.reports).toHaveLength(1);
    expect(refused.sink.reports[0]?.kind).toBe("failed");
    expect(refused.sink.reports[0]?.summary).toMatch(/failed to prompt/i);
  });

  it("reports failed when initialization fails and retries on the next dispatch", async () => {
    const ctx = setup();
    ctx.client.failInitialize = new Error("no model route");
    await ctx.executor.start(task(), ctx.sink);

    expect(ctx.sink.reports).toHaveLength(1);
    expect(ctx.sink.reports[0]?.kind).toBe("failed");
    expect(ctx.sink.reports[0]?.summary).toMatch(/failed to start DSH SDK runtime/);
    expect(ctx.client.closeCount).toBe(1);

    ctx.client.failInitialize = undefined;
    await ctx.executor.start(task({ assignmentId: "a2" }), ctx.sink);
    expect(ctx.client.initializeCalls).toHaveLength(2);
    expect(ctx.client.prompts).toEqual([{ sessionId: "a2", text: "You are the Gethen Worker." }]);
  });

  it("cancels by closing the runtime and reporting failed exactly once", async () => {
    const ctx = setup();
    const handle = await ctx.executor.start(task(), ctx.sink);
    const approval = ctx.client.askApproval({ sessionId: "a1", toolName: "shell" });

    await handle.cancel("superseded by a person");
    await tick();

    expect(ctx.client.closeCount).toBe(1);
    expect(ctx.sink.reports).toEqual([
      { kind: "failed", summary: "cancelled: superseded by a person" },
    ]);
    await expect(approval).resolves.toBe("rejected");
    await tick();
    expect(ctx.sink.reports).toHaveLength(1);
  });

  it("refuses an assignment whose deadline already passed, without spawning", async () => {
    let created = 0;
    const sink = new RecordingSink();
    const executor = createSdkExecutor({
      provider: "deepseek",
      model: "deepseek-v4",
      clientFactory: () => {
        created += 1;
        return new FakeSdkClient();
      },
      now: () => new Date("2026-11-16T10:00:00.000Z"),
    });
    running.push(executor);

    const handle = await executor.start(task({ deadlineAt: "2026-11-16T09:59:59.000Z" }), sink);

    expect(handle).toBeDefined();
    expect(created).toBe(0);
    expect(sink.reports).toEqual([
      { kind: "failed", summary: "assignment deadline had already passed" },
    ]);
  });
});

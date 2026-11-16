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
  mapExecutorReport,
  mapHarnessResolution,
  type SdkHarnessExecutor,
  type SdkHarnessExecutorOptions,
  toExecutorTask,
} from "./harness-adapter";
import { FakeSdkClient } from "./test-doubles";

const AT = "2026-11-17T00:00:00.000Z";
const FUTURE = "2026-11-17T01:00:00.000Z";

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

async function tick(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

describe("toExecutorTask", () => {
  it("folds the prompt into the envelope and maps packDigest/model/deadline", () => {
    expect(
      toExecutorTask(
        task({
          envelope: { proto: "gethen/assignment/v1", ticket: { id: "t1" } },
          metadata: { packDigest: "sha256:pack", origin: "master" },
          model: "deepseek-v4.1-flash",
          deadlineAt: FUTURE,
        }),
      ),
    ).toEqual({
      assignmentId: "a1",
      ticketId: "t1",
      workspace: "/work/ws",
      packDigest: "sha256:pack",
      envelope: {
        proto: "gethen/assignment/v1",
        ticket: { id: "t1" },
        prompt: "do the work",
      },
      model: "deepseek-v4.1-flash",
      deadlineAt: FUTURE,
    });

    expect(toExecutorTask(task()).packDigest).toBe("");
    expect(toExecutorTask(task()).envelope).toEqual({ prompt: "do the work" });
  });
});

describe("createSdkHarnessExecutor", () => {
  it("starts the DSH session for a harness task and prompts it", async () => {
    const { client, sink, executor } = setup();
    const handle = await executor.start(
      task({ metadata: { packDigest: "sha256:pack" }, model: "m1" }),
      sink,
    );

    expect(handle).toBeDefined();
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
  });

  it("maps approval requests to escalations and resolutions back", async () => {
    const { client, sink, executor } = setup();
    const handle = await executor.start(task(), sink);

    const approval = client.askApproval({ sessionId: "a1", toolName: "shell" });
    expect(sink.escalations).toHaveLength(1);
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

    expect(mapHarnessResolution({ kind: "answer", option: "allow" })).toEqual({
      kind: "answer",
      option: "allow",
      text: "",
    });
    await handle.sendResolution({ kind: "answer", option: "allow", text: "go ahead" });
    await expect(approval).resolves.toBe("allowed-once");
  });

  it.each([
    ["ready", { kind: "ready", summary: "done" }, { kind: "ready", summary: "done" }],
    [
      "waiting",
      { kind: "waiting", summary: "need a person", question: "who reviews?" },
      { kind: "waiting", summary: "need a person", question: "who reviews?" },
    ],
    [
      "blocked",
      { kind: "blocked", summary: "blocked", question: "which env?" },
      { kind: "waiting", summary: "blocked", question: "which env?" },
    ],
    ["failed", { kind: "failed", summary: "broke" }, { kind: "failed", summary: "broke" }],
  ] as const)(
    "maps a %s report onto the harness three-state report",
    async (_label, body, expected) => {
      const { client, sink, executor } = setup();
      await executor.start(task(), sink);

      client.emitAssistantText("a1", reportBlock({ ...body, at: AT }));
      client.resolveWait("a1");
      await tick();

      expect(sink.reports).toEqual([expected]);
    },
  );

  it("maps the remaining worker-protocol report kinds predictably", () => {
    const base = { summary: "report" };
    expect(mapExecutorReport({ ...base, kind: "progress" })).toEqual({
      kind: "waiting",
      summary: "report",
    });
    expect(mapExecutorReport({ ...base, kind: "dropped" })).toEqual({
      kind: "ready",
      summary: "report",
    });
    expect(
      mapExecutorReport({
        ...base,
        kind: "blocked",
        question: "which env?",
        usage: { outputTokens: 3 },
      }),
    ).toEqual({
      kind: "waiting",
      summary: "report",
      question: "which env?",
      usage: { outputTokens: 3 },
    });
  });

  it("delegates cancel to the DSH execution and reports failed", async () => {
    const { client, sink, executor } = setup();
    const handle = await executor.start(task(), sink);

    await handle.cancel("superseded by a person");

    expect(client.closeCount).toBe(1);
    expect(sink.reports).toHaveLength(1);
    expect(sink.reports[0]).toMatchObject({ kind: "failed" });
    expect(sink.reports[0]?.summary).toMatch(/cancelled: superseded by a person/);
  });

  it("reports failures as failed reports instead of throwing", async () => {
    const init = setup();
    init.client.failInitialize = new Error("no model route");
    const initHandle = await init.executor.start(task(), init.sink);
    expect(init.sink.reports).toHaveLength(1);
    expect(init.sink.reports[0]).toMatchObject({ kind: "failed" });
    expect(init.sink.reports[0]?.summary).toMatch(/failed to start DSH SDK runtime/);
    // The handle is still usable and duplicate calls do not throw or re-report.
    await expect(initHandle.sendResolution({ kind: "drop" })).resolves.toBeUndefined();
    await expect(initHandle.cancel("again")).resolves.toBeUndefined();
    expect(init.sink.reports).toHaveLength(1);

    const prompt = setup();
    prompt.client.failPrompt = new Error("prompt rejected");
    await prompt.executor.start(task(), prompt.sink);
    expect(prompt.sink.reports).toHaveLength(1);
    expect(prompt.sink.reports[0]?.summary).toMatch(/failed to prompt DSH SDK session/);

    const expired = setup();
    await expired.executor.start(task({ deadlineAt: "2026-11-16T00:00:00.000Z" }), expired.sink);
    expect(expired.sink.reports).toEqual([
      { kind: "failed", summary: "assignment deadline had already passed" },
    ]);
    expect(expired.client.initializeCalls).toEqual([]);
  });

  it("reuses one client per workspace and releases it on close", async () => {
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

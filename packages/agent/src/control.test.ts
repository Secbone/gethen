import type { HarnessTaskState } from "@gethen/harness";
import type { Envelope } from "@gethen/mailbox";
import { newEnvelope } from "@gethen/mailbox";
import { afterEach, describe, expect, it } from "vitest";

import {
  agentMailboxName,
  createMailboxAgent,
  type MailboxAgent,
  type MailboxAgentErrorContext,
  type MailboxAgentOptions,
  MESSAGE_TYPES,
} from "./consumer";
import { FakeHarness, type FakeHarnessOptions, FakeMailbox } from "./test-doubles";

const AT = "2026-11-17T00:00:00.000Z";
const WORKER = "w1";
const MAILBOX = agentMailboxName(WORKER);
const ACTIVE: MailboxAgent[] = [];

const LIVE_STATE: HarnessTaskState = {
  assignmentId: "a1",
  phase: "thinking",
  step: 2,
  lastEventAt: AT,
  lastMessage: "working on it",
  seq: 3,
};

afterEach(async () => {
  for (const agent of ACTIVE.splice(0)) await agent.stop();
});

async function tick(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

function setup(
  harnessOptions: FakeHarnessOptions = {},
  agentOptions: Partial<MailboxAgentOptions> = {},
) {
  const mailbox = new FakeMailbox();
  const harness = new FakeHarness(harnessOptions);
  const errors: Array<{ error: unknown; context: MailboxAgentErrorContext }> = [];
  const agent = createMailboxAgent({
    workerId: WORKER,
    mailbox,
    harness,
    now: () => new Date(AT),
    stopTimeoutMs: 10,
    ...agentOptions,
    onError: (error, context) => {
      errors.push({ error, context });
    },
  });
  ACTIVE.push(agent);
  return { agent, mailbox, harness, errors };
}

function assignEnvelope(id: string, assignmentId: string): Envelope {
  return newEnvelope({
    id,
    source: "master:router",
    type: MESSAGE_TYPES.assign,
    to: MAILBOX,
    time: AT,
    subject: assignmentId,
    data: {
      assignmentId,
      ticketId: "t1",
      workspace: "/work/ws",
      prompt: "do the work",
    },
  });
}

function statusEnvelope(
  overrides: { id?: string; data?: Record<string, unknown>; extra?: Partial<Envelope> } = {},
): Envelope {
  return newEnvelope({
    id: overrides.id ?? "env-status-1",
    source: "master:router",
    type: MESSAGE_TYPES.status,
    to: MAILBOX,
    time: AT,
    subject: "a1",
    correlationId: "a1",
    data: { assignmentId: "a1", ...(overrides.data ?? {}) },
    ...(overrides.extra ?? {}),
  });
}

function steerEnvelope(
  overrides: { id?: string; assignmentId?: string; text?: unknown; extra?: Partial<Envelope> } = {},
): Envelope {
  const assignmentId = overrides.assignmentId ?? "a1";
  return newEnvelope({
    id: overrides.id ?? "env-steer-1",
    source: "master:router",
    type: MESSAGE_TYPES.steer,
    to: MAILBOX,
    time: AT,
    subject: assignmentId,
    correlationId: assignmentId,
    data: {
      assignmentId,
      ...(overrides.text === undefined ? { text: "focus on the tests" } : { text: overrides.text }),
    },
    ...(overrides.extra ?? {}),
  });
}

function cancelEnvelope(
  overrides: { id?: string; assignmentId?: string; extra?: Partial<Envelope> } = {},
): Envelope {
  const assignmentId = overrides.assignmentId ?? "a1";
  return newEnvelope({
    id: overrides.id ?? "env-cancel-1",
    source: "master:router",
    type: MESSAGE_TYPES.cancel,
    to: MAILBOX,
    time: AT,
    subject: assignmentId,
    correlationId: assignmentId,
    data: { assignmentId, reason: "user asked" },
    ...(overrides.extra ?? {}),
  });
}

describe("status/steer/cancel control surface", () => {
  it("exposes the message types the mailbox bridge already uses", () => {
    expect(MESSAGE_TYPES).toMatchObject({
      status: "gethen.task.status",
      statusResult: "gethen.task.status.result",
      steer: "gethen.task.steer",
      cancel: "gethen.task.cancel",
    });
  });

  it("replies to a status request with the live execution state and acks it", async () => {
    const { agent, mailbox } = setup({ withState: true, state: LIVE_STATE });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    mailbox.enqueue(MAILBOX, statusEnvelope());
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.statusResult)).toMatchObject({
      source: "agent:w1",
      to: "mailbox:master",
      subject: "a1",
      correlationId: "env-status-1",
      causationId: "env-status-1",
      data: { assignmentId: "a1", state: LIVE_STATE, at: AT },
    });
    expect(mailbox.ackIds()).toContain("env-status-1");
  });

  it("replies state null for an unknown assignment, honouring replyTo", async () => {
    const { agent, mailbox } = setup();
    mailbox.enqueue(
      MAILBOX,
      statusEnvelope({
        id: "env-status-2",
        data: { assignmentId: "missing" },
        extra: { replyTo: "mailbox:master-2", subject: "missing", correlationId: "missing" },
      }),
    );
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.statusResult)).toMatchObject({
      to: "mailbox:master-2",
      correlationId: "env-status-2",
      data: { assignmentId: "missing", state: null, at: AT },
    });
    expect(mailbox.ackIds()).toContain("env-status-2");
  });

  it("synthesizes a minimal state when the harness has no state()", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    harness.emitEvent(0, { type: "message", at: AT, data: { text: "working" } });
    await tick();
    const progress = mailbox.lastSent(MESSAGE_TYPES.progress);
    expect(progress?.data).not.toHaveProperty("state");

    mailbox.enqueue(MAILBOX, statusEnvelope());
    await agent.runOnce();
    expect(mailbox.lastSent(MESSAGE_TYPES.statusResult)?.data).toEqual({
      assignmentId: "a1",
      at: AT,
      state: {
        assignmentId: "a1",
        phase: "thinking",
        step: 0,
        lastEventAt: AT,
        seq: 1,
      },
    });
  });

  it("nacks a status request once when sending the reply fails", async () => {
    const { agent, mailbox, errors } = setup({ withState: true, state: LIVE_STATE });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    mailbox.failSendTimes = 1;
    mailbox.enqueue(MAILBOX, statusEnvelope());
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.statusResult)).toBeUndefined();
    expect(mailbox.nacks.at(-1)).toMatchObject({ action: "requeue", ids: ["env-status-1"] });
    expect(mailbox.ackIds()).not.toContain("env-status-1");
    expect(errors.some((entry) => entry.context.phase === "status")).toBe(true);
    // The request was not double-nacked by the outer runOnce catch.
    expect(mailbox.nacks.filter((nack) => nack.ids.includes("env-status-1"))).toHaveLength(1);
  });

  it("attaches the live state to progress messages", async () => {
    const { agent, mailbox, harness } = setup({ withState: true, state: LIVE_STATE });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    harness.emitEvent(0, { type: "message", at: AT, data: { text: "working" } });
    await tick();
    expect(mailbox.lastSent(MESSAGE_TYPES.progress)?.data).toMatchObject({
      assignmentId: "a1",
      state: LIVE_STATE,
    });
  });

  it("lets a phase change through the progress throttle", async () => {
    let clock = new Date(AT);
    const thinking: HarnessTaskState = { ...LIVE_STATE, phase: "thinking" };
    const tool: HarnessTaskState = { ...LIVE_STATE, phase: "tool" };
    const { agent, mailbox, harness } = setup(
      { withState: true, state: thinking },
      { progressThrottleMs: 1_000, now: () => clock },
    );
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    harness.emitEvent(0, { type: "message", at: AT, data: { n: 1 } });
    await tick();
    expect(mailbox.sentByType(MESSAGE_TYPES.progress)).toHaveLength(1);

    harness.emitEvent(0, { type: "message", at: AT, data: { n: 2 } });
    await tick();
    expect(mailbox.sentByType(MESSAGE_TYPES.progress)).toHaveLength(1);

    harness.setState(0, tool);
    harness.emitEvent(0, { type: "tool", at: AT, data: { tool: "bash" } });
    await tick();
    const afterPhaseChange = mailbox.sentByType(MESSAGE_TYPES.progress);
    expect(afterPhaseChange).toHaveLength(2);
    expect(afterPhaseChange[1]?.data).toMatchObject({ state: { phase: "tool" } });

    clock = new Date(Date.parse(AT) + 1_000);
    harness.emitEvent(0, { type: "message", at: AT, data: { n: 3 } });
    await tick();
    expect(mailbox.sentByType(MESSAGE_TYPES.progress)).toHaveLength(3);
  });
});

describe("steer", () => {
  it("forwards steer text to the execution and acks", async () => {
    const { agent, mailbox, harness, errors } = setup({ withSteer: true });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    mailbox.enqueue(MAILBOX, steerEnvelope());
    await agent.runOnce();

    expect(harness.executions[0]?.steerCalls).toEqual(["focus on the tests"]);
    expect(mailbox.ackIds()).toContain("env-steer-1");
    expect(errors).toHaveLength(0);
  });

  it("reports harnesses without steer capability and still acks", async () => {
    const { agent, mailbox, errors } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    mailbox.enqueue(MAILBOX, steerEnvelope());
    await agent.runOnce();

    expect(mailbox.ackIds()).toContain("env-steer-1");
    expect(mailbox.nacks.some((nack) => nack.ids.includes("env-steer-1"))).toBe(false);
    expect(errors[0]?.context.phase).toBe("steer");
    expect(String(errors[0]?.error)).toMatch(/steer capability/);
  });

  it("reports a failed steer and requeues it once", async () => {
    const { agent, mailbox, errors } = setup({
      withSteer: true,
      steerError: new Error("steer broke"),
    });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    mailbox.enqueue(MAILBOX, steerEnvelope());
    await agent.runOnce();

    expect(mailbox.nacks.at(-1)).toMatchObject({ action: "requeue", ids: ["env-steer-1"] });
    expect(mailbox.ackIds()).not.toContain("env-steer-1");
    expect(errors[0]?.context.phase).toBe("steer");
  });
});

describe("cancel", () => {
  it("cancels through execution.cancel even when the harness exposes interrupt", async () => {
    const { agent, mailbox, harness } = setup({ withInterrupt: true });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    mailbox.enqueue(MAILBOX, cancelEnvelope());
    await agent.runOnce();

    // `cancel` owns the terminal report; `interrupt` alone may leave the
    // assignment open, so the agent must not bypass it.
    expect(harness.cancels).toEqual(["user asked"]);
    expect(harness.executions[0]?.interruptCalls).toBe(0);
    expect(mailbox.ackIds()).toContain("env-cancel-1");
  });

  it("falls back to execution.cancel when the harness has no interrupt", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    mailbox.enqueue(MAILBOX, cancelEnvelope());
    await agent.runOnce();

    expect(harness.cancels).toEqual(["user asked"]);
    expect(mailbox.ackIds()).toContain("env-cancel-1");
  });

  it("cancels one execution only once", async () => {
    const { agent, mailbox, harness } = setup({ withInterrupt: true });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();

    mailbox.enqueue(MAILBOX, cancelEnvelope({ id: "env-cancel-1" }));
    await agent.runOnce();
    mailbox.enqueue(MAILBOX, cancelEnvelope({ id: "env-cancel-2" }));
    await agent.runOnce();

    expect(harness.cancels).toEqual(["user asked"]);
    expect(harness.executions[0]?.interruptCalls).toBe(0);
    expect(mailbox.ackIds()).toEqual(expect.arrayContaining(["env-cancel-1", "env-cancel-2"]));
  });

  it("acks a cancel for an unknown assignment without control calls", async () => {
    const { agent, mailbox, harness, errors } = setup();
    mailbox.enqueue(MAILBOX, cancelEnvelope({ assignmentId: "missing" }));
    await agent.runOnce();

    expect(mailbox.ackIds()).toContain("env-cancel-1");
    expect(harness.cancels).toEqual([]);
    expect(errors).toHaveLength(0);
  });
});

describe("concurrent assignments", () => {
  it("routes status/steer/cancel to the right execution", async () => {
    const { agent, mailbox, harness } = setup(
      { withState: true, withSteer: true, withInterrupt: true, state: LIVE_STATE },
      { quota: 2, batchSize: 4 },
    );
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-2", "a2"));
    await agent.runOnce();
    expect(harness.tasks.map((task) => task.assignmentId)).toEqual(["a1", "a2"]);
    harness.setState(0, { ...LIVE_STATE, assignmentId: "a1" });
    harness.setState(1, { ...LIVE_STATE, assignmentId: "a2", seq: 9 });

    mailbox.enqueue(
      MAILBOX,
      statusEnvelope({
        id: "env-status-a2",
        data: { assignmentId: "a2" },
        extra: { subject: "a2", correlationId: "a2" },
      }),
    );
    mailbox.enqueue(MAILBOX, steerEnvelope({ id: "env-steer-a1", assignmentId: "a1" }));
    mailbox.enqueue(MAILBOX, cancelEnvelope({ id: "env-cancel-a2", assignmentId: "a2" }));
    await agent.runOnce();

    const reply = mailbox.lastSent(MESSAGE_TYPES.statusResult);
    if (reply === undefined) throw new Error("expected a status.result envelope");
    const replyData = reply.data as { assignmentId: string; state: HarnessTaskState };
    expect(replyData.assignmentId).toBe("a2");
    expect(replyData.state).toMatchObject({ assignmentId: "a2", seq: 9 });
    expect(harness.executions[0]?.steerCalls).toEqual(["focus on the tests"]);
    expect(harness.executions[1]?.steerCalls).toEqual([]);
    expect(harness.cancels).toEqual(["user asked"]);
    expect(harness.executions[0]?.interruptCalls).toBe(0);
    expect(harness.executions[1]?.interruptCalls).toBe(0);
    expect(mailbox.ackIds()).toEqual(
      expect.arrayContaining(["env-status-a2", "env-steer-a1", "env-cancel-a2"]),
    );
  });
});

describe("progress heartbeat", () => {
  it("sends periodic progress while an execution is running and stops after it settles", async () => {
    const { agent, mailbox, harness } = setup(
      { withState: true, state: LIVE_STATE },
      { progressHeartbeatMs: 10 },
    );
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1", "a1"));
    await agent.runOnce();
    await new Promise((resolve) => setTimeout(resolve, 45));
    const during = mailbox.sentByType(MESSAGE_TYPES.progress).length;
    expect(during).toBeGreaterThanOrEqual(1);
    expect(mailbox.sentByType(MESSAGE_TYPES.progress).at(-1)?.data).toMatchObject({
      assignmentId: "a1",
      state: { assignmentId: "a1" },
    });

    harness.report(0, { kind: "ready", summary: "done" });
    await new Promise((resolve) => setTimeout(resolve, 5));
    const afterReport = mailbox.sentByType(MESSAGE_TYPES.progress).length;
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(mailbox.sentByType(MESSAGE_TYPES.progress).length).toBe(afterReport);
  });
});

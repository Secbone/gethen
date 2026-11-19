import type { Envelope, ReceivedMessage } from "@gethen/mailbox";
import { newEnvelope } from "@gethen/mailbox";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  agentMailboxName,
  assignToHarnessTask,
  createMailboxAgent,
  type MailboxAgent,
  type MailboxAgentErrorContext,
  type MailboxAgentOptions,
  MESSAGE_TYPES,
  toMailboxLike,
} from "./consumer";
import { FakeHarness, FakeMailbox } from "./test-doubles";

const AT = "2026-11-17T00:00:00.000Z";
const WORKER = "w1";
const MAILBOX = agentMailboxName(WORKER);
const ACTIVE: MailboxAgent[] = [];

afterEach(async () => {
  for (const agent of ACTIVE.splice(0)) await agent.stop();
});

async function tick(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

function setup(overrides: Partial<MailboxAgentOptions> = {}) {
  const mailbox = new FakeMailbox();
  const harness = new FakeHarness();
  const errors: Array<{ error: unknown; context: MailboxAgentErrorContext }> = [];
  const agent = createMailboxAgent({
    workerId: WORKER,
    mailbox,
    harness,
    now: () => new Date(AT),
    stopTimeoutMs: 10,
    ...overrides,
    onError: (error, context) => {
      errors.push({ error, context });
    },
  });
  ACTIVE.push(agent);
  return { agent, mailbox, harness, errors };
}

interface AssignOverrides {
  id?: string;
  data?: Record<string, unknown>;
  extra?: Partial<Envelope>;
}

function assignEnvelope(overrides: AssignOverrides = {}): Envelope {
  return newEnvelope({
    id: overrides.id ?? "env-assign-1",
    source: "agent:master",
    type: MESSAGE_TYPES.assign,
    to: MAILBOX,
    time: AT,
    data: {
      assignmentId: "a1",
      ticketId: "t1",
      workspace: "/work/ws",
      prompt: "do the work",
      ...(overrides.data ?? {}),
    },
    ...(overrides.extra ?? {}),
  });
}

function answerEnvelope(
  ask: Envelope,
  overrides: { id?: string; data?: Record<string, unknown> } = {},
) {
  return newEnvelope({
    id: overrides.id ?? "env-answer-1",
    source: "agent:master",
    type: MESSAGE_TYPES.answer,
    to: MAILBOX,
    time: AT,
    correlationId: ask.id,
    data: {
      assignmentId: "a1",
      escalationId: "esc-1",
      resolution: { kind: "answer", option: "yes", text: "go ahead" },
      ...(overrides.data ?? {}),
    },
  });
}

function cancelEnvelope(overrides: { id?: string; data?: Record<string, unknown> } = {}): Envelope {
  return newEnvelope({
    id: overrides.id ?? "env-cancel-1",
    source: "agent:master",
    type: MESSAGE_TYPES.cancel,
    to: MAILBOX,
    time: AT,
    data: { assignmentId: "a1", reason: "user asked", ...(overrides.data ?? {}) },
  });
}

describe("assignToHarnessTask", () => {
  it("maps the documented fields from an assign envelope", () => {
    const validation = assignToHarnessTask(
      assignEnvelope({
        extra: { deadline: "2026-11-17T02:00:00.000Z", replyTo: "mailbox:master-2" },
        data: {
          model: "deepseek-v4",
          metadata: { packDigest: "sha256:pack" },
          envelope: { proto: "gethen/assignment/v1" },
        },
      }),
    );
    expect(validation.ok).toBe(true);
    if (!validation.ok) return;
    expect(validation.task).toEqual({
      assignmentId: "a1",
      ticketId: "t1",
      workspace: "/work/ws",
      prompt: "do the work",
      model: "deepseek-v4",
      metadata: { packDigest: "sha256:pack" },
      envelope: { proto: "gethen/assignment/v1" },
      deadlineAt: "2026-11-17T02:00:00.000Z",
    });
  });

  it("rejects wrong types, missing fields and bad deadlines", () => {
    expect(assignToHarnessTask(assignEnvelope({ data: { prompt: "" } }))).toMatchObject({
      ok: false,
      code: "missing_prompt",
    });
    expect(assignToHarnessTask(assignEnvelope({ data: { workspace: 7 } }))).toMatchObject({
      ok: false,
      code: "missing_workspace",
    });
    expect(
      assignToHarnessTask(assignEnvelope({ data: { assignmentId: undefined, prompt: "x" } })),
    ).toMatchObject({ ok: false, code: "missing_assignment_id" });
    expect(
      assignToHarnessTask(assignEnvelope({ data: { deadlineAt: "not-a-time" } })),
    ).toMatchObject({ ok: false, code: "invalid_deadline" });

    const wrongType = newEnvelope({
      source: "agent:master",
      type: MESSAGE_TYPES.progress,
      to: MAILBOX,
      time: AT,
      data: {},
    });
    expect(assignToHarnessTask(wrongType)).toMatchObject({ ok: false, code: "wrong_type" });
  });
});

describe("MailboxAgent intake", () => {
  it("starts the harness for an assign and forwards events as progress", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope());

    await expect(agent.runOnce()).resolves.toBe(1);
    expect(harness.tasks).toHaveLength(1);
    expect(harness.tasks[0]).toMatchObject({
      assignmentId: "a1",
      ticketId: "t1",
      workspace: "/work/ws",
      prompt: "do the work",
    });
    expect(agent.active).toBe(1);

    harness.emitEvent(0, { type: "message", at: AT, data: { text: "working" } });
    const progress = mailbox.sentByType(MESSAGE_TYPES.progress);
    expect(progress).toHaveLength(1);
    expect(progress[0]).toMatchObject({
      source: "agent:w1",
      to: "mailbox:master",
      subject: "a1",
      correlationId: "env-assign-1",
      data: { assignmentId: "a1", event: { type: "message", at: AT, data: { text: "working" } } },
    });
  });

  it("sends the result with the assign correlation and acks the assign", async () => {
    const first = setup();
    first.mailbox.enqueue(MAILBOX, assignEnvelope({ extra: { replyTo: "mailbox:master-2" } }));
    await first.agent.runOnce();
    first.harness.report(0, { kind: "ready", summary: "done", usage: { outputTokens: 7 } });
    await tick();

    const result = first.mailbox.lastSent(MESSAGE_TYPES.result);
    expect(result).toMatchObject({
      source: "agent:w1",
      to: "mailbox:master-2",
      subject: "a1",
      correlationId: "env-assign-1",
      data: {
        assignmentId: "a1",
        kind: "ready",
        summary: "done",
        usage: { outputTokens: 7 },
      },
    });
    expect(first.mailbox.ackIds()).toContain("env-assign-1");
    expect(first.agent.active).toBe(0);

    const waiting = setup();
    waiting.mailbox.enqueue(MAILBOX, assignEnvelope());
    await waiting.agent.runOnce();
    waiting.harness.report(0, { kind: "waiting", summary: "need a person" });
    await tick();
    expect(waiting.mailbox.lastSent(MESSAGE_TYPES.result)?.data).toMatchObject({
      assignmentId: "a1",
      kind: "waiting",
      summary: "need a person",
    });
    expect(waiting.mailbox.ackIds()).toContain("env-assign-1");
  });

  it("dead-letters an invalid assign without starting the harness", async () => {
    const { agent, mailbox, harness, errors } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope({ data: { ticketId: 7 } }));

    await agent.runOnce();

    expect(harness.tasks).toHaveLength(0);
    expect(mailbox.nacks.at(-1)).toMatchObject({ action: "dead", ids: ["env-assign-1"] });
    expect(errors).toHaveLength(1);
    expect(errors[0]?.context.phase).toBe("assign");
    expect(String(errors[0]?.error)).toMatch(/ticketId/);
  });

  it("acks duplicate assigns without starting a second execution", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();
    mailbox.enqueue(MAILBOX, assignEnvelope({ id: "env-assign-2" }));
    await agent.runOnce();

    expect(harness.tasks).toHaveLength(1);
    expect(mailbox.ackIds()).toContain("env-assign-2");

    harness.report(0, { kind: "ready", summary: "done" });
    mailbox.enqueue(MAILBOX, assignEnvelope({ id: "env-assign-3" }));
    await agent.runOnce();
    expect(harness.tasks).toHaveLength(1);
    expect(mailbox.ackIds()).toContain("env-assign-3");
  });
});

describe("MailboxAgent escalations", () => {
  it("asks the master, applies the answer and finishes the assignment", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();

    harness.emitEscalation(0, {
      id: "esc-1",
      kind: "decision",
      question: "Merge or rebase?",
      options: [
        { id: "yes", label: "Yes" },
        { id: "no", label: "No" },
      ],
      blocking: true,
    });
    const ask = mailbox.lastSent(MESSAGE_TYPES.ask);
    expect(ask).toMatchObject({
      to: "mailbox:master",
      replyTo: MAILBOX,
      subject: "a1",
      correlationId: "env-assign-1",
      data: {
        assignmentId: "a1",
        escalationId: "esc-1",
        kind: "decision",
        question: "Merge or rebase?",
        options: [
          { id: "yes", label: "Yes" },
          { id: "no", label: "No" },
        ],
      },
    });
    if (ask === undefined) throw new Error("expected an ask envelope");

    mailbox.enqueue(MAILBOX, answerEnvelope(ask));
    await agent.runOnce();

    expect(harness.resolutions).toEqual([{ kind: "answer", option: "yes", text: "go ahead" }]);
    expect(mailbox.ackIds()).toContain("env-answer-1");
    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(1);
    expect(mailbox.ackIds()).toContain("env-assign-1");
  });

  it("ignores answers for another assignment or escalation and keeps waiting", async () => {
    const { agent, mailbox, harness } = setup({ batchSize: 4 });
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();
    harness.emitEscalation(0, {
      id: "esc-1",
      kind: "decision",
      question: "go?",
      options: [{ id: "yes", label: "Yes" }],
      blocking: true,
    });
    const ask = mailbox.lastSent(MESSAGE_TYPES.ask);
    if (ask === undefined) throw new Error("expected an ask envelope");

    mailbox.enqueue(
      MAILBOX,
      answerEnvelope(ask, { id: "env-answer-other", data: { assignmentId: "other" } }),
    );
    mailbox.enqueue(
      MAILBOX,
      answerEnvelope(ask, { id: "env-answer-stale", data: { escalationId: "esc-0" } }),
    );
    await agent.runOnce();

    expect(harness.resolutions).toEqual([]);
    expect(mailbox.ackIds()).toEqual(
      expect.arrayContaining(["env-answer-other", "env-answer-stale"]),
    );

    mailbox.enqueue(MAILBOX, answerEnvelope(ask, { id: "env-answer-ok" }));
    await agent.runOnce();
    expect(harness.resolutions).toHaveLength(1);
  });

  it("cancels the execution and lets the harness report failed once", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();

    mailbox.enqueue(MAILBOX, cancelEnvelope());
    await agent.runOnce();

    expect(harness.cancels).toEqual(["user asked"]);
    const result = mailbox.lastSent(MESSAGE_TYPES.result);
    expect(result?.data).toMatchObject({
      assignmentId: "a1",
      kind: "failed",
      summary: "cancelled: user asked",
    });
    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(1);
    expect(mailbox.ackIds()).toContain("env-assign-1");
    expect(agent.active).toBe(0);
  });
});

describe("MailboxAgent failures and limits", () => {
  it("requeues a failed harness start, then reports failed when attempts run out", async () => {
    const { agent, mailbox, harness, errors } = setup({ maxAttempts: 2 });
    harness.startError = new Error("spawn failed");
    mailbox.enqueue(MAILBOX, assignEnvelope());

    await agent.runOnce();
    expect(mailbox.nacks.at(-1)).toMatchObject({ action: "requeue", ids: ["env-assign-1"] });
    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(0);

    await agent.runOnce();
    const result = mailbox.lastSent(MESSAGE_TYPES.result);
    if (result === undefined) throw new Error("expected a failed result");
    expect(result.data).toMatchObject({ assignmentId: "a1", kind: "failed" });
    expect((result.data as { summary: string }).summary).toMatch(
      /harness failed to start: spawn failed/,
    );
    expect(mailbox.ackIds()).toContain("env-assign-1");
    expect(errors).toHaveLength(2);
    expect(errors[0]?.context.phase).toBe("harness");
  });

  it("never runs more than the quota and requeues extra assigns", async () => {
    const { agent, mailbox, harness } = setup({ quota: 1, batchSize: 2 });
    mailbox.enqueue(MAILBOX, assignEnvelope());
    mailbox.enqueue(MAILBOX, assignEnvelope({ id: "env-assign-2", data: { assignmentId: "a2" } }));

    await agent.runOnce();
    expect(harness.tasks.map((task) => task.assignmentId)).toEqual(["a1"]);

    await agent.runOnce();
    expect(harness.tasks).toHaveLength(1);
    expect(mailbox.nacks.at(-1)).toMatchObject({
      action: "requeue",
      ids: ["env-assign-2"],
    });

    harness.report(0, { kind: "ready", summary: "first done" });
    await agent.runOnce();
    expect(harness.tasks.map((task) => task.assignmentId)).toEqual(["a1", "a2"]);
  });

  it("answers a status probe for a finished assignment with its retained report", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();
    harness.report(0, { kind: "ready", summary: "A-done" });
    await tick();
    expect(agent.active).toBe(0);

    // The result envelope may be lost on the way to the Master; a later status
    // probe must still recover the report instead of reporting "unknown".
    mailbox.enqueue(
      MAILBOX,
      newEnvelope({
        id: "env-status-1",
        source: "master:router",
        type: MESSAGE_TYPES.status,
        to: MAILBOX,
        time: AT,
        data: { assignmentId: "a1" },
        correlationId: "a1",
        replyTo: "mailbox:master",
      }),
    );
    await agent.runOnce();

    const reply = mailbox.lastSent(MESSAGE_TYPES.statusResult);
    expect(reply?.data).toMatchObject({
      assignmentId: "a1",
      state: { assignmentId: "a1", phase: "done", lastMessage: "A-done" },
      report: { assignmentId: "a1", kind: "ready", summary: "A-done" },
    });
  });

  it("keeps a result queued when its send fails and ack after a later flush", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();

    mailbox.failSendTimes = 1;
    harness.report(0, { kind: "ready", summary: "done" });
    await tick();

    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(0);
    expect(mailbox.ackIds()).not.toContain("env-assign-1");
    expect(agent.active).toBe(1);

    await expect(agent.flush()).resolves.toBe(1);
    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(1);
    expect(mailbox.ackIds()).toContain("env-assign-1");
    expect(agent.active).toBe(0);
  });

  it("returns the retained report on status while result delivery still fails", async () => {
    const { agent, mailbox, harness } = setup({ retryDelayMs: 60_000 });
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();

    // Emit the report, then make its result envelope undeliverable. The entry
    // stays settled but present; a status probe must still see the report.
    mailbox.failSendTimes = 1;
    harness.report(0, { kind: "ready", summary: "kept for status" });
    await tick();
    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(0);
    expect(agent.active).toBe(1);

    mailbox.failSendTimes = 1;
    mailbox.enqueue(
      MAILBOX,
      newEnvelope({
        id: "env-status-lost",
        source: "master:router",
        type: MESSAGE_TYPES.status,
        to: MAILBOX,
        time: AT,
        data: { assignmentId: "a1" },
        correlationId: "a1",
        replyTo: "mailbox:master",
      }),
    );
    await agent.runOnce();

    const reply = mailbox.lastSent(MESSAGE_TYPES.statusResult);
    expect(reply?.data).toMatchObject({
      assignmentId: "a1",
      state: { assignmentId: "a1", phase: "done", lastMessage: "kept for status" },
      report: { assignmentId: "a1", kind: "ready", summary: "kept for status" },
    });
    expect(agent.active).toBe(0);

    mailbox.failSendTimes = 0;
    await agent.flush();
  });

  it("fails fast on a workspace resolution error instead of retrying", async () => {
    const { agent, mailbox, harness } = setup({ maxAttempts: 5 });
    harness.startError = new Error(
      'WORKSTREAM_NOT_OWNED: workspace "/srv/projects" has no default',
    );
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();
    await agent.flush();

    const result = mailbox.lastSent(MESSAGE_TYPES.result);
    expect(result?.data).toMatchObject({
      assignmentId: "a1",
      kind: "failed",
      summary: expect.stringContaining("WORKSTREAM_NOT_OWNED"),
    });
    expect(mailbox.nacks).toHaveLength(0);
    expect(agent.active).toBe(0);
  });

  it("retries a failed result send without another inbound message", async () => {
    const { agent, mailbox, harness } = setup({
      retryDelayMs: 10,
      pollIntervalMs: 10_000,
    });
    agent.start();
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();

    mailbox.failSendTimes = 1;
    harness.report(0, { kind: "ready", summary: "retried" });
    await tick();
    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(0);

    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(1);
    expect(mailbox.ackIds()).toContain("env-assign-1");
  });

  it("throttles progress events when configured", async () => {
    let clock = new Date(AT);
    const { agent, mailbox, harness } = setup({
      progressThrottleMs: 1_000,
      now: () => clock,
    });
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();

    harness.emitEvent(0, { type: "message", at: AT, data: { text: "one" } });
    harness.emitEvent(0, { type: "message", at: AT, data: { text: "two" } });
    await tick();
    expect(mailbox.sentByType(MESSAGE_TYPES.progress)).toHaveLength(1);

    clock = new Date(Date.parse(AT) + 1_000);
    harness.emitEvent(0, { type: "message", at: AT, data: { text: "three" } });
    await tick();
    expect(mailbox.sentByType(MESSAGE_TYPES.progress)).toHaveLength(2);
  });
});

describe("MailboxAgent lifecycle", () => {
  it("renews the assign lease while the execution waits for an answer", async () => {
    vi.useFakeTimers();
    try {
      const { agent, mailbox, harness } = setup({ leaseRenewIntervalMs: 1_000 });
      mailbox.enqueue(MAILBOX, assignEnvelope());
      await agent.runOnce();
      harness.emitEscalation(0, {
        id: "esc-1",
        kind: "decision",
        question: "go?",
        options: [{ id: "yes", label: "Yes" }],
        blocking: true,
      });
      expect(mailbox.renewals).toHaveLength(0);

      await vi.advanceTimersByTimeAsync(1_000);
      expect(mailbox.renewals[0]?.ids).toEqual(["env-assign-1"]);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(mailbox.renewals.length).toBeGreaterThanOrEqual(2);

      const stopping = agent.stop();
      await vi.advanceTimersByTimeAsync(50);
      await stopping;
    } finally {
      vi.useRealTimers();
    }
  });

  it("stop cancels a running execution after the grace period", async () => {
    const { agent, mailbox, harness } = setup({ stopTimeoutMs: 5 });
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();

    await agent.stop();

    expect(harness.cancels).toEqual(["agent stopped"]);
    expect(mailbox.sentByType(MESSAGE_TYPES.result)).toHaveLength(1);
    expect(mailbox.ackIds()).toContain("env-assign-1");
    expect(agent.active).toBe(0);
  });

  it("start polls until stopped, then leaves new assigns untouched", async () => {
    vi.useFakeTimers();
    try {
      const { agent, mailbox, harness } = setup({ pollIntervalMs: 100 });
      mailbox.enqueue(MAILBOX, assignEnvelope());
      agent.start();
      await vi.advanceTimersByTimeAsync(100);
      expect(harness.tasks).toHaveLength(1);

      harness.report(0, { kind: "ready", summary: "done" });
      const stopping = agent.stop();
      await vi.advanceTimersByTimeAsync(10);
      await stopping;

      mailbox.enqueue(
        MAILBOX,
        assignEnvelope({ id: "env-assign-2", data: { assignmentId: "a2" } }),
      );
      await vi.advanceTimersByTimeAsync(300);
      expect(harness.tasks).toHaveLength(1);
      expect(agent.active).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("accepts pushed messages when a listen() source is injected", async () => {
    let push: ((message: ReceivedMessage) => Promise<void>) | undefined;
    const listen = vi.fn(
      (mailboxName: string, onMessage: (message: ReceivedMessage) => Promise<void>) => {
        expect(mailboxName).toBe(MAILBOX);
        push = onMessage;
        return () => undefined;
      },
    );
    const { agent, mailbox, harness } = setup({ listen });
    agent.start();
    expect(listen).toHaveBeenCalledTimes(1);

    mailbox.enqueue(MAILBOX, assignEnvelope());
    const [message] = mailbox.receive(MAILBOX, { limit: 1 });
    if (message === undefined || push === undefined) throw new Error("expected a pushed message");
    await push(message);

    expect(harness.tasks).toHaveLength(1);
    harness.report(0, { kind: "ready", summary: "done" });
    await tick();
    expect(mailbox.ackIds()).toContain("env-assign-1");
  });

  it("adapts a bound mailbox-link client through toMailboxLike", async () => {
    const bound: {
      mailbox: string;
      received: ReceivedMessage[];
      sent: Envelope[];
      acks: string[];
      nacks: number;
      renewals: number;
      receive(options?: { limit?: number }): Promise<ReceivedMessage[]>;
      ack(ids: string[]): Promise<number>;
      nack(): Promise<number>;
      renewLeases(): Promise<number>;
      send(envelope: Envelope): Promise<{ mailbox: string; duplicate: boolean }>;
    } = {
      mailbox: MAILBOX,
      received: [],
      sent: [],
      acks: [],
      nacks: 0,
      renewals: 0,
      async receive() {
        return this.received.splice(0, Number.MAX_SAFE_INTEGER);
      },
      async ack(ids) {
        this.acks.push(...ids);
        return ids.length;
      },
      async nack() {
        this.nacks += 1;
        return 0;
      },
      async renewLeases() {
        this.renewals += 1;
        return 0;
      },
      async send(envelope) {
        this.sent.push(envelope);
        return { mailbox: envelope.to ?? "", duplicate: false };
      },
    };
    const adapted = toMailboxLike(bound);
    expect(adapted).not.toBe(bound);

    const harness = new FakeHarness();
    const agent = createMailboxAgent({
      workerId: WORKER,
      mailbox: bound,
      harness,
      now: () => new Date(AT),
      stopTimeoutMs: 10,
    });
    ACTIVE.push(agent);
    bound.received.push({
      mailbox: MAILBOX,
      envelope: assignEnvelope(),
      attempts: 1,
      leaseUntil: AT,
    });

    await agent.runOnce();
    expect(harness.tasks).toHaveLength(1);

    harness.report(0, { kind: "ready", summary: "done" });
    await tick();
    expect(bound.sent.map((envelope) => envelope.type)).toEqual([MESSAGE_TYPES.result]);
    expect(bound.acks).toContain("env-assign-1");
    expect(agent.active).toBe(0);
  });

  it("serializes concurrent flushes so a burst never drops a queued envelope", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(MAILBOX, assignEnvelope());
    await agent.runOnce();
    expect(harness.tasks).toHaveLength(1);

    // A harness that emits an event and a terminal report back-to-back queues
    // two outgoing envelopes; two overlapping flushes used to drop the second.
    harness.emitEvent(0, { type: "message", at: AT, data: { text: "burst" } });
    harness.report(0, { kind: "ready", summary: "done" });

    const [first, second] = await Promise.all([agent.flush(), agent.flush()]);
    // Both callers share the same in-flight drain.
    expect(first).toBe(2);
    expect(second).toBe(2);
    const types = mailbox.sent.map((envelope) => envelope.type);
    expect(types.filter((type) => type === MESSAGE_TYPES.progress)).toHaveLength(1);
    expect(types.filter((type) => type === MESSAGE_TYPES.result)).toHaveLength(1);
    expect(mailbox.acks.some((record) => record.ids.includes("env-assign-1"))).toBe(true);
  });
});

import type { HarnessSessionInfo, HarnessTaskState } from "@gethen/harness";
import type { Envelope } from "@gethen/mailbox";
import { newEnvelope } from "@gethen/mailbox";
import { afterEach, describe, expect, it } from "vitest";

import {
  agentMailboxName,
  assignToHarnessTask,
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

const SESSION_STATE: HarnessTaskState = {
  assignmentId: "a1",
  phase: "thinking",
  step: 1,
  lastEventAt: AT,
  seq: 2,
  sessionId: "sess-1",
};

const SESSION_INFO: HarnessSessionInfo = {
  sessionId: "sess-1",
  key: "build-key",
  workspace: "/work/ws",
  lastUsedAt: AT,
  activeAssignmentId: "a1",
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

function assignEnvelope(id: string, data: Record<string, unknown> = {}): Envelope {
  return newEnvelope({
    id,
    source: "master:router",
    type: MESSAGE_TYPES.assign,
    to: MAILBOX,
    time: AT,
    data: {
      assignmentId: "a1",
      ticketId: "t1",
      workspace: "/work/ws",
      prompt: "do the work",
      ...data,
    },
  });
}

function sessionListEnvelope(overrides: { id?: string; extra?: Partial<Envelope> } = {}): Envelope {
  return newEnvelope({
    id: overrides.id ?? "env-sessions-1",
    source: "master:router",
    type: MESSAGE_TYPES.sessionList,
    to: MAILBOX,
    time: AT,
    data: {},
    ...(overrides.extra ?? {}),
  });
}

describe("assign session pass-through", () => {
  it("maps new, reuse-by-id and reuse-by-key specs and omits an absent one", () => {
    const cases: Array<[unknown, unknown]> = [
      [undefined, undefined],
      [{ mode: "new" }, { mode: "new" }],
      [
        { mode: "reuse", sessionId: "s-1" },
        { mode: "reuse", sessionId: "s-1" },
      ],
      [
        { mode: "reuse", key: "k-1" },
        { mode: "reuse", key: "k-1" },
      ],
    ];
    for (const [session, expected] of cases) {
      const validation = assignToHarnessTask(assignEnvelope("env-assign-1", { session }));
      expect(validation.ok).toBe(true);
      if (!validation.ok) continue;
      expect(validation.task.session).toEqual(expected);
    }
  });

  it("rejects invalid session specs with invalid_session", () => {
    const cases: Array<[unknown, RegExp]> = [
      ["new", /JSON object/],
      [{ mode: "other" }, /mode/],
      [{ mode: "new", sessionId: "s" }, /must not carry/],
      [{ mode: "new", key: "k" }, /must not carry/],
      [{ mode: "reuse" }, /exactly one/],
      [{ mode: "reuse", sessionId: "s", key: "k" }, /exactly one/],
      [{ mode: "reuse", sessionId: "" }, /sessionId/],
      [{ mode: "reuse", key: 7 }, /key/],
    ];
    for (const [session, pattern] of cases) {
      const validation = assignToHarnessTask(assignEnvelope("env-assign-1", { session }));
      expect(validation).toMatchObject({ ok: false, code: "invalid_session" });
      if (validation.ok) continue;
      expect(validation.message).toMatch(pattern);
    }
  });

  it("dead-letters an assign carrying an invalid session spec", async () => {
    const { agent, mailbox, harness, errors } = setup();
    mailbox.enqueue(
      MAILBOX,
      assignEnvelope("env-assign-1", { session: { mode: "reuse", sessionId: "s", key: "k" } }),
    );
    await agent.runOnce();

    expect(harness.tasks).toHaveLength(0);
    expect(mailbox.nacks.at(-1)).toMatchObject({ action: "dead", ids: ["env-assign-1"] });
    expect(errors[0]?.context.phase).toBe("assign");
    expect(String(errors[0]?.error)).toMatch(/invalid_session|session/);
  });

  it("passes a session spec through to the harness task at runtime", async () => {
    const { agent, mailbox, harness } = setup();
    mailbox.enqueue(
      MAILBOX,
      assignEnvelope("env-assign-1", { session: { mode: "reuse", key: "k" } }),
    );
    await agent.runOnce();

    expect(harness.tasks[0]?.session).toEqual({ mode: "reuse", key: "k" });
  });
});

describe("gethen.session.list", () => {
  it("announces the bridge message types", () => {
    expect(MESSAGE_TYPES.sessionList).toBe("gethen.session.list");
    expect(MESSAGE_TYPES.sessionListResult).toBe("gethen.session.list.result");
  });

  it("replies with the harness session directory", async () => {
    const { agent, mailbox } = setup({ withSessions: true, sessions: [SESSION_INFO] });
    mailbox.enqueue(MAILBOX, sessionListEnvelope({ extra: { replyTo: "mailbox:master-2" } }));
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.sessionListResult)).toMatchObject({
      source: "agent:w1",
      to: "mailbox:master-2",
      correlationId: "env-sessions-1",
      causationId: "env-sessions-1",
      data: { sessions: [SESSION_INFO], at: AT },
    });
    expect(mailbox.ackIds()).toContain("env-sessions-1");
  });

  it("replies with an empty list when the harness has no sessions()", async () => {
    const { agent, mailbox } = setup();
    mailbox.enqueue(MAILBOX, sessionListEnvelope());
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.sessionListResult)).toMatchObject({
      to: "mailbox:master",
      data: { sessions: [], at: AT },
    });
    expect(mailbox.ackIds()).toContain("env-sessions-1");
  });

  it("nacks the request once when the reply cannot be sent", async () => {
    const { agent, mailbox, errors } = setup({ withSessions: true, sessions: [SESSION_INFO] });
    mailbox.failSendTimes = 1;
    mailbox.enqueue(MAILBOX, sessionListEnvelope());
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.sessionListResult)).toBeUndefined();
    expect(mailbox.nacks.at(-1)).toMatchObject({ action: "requeue", ids: ["env-sessions-1"] });
    expect(mailbox.ackIds()).not.toContain("env-sessions-1");
    expect(errors.some((entry) => entry.context.phase === "session")).toBe(true);
    expect(mailbox.nacks.filter((nack) => nack.ids.includes("env-sessions-1"))).toHaveLength(1);
  });
});

describe("sessionId propagation", () => {
  it("includes sessionId on progress messages and their state", async () => {
    const { agent, mailbox, harness } = setup({ withState: true, state: SESSION_STATE });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1"));
    await agent.runOnce();

    harness.emitEvent(0, { type: "message", at: AT, data: { text: "working" } });
    await tick();

    const progress = mailbox.lastSent(MESSAGE_TYPES.progress);
    expect(progress?.data).toMatchObject({
      assignmentId: "a1",
      sessionId: "sess-1",
      state: { sessionId: "sess-1" },
    });
  });

  it("keeps the last known sessionId when the state stops reporting it", async () => {
    const { agent, mailbox, harness } = setup({ withState: true, state: SESSION_STATE });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1"));
    await agent.runOnce();

    harness.emitEvent(0, { type: "message", at: AT, data: { n: 1 } });
    await tick();
    harness.setState(0, {
      assignmentId: "a1",
      phase: "thinking",
      step: 2,
      lastEventAt: AT,
      seq: 3,
    });
    harness.emitEvent(0, { type: "message", at: AT, data: { n: 2 } });
    await tick();

    const progress = mailbox.sentByType(MESSAGE_TYPES.progress);
    expect(progress).toHaveLength(2);
    const latest = progress[1]?.data as { sessionId?: string; state?: { sessionId?: string } };
    expect(latest.sessionId).toBe("sess-1");
    expect(latest.state).not.toHaveProperty("sessionId");
  });

  it("includes sessionId on status and result payloads", async () => {
    const { agent, mailbox, harness } = setup({ withState: true, state: SESSION_STATE });
    mailbox.enqueue(MAILBOX, assignEnvelope("env-assign-1"));
    await agent.runOnce();

    mailbox.enqueue(
      MAILBOX,
      newEnvelope({
        id: "env-status-1",
        source: "master:router",
        type: MESSAGE_TYPES.status,
        to: MAILBOX,
        time: AT,
        subject: "a1",
        correlationId: "a1",
        data: { assignmentId: "a1" },
      }),
    );
    await agent.runOnce();
    expect(mailbox.lastSent(MESSAGE_TYPES.statusResult)?.data).toMatchObject({
      assignmentId: "a1",
      sessionId: "sess-1",
    });

    // The live state disappears; the last observed sessionId is still reported
    // by status queries and by the final result.
    harness.setState(0, undefined);
    mailbox.enqueue(
      MAILBOX,
      newEnvelope({
        id: "env-status-2",
        source: "master:router",
        type: MESSAGE_TYPES.status,
        to: MAILBOX,
        time: AT,
        subject: "a1",
        correlationId: "a1",
        data: { assignmentId: "a1" },
      }),
    );
    await agent.runOnce();
    expect(mailbox.lastSent(MESSAGE_TYPES.statusResult)?.data).toMatchObject({
      assignmentId: "a1",
      sessionId: "sess-1",
    });

    harness.report(0, { kind: "ready", summary: "done" });
    await tick();
    expect(mailbox.lastSent(MESSAGE_TYPES.result)?.data).toMatchObject({
      assignmentId: "a1",
      kind: "ready",
      sessionId: "sess-1",
    });
    expect(mailbox.ackIds()).toContain("env-assign-1");
  });
});

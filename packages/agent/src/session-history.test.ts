import type { HarnessSessionEvent } from "@gethen/harness";
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

const EVENTS: HarnessSessionEvent[] = [
  { seq: 1, type: "assistant/message", at: AT, data: { text: "hello" } },
  { seq: 2, type: "tool/call", at: AT, data: { tool: "bash" } },
];

afterEach(async () => {
  for (const agent of ACTIVE.splice(0)) await agent.stop();
});

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

function historyRequestEnvelope(
  overrides: { id?: string; data?: Record<string, unknown>; extra?: Partial<Envelope> } = {},
): Envelope {
  return newEnvelope({
    id: overrides.id ?? "env-history-1",
    source: "master:router",
    type: MESSAGE_TYPES.sessionHistory,
    to: MAILBOX,
    time: AT,
    data: { sessionId: "sess-1", ...(overrides.data ?? {}) },
    ...(overrides.extra ?? {}),
  });
}

describe("gethen.session.history", () => {
  it("replies with the requested history and acks the request", async () => {
    const { agent, mailbox } = setup({ withSessionHistory: true, historyEvents: EVENTS });

    expect(MESSAGE_TYPES.sessionHistory).toBe("gethen.session.history");
    expect(MESSAGE_TYPES.sessionHistoryResult).toBe("gethen.session.history.result");

    mailbox.enqueue(MAILBOX, historyRequestEnvelope({ extra: { replyTo: "mailbox:master-2" } }));
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.sessionHistoryResult)).toMatchObject({
      source: "agent:w1",
      to: "mailbox:master-2",
      correlationId: "env-history-1",
      causationId: "env-history-1",
      data: { sessionId: "sess-1", events: EVENTS, at: AT },
    });
    expect(mailbox.ackIds()).toContain("env-history-1");
    expect(mailbox.nacks).toHaveLength(0);
  });

  it("forwards limit and afterSeq to the harness", async () => {
    const { agent, mailbox, harness } = setup({
      withSessionHistory: true,
      historyEvents: EVENTS,
    });

    mailbox.enqueue(MAILBOX, historyRequestEnvelope({ data: { limit: 2, afterSeq: 5 } }));
    await agent.runOnce();
    expect(harness.sessionHistoryCalls).toEqual([
      { sessionId: "sess-1", options: { limit: 2, afterSeq: 5 } },
    ]);

    mailbox.enqueue(MAILBOX, historyRequestEnvelope({ id: "env-history-2" }));
    await agent.runOnce();
    expect(harness.sessionHistoryCalls[1]).toEqual({ sessionId: "sess-1", options: {} });
    expect(mailbox.sentByType(MESSAGE_TYPES.sessionHistoryResult)).toHaveLength(2);
  });

  it("replies with empty events when the harness has no sessionHistory()", async () => {
    const { agent, mailbox } = setup();
    mailbox.enqueue(MAILBOX, historyRequestEnvelope());
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.sessionHistoryResult)).toMatchObject({
      to: "mailbox:master",
      correlationId: "env-history-1",
      data: { sessionId: "sess-1", events: [], at: AT },
    });
    expect(mailbox.ackIds()).toContain("env-history-1");
  });

  it("reports invalid requests and acks them without retrying", async () => {
    const { agent, mailbox, harness, errors } = setup({
      withSessionHistory: true,
      historyEvents: EVENTS,
    });
    const cases: Record<string, unknown>[] = [
      { sessionId: undefined },
      { sessionId: "" },
      { sessionId: "   " },
      { sessionId: "sess-1", limit: 0 },
      { sessionId: "sess-1", limit: -1 },
      { sessionId: "sess-1", limit: 1.5 },
      { sessionId: "sess-1", limit: "5" },
      { sessionId: "sess-1", afterSeq: -1 },
      { sessionId: "sess-1", afterSeq: 1.5 },
    ];

    for (const [index, data] of cases.entries()) {
      const id = `env-history-bad-${index}`;
      mailbox.enqueue(MAILBOX, historyRequestEnvelope({ id, data }));
      await agent.runOnce();
      expect(mailbox.ackIds()).toContain(id);
    }

    expect(harness.sessionHistoryCalls).toHaveLength(0);
    expect(mailbox.sentByType(MESSAGE_TYPES.sessionHistoryResult)).toHaveLength(0);
    expect(
      mailbox.nacks.filter((nack) => nack.ids.some((id) => id.startsWith("env-history-bad-"))),
    ).toHaveLength(0);
    expect(errors).toHaveLength(cases.length);
    expect(errors.every((entry) => entry.context.phase === "session")).toBe(true);
    expect(String(errors[0]?.error)).toMatch(/sessionId/);
    expect(String(errors.at(-1)?.error)).toMatch(/afterSeq/);
  });

  it("nacks the request once when the reply cannot be sent", async () => {
    const { agent, mailbox, errors } = setup({
      withSessionHistory: true,
      historyEvents: EVENTS,
    });
    mailbox.failSendTimes = 1;
    mailbox.enqueue(MAILBOX, historyRequestEnvelope());
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.sessionHistoryResult)).toBeUndefined();
    expect(mailbox.nacks.at(-1)).toMatchObject({ action: "requeue", ids: ["env-history-1"] });
    expect(mailbox.ackIds()).not.toContain("env-history-1");
    expect(errors.some((entry) => entry.context.phase === "session")).toBe(true);
    expect(mailbox.nacks.filter((nack) => nack.ids.includes("env-history-1"))).toHaveLength(1);
  });

  it("nacks when the harness fails to read the history", async () => {
    const { agent, mailbox, errors } = setup({
      withSessionHistory: true,
      sessionHistoryError: new Error("history store unavailable"),
    });
    mailbox.enqueue(MAILBOX, historyRequestEnvelope());
    await agent.runOnce();

    expect(mailbox.lastSent(MESSAGE_TYPES.sessionHistoryResult)).toBeUndefined();
    expect(mailbox.nacks.at(-1)).toMatchObject({ action: "requeue", ids: ["env-history-1"] });
    expect(errors.some((entry) => entry.context.phase === "session")).toBe(true);
  });

  it("keeps concurrent history replies isolated by correlation id", async () => {
    const { agent, mailbox, harness } = setup({
      withSessionHistory: true,
      historyEvents: EVENTS,
    });

    mailbox.enqueue(
      MAILBOX,
      historyRequestEnvelope({ id: "env-history-a", data: { sessionId: "sess-a" } }),
    );
    mailbox.enqueue(
      MAILBOX,
      historyRequestEnvelope({ id: "env-history-b", data: { sessionId: "sess-b" } }),
    );
    await agent.runOnce();
    await agent.runOnce();

    const replies = mailbox.sentByType(MESSAGE_TYPES.sessionHistoryResult);
    expect(replies).toHaveLength(2);
    const byCorrelation = new Map(
      replies.map((reply) => [reply.correlationId, reply.data as { sessionId: string }]),
    );
    expect(byCorrelation.get("env-history-a")?.sessionId).toBe("sess-a");
    expect(byCorrelation.get("env-history-b")?.sessionId).toBe("sess-b");
    expect(harness.sessionHistoryCalls.map((call) => call.sessionId)).toEqual(["sess-a", "sess-b"]);
    expect(mailbox.ackIds()).toEqual(expect.arrayContaining(["env-history-a", "env-history-b"]));
  });
});

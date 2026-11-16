import {
  type EnqueueResult,
  type Envelope,
  MailboxClient,
  MemoryMailboxStore,
  newEnvelope,
  type ReceivedMessage,
} from "@gethen/mailbox";
import type { Assignment, EscalationRecord, LaunchRequest, Ticket } from "@gethen/master";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createMailboxLaunchPort,
  DEFAULT_MAILBOX_LAUNCH_TIMEOUT_MS,
  MAILBOX_LAUNCH_ERROR_CODES,
  type MailboxClientLike,
  type MailboxLaunchBridge,
  MailboxLaunchError,
  type MailboxLaunchOptions,
} from "./launch";

const NOW = new Date("2026-11-17T02:00:00.000Z");
const AT = NOW.toISOString();

const ticket: Ticket = {
  id: "ticket-1",
  mandate: "mandate-1",
  source: "local",
  threads: ["thread-1"],
  subscriptions: ["thread-1"],
  channels: [],
  inputs: [],
  assignments: ["assignment-1"],
  openedAt: AT,
};

function assignment(assignmentId: string): Assignment {
  return {
    id: assignmentId,
    ticket: ticket.id,
    worker: "worker-a",
    workspace: "/ws",
    packDigest: "sha256:pack",
    state: "pending",
    operationId: `launch-${assignmentId}`,
  };
}

function launchRequest(assignmentId = "assignment-1"): LaunchRequest {
  return {
    assignment: assignment(assignmentId),
    ticket: { ...ticket, assignments: [assignmentId] },
    packDigest: "sha256:pack",
    prompt: "prompt",
  };
}

class FakeMailbox implements MailboxClientLike {
  readonly sent: Envelope[] = [];
  readonly acked: Array<{ mailbox: string; ids: string[] }> = [];
  readonly received: ReceivedMessage[] = [];
  readonly pollIntervalMs = 5;
  receiveCalls = 0;
  failSend: Error | undefined;

  send(envelope: Envelope): EnqueueResult {
    if (this.failSend !== undefined) throw this.failSend;
    this.sent.push(envelope);
    return { mailbox: envelope.to ?? "", duplicate: false };
  }

  receive(mailbox: string): ReceivedMessage[] {
    this.receiveCalls += 1;
    return this.received.splice(0).map((message) => ({ ...message, mailbox }));
  }

  ack(mailbox: string, ids: string[]): number {
    this.acked.push({ mailbox, ids });
    return ids.length;
  }

  nack(): number {
    return 0;
  }

  push(envelope: Envelope): void {
    this.received.push({ mailbox: "", envelope, attempts: 1, leaseUntil: AT });
  }
}

let seq = 0;

function resultEnvelope(
  assignmentId: string,
  overrides: { kind?: string; summary?: string; question?: string; correlationId?: string } = {},
): Envelope {
  seq += 1;
  const data: Record<string, unknown> = {
    assignmentId,
    kind: overrides.kind ?? "ready",
    summary: overrides.summary ?? "done",
  };
  if (overrides.question !== undefined) data.question = overrides.question;
  return newEnvelope({
    id: `result-${seq}`,
    source: "agent:worker-a",
    type: "gethen.task.result",
    subject: assignmentId,
    time: AT,
    data,
    correlationId: overrides.correlationId ?? assignmentId,
    to: "mailbox:master",
  });
}

function askEnvelope(
  assignmentId: string,
  overrides: {
    escalationId?: string;
    kind?: string;
    question?: string;
    options?: Array<{ id: string; label: string }>;
    defaultOption?: string;
    replyTo?: string;
  } = {},
): Envelope {
  seq += 1;
  const data: Record<string, unknown> = {
    assignmentId,
    escalationId: overrides.escalationId ?? "esc-1",
    kind: overrides.kind ?? "decision",
    question: overrides.question ?? "Which branch?",
    options: overrides.options ?? [
      { id: "merge", label: "Merge" },
      { id: "rebase", label: "Rebase" },
    ],
  };
  if (overrides.defaultOption !== undefined) data.defaultOption = overrides.defaultOption;
  return newEnvelope({
    id: `ask-${seq}`,
    source: "agent:worker-a",
    type: "gethen.task.ask",
    subject: assignmentId,
    time: AT,
    data,
    correlationId: assignmentId,
    to: "mailbox:master",
    ...(overrides.replyTo === undefined ? {} : { replyTo: overrides.replyTo }),
  });
}

function progressEnvelope(assignmentId: string): Envelope {
  seq += 1;
  return newEnvelope({
    id: `progress-${seq}`,
    source: "agent:worker-a",
    type: "gethen.task.progress",
    subject: assignmentId,
    time: AT,
    data: { assignmentId, event: { type: "message", at: AT, data: { text: "working" } } },
    correlationId: assignmentId,
    to: "mailbox:master",
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 1_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("timed out waiting for condition");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

const bridges: MailboxLaunchBridge[] = [];

function makeBridge(
  client: FakeMailbox,
  overrides: Partial<MailboxLaunchOptions> = {},
): MailboxLaunchBridge {
  const bridge = createMailboxLaunchPort({
    client,
    workerId: "worker-a",
    now: () => NOW,
    autoStart: false,
    ...overrides,
  });
  bridges.push(bridge);
  return bridge;
}

afterEach(() => {
  while (bridges.length > 0) bridges.pop()?.close();
});

describe("createMailboxLaunchPort", () => {
  it("sends gethen.task.assign and resolves on the correlated result", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const resultPromise = bridge.launchPort.launch(launchRequest());

    expect(bridge.workerMailbox).toBe("mailbox:worker-a");
    expect(bridge.masterMailbox).toBe("mailbox:master");
    const assign = client.sent.at(0);
    expect(assign).toMatchObject({
      specversion: "1.0",
      id: "assignment-1",
      source: "master:router",
      type: "gethen.task.assign",
      subject: "ticket-1",
      time: AT,
      to: "mailbox:worker-a",
      replyTo: "mailbox:master",
      correlationId: "assignment-1",
    });
    expect(assign?.data).toEqual({
      assignmentId: "assignment-1",
      ticketId: "ticket-1",
      workspace: "/ws",
      packDigest: "sha256:pack",
      prompt: "prompt",
    });

    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    const result = await resultPromise;
    expect(result.report).toEqual({ kind: "ready", summary: "done", at: AT });
    expect(result.receipt).toEqual({
      assignment: "assignment-1",
      ticket: "ticket-1",
      worker: "worker-a",
      workspace: "/ws",
      packDigest: "sha256:pack",
      at: AT,
      outcome: "completed",
      summary: "done",
      events: 0,
    });
    expect(result.escalations).toBeUndefined();
  });

  it("counts progress envelopes as receipt events", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(progressEnvelope("assignment-1"));
    bridge.handleEnvelope(progressEnvelope("assignment-1"));
    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    const result = await resultPromise;
    expect(result.receipt.events).toBe(2);
  });

  it("closes the ask loop: escalation, answer envelope, then result", async () => {
    const statuses: string[] = [];
    const resolveEscalation = vi.fn(async () => ({
      kind: "answer" as const,
      text: "Merge it.",
      option: "merge",
    }));
    const client = new FakeMailbox();
    const bridge = makeBridge(client, {
      resolveEscalation,
      onEscalation: (record) => statuses.push(`escalation:${record.status}`),
      onResolution: (record) => statuses.push(`resolution:${record.status}`),
    });
    const resultPromise = bridge.launchPort.launch(launchRequest());

    const ask = askEnvelope("assignment-1", { replyTo: "mailbox:worker-a" });
    bridge.handleEnvelope(ask);
    await waitFor(() => client.sent.length === 2);

    expect(resolveEscalation).toHaveBeenCalledTimes(1);
    expect(statuses).toEqual(["escalation:waiting", "resolution:resolved"]);

    const answer = client.sent.at(1);
    expect(answer).toMatchObject({
      type: "gethen.task.answer",
      subject: "assignment-1",
      to: "mailbox:worker-a",
      correlationId: "assignment-1",
      causationId: ask.id,
      data: {
        assignmentId: "assignment-1",
        escalationId: "esc-1",
        resolution: { kind: "answer", text: "Merge it.", option: "merge" },
      },
    });

    bridge.handleEnvelope(resultEnvelope("assignment-1", { summary: "merged" }));
    const result = await resultPromise;
    expect(result.report).toMatchObject({ kind: "ready", summary: "merged" });
    const record = result.escalations?.at(0);
    expect(record).toMatchObject({
      id: "esc-1",
      ticket: "ticket-1",
      assignment: "assignment-1",
      session: "worker-a",
      kind: "decision",
      question: "Which branch?",
      status: "resolved",
    });
    expect(record?.options).toEqual([
      { id: "merge", label: "Merge" },
      { id: "rebase", label: "Rebase" },
    ]);
    expect(record?.resolution).toEqual({
      kind: "answer",
      text: "Merge it.",
      option: "merge",
      at: AT,
    });
  });

  it("parks for a person when no resolver is configured", async () => {
    const resolutions: EscalationRecord[] = [];
    const client = new FakeMailbox();
    const bridge = makeBridge(client, {
      onResolution: (record) => resolutions.push(record),
    });
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(progressEnvelope("assignment-1"));
    bridge.handleEnvelope(askEnvelope("assignment-1"));
    const result = await resultPromise;

    expect(result.report).toEqual({
      kind: "waiting",
      summary: "waiting for a person: Which branch?",
      question: "Which branch?",
      at: AT,
    });
    expect(result.receipt).toMatchObject({
      outcome: "completed",
      summary: "waiting for a person: Which branch?",
      events: 1,
    });
    expect(result.escalations?.at(0)).toMatchObject({ status: "parked" });
    expect(resolutions.at(0)?.status).toBe("parked");
    // No answer envelope: a person still has to decide.
    expect(client.sent).toHaveLength(1);
  });

  it("parks when the resolver returns undefined", async () => {
    const resolveEscalation = vi.fn(async () => undefined);
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { resolveEscalation });
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(askEnvelope("assignment-1"));
    const result = await resultPromise;

    expect(resolveEscalation).toHaveBeenCalledTimes(1);
    expect(result.report.kind).toBe("waiting");
    expect(result.escalations?.at(0)?.status).toBe("parked");
    expect(result.escalations?.at(0)?.resolution).toBeUndefined();
    expect(client.sent).toHaveLength(1);
  });

  it("parks (and still resumes) on an escalate resolution", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, {
      resolveEscalation: async () => ({ kind: "escalate", text: "A person must decide." }),
    });
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(askEnvelope("assignment-1"));
    await waitFor(() => client.sent.length === 2);

    expect(client.sent.at(1)?.data).toMatchObject({
      resolution: { kind: "escalate", text: "A person must decide." },
    });

    bridge.handleEnvelope(resultEnvelope("assignment-1", { summary: "merged" }));
    const result = await resultPromise;
    expect(result.report.kind).toBe("ready");
    expect(result.escalations?.at(0)).toMatchObject({
      status: "parked",
      resolution: { kind: "escalate", text: "A person must decide." },
    });
  });

  it("fails with MAILBOX_LAUNCH_TIMEOUT when no result arrives", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { timeoutMs: 10 });
    const failure = await bridge.launchPort.launch(launchRequest()).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(MailboxLaunchError);
    expect((failure as MailboxLaunchError).code).toBe("MAILBOX_LAUNCH_TIMEOUT");
    // A late result for the timed-out assignment is ignored rather than throwing.
    expect(() => bridge.handleEnvelope(resultEnvelope("assignment-1"))).not.toThrow();
  });

  it("fails with MAILBOX_DISPATCH_FAILED when the mailbox send throws", async () => {
    const client = new FakeMailbox();
    client.failSend = new Error("mailbox store unavailable");
    const bridge = makeBridge(client);
    await expect(bridge.launchPort.launch(launchRequest())).rejects.toThrow(
      /MAILBOX_DISPATCH_FAILED/,
    );
  });

  it("turns an invalid result into a failed report", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(resultEnvelope("assignment-1", { kind: "bogus", summary: "x" }));
    const result = await resultPromise;
    expect(result.report).toMatchObject({ kind: "failed", at: AT });
    expect(result.report.summary).toContain("invalid task result");
    expect(result.receipt.outcome).toBe("failed");
  });

  it("keeps two concurrent assignments isolated", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const first = bridge.launchPort.launch(launchRequest("assignment-1"));
    const second = bridge.launchPort.launch(launchRequest("assignment-2"));
    expect(client.sent.map((envelope) => envelope.correlationId)).toEqual([
      "assignment-1",
      "assignment-2",
    ]);

    // A result for an unknown assignment must not disturb the pending two.
    bridge.handleEnvelope(resultEnvelope("assignment-3", { summary: "stranger" }));
    bridge.handleEnvelope(resultEnvelope("assignment-2", { summary: "second" }));
    bridge.handleEnvelope(resultEnvelope("assignment-1", { summary: "first" }));

    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(firstResult.report.summary).toBe("first");
    expect(firstResult.receipt.assignment).toBe("assignment-1");
    expect(secondResult.report.summary).toBe("second");
    expect(secondResult.receipt.assignment).toBe("assignment-2");
    expect(secondResult.receipt.ticket).toBe("ticket-1");
  });

  it("processes a repeated ask id only once", async () => {
    const resolveEscalation = vi.fn(async () => ({ kind: "answer" as const, text: "ok" }));
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { resolveEscalation });
    const resultPromise = bridge.launchPort.launch(launchRequest());
    const ask = askEnvelope("assignment-1");
    bridge.handleEnvelope(ask);
    bridge.handleEnvelope(ask);
    await waitFor(() => client.sent.length === 2);

    expect(resolveEscalation).toHaveBeenCalledTimes(1);
    expect(client.sent.filter((envelope) => envelope.type === "gethen.task.answer")).toHaveLength(
      1,
    );

    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    const result = await resultPromise;
    expect(result.escalations).toHaveLength(1);
  });

  it("polls the master mailbox, acks handled envelopes and stops on close", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { autoStart: true, pollIntervalMs: 1 });
    const resultPromise = bridge.launchPort.launch(launchRequest());
    const result = resultEnvelope("assignment-1");
    client.push(result);

    await waitFor(() => client.acked.length === 1);
    expect(client.acked.at(0)).toEqual({ mailbox: "mailbox:master", ids: [result.id] });
    await expect(resultPromise).resolves.toMatchObject({ report: { kind: "ready" } });

    bridge.close();
    const callsAtClose = client.receiveCalls;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(client.receiveCalls).toBe(callsAtClose);
  });

  it("honours a custom mailbox prefix and rejects in-flight launches on close", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { mailboxPrefix: "agent:" });
    expect(bridge.workerMailbox).toBe("agent:worker-a");
    expect(bridge.masterMailbox).toBe("agent:master");

    const resultPromise = bridge.launchPort.launch(launchRequest());
    expect(client.sent.at(0)).toMatchObject({
      to: "agent:worker-a",
      replyTo: "agent:master",
    });

    bridge.close();
    bridge.close(); // idempotent
    await expect(resultPromise).rejects.toMatchObject({ code: "MAILBOX_CLOSED" });
  });
});

describe("with a real MailboxClient", () => {
  it("closes the loop against an in-memory mailbox store", async () => {
    const client = new MailboxClient({
      store: new MemoryMailboxStore(),
      now: () => NOW,
      pollIntervalMs: 1,
    });
    const bridge = createMailboxLaunchPort({
      client,
      workerId: "worker-a",
      now: () => NOW,
      autoStart: true,
      pollIntervalMs: 1,
    });
    bridges.push(bridge);

    const resultPromise = bridge.launchPort.launch(launchRequest());
    // Consume the assign like a worker agent would, then reply on the master
    // mailbox exactly as `@gethen/agent` does (correlationId = assign id).
    let assign: ReturnType<MailboxClient["receive"]>[number] | undefined;
    await waitFor(() => {
      assign = client.receive("mailbox:worker-a").at(0);
      return assign !== undefined;
    });
    if (assign === undefined) throw new Error("assign was not delivered");
    expect(assign.envelope.type).toBe("gethen.task.assign");
    client.ack("mailbox:worker-a", [assign.envelope.id]);
    client.send(
      newEnvelope({
        source: "agent:worker-a",
        type: "gethen.task.result",
        subject: "assignment-1",
        time: AT,
        data: { assignmentId: "assignment-1", kind: "ready", summary: "done" },
        correlationId: "assignment-1",
        to: "mailbox:master",
      }),
    );

    const result = await resultPromise;
    expect(result.report).toEqual({ kind: "ready", summary: "done", at: AT });
    expect(result.receipt.worker).toBe("worker-a");
    await waitFor(() => client.stats("mailbox:master").pending === 0);
  });
});

describe("mailbox launch constants", () => {
  it("exports the protocol defaults", () => {
    expect(DEFAULT_MAILBOX_LAUNCH_TIMEOUT_MS).toBe(600_000);
    expect(MAILBOX_LAUNCH_ERROR_CODES).toEqual([
      "MAILBOX_LAUNCH_TIMEOUT",
      "MAILBOX_DISPATCH_FAILED",
      "MAILBOX_RESOLUTION_FAILED",
      "MAILBOX_CLOSED",
    ]);
  });
});

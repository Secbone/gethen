import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
  DEFAULT_MAX_EVENT_BYTES,
  DEFAULT_MAX_TASK_EVENTS,
  DEFAULT_STATUS_TIMEOUT_MS,
  DEFAULT_TASK_EVENT_LIMIT,
  type LateLaunchResult,
  MAILBOX_LAUNCH_ERROR_CODES,
  type MailboxClientLike,
  type MailboxLaunchBridge,
  MailboxLaunchError,
  type MailboxLaunchOptions,
  type TaskState,
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

function rawState(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    phase: "thinking",
    step: 3,
    lastEventAt: AT,
    lastMessage: "reading files",
    seq: 7,
    ...overrides,
  };
}

function stateProgressEnvelope(assignmentId: string, state: Record<string, unknown>): Envelope {
  seq += 1;
  return newEnvelope({
    id: `state-progress-${seq}`,
    source: "agent:worker-a",
    type: "gethen.task.progress",
    subject: assignmentId,
    time: AT,
    data: { assignmentId, state },
    correlationId: assignmentId,
    to: "mailbox:master",
  });
}

function statusResultEnvelope(
  requestId: string,
  assignmentId: string,
  state: unknown,
  report?: Record<string, unknown>,
): Envelope {
  seq += 1;
  return newEnvelope({
    id: `status-result-${seq}`,
    source: "agent:worker-a",
    type: "gethen.task.status.result",
    subject: assignmentId,
    time: AT,
    data: { assignmentId, state, at: AT, ...(report === undefined ? {} : { report }) },
    correlationId: requestId,
    to: "mailbox:master",
  });
}

function toolEvent(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: "tool.start",
    at: AT,
    data: { name: "read", path: "/tmp/file.txt" },
    ...overrides,
  };
}

function eventProgressEnvelope(
  assignmentId: string,
  options: {
    seq?: number;
    event?: Record<string, unknown>;
    state?: Record<string, unknown>;
  } = {},
): Envelope {
  seq += 1;
  const data: Record<string, unknown> = { assignmentId };
  if (options.seq !== undefined) data.seq = options.seq;
  if (options.event !== undefined) data.event = options.event;
  if (options.state !== undefined) data.state = options.state;
  return newEnvelope({
    id: `event-progress-${seq}`,
    source: "agent:worker-a",
    type: "gethen.task.progress",
    subject: assignmentId,
    time: AT,
    data,
    correlationId: assignmentId,
    to: "mailbox:master",
  });
}

/** Progress from an older worker: text only, no `event` and no `state`. */
function textProgressEnvelope(assignmentId: string): Envelope {
  seq += 1;
  return newEnvelope({
    id: `text-progress-${seq}`,
    source: "agent:worker-a",
    type: "gethen.task.progress",
    subject: assignmentId,
    time: AT,
    data: { assignmentId, text: "working" },
    correlationId: assignmentId,
    to: "mailbox:master",
  });
}

function sessionListResultEnvelope(
  requestId: string,
  sessions: unknown,
  overrides: { at?: string; correlationId?: string } = {},
): Envelope {
  seq += 1;
  return newEnvelope({
    id: `session-list-result-${seq}`,
    source: "agent:worker-a",
    type: "gethen.session.list.result",
    time: AT,
    data: { sessions, at: overrides.at ?? AT },
    correlationId: overrides.correlationId ?? requestId,
    to: "mailbox:master",
  });
}

function sessionHistoryResultEnvelope(
  requestId: string,
  sessionId: string | undefined,
  events: unknown,
  overrides: { at?: string; correlationId?: string } = {},
): Envelope {
  seq += 1;
  return newEnvelope({
    id: `session-history-result-${seq}`,
    source: "agent:worker-a",
    type: "gethen.session.history.result",
    time: AT,
    data: {
      ...(sessionId === undefined ? {} : { sessionId }),
      events,
      at: overrides.at ?? AT,
    },
    correlationId: overrides.correlationId ?? requestId,
    to: "mailbox:master",
  });
}

/** Launch promises that a test may leave pending; the rejection is pre-handled. */
const pendingLaunches: Array<Promise<unknown>> = [];

function launchPending(
  bridge: MailboxLaunchBridge,
  assignmentId = "assignment-1",
): Promise<unknown> {
  const promise = bridge.launchPort.launch(launchRequest(assignmentId));
  pendingLaunches.push(promise.catch(() => undefined));
  return promise;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Launch and park on an ask; resolves with the already-settled waiting result. */
async function parkAndWait(
  bridge: MailboxLaunchBridge,
  assignmentId = "assignment-1",
  escalationId = "esc-1",
): Promise<Awaited<ReturnType<MailboxLaunchBridge["launchPort"]["launch"]>>> {
  const launch = bridge.launchPort.launch(launchRequest(assignmentId));
  bridge.handleEnvelope(askEnvelope(assignmentId, { escalationId }));
  return launch;
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
  pendingLaunches.length = 0;
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

  it("carries the assignment session spec in the assign envelope when present", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const request = launchRequest();
    request.assignment.session = { mode: "reuse", sessionId: "session-9" };

    const resultPromise = bridge.launchPort.launch(request);
    expect(client.sent.at(0)?.data).toEqual({
      assignmentId: "assignment-1",
      ticketId: "ticket-1",
      workspace: "/ws",
      packDigest: "sha256:pack",
      prompt: "prompt",
      session: { mode: "reuse", sessionId: "session-9" },
    });
    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    await resultPromise;

    // An Assignment without a session spec adds no `session` key.
    const plainClient = new FakeMailbox();
    const plainBridge = makeBridge(plainClient);
    const plainPromise = plainBridge.launchPort.launch(launchRequest());
    expect(plainClient.sent.at(0)?.data).not.toHaveProperty("session");
    plainBridge.handleEnvelope(resultEnvelope("assignment-1"));
    await plainPromise;
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

  it("treats timeout as an inactivity window and refreshes on progress", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { timeoutMs: 120 });
    const pending = bridge.launchPort.launch(launchRequest());
    await new Promise((resolve) => setTimeout(resolve, 80));
    bridge.handleEnvelope(progressEnvelope("assignment-1"));
    await new Promise((resolve) => setTimeout(resolve, 80));
    // 160ms total > 120ms timeout, but only 80ms since the progress update.
    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    const result = await pending;
    expect(result.report.kind).toBe("ready");
  });

  it("recovers a finished report from a status probe instead of timing out", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { timeoutMs: 120 });
    const pending = bridge.launchPort.launch(launchRequest());

    // The result envelope was lost; the silence window expires and the bridge
    // asks the Worker, which still remembers the report.
    await waitFor(() => client.sent.some((envelope) => envelope.type === "gethen.task.status"));
    const status = client.sent.find((envelope) => envelope.type === "gethen.task.status");
    if (status === undefined) throw new Error("missing status request");
    bridge.handleEnvelope(
      statusResultEnvelope(
        status.id,
        "assignment-1",
        rawState({ phase: "done", step: 2, lastMessage: "A-done" }),
        { assignmentId: "assignment-1", kind: "ready", summary: "A-done" },
      ),
    );

    const result = await pending;
    expect(result.report.kind).toBe("ready");
    expect(result.report.summary).toContain("A-done");
    expect(result.receipt.outcome).toBe("completed");
    expect(bridge.taskState("assignment-1")?.phase).toBe("done");
  });

  it("re-arms when the status probe proves recent activity", async () => {
    const client = new FakeMailbox();
    // A real clock keeps the probed state timestamps comparable to the timer.
    const bridge = makeBridge(client, { timeoutMs: 120, now: () => new Date() });
    const pending = bridge.launchPort.launch(launchRequest());

    await waitFor(() => client.sent.some((envelope) => envelope.type === "gethen.task.status"));
    const status = client.sent.find((envelope) => envelope.type === "gethen.task.status");
    if (status === undefined) throw new Error("missing status request");
    bridge.handleEnvelope(
      statusResultEnvelope(status.id, "assignment-1", {
        phase: "tool",
        step: 2,
        lastEventAt: new Date().toISOString(),
        seq: 4,
      }),
    );
    bridge.handleEnvelope(resultEnvelope("assignment-1", { summary: "finished later" }));

    const result = await pending;
    expect(result.report).toMatchObject({ kind: "ready", summary: "finished later" });
  });

  it("fails with MAILBOX_LAUNCH_TIMEOUT when no result arrives", async () => {
    const client = new FakeMailbox();
    const late: LateLaunchResult[] = [];
    const bridge = makeBridge(client, {
      timeoutMs: 10,
      onLateResult: (result) => late.push(result),
    });
    const failure = await bridge.launchPort.launch(launchRequest()).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(MailboxLaunchError);
    expect((failure as MailboxLaunchError).code).toBe("MAILBOX_LAUNCH_TIMEOUT");

    // The dispatched task may still finish: its late result settles the
    // assignment through onLateResult exactly once.
    expect(() => bridge.handleEnvelope(resultEnvelope("assignment-1"))).not.toThrow();
    expect(late).toHaveLength(1);
    expect(late[0]?.report).toMatchObject({ kind: "ready", summary: "done" });
    expect(late[0]?.receipt).toMatchObject({ assignment: "assignment-1", outcome: "completed" });
    expect(late[0]?.request.ticket.id).toBe("ticket-1");

    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    expect(late).toHaveLength(1);
    expect(bridge.taskState("assignment-1")?.phase).toBe("done");
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

describe("task state, status, steer and cancel", () => {
  it("updates TaskState from progress and notifies onTaskState", async () => {
    const client = new FakeMailbox();
    const updates: Array<{ assignmentId: string; state: TaskState }> = [];
    const bridge = makeBridge(client, {
      onTaskState: (assignmentId, state) => updates.push({ assignmentId, state }),
    });
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(
      stateProgressEnvelope(
        "assignment-1",
        rawState({ lastTool: { name: "read", startedAt: AT } }),
      ),
    );

    const state = bridge.taskState("assignment-1");
    expect(state).toEqual({
      phase: "thinking",
      step: 3,
      lastTool: { name: "read", startedAt: AT },
      lastEventAt: AT,
      lastMessage: "reading files",
      seq: 7,
      updatedAt: AT,
      worker: "worker-a",
    });
    expect(bridge.taskStates()).toEqual({ "assignment-1": state });
    expect(updates).toHaveLength(1);
    expect(updates.at(0)?.assignmentId).toBe("assignment-1");

    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    await resultPromise;
  });

  it("still counts progress from older workers that send no state", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(progressEnvelope("assignment-1"));

    expect(bridge.taskState("assignment-1")).toBeUndefined();
    expect(bridge.taskStates()).toEqual({});

    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    const result = await resultPromise;
    expect(result.receipt.events).toBe(1);
  });

  it("keeps the latest progress state", async () => {
    const client = new FakeMailbox();
    const updates: TaskState[] = [];
    const bridge = makeBridge(client, {
      onTaskState: (_assignmentId, state) => updates.push(state),
    });
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(stateProgressEnvelope("assignment-1", rawState({ seq: 1, step: 1 })));
    bridge.handleEnvelope(
      stateProgressEnvelope("assignment-1", rawState({ seq: 2, step: 2, phase: "tool" })),
    );
    expect(bridge.taskState("assignment-1")).toMatchObject({ phase: "tool", step: 2, seq: 2 });
    expect(updates.map((state) => state.seq)).toEqual([1, 2]);
    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    await resultPromise;
  });

  it("retains the final done state after the result", async () => {
    const client = new FakeMailbox();
    const updates: TaskState[] = [];
    const bridge = makeBridge(client, {
      onTaskState: (_assignmentId, state) => updates.push(state),
    });
    const resultPromise = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(stateProgressEnvelope("assignment-1", rawState()));
    bridge.handleEnvelope(resultEnvelope("assignment-1", { summary: "all done" }));
    const result = await resultPromise;

    expect(result.report.kind).toBe("ready");
    expect(bridge.taskState("assignment-1")).toEqual({
      phase: "done",
      step: 3,
      lastEventAt: AT,
      lastMessage: "all done",
      seq: 7,
      updatedAt: AT,
      worker: "worker-a",
    });
    expect(updates.map((state) => state.phase)).toEqual(["thinking", "done"]);
  });

  it("marks the final state failed and synthesizes one when none was reported", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const first = bridge.launchPort.launch(launchRequest("assignment-1"));
    bridge.handleEnvelope(stateProgressEnvelope("assignment-1", rawState()));
    bridge.handleEnvelope(resultEnvelope("assignment-1", { kind: "failed", summary: "boom" }));
    await first;
    expect(bridge.taskState("assignment-1")).toMatchObject({ phase: "failed", step: 3 });

    const second = bridge.launchPort.launch(launchRequest("assignment-2"));
    bridge.handleEnvelope(resultEnvelope("assignment-2", { kind: "failed", summary: "boom" }));
    await second;
    expect(bridge.taskState("assignment-2")).toEqual({
      phase: "failed",
      step: 0,
      lastEventAt: AT,
      lastMessage: "boom",
      seq: 0,
      updatedAt: AT,
      worker: "worker-a",
    });
  });

  it("requests status and resolves the correlated state", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const statusPromise = bridge.requestStatus("assignment-1", { timeoutMs: 500 });
    const request = client.sent.at(-1);
    expect(request).toMatchObject({
      type: "gethen.task.status",
      subject: "assignment-1",
      to: "mailbox:worker-a",
      replyTo: "mailbox:master",
      data: { assignmentId: "assignment-1" },
    });
    if (request?.id === undefined) throw new Error("status request has no envelope id");

    bridge.handleEnvelope(statusResultEnvelope(request.id, "assignment-1", rawState()));
    await expect(statusPromise).resolves.toEqual({
      phase: "thinking",
      step: 3,
      lastEventAt: AT,
      lastMessage: "reading files",
      seq: 7,
      updatedAt: AT,
    });
    // A status query reports but does not overwrite the progress-derived state.
    expect(bridge.taskState("assignment-1")).toBeUndefined();
  });

  it("resolves status.result with a null or missing state as null", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);

    const first = bridge.requestStatus("assignment-1", { timeoutMs: 500 });
    const firstRequest = client.sent.at(-1);
    if (firstRequest?.id === undefined) throw new Error("status request has no envelope id");
    bridge.handleEnvelope(statusResultEnvelope(firstRequest.id, "assignment-1", null));
    await expect(first).resolves.toBeNull();

    const second = bridge.requestStatus("assignment-1", { timeoutMs: 500 });
    const secondRequest = client.sent.at(-1);
    if (secondRequest?.id === undefined) throw new Error("status request has no envelope id");
    const noState = newEnvelope({
      id: "status-result-no-state",
      source: "agent:worker-a",
      type: "gethen.task.status.result",
      subject: "assignment-1",
      time: AT,
      data: { assignmentId: "assignment-1", at: AT },
      correlationId: secondRequest.id,
      to: "mailbox:master",
    });
    bridge.handleEnvelope(noState);
    await expect(second).resolves.toBeNull();
  });

  it("times out an unanswered status request", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const failure = await bridge.requestStatus("assignment-1", { timeoutMs: 10 }).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(MailboxLaunchError);
    expect((failure as MailboxLaunchError).code).toBe("MAILBOX_STATUS_TIMEOUT");
  });

  it("acks a status.result consumed by the poll loop", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const statusPromise = bridge.requestStatus("assignment-1", { timeoutMs: 500 });
    const request = client.sent.at(-1);
    if (request?.id === undefined) throw new Error("status request has no envelope id");
    const reply = statusResultEnvelope(request.id, "assignment-1", rawState());
    client.push(reply);

    expect(bridge.pollOnce()).toBe(1);
    await expect(statusPromise).resolves.toMatchObject({ seq: 7 });
    expect(client.acked.at(0)).toEqual({ mailbox: "mailbox:master", ids: [reply.id] });
  });

  it("rejects pending status requests on close", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const pending = bridge.requestStatus("assignment-1", { timeoutMs: 500 });
    bridge.close();
    await expect(pending).rejects.toMatchObject({ code: "MAILBOX_CLOSED" });
    await expect(bridge.requestStatus("assignment-1")).rejects.toMatchObject({
      code: "MAILBOX_CLOSED",
    });
  });

  it("sends a well-formed steer envelope", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    await bridge.steer("assignment-1", "focus on the failing test");

    const sent = client.sent.at(-1);
    expect(sent).toMatchObject({
      type: "gethen.task.steer",
      subject: "assignment-1",
      to: "mailbox:worker-a",
      correlationId: "assignment-1",
      data: { assignmentId: "assignment-1", text: "focus on the failing test" },
    });
    await expect(bridge.steer("assignment-1", "   ")).rejects.toThrow(/non-empty/);
    bridge.close();
    await expect(bridge.steer("assignment-1", "too late")).rejects.toMatchObject({
      code: "MAILBOX_CLOSED",
    });
  });

  it("sends a cancel envelope without waiting for the result", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const resultPromise = bridge.launchPort.launch(launchRequest());

    await bridge.cancel("assignment-1", "user stopped it");
    expect(client.sent.at(-1)).toMatchObject({
      type: "gethen.task.cancel",
      subject: "assignment-1",
      to: "mailbox:worker-a",
      correlationId: "assignment-1",
      data: { assignmentId: "assignment-1", reason: "user stopped it" },
    });
    await bridge.cancel("assignment-1");
    const withoutReason = client.sent.at(-1)?.data as Record<string, unknown>;
    expect(withoutReason).toEqual({ assignmentId: "assignment-1" });

    bridge.handleEnvelope(resultEnvelope("assignment-1", { kind: "failed", summary: "cancelled" }));
    await expect(resultPromise).resolves.toMatchObject({ report: { kind: "failed" } });
  });

  it("keeps concurrent assignments and status correlations isolated", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const first = bridge.launchPort.launch(launchRequest("assignment-1"));
    const second = bridge.launchPort.launch(launchRequest("assignment-2"));

    bridge.handleEnvelope(stateProgressEnvelope("assignment-1", rawState({ seq: 1, step: 1 })));
    bridge.handleEnvelope(stateProgressEnvelope("assignment-2", rawState({ seq: 5, step: 9 })));
    bridge.handleEnvelope(stateProgressEnvelope("assignment-x", rawState({ seq: 1, step: 1 })));
    expect(bridge.taskStates()).toMatchObject({
      "assignment-1": { seq: 1, step: 1 },
      "assignment-2": { seq: 5, step: 9 },
    });
    expect(bridge.taskState("assignment-x")).toBeUndefined();

    const statusOne = bridge.requestStatus("assignment-1", { timeoutMs: 500 });
    const requestOne = client.sent.at(-1);
    const statusTwo = bridge.requestStatus("assignment-2", { timeoutMs: 500 });
    const requestTwo = client.sent.at(-1);
    if (requestOne?.id === undefined || requestTwo?.id === undefined) {
      throw new Error("status requests have no envelope ids");
    }
    // Reply out of order; correlationId pairs each reply with its request.
    bridge.handleEnvelope(
      statusResultEnvelope(requestTwo.id, "assignment-2", rawState({ seq: 20 })),
    );
    bridge.handleEnvelope(
      statusResultEnvelope(requestOne.id, "assignment-1", rawState({ seq: 10 })),
    );
    await expect(statusOne).resolves.toMatchObject({ seq: 10 });
    await expect(statusTwo).resolves.toMatchObject({ seq: 20 });

    // Unknown correlation ids are ignored (no throw, no state change).
    bridge.handleEnvelope(statusResultEnvelope("unknown-request", "assignment-1", rawState()));
    bridge.handleEnvelope(resultEnvelope("assignment-2"));
    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    await Promise.all([first, second]);
  });
});

describe("task event stream cache", () => {
  it("caches progress events in arrival order with worker seq values", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    launchPending(bridge);

    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-1", {
        seq: 1,
        event: { type: "message", at: AT, data: { text: "hello" } },
      }),
    );
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { seq: 2, event: toolEvent() }));
    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-1", {
        seq: 3,
        event: { type: "tool.result", at: AT, data: { ok: true } },
      }),
    );

    expect(bridge.taskEventCount("assignment-1")).toBe(3);
    expect(bridge.taskEventDropped("assignment-1")).toBe(0);
    expect(bridge.taskEvents("assignment-1")).toEqual([
      { seq: 1, type: "message", at: AT, data: { text: "hello" } },
      { seq: 2, type: "tool.start", at: AT, data: { name: "read", path: "/tmp/file.txt" } },
      { seq: 3, type: "tool.result", at: AT, data: { ok: true } },
    ]);
  });

  it("assigns a monotonic seq when the worker omits it", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    launchPending(bridge);

    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { event: toolEvent() }));
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { event: toolEvent() }));
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { seq: 10, event: toolEvent() }));
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { event: toolEvent() }));

    expect(bridge.taskEvents("assignment-1").map((event) => event.seq)).toEqual([1, 2, 10, 11]);
  });

  it("returns the newest events for the default tail query", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    launchPending(bridge);
    for (let index = 1; index <= 5; index += 1) {
      bridge.handleEnvelope(
        eventProgressEnvelope("assignment-1", {
          seq: index,
          event: { type: "message", at: AT, data: { index } },
        }),
      );
    }

    expect(bridge.taskEvents("assignment-1").map((event) => event.seq)).toEqual([1, 2, 3, 4, 5]);
    expect(bridge.taskEvents("assignment-1", { limit: 2 }).map((event) => event.seq)).toEqual([
      4, 5,
    ]);
  });

  it("pages forward with afterSeq", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    launchPending(bridge);
    for (let index = 1; index <= 5; index += 1) {
      bridge.handleEnvelope(
        eventProgressEnvelope("assignment-1", {
          seq: index,
          event: { type: "message", at: AT, data: { index } },
        }),
      );
    }

    expect(
      bridge.taskEvents("assignment-1", { afterSeq: 2, limit: 2 }).map((event) => event.seq),
    ).toEqual([3, 4]);
    expect(bridge.taskEvents("assignment-1", { afterSeq: 4 }).map((event) => event.seq)).toEqual([
      5,
    ]);
    expect(bridge.taskEvents("assignment-1", { afterSeq: 5 })).toEqual([]);
  });

  it("filters events by type", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    launchPending(bridge);
    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-1", {
        seq: 1,
        event: { type: "message", at: AT, data: { text: "hi" } },
      }),
    );
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { seq: 2, event: toolEvent() }));
    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-1", {
        seq: 3,
        event: { type: "message", at: AT, data: { text: "again" } },
      }),
    );

    expect(
      bridge.taskEvents("assignment-1", { type: "message" }).map((event) => event.seq),
    ).toEqual([1, 3]);
    expect(bridge.taskEvents("assignment-1", { type: "nope" })).toEqual([]);
  });

  it("evicts the oldest events past maxTaskEvents and counts them", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { maxTaskEvents: 3 });
    launchPending(bridge);
    for (let index = 1; index <= 5; index += 1) {
      bridge.handleEnvelope(
        eventProgressEnvelope("assignment-1", {
          seq: index,
          event: { type: "message", at: AT, data: { index } },
        }),
      );
    }

    expect(bridge.taskEventCount("assignment-1")).toBe(3);
    expect(bridge.taskEventDropped("assignment-1")).toBe(2);
    expect(bridge.taskEvents("assignment-1").map((event) => event.seq)).toEqual([3, 4, 5]);
  });

  it("truncates oversized events into a preview", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { maxEventBytes: 256 });
    launchPending(bridge);
    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-1", {
        seq: 1,
        event: { type: "message", at: AT, data: { text: "x".repeat(4_000) } },
      }),
    );
    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-1", {
        seq: 2,
        event: { type: "message", at: AT, data: { text: "small" } },
      }),
    );

    const [truncated, small] = bridge.taskEvents("assignment-1");
    expect(truncated?.truncated).toBe(true);
    expect(typeof (truncated?.data as Record<string, unknown> | undefined)?.preview).toBe("string");
    const preview = (truncated?.data as Record<string, unknown> | undefined)?.preview as string;
    expect(preview.length).toBeGreaterThan(0);
    expect(new TextEncoder().encode(JSON.stringify(truncated)).length).toBeLessThanOrEqual(256);
    expect(small).toEqual({ seq: 2, type: "message", at: AT, data: { text: "small" } });
    expect(small?.truncated).toBeUndefined();
  });

  it("appends a synthetic gethen.result event and keeps progress events", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const launch = launchPending(bridge);
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { seq: 1, event: toolEvent() }));
    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-1", {
        seq: 2,
        event: { type: "message", at: AT, data: { text: "done working" } },
      }),
    );
    bridge.handleEnvelope(resultEnvelope("assignment-1", { summary: "all done" }));
    await launch;

    expect(bridge.taskEventCount("assignment-1")).toBe(3);
    const events = bridge.taskEvents("assignment-1");
    expect(events.map((event) => event.seq)).toEqual([1, 2, 3]);
    expect(events[0]?.type).toBe("tool.start");
    expect(events[2]).toEqual({
      seq: 3,
      type: "gethen.result",
      at: AT,
      data: { kind: "ready", summary: "all done" },
    });
  });

  it("records a failed result in the synthetic event", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const launch = launchPending(bridge);
    // An invalid result still becomes a failed report and a failed event.
    bridge.handleEnvelope(resultEnvelope("assignment-1", { kind: "bogus", summary: "x" }));
    await launch;

    const last = bridge.taskEvents("assignment-1").at(-1);
    expect(last?.type).toBe("gethen.result");
    expect(last?.data).toMatchObject({ kind: "failed" });
    expect(String((last?.data as Record<string, unknown> | undefined)?.summary)).toContain(
      "invalid task result",
    );
  });

  it("ignores event-less progress but still updates state", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    launchPending(bridge);

    // Heartbeat-style progress: state only, no event.
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { state: rawState() }));
    expect(bridge.taskEventCount("assignment-1")).toBe(0);
    expect(bridge.taskState("assignment-1")).toMatchObject({ seq: 7 });

    // Text-only old-worker progress: neither event nor state.
    bridge.handleEnvelope(textProgressEnvelope("assignment-1"));
    expect(bridge.taskEventCount("assignment-1")).toBe(0);

    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { event: toolEvent() }));
    expect(bridge.taskEventCount("assignment-1")).toBe(1);
  });

  it("returns empty results for an unknown assignment", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    expect(bridge.taskEvents("ghost")).toEqual([]);
    expect(bridge.taskEventCount("ghost")).toBe(0);
    expect(bridge.taskEventDropped("ghost")).toBe(0);
    expect(() =>
      bridge.handleEnvelope(eventProgressEnvelope("ghost", { event: toolEvent() })),
    ).not.toThrow();
    expect(bridge.taskEvents("ghost")).toEqual([]);
  });

  it("keeps concurrent assignment event streams isolated", () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    launchPending(bridge, "assignment-1");
    launchPending(bridge, "assignment-2");

    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-1", {
        seq: 1,
        event: { type: "message", at: AT, data: { which: "one" } },
      }),
    );
    bridge.handleEnvelope(
      eventProgressEnvelope("assignment-2", {
        seq: 1,
        event: { type: "message", at: AT, data: { which: "two" } },
      }),
    );
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { seq: 2, event: toolEvent() }));

    expect(bridge.taskEventCount("assignment-1")).toBe(2);
    expect(bridge.taskEventCount("assignment-2")).toBe(1);
    expect(
      bridge.taskEvents("assignment-1", { type: "message" }).map((event) => event.data),
    ).toEqual([{ which: "one" }]);
    expect(bridge.taskEvents("assignment-2").map((event) => event.data)).toEqual([
      { which: "two" },
    ]);
  });

  it("retains events after the result and after close", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const launch = launchPending(bridge);
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { seq: 1, event: toolEvent() }));
    bridge.handleEnvelope(resultEnvelope("assignment-1"));
    await launch;
    expect(bridge.taskEventCount("assignment-1")).toBe(2);

    bridge.close();
    expect(bridge.taskEventCount("assignment-1")).toBe(2);
    expect(bridge.taskEvents("assignment-1").map((event) => event.type)).toEqual([
      "tool.start",
      "gethen.result",
    ]);
  });

  it("validates maxTaskEvents and maxEventBytes", () => {
    const client = new FakeMailbox();
    expect(() => makeBridge(client, { maxTaskEvents: 0 })).toThrow(/maxTaskEvents/);
    expect(() => makeBridge(client, { maxEventBytes: -1 })).toThrow(/maxEventBytes/);
    expect(() => makeBridge(client, { maxTaskEvents: 1.5 })).toThrow(/maxTaskEvents/);
  });
});

describe("session list and history requests", () => {
  it("sends a gethen.session.list request and parses the correlated reply", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const promise = bridge.listSessions({ timeoutMs: 500 });

    const request = client.sent.at(-1);
    expect(request).toMatchObject({
      type: "gethen.session.list",
      to: "mailbox:worker-a",
      replyTo: "mailbox:master",
      data: {},
    });
    expect(request?.subject).toBeUndefined();
    if (request?.id === undefined) throw new Error("session list request has no envelope id");

    const sessions = [
      {
        sessionId: "sess-1",
        key: "main",
        workspace: "/ws",
        lastUsedAt: AT,
        activeAssignmentId: "assignment-1",
        config: { cwd: "/ws", createdAt: AT, title: "demo", model: { provider: "p", model: "m" } },
        state: "idle",
        lastActivityAt: AT,
        eventCount: 3,
        tokenUsage: { inputTokens: 7 },
        hasLog: true,
      },
    ];
    bridge.handleEnvelope(sessionListResultEnvelope(request.id, sessions));
    await expect(promise).resolves.toEqual(sessions);
  });

  it("filters malformed session entries and accepts an empty directory", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);

    const first = bridge.listSessions({ timeoutMs: 500 });
    const firstRequest = client.sent.at(-1);
    if (firstRequest?.id === undefined) throw new Error("missing request id");
    bridge.handleEnvelope(
      sessionListResultEnvelope(firstRequest.id, [
        { sessionId: "sess-ok" },
        { key: "no-session-id" },
        42,
        { sessionId: "sess-ok-2", key: 7, state: "bogus" },
      ]),
    );
    await expect(first).resolves.toEqual([{ sessionId: "sess-ok" }, { sessionId: "sess-ok-2" }]);

    const empty = bridge.listSessions({ timeoutMs: 500 });
    const emptyRequest = client.sent.at(-1);
    if (emptyRequest?.id === undefined) throw new Error("missing request id");
    bridge.handleEnvelope(sessionListResultEnvelope(emptyRequest.id, []));
    await expect(empty).resolves.toEqual([]);

    const malformed = bridge.listSessions({ timeoutMs: 500 });
    const malformedRequest = client.sent.at(-1);
    if (malformedRequest?.id === undefined) throw new Error("missing request id");
    bridge.handleEnvelope(sessionListResultEnvelope(malformedRequest.id, "not-an-array"));
    await expect(malformed).resolves.toEqual([]);
  });

  it("forwards history options and parses the correlated events", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const promise = bridge.sessionHistory("sess-1", {
      limit: 50,
      afterSeq: 10,
      timeoutMs: 500,
    });

    const request = client.sent.at(-1);
    expect(request).toMatchObject({
      type: "gethen.session.history",
      to: "mailbox:worker-a",
      replyTo: "mailbox:master",
      data: { sessionId: "sess-1", limit: 50, afterSeq: 10 },
    });
    if (request?.id === undefined) throw new Error("history request has no envelope id");

    bridge.handleEnvelope(
      sessionHistoryResultEnvelope(request.id, "sess-1", [
        { seq: 11, type: "message", at: AT, data: { text: "hi" } },
        { seq: 12, type: "tool.start", at: AT },
        { seq: "bad", type: "message", at: AT },
        { seq: 13, type: "message", at: AT, data: [1, 2] },
      ]),
    );
    await expect(promise).resolves.toEqual({
      sessionId: "sess-1",
      at: AT,
      events: [
        { seq: 11, type: "message", at: AT, data: { text: "hi" } },
        { seq: 12, type: "tool.start", at: AT },
        { seq: 13, type: "message", at: AT, data: [1, 2] },
      ],
    });
  });

  it("falls back to the requested sessionId when the reply omits it", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const promise = bridge.sessionHistory("sess-9", { timeoutMs: 500 });
    const request = client.sent.at(-1);
    if (request?.id === undefined) throw new Error("history request has no envelope id");

    bridge.handleEnvelope(sessionHistoryResultEnvelope(request.id, undefined, []));
    await expect(promise).resolves.toEqual({ sessionId: "sess-9", at: AT, events: [] });
  });

  it("times out unanswered session requests", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);

    const listFailure = await bridge.listSessions({ timeoutMs: 10 }).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(listFailure).toBeInstanceOf(MailboxLaunchError);
    expect((listFailure as MailboxLaunchError).code).toBe("MAILBOX_STATUS_TIMEOUT");

    const historyFailure = await bridge.sessionHistory("sess-1", { timeoutMs: 10 }).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect((historyFailure as MailboxLaunchError).code).toBe("MAILBOX_STATUS_TIMEOUT");
  });

  it("reports send failures for session requests", async () => {
    const client = new FakeMailbox();
    client.failSend = new Error("mailbox store unavailable");
    const bridge = makeBridge(client);
    await expect(bridge.listSessions()).rejects.toMatchObject({ code: "MAILBOX_SEND_FAILED" });
    await expect(bridge.sessionHistory("sess-1")).rejects.toMatchObject({
      code: "MAILBOX_SEND_FAILED",
    });
  });

  it("rejects session requests after close, including pending ones", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const pending = bridge.listSessions({ timeoutMs: 500 });
    bridge.close();
    await expect(pending).rejects.toMatchObject({ code: "MAILBOX_CLOSED" });
    await expect(bridge.listSessions()).rejects.toMatchObject({ code: "MAILBOX_CLOSED" });
    await expect(bridge.sessionHistory("sess-1")).rejects.toMatchObject({
      code: "MAILBOX_CLOSED",
    });
  });

  it("acks session replies through the poll loop", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const promise = bridge.listSessions({ timeoutMs: 500 });
    const request = client.sent.at(-1);
    if (request?.id === undefined) throw new Error("missing request id");
    const reply = sessionListResultEnvelope(request.id, [{ sessionId: "sess-1" }]);
    client.push(reply);

    expect(bridge.pollOnce()).toBe(1);
    await expect(promise).resolves.toEqual([{ sessionId: "sess-1" }]);
    expect(client.acked.at(0)).toEqual({ mailbox: "mailbox:master", ids: [reply.id] });
  });

  it("keeps concurrent session requests isolated by correlationId", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const listOne = bridge.listSessions({ timeoutMs: 500 });
    const requestOne = client.sent.at(-1);
    const history = bridge.sessionHistory("sess-2", { timeoutMs: 500 });
    const historyRequest = client.sent.at(-1);
    const listTwo = bridge.listSessions({ timeoutMs: 500 });
    const requestTwo = client.sent.at(-1);
    if (
      requestOne?.id === undefined ||
      requestTwo?.id === undefined ||
      historyRequest?.id === undefined
    ) {
      throw new Error("session requests have no envelope ids");
    }

    // Unknown correlation ids are ignored; replies may arrive out of order.
    bridge.handleEnvelope(sessionListResultEnvelope("unknown-request", [{ sessionId: "ghost" }]));
    bridge.handleEnvelope(sessionListResultEnvelope(requestTwo.id, [{ sessionId: "sess-two" }]));
    bridge.handleEnvelope(sessionHistoryResultEnvelope(historyRequest.id, "sess-2", []));
    bridge.handleEnvelope(sessionListResultEnvelope(requestOne.id, [{ sessionId: "sess-one" }]));
    // A duplicate reply after settle is ignored.
    bridge.handleEnvelope(sessionListResultEnvelope(requestOne.id, [{ sessionId: "again" }]));

    await expect(listOne).resolves.toEqual([{ sessionId: "sess-one" }]);
    await expect(listTwo).resolves.toEqual([{ sessionId: "sess-two" }]);
    await expect(history).resolves.toEqual({ sessionId: "sess-2", at: AT, events: [] });
  });

  it("validates sessionId and history paging options", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    await expect(bridge.sessionHistory("   ")).rejects.toThrow(/sessionId/);
    await expect(bridge.sessionHistory("sess-1", { limit: 0 })).rejects.toThrow(/limit/);
    await expect(bridge.sessionHistory("sess-1", { afterSeq: -1 })).rejects.toThrow(/afterSeq/);
    expect(() => makeBridge(client, { maxSessionListTimeoutMs: 0 })).toThrow(
      /maxSessionListTimeoutMs/,
    );
  });
});

describe("parked escalation answers and late results", () => {
  it("resolves the launch with a waiting report but keeps the pending entry", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const result = await parkAndWait(bridge);

    expect(result.report).toMatchObject({ kind: "waiting", question: "Which branch?" });
    expect(result.escalations?.at(0)).toMatchObject({ id: "esc-1", status: "parked" });

    // The pending entry survived the resolution, so a person can still answer.
    const record = bridge.answer("assignment-1", {
      kind: "answer",
      text: "Merge it.",
      option: "merge",
    });
    expect(record).toMatchObject({ id: "esc-1", status: "resolved" });
    expect(record.resolution).toEqual({
      kind: "answer",
      text: "Merge it.",
      option: "merge",
      at: AT,
    });
  });

  it("sends a correlated gethen.task.answer envelope matching the parked ask", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const ask = askEnvelope("assignment-1", {
      escalationId: "esc-42",
      replyTo: "mailbox:worker-a",
    });
    const launch = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(ask);
    await launch;

    bridge.answer(
      "assignment-1",
      { kind: "answer", text: "Use main." },
      { escalationId: "esc-42" },
    );
    const sent = client.sent.at(-1);
    expect(sent).toMatchObject({
      type: "gethen.task.answer",
      subject: "assignment-1",
      to: "mailbox:worker-a",
      correlationId: "assignment-1",
      causationId: ask.id,
      data: {
        assignmentId: "assignment-1",
        escalationId: "esc-42",
        resolution: { kind: "answer", text: "Use main." },
      },
    });
  });

  it("notifies onResolution and clears the parked fields after answering", async () => {
    const client = new FakeMailbox();
    const resolutions: string[] = [];
    const bridge = makeBridge(client, {
      onResolution: (record) => resolutions.push(record.status),
    });
    await parkAndWait(bridge);
    bridge.answer("assignment-1", { kind: "answer", text: "Merge it." });
    expect(resolutions).toEqual(["parked", "resolved"]);
    // Nothing left to answer twice.
    expect(() => bridge.answer("assignment-1", { kind: "answer", text: "again" })).toThrow(
      /MAILBOX_NO_PARKED_ESCALATION/,
    );
  });

  it("delivers a late result through onLateResult and finishes the assignment", async () => {
    const client = new FakeMailbox();
    const late: LateLaunchResult[] = [];
    const bridge = makeBridge(client, { onLateResult: (input) => late.push(input) });
    await parkAndWait(bridge);
    bridge.answer("assignment-1", { kind: "answer", text: "Merge it.", option: "merge" });
    bridge.handleEnvelope(resultEnvelope("assignment-1", { summary: "merged" }));

    expect(late).toHaveLength(1);
    expect(late[0]).toMatchObject({
      request: { assignment: { id: "assignment-1" } },
      report: { kind: "ready", summary: "merged", at: AT },
      receipt: {
        assignment: "assignment-1",
        ticket: "ticket-1",
        worker: "worker-a",
        workspace: "/ws",
        packDigest: "sha256:pack",
        outcome: "completed",
        summary: "merged",
      },
    });
    expect(late[0]?.escalations?.at(0)).toMatchObject({
      id: "esc-1",
      status: "resolved",
      resolution: { kind: "answer", text: "Merge it." },
    });
    // The entry is gone: another answer reports an unknown assignment.
    expect(() => bridge.answer("assignment-1", { kind: "answer", text: "again" })).toThrow(
      /MAILBOX_UNKNOWN_ASSIGNMENT/,
    );
  });

  it("delivers a late result even when the escalation was not answered", async () => {
    const client = new FakeMailbox();
    const late: LateLaunchResult[] = [];
    const bridge = makeBridge(client, { onLateResult: (input) => late.push(input) });
    await parkAndWait(bridge);
    bridge.handleEnvelope(resultEnvelope("assignment-1", { kind: "failed", summary: "gave up" }));
    expect(late).toHaveLength(1);
    expect(late[0]?.report).toMatchObject({ kind: "failed", summary: "gave up" });
  });

  it("keeps updating state/events from late progress without re-arming the timer", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { timeoutMs: 30 });
    await parkAndWait(bridge);
    // Longer than the silence window: a re-armed timer would delete the entry.
    await sleep(80);

    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { event: toolEvent() }));
    bridge.handleEnvelope(eventProgressEnvelope("assignment-1", { state: rawState({ step: 9 }) }));
    expect(bridge.taskEventCount("assignment-1")).toBe(1);
    expect(bridge.taskState("assignment-1")).toMatchObject({ step: 9 });
    expect(() =>
      bridge.answer("assignment-1", { kind: "answer", text: "Merge it." }),
    ).not.toThrow();
  });

  it("parks a second late ask again so it can be answered in turn", async () => {
    const client = new FakeMailbox();
    const parkedRecords: string[] = [];
    const bridge = makeBridge(client, {
      onEscalation: (record) => parkedRecords.push(record.id),
    });
    await parkAndWait(bridge, "assignment-1", "esc-1");
    bridge.answer("assignment-1", { kind: "answer", text: "first" });

    // The resumed Worker asks again after the launch already resolved.
    bridge.handleEnvelope(askEnvelope("assignment-1", { escalationId: "esc-2" }));
    expect(parkedRecords).toEqual(["esc-1", "esc-2"]);
    expect(() =>
      bridge.answer("assignment-1", { kind: "answer", text: "stale" }, { escalationId: "esc-1" }),
    ).toThrow(/MAILBOX_NO_PARKED_ESCALATION/);

    bridge.answer("assignment-1", { kind: "answer", text: "second" }, { escalationId: "esc-2" });
    expect(client.sent.at(-1)?.data).toMatchObject({
      escalationId: "esc-2",
      resolution: { text: "second" },
    });
  });

  it("rejects answers for unknown assignments or without a parked escalation", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    expect(() => bridge.answer("ghost", { kind: "answer", text: "x" })).toThrow(
      /MAILBOX_UNKNOWN_ASSIGNMENT/,
    );

    // Launched but never parked.
    launchPending(bridge);
    expect(() => bridge.answer("assignment-1", { kind: "answer", text: "x" })).toThrow(
      /MAILBOX_NO_PARKED_ESCALATION/,
    );
  });

  it("validates the resolution before sending the answer", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    await parkAndWait(bridge);
    const invalid: Array<Parameters<MailboxLaunchBridge["answer"]>[1]> = [
      { kind: "bogus", text: "x" } as never,
      { kind: "answer", text: "   " },
      { kind: "answer", text: "ok", option: "" },
    ];
    for (const resolution of invalid) {
      expect(() => bridge.answer("assignment-1", resolution)).toThrow(/MAILBOX_INVALID_RESOLUTION/);
    }
    // The parked escalation is still answerable after the failed attempts.
    expect(() => bridge.answer("assignment-1", { kind: "answer", text: "ok" })).not.toThrow();
  });

  it("rejects answers after close", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    await parkAndWait(bridge);
    bridge.close();
    expect(() => bridge.answer("assignment-1", { kind: "answer", text: "x" })).toThrow(
      /MAILBOX_CLOSED/,
    );
  });

  it("keeps the parked escalation answerable after a send failure", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    await parkAndWait(bridge);
    client.failSend = new Error("mailbox down");
    expect(() => bridge.answer("assignment-1", { kind: "answer", text: "x" })).toThrow(
      /MAILBOX_SEND_FAILED/,
    );
    client.failSend = undefined;
    const record = bridge.answer("assignment-1", { kind: "answer", text: "retry" });
    expect(record).toMatchObject({ status: "resolved", resolution: { text: "retry" } });
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

describe("pending state persistence", () => {
  it("restores a parked escalation and settles its late result after a restart", async () => {
    const file = join(mkdtempSync(join(tmpdir(), "gethen-bridge-pending-")), "pending.json");
    const firstClient = new FakeMailbox();
    const first = makeBridge(firstClient, { pendingStateFile: file });
    const launched = first.launchPort.launch(launchRequest());
    first.handleEnvelope(askEnvelope("assignment-1", { escalationId: "esc-1" }));
    const waiting = await launched;
    expect(waiting.report.kind).toBe("waiting");
    expect(JSON.parse(readFileSync(file, "utf8"))).toHaveLength(1);
    first.close();

    const late: LateLaunchResult[] = [];
    const secondClient = new FakeMailbox();
    const second = makeBridge(secondClient, {
      pendingStateFile: file,
      onLateResult: (input) => late.push(input),
    });
    const answered = second.answer("assignment-1", { kind: "answer", text: "Merge it." });
    expect(answered.status).toBe("resolved");
    expect(secondClient.sent.at(-1)).toMatchObject({
      type: "gethen.task.answer",
      correlationId: "assignment-1",
    });

    second.handleEnvelope(resultEnvelope("assignment-1", { summary: "merged" }));
    expect(late).toHaveLength(1);
    expect(late[0]?.report).toMatchObject({ kind: "ready", summary: "merged" });
    expect(JSON.parse(readFileSync(file, "utf8"))).toEqual([]);
  });

  it("writes nothing without pendingStateFile", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client);
    const launched = bridge.launchPort.launch(launchRequest());
    bridge.handleEnvelope(askEnvelope("assignment-1", { escalationId: "esc-1" }));
    await launched;
    expect(client.sent.some((envelope) => envelope.type === "gethen.task.answer")).toBe(false);
  });
});

describe("mailbox launch constants", () => {
  it("exports the protocol defaults", () => {
    expect(DEFAULT_MAILBOX_LAUNCH_TIMEOUT_MS).toBe(600_000);
    expect(DEFAULT_STATUS_TIMEOUT_MS).toBe(5_000);
    expect(DEFAULT_MAX_TASK_EVENTS).toBe(1_000);
    expect(DEFAULT_MAX_EVENT_BYTES).toBe(65_536);
    expect(DEFAULT_TASK_EVENT_LIMIT).toBe(200);
    expect(MAILBOX_LAUNCH_ERROR_CODES).toEqual([
      "MAILBOX_LAUNCH_TIMEOUT",
      "MAILBOX_DISPATCH_FAILED",
      "MAILBOX_RESOLUTION_FAILED",
      "MAILBOX_CLOSED",
      "MAILBOX_STATUS_TIMEOUT",
      "MAILBOX_SEND_FAILED",
      "MAILBOX_UNKNOWN_ASSIGNMENT",
      "MAILBOX_UNKNOWN_WORKER",
      "MAILBOX_NO_PARKED_ESCALATION",
      "MAILBOX_INVALID_RESOLUTION",
    ]);
  });
});

describe("multi-worker dispatch", () => {
  function requestFor(assignmentId: string, worker: string): LaunchRequest {
    const request = launchRequest(assignmentId);
    request.assignment = { ...request.assignment, worker };
    return request;
  }

  it("routes assignments to the selected worker mailbox and receipts that worker", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { workerId: undefined, workers: ["worker-a", "worker-b"] });

    const first = bridge.launchPort.launch(requestFor("assignment-a", "worker-a"));
    const second = bridge.launchPort.launch(requestFor("assignment-b", "worker-b"));
    const assigns = client.sent.filter((envelope) => envelope.type === "gethen.task.assign");
    expect(assigns.map((envelope) => envelope.to)).toEqual([
      "mailbox:worker-a",
      "mailbox:worker-b",
    ]);

    bridge.handleEnvelope(resultEnvelope("assignment-a", { summary: "a done" }));
    bridge.handleEnvelope(resultEnvelope("assignment-b", { summary: "b done" }));
    expect((await first).receipt.worker).toBe("worker-a");
    expect((await second).receipt.worker).toBe("worker-b");

    await bridge.steer("assignment-b", "keep going");
    expect(client.sent.at(-1)?.to).toBe("mailbox:worker-b");
    await bridge.cancel("assignment-a", "stop");
    expect(client.sent.at(-1)?.to).toBe("mailbox:worker-a");
  });

  it("rejects an assignment routed to a worker the bridge does not know", async () => {
    const client = new FakeMailbox();
    const bridge = makeBridge(client, { workerId: undefined, workers: ["worker-a", "worker-b"] });
    await expect(
      bridge.launchPort.launch(requestFor("assignment-x", "worker-z")),
    ).rejects.toMatchObject({ code: "MAILBOX_UNKNOWN_WORKER" });
    expect(client.sent).toEqual([]);
  });
});

import { mkdtempSync, rmSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type Envelope, newEnvelope, type ReceivedMessage } from "@gethen/mailbox";
import { createMailboxClientLink, type MailboxClientLink } from "@gethen/mailbox-link";
import type {
  Assignment,
  EscalationRecord,
  EscalationResolutionInput,
  LaunchRequest,
  Ticket,
} from "@gethen/master";
import { createWsTransport, type TransportSession } from "@gethen/transport";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createMasterRuntime, type MasterRuntime, type MasterRuntimeOptions } from "./runtime";

const NOW = new Date("2026-11-17T08:00:00.000Z");
const AT = NOW.toISOString();
const WORKER = "worker-1";
const TOKEN = "secret-1";
const WORKER_MAILBOX = `mailbox:${WORKER}`;
const MASTER_MAILBOX = "mailbox:master";

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

function launchRequest(assignmentId = "assignment-1"): LaunchRequest {
  const assignment: Assignment = {
    id: assignmentId,
    ticket: ticket.id,
    worker: WORKER,
    workspace: "/ws",
    packDigest: "sha256:pack",
    state: "pending",
    operationId: `launch-${assignmentId}`,
  };
  return {
    assignment,
    ticket: { ...ticket, assignments: [assignmentId] },
    packDigest: "sha256:pack",
    prompt: "prompt",
  };
}

interface Harness {
  runtime: MasterRuntime;
  server?: Server;
  url?: string;
}

interface HarnessOptions {
  tokens?: Array<{ workerId: string; token: string }>;
  verifyToken?: (token: string) => Promise<{ workerId: string } | undefined>;
  resolveEscalation?: (record: EscalationRecord) => Promise<EscalationResolutionInput | undefined>;
  onEscalation?: (record: EscalationRecord) => void;
  onResolution?: (record: EscalationRecord) => void;
  workerId?: string;
  masterMailbox?: string;
  withServer?: boolean;
}

let dir: string;
let sequence = 0;
const harnesses: Harness[] = [];
const sessions: TransportSession[] = [];
const links: MailboxClientLink[] = [];

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "gethen-master-runtime-mailbox-"));
});

afterEach(async () => {
  for (const link of links.splice(0)) link.close();
  for (const session of sessions.splice(0)) {
    try {
      await session.close("test_cleanup");
    } catch {
      // already closed by the runtime
    }
  }
  for (const harness of harnesses.splice(0)) {
    try {
      await harness.runtime.close();
    } catch {
      // best effort
    }
    if (harness.server !== undefined) {
      await new Promise<void>((resolve) => {
        harness.server?.close(() => resolve());
      });
    }
  }
  rmSync(dir, { recursive: true, force: true });
});

async function startHarness(options: HarnessOptions = {}): Promise<Harness> {
  sequence += 1;
  const runtimeOptions: MasterRuntimeOptions = {
    dbPath: join(dir, `master-${sequence}.sqlite`),
    tokens: options.tokens ?? [{ workerId: WORKER, token: TOKEN }],
    workspace: "/ws",
    now: () => NOW,
    mailboxDbPath: join(dir, `mailbox-${sequence}.sqlite`),
    pollIntervalMs: 5,
    visibilityTimeoutMs: 1_000,
    ...(options.workerId === undefined ? {} : { workerId: options.workerId }),
    ...(options.masterMailbox === undefined ? {} : { masterMailbox: options.masterMailbox }),
    ...(options.verifyToken === undefined ? {} : { verifyToken: options.verifyToken }),
    ...(options.resolveEscalation === undefined
      ? {}
      : { resolveEscalation: options.resolveEscalation }),
    ...(options.onEscalation === undefined ? {} : { onEscalation: options.onEscalation }),
    ...(options.onResolution === undefined ? {} : { onResolution: options.onResolution }),
  };
  const runtime = createMasterRuntime(runtimeOptions);
  const harness: Harness = { runtime };
  harnesses.push(harness);

  if ((options.withServer ?? true) === false) return harness;

  const server = createServer((_request, response) => {
    response.statusCode = 404;
    response.end("not found");
  });
  server.on("upgrade", (request, socket, head) => {
    runtime.handleUpgrade?.(request, socket, head);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : 0;
  harness.server = server;
  harness.url = `ws://127.0.0.1:${port}/gethen/worker`;
  return harness;
}

async function attachWorker(
  harness: Harness,
  options: { token?: string; mailbox?: string } = {},
): Promise<{ session: TransportSession; link: MailboxClientLink }> {
  if (harness.url === undefined) throw new Error("harness has no server url");
  const session = await createWsTransport({ heartbeat: false }).connect(
    { url: harness.url },
    { kind: "bearer", token: options.token ?? TOKEN },
  );
  sessions.push(session);
  const link = await createMailboxClientLink({
    session,
    mailbox: options.mailbox ?? WORKER_MAILBOX,
    receiveTimeoutMs: 500,
    requestTimeoutMs: 2_000,
    visibilityTimeoutMs: 1_000,
    now: () => NOW,
  });
  links.push(link);
  return { session, link };
}

async function receiveOne(link: MailboxClientLink, timeoutMs = 3_000): Promise<ReceivedMessage> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error("timed out waiting for a mailbox delivery");
    const messages = await link.receive({ limit: 1, timeoutMs: Math.min(remaining, 100) });
    const [message] = messages;
    if (message !== undefined) return message;
  }
}

function assignmentIdOf(assign: Envelope): string {
  const data = assign.data as Record<string, unknown>;
  const assignmentId = data.assignmentId;
  if (typeof assignmentId !== "string") throw new Error("assign envelope has no assignmentId");
  return assignmentId;
}

function resultEnvelope(
  assign: Envelope,
  overrides: { kind?: string; summary?: string } = {},
): Envelope {
  const assignmentId = assignmentIdOf(assign);
  return newEnvelope({
    source: `agent:${WORKER}`,
    type: "gethen.task.result",
    subject: assignmentId,
    time: AT,
    data: {
      assignmentId,
      kind: overrides.kind ?? "ready",
      summary: overrides.summary ?? "done",
    },
    correlationId: assign.id,
    to: assign.replyTo ?? MASTER_MAILBOX,
  });
}

function askEnvelope(assign: Envelope, question = "Which branch?"): Envelope {
  const assignmentId = assignmentIdOf(assign);
  return newEnvelope({
    source: `agent:${WORKER}`,
    type: "gethen.task.ask",
    subject: assignmentId,
    time: AT,
    data: {
      assignmentId,
      escalationId: "esc-1",
      kind: "decision",
      question,
      options: [
        { id: "merge", label: "Merge" },
        { id: "rebase", label: "Rebase" },
      ],
    },
    correlationId: assign.id,
    to: assign.replyTo ?? MASTER_MAILBOX,
    replyTo: WORKER_MAILBOX,
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("timed out waiting for condition");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

describe("createMasterRuntime mailbox stack", () => {
  it("assembles the L1 transport + L2 mailbox stack", async () => {
    const harness = await startHarness({ withServer: false });
    const runtime = harness.runtime;
    expect(runtime.launch).toBe("ws");
    expect(runtime.bridge).toBeDefined();
    expect(runtime.handleUpgrade).toBeTypeOf("function");
    expect(runtime.health()).toEqual({
      launch: "ws",
      workers: 0,
      tickets: 0,
      open: 0,
      receipts: 0,
      escalations: 0,
      mailbox: { pending: 0, leased: 0, dead: 0 },
    });
    expect(runtime.bridge?.workerMailbox).toBe(WORKER_MAILBOX);
    expect(runtime.bridge?.masterMailbox).toBe(MASTER_MAILBOX);
  });

  it("runs a launch through the real loopback ws transport and mailbox link", async () => {
    const harness = await startHarness();
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a mailbox bridge");

    const assign = await receiveOne(link);
    expect(assign.envelope).toMatchObject({
      type: "gethen.task.assign",
      to: WORKER_MAILBOX,
      replyTo: MASTER_MAILBOX,
      correlationId: "assignment-1",
    });
    // The leased assign is visible in the mailbox health counters.
    expect(runtime.health().mailbox).toEqual({ pending: 1, leased: 1, dead: 0 });
    await link.ack([assign.envelope.id]);
    await link.send(resultEnvelope(assign.envelope));

    const result = await resultPromise;
    expect(result.report).toEqual({ kind: "ready", summary: "done", at: AT });
    expect(result.receipt).toMatchObject({
      assignment: "assignment-1",
      ticket: "ticket-1",
      worker: WORKER,
      workspace: "/ws",
      packDigest: "sha256:pack",
      outcome: "completed",
      events: 0,
    });
    await waitFor(() => runtime.health().mailbox.pending === 0);
    expect(runtime.health()).toMatchObject({ launch: "ws", workers: 1 });
  });

  it("closes the ask loop over the link: assign, ask, answer, result", async () => {
    const statuses: string[] = [];
    const harness = await startHarness({
      resolveEscalation: async () => ({ kind: "answer", text: "Merge it.", option: "merge" }),
      onEscalation: (record) => statuses.push(`escalation:${record.status}`),
      onResolution: (record) => statuses.push(`resolution:${record.status}`),
    });
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a mailbox bridge");

    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);
    await link.send(askEnvelope(assign.envelope));

    const answer = await receiveOne(link);
    expect(answer.envelope).toMatchObject({
      type: "gethen.task.answer",
      to: WORKER_MAILBOX,
      correlationId: "assignment-1",
    });
    expect((answer.envelope.data as Record<string, unknown>).resolution).toEqual({
      kind: "answer",
      text: "Merge it.",
      option: "merge",
    });
    await link.ack([answer.envelope.id]);

    await link.send(resultEnvelope(assign.envelope, { summary: "merged" }));
    const result = await resultPromise;
    expect(result.report).toMatchObject({ kind: "ready", summary: "merged" });
    expect(result.escalations?.at(0)).toMatchObject({
      id: "esc-1",
      ticket: "ticket-1",
      assignment: "assignment-1",
      session: WORKER,
      status: "resolved",
      resolution: { kind: "answer", text: "Merge it.", option: "merge" },
    });
    expect(statuses).toEqual(["escalation:waiting", "resolution:resolved"]);
  });

  it("parks an ask for a person when no resolver is configured", async () => {
    const resolutions: string[] = [];
    const harness = await startHarness({
      onResolution: (record) => resolutions.push(record.status),
    });
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a mailbox bridge");

    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);
    await link.send(askEnvelope(assign.envelope));

    const result = await resultPromise;
    expect(result.report).toEqual({
      kind: "waiting",
      summary: "waiting for a person: Which branch?",
      question: "Which branch?",
      at: AT,
    });
    expect(result.receipt.outcome).toBe("completed");
    expect(result.escalations?.at(0)).toMatchObject({ status: "parked" });
    expect(resolutions).toEqual(["parked"]);
    // No answer is sent back: a person still has to decide.
    await expect(link.receive({ limit: 1, timeoutMs: 100 })).resolves.toEqual([]);
  });

  it("counts attached transport sessions as workers", async () => {
    const harness = await startHarness();
    const runtime = harness.runtime;
    expect(runtime.health().workers).toBe(0);
    await attachWorker(harness);
    await waitFor(() => runtime.health().workers === 1);
    expect(runtime.health()).toMatchObject({
      launch: "ws",
      workers: 1,
      mailbox: { pending: 0, leased: 0, dead: 0 },
    });
  });

  it("rejects a bad token and a token for another worker", async () => {
    const harness = await startHarness({
      workerId: WORKER,
      tokens: [
        { workerId: WORKER, token: TOKEN },
        { workerId: "worker-2", token: "secret-2" },
      ],
    });
    if (harness.url === undefined) throw new Error("harness has no url");
    const transport = createWsTransport({ heartbeat: false });
    await expect(
      transport.connect({ url: harness.url }, { kind: "bearer", token: "wrong" }),
    ).rejects.toThrow();
    // C3 is single-worker: another valid worker token is refused too.
    await expect(
      transport.connect({ url: harness.url }, { kind: "bearer", token: "secret-2" }),
    ).rejects.toThrow();
  });

  it("closes the transport stack before the service and rejects in-flight launches", async () => {
    const harness = await startHarness();
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);
    await waitFor(() => runtime.health().workers === 1);

    const pending = runtime.bridge?.launchPort.launch(launchRequest("assignment-c"));
    if (pending === undefined) throw new Error("expected a mailbox bridge");
    const settled = pending.then(
      () => undefined,
      (error: unknown) => error,
    );
    // The assign reaches the worker, but the runtime closes before any result.
    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);

    await runtime.close();
    expect(await settled).toMatchObject({ code: "MAILBOX_CLOSED" });
    expect(runtime.health()).toEqual({
      launch: "ws",
      workers: 0,
      tickets: 0,
      open: 0,
      receipts: 0,
      escalations: 0,
      mailbox: { pending: 0, leased: 0, dead: 0 },
    });
    await expect(runtime.close()).resolves.toBeUndefined();

    // The transport server is closed: a new worker can no longer attach.
    if (harness.url === undefined) throw new Error("harness has no url");
    await expect(
      createWsTransport({ heartbeat: false }).connect(
        { url: harness.url },
        { kind: "bearer", token: TOKEN },
      ),
    ).rejects.toThrow();
    expect(() => runtime.service.store.tickets.list()).toThrow();
  });
});

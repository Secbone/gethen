import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
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
  Trigger,
} from "@gethen/master";
import { createWsTransport, type TransportSession } from "@gethen/transport";
import type { WorkerManifest } from "@gethen/worker-bridge";
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
  launchTimeoutMs?: number;
  reconcileIntervalMs?: number;
  recoveryGraceMs?: number;
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
    ...(options.launchTimeoutMs === undefined ? {} : { launchTimeoutMs: options.launchTimeoutMs }),
    ...(options.reconcileIntervalMs === undefined
      ? {}
      : { reconcileIntervalMs: options.reconcileIntervalMs }),
    ...(options.recoveryGraceMs === undefined ? {} : { recoveryGraceMs: options.recoveryGraceMs }),
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
  options: { token?: string; mailbox?: string; worker?: WorkerManifest } = {},
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
    ...(options.worker === undefined ? {} : { worker: options.worker }),
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

/** A reply to a `gethen.task.status` probe carrying the retained report. */
function statusResultEnvelope(status: Envelope, summary: string): Envelope {
  const data = status.data as Record<string, unknown>;
  const assignmentId = String(data.assignmentId);
  return newEnvelope({
    source: `agent:${WORKER}`,
    type: "gethen.task.status.result",
    subject: assignmentId,
    time: AT,
    data: {
      assignmentId,
      state: { assignmentId, phase: "done", step: 1, seq: 1, lastEventAt: AT },
      report: { assignmentId, kind: "ready", summary },
    },
    correlationId: status.id,
    to: status.replyTo ?? MASTER_MAILBOX,
  });
}

function askEnvelope(
  assign: Envelope,
  question = "Which branch?",
  kind: "decision" | "permission" = "decision",
): Envelope {
  const assignmentId = assignmentIdOf(assign);
  return newEnvelope({
    source: `agent:${WORKER}`,
    type: "gethen.task.ask",
    subject: assignmentId,
    time: AT,
    data: {
      assignmentId,
      escalationId: "esc-1",
      kind,
      question,
      options:
        kind === "permission"
          ? [{ id: "allow", label: "Allow" }]
          : [
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
      scheduling: {
        queued: 0,
        running: 0,
        dispatched: 0,
        reused: 0,
        queueEntries: 0,
        queueTimeouts: 0,
        maxQueueDepth: 0,
        parked: 0,
      },
      settlements: { directCompleted: 0, recoveredCompleted: 0, recoveredWithoutReport: 0 },
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

  it("lets the direct result win over a concurrent status recovery", async () => {
    const harness = await startHarness({ reconcileIntervalMs: 1_000, recoveryGraceMs: 5_000 });
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a mailbox bridge");
    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);

    // The bridge's periodic reconciliation sends a status probe. Answer it with
    // the retained report, then deliver the real result envelope moments later.
    // The direct result must settle the launch; the probe must only become a
    // recovery candidate after the grace window.
    const status = await receiveOne(link, 4_000);
    expect(status.envelope.type).toBe("gethen.task.status");
    await link.ack([status.envelope.id]);
    await link.send(statusResultEnvelope(status.envelope, "probe-only"));
    await new Promise((resolve) => setTimeout(resolve, 50));
    await link.send(resultEnvelope(assign.envelope, { summary: "direct" }));

    const result = await resultPromise;
    expect(result.report.summary).toBe("direct");
    expect(result.report.summary).not.toContain("recovered from worker status");
    expect(runtime.health().settlements).toEqual({
      directCompleted: 1,
      recoveredCompleted: 0,
      recoveredWithoutReport: 0,
    });
  });

  it("counts a status-recovered report separately from direct delivery", async () => {
    const harness = await startHarness({ reconcileIntervalMs: 1_000, recoveryGraceMs: 0 });
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a mailbox bridge");
    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);
    const status = await receiveOne(link, 4_000);
    expect(status.envelope.type).toBe("gethen.task.status");
    await link.ack([status.envelope.id]);
    await link.send(statusResultEnvelope(status.envelope, "recovered-only"));

    const result = await resultPromise;
    expect(result.report.summary).toContain("(recovered from worker status)");
    expect(runtime.health().settlements).toEqual({
      directCompleted: 0,
      recoveredCompleted: 1,
      recoveredWithoutReport: 0,
    });
  });

  it("answers a parked escalation and applies the late result to the ticket", async () => {
    const harness = await startHarness();
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);
    const trigger: Trigger = {
      id: "trigger-parked",
      source: "local",
      thread: "local",
      actor: "local-user",
      bot: false,
      addressed: "mention",
      text: "merge it",
      at: AT,
      meta: { scope: "local", workspace: "/ws" },
    };

    const submitted = runtime.service.submit(trigger);
    const assign = await receiveOne(link);
    const assignmentId = assignmentIdOf(assign.envelope);
    await link.ack([assign.envelope.id]);
    await link.send(askEnvelope(assign.envelope));

    // gethen_run resolves promptly with a waiting report, it does not block.
    const launch = await submitted;
    expect(launch.decision?.outcome).toBe("accept");
    const ticket = runtime.service.store.tickets.list().at(-1);
    if (ticket === undefined) throw new Error("expected a ticket");
    await waitFor(() => runtime.service.store.tickets.get(ticket.id)?.latestReport === "waiting");

    const answered = runtime.tasks.answer(assignmentId, {
      kind: "answer",
      text: "Merge it.",
      option: "merge",
    });
    expect(answered.status).toBe("resolved");
    const answer = await receiveOne(link);
    expect(answer.envelope).toMatchObject({
      type: "gethen.task.answer",
      correlationId: assignmentId,
    });
    await link.ack([answer.envelope.id]);

    await link.send(resultEnvelope(assign.envelope, { kind: "ready", summary: "merged" }));
    await waitFor(() => runtime.service.store.tickets.get(ticket.id)?.latestReport === "ready");
    const receipts = runtime.service.store.listReceipts().filter((row) => row.ticket === ticket.id);
    expect(receipts.at(-1)?.summary).toBe("merged");
  });

  it("records durable wake notifications for results and parked escalations", async () => {
    const harness = await startHarness();
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a mailbox bridge");
    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);
    await link.send(resultEnvelope(assign.envelope, { summary: "done" }));
    await resultPromise;
    const resultRows = runtime.notifications.pending();
    expect(resultRows[0]).toMatchObject({
      kind: "assignment.result",
      assignment: "assignment-1",
      ticket: "ticket-1",
    });
    expect(resultRows[0]?.data).toMatchObject({ report: "ready", late: false });
    runtime.notifications.markDelivered(resultRows[0]?.id as string);
    expect(runtime.notifications.pending()).toEqual([]);
    await harness.runtime.close();

    const parked = await startHarness();
    const parkedClient = await attachWorker(parked);
    const parkedLaunch = parked.runtime.bridge?.launchPort.launch(launchRequest());
    if (parkedLaunch === undefined) throw new Error("expected a mailbox bridge");
    const parkedAssign = await receiveOne(parkedClient.link);
    await parkedClient.link.ack([parkedAssign.envelope.id]);
    await parkedClient.link.send(askEnvelope(parkedAssign.envelope, "Which branch?"));
    const waiting = await parkedLaunch;
    expect(waiting.report.kind).toBe("waiting");
    const parkedRows = parked.runtime.notifications.pending();
    expect(parkedRows[0]).toMatchObject({
      kind: "escalation.parked",
      assignment: "assignment-1",
    });
    expect(parkedRows[0]?.data).toMatchObject({ question: "Which branch?" });
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

  it("counts a recovery that had no report content", async () => {
    const harness = await startHarness({ reconcileIntervalMs: 1_000, recoveryGraceMs: 0 });
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a mailbox bridge");
    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);
    const status = await receiveOne(link, 4_000);
    expect(status.envelope.type).toBe("gethen.task.status");
    await link.ack([status.envelope.id]);
    const data = status.envelope.data as Record<string, unknown>;
    const assignmentId = String(data.assignmentId);
    await link.send(
      newEnvelope({
        source: `agent:${WORKER}`,
        type: "gethen.task.status.result",
        subject: assignmentId,
        time: AT,
        data: {
          assignmentId,
          // Finished, but without a retained report or message.
          state: { assignmentId, phase: "done", step: 1, seq: 1, lastEventAt: AT },
        },
        correlationId: status.envelope.id,
        to: status.envelope.replyTo ?? MASTER_MAILBOX,
      }),
    );

    const result = await resultPromise;
    expect(result.report.summary).toBe("worker finished without a delivered result");
    expect(runtime.health().settlements).toEqual({
      directCompleted: 0,
      recoveredCompleted: 1,
      recoveredWithoutReport: 1,
    });
  });

  it("answers a parked escalation and applies the resumed result to the ticket", async () => {
    const harness = await startHarness();
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const trigger: Trigger = {
      id: `trigger-${crypto.randomUUID()}`,
      source: "local",
      thread: "local",
      actor: "local-user",
      bot: false,
      addressed: "mention",
      text: "fix the failing test",
      at: AT,
      meta: { scope: "local", workspace: "/ws" },
    };
    const submitted = runtime.service.submit(trigger);
    const ticketId = runtime.service.store.tickets.list().at(-1)?.id;
    if (ticketId === undefined) throw new Error("expected an accepted ticket");

    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);
    await link.send(askEnvelope(assign.envelope, "Allow the tool?", "permission"));
    await submitted;
    await waitFor(() => runtime.service.store.tickets.get(ticketId)?.attention !== undefined);

    const answered = await runtime.control.answer({
      ticketId,
      text: "Allow it.",
      option: "allow",
    });
    expect(answered).toMatchObject({
      delivered: true,
      ticket: { id: ticketId, needsAction: false },
      escalation: {
        status: "resolved",
        resolution: { kind: "answer", text: "Allow it.", option: "allow" },
      },
    });

    const answer = await receiveOne(link);
    expect(answer.envelope).toMatchObject({
      type: "gethen.task.answer",
      correlationId: answered.assignmentId,
      data: {
        escalationId: "esc-1",
        resolution: { kind: "answer", text: "Allow it.", option: "allow" },
      },
    });
    await link.ack([answer.envelope.id]);

    // The answered Worker resumes and its result lands on the same Ticket.
    await link.send(resultEnvelope(assign.envelope, { summary: "allowed and done" }));
    await waitFor(() => runtime.service.store.tickets.get(ticketId)?.latestReport === "ready");
    expect(runtime.service.store.listReceipts().map((receipt) => receipt.summary)).toEqual([
      "waiting for a person: Allow the tool?",
      "allowed and done",
    ]);
    expect(runtime.service.store.tickets.get(ticketId)?.attention).toBeUndefined();
    // The resumed result arrived as a normal envelope, not via status recovery.
    expect(runtime.health().settlements).toEqual({
      directCompleted: 1,
      recoveredCompleted: 0,
      recoveredWithoutReport: 0,
    });
  });

  it("settles a silent launch and applies the Worker's late result", async () => {
    const harness = await startHarness({ launchTimeoutMs: 40 });
    const runtime = harness.runtime;
    const { link } = await attachWorker(harness);

    const trigger: Trigger = {
      id: `trigger-${crypto.randomUUID()}`,
      source: "local",
      thread: "local",
      actor: "local-user",
      bot: false,
      addressed: "mention",
      text: "sleep 40 and report",
      at: AT,
      meta: { scope: "local", workspace: "/ws" },
    };
    const submitted = runtime.service.submit(trigger);
    const ticketId = runtime.service.store.tickets.list().at(-1)?.id;
    if (ticketId === undefined) throw new Error("expected an accepted ticket");

    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);
    await submitted;
    const assignmentId = assignmentIdOf(assign.envelope);

    // The silence timeout is a settlement, not a retry: failed report, receipt,
    // ended Assignment, effect done.
    await waitFor(() => runtime.service.store.tickets.get(ticketId)?.latestReport === "failed");
    expect(runtime.service.store.assignments.get(assignmentId)?.state).toBe("ended");
    expect(runtime.service.store.listReceipts()).toMatchObject([
      { assignment: assignmentId, outcome: "failed" },
    ]);
    expect(runtime.service.store.listReceipts()).toHaveLength(1);

    // The task was already dispatched: its late result still lands exactly once.
    await link.send(resultEnvelope(assign.envelope, { summary: "slept" }));
    await waitFor(() => runtime.service.store.tickets.get(ticketId)?.latestReport === "ready");
    expect(runtime.service.store.listReceipts()).toMatchObject([
      { assignment: assignmentId, outcome: "failed" },
      { assignment: assignmentId, outcome: "completed", summary: "slept" },
    ]);
    expect(runtime.service.store.tickets.get(ticketId)?.attention).toBeUndefined();

    // At-least-once mailbox: a redelivered result (new envelope id, same
    // assignment) must not append a third receipt or move the Ticket again.
    await link.send(resultEnvelope(assign.envelope, { summary: "slept" }));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(runtime.service.store.listReceipts()).toHaveLength(2);
    expect(runtime.service.store.tickets.get(ticketId)?.latestReport).toBe("ready");
  });

  it("persists a parked launch under the master db directory", async () => {
    const harness = await startHarness();
    const runtime = harness.runtime;
    const bridge = runtime.bridge;
    if (bridge === undefined) throw new Error("expected a mailbox bridge");
    const pending = bridge.launchPort.launch(launchRequest("assignment-persist"));

    bridge.handleEnvelope(
      newEnvelope({
        source: `agent:${WORKER}`,
        type: "gethen.task.ask",
        time: AT,
        data: {
          assignmentId: "assignment-persist",
          escalationId: "esc-persist",
          kind: "permission",
          question: "allow bash?",
          options: [
            { id: "allow", label: "Allow once" },
            { id: "reject", label: "Reject" },
          ],
        },
        to: MASTER_MAILBOX,
        replyTo: WORKER_MAILBOX,
      }),
    );
    expect((await pending).report.kind).toBe("waiting");

    const file = join(dir, `master-${sequence}.sqlite.pending.json`);
    expect(existsSync(file)).toBe(true);
    const persisted = JSON.parse(readFileSync(file, "utf8")) as Array<{
      request?: { assignment?: { id?: string } };
    }>;
    expect(persisted).toHaveLength(1);
    expect(persisted[0]?.request?.assignment?.id).toBe("assignment-persist");
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
      scheduling: {
        queued: 0,
        running: 0,
        dispatched: 0,
        reused: 0,
        queueEntries: 0,
        queueTimeouts: 0,
        maxQueueDepth: 0,
        parked: 0,
      },
      settlements: { directCompleted: 0, recoveredCompleted: 0, recoveredWithoutReport: 0 },
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

describe("multi-worker routing", () => {
  function manifest(id: string, workspace: string, quota = 1): WorkerManifest {
    return {
      id,
      machine: id,
      runtime: "dsh",
      workspaces: [{ id: workspace }],
      capabilities: {
        promptInjection: "system",
        skills: "filesystem",
        mcp: false,
        permissions: "approval",
        streaming: true,
        steer: true,
        fork: false,
      },
      models: ["deepseek-v4.1-flash"],
      costClass: "mid",
      load: { running: 0, quota },
      health: { state: "online", lastHeartbeatAt: AT },
    };
  }

  it("recovers a routed assignment from a status probe after its result is lost", async () => {
    const harness = await startHarness({
      tokens: [
        { workerId: "worker-a", token: "secret-a" },
        { workerId: "worker-b", token: "secret-b" },
      ],
      reconcileIntervalMs: 1_000,
      recoveryGraceMs: 500,
    });
    const runtime = harness.runtime;
    const a = await attachWorker(harness, {
      token: "secret-a",
      mailbox: "mailbox:worker-a",
      worker: manifest("worker-a", "/ws-a", 1),
    });
    await attachWorker(harness, {
      token: "secret-b",
      mailbox: "mailbox:worker-b",
      worker: manifest("worker-b", "/ws-b", 1),
    });
    await waitFor(() => runtime.workers().filter((worker) => worker.registered).length === 2);

    const trigger: Trigger = {
      id: "trigger-routed-recovery",
      source: "local",
      thread: "local",
      actor: "local-user",
      bot: false,
      addressed: "mention",
      text: "recover me",
      at: AT,
      meta: { scope: "local", workspace: "/ws-a" },
    };
    const submitted = runtime.service.submit(trigger);
    const assign = await receiveOne(a.link);
    const assignmentId = assignmentIdOf(assign.envelope);
    await a.link.ack([assign.envelope.id]);

    // Do not send a result envelope: only the routed port's status probe can
    // settle this assignment. The probe still goes through the shared bridge.
    // Two successful probes are needed with the grace window; a stale progress
    // heartbeat after the first must not discard the candidate.
    let statusId = "";
    for (let probe = 0; probe < 2; probe += 1) {
      const status = await receiveOne(a.link, 4_000);
      expect(status.envelope.type).toBe("gethen.task.status");
      await a.link.ack([status.envelope.id]);
      const data = status.envelope.data as Record<string, unknown>;
      statusId = String(data.assignmentId);
      await a.link.send(
        newEnvelope({
          source: "agent:worker-a",
          type: "gethen.task.status.result",
          subject: statusId,
          time: AT,
          data: {
            assignmentId: statusId,
            state: { assignmentId: statusId, phase: "done", step: 1, seq: 1, lastEventAt: AT },
            report: { assignmentId: statusId, kind: "ready", summary: "routed status done" },
          },
          correlationId: status.envelope.id,
          to: MASTER_MAILBOX,
        }),
      );
      if (probe === 0) {
        await a.link.send(
          newEnvelope({
            source: "agent:worker-a",
            type: "gethen.task.progress",
            subject: statusId,
            time: AT,
            data: {
              assignmentId: statusId,
              seq: 1,
              state: { assignmentId: statusId, phase: "done", step: 1, seq: 1, lastEventAt: AT },
            },
            correlationId: assign.envelope.id,
            to: MASTER_MAILBOX,
          }),
        );
      }
    }

    await submitted;
    const ticketId = runtime.service.store.tickets.list().at(-1)?.id;
    if (ticketId === undefined) throw new Error("expected a ticket");
    await waitFor(
      () => runtime.service.store.tickets.get(ticketId)?.latestReport === "ready",
      2_500,
    );
    const receipt = runtime.service.store.listReceipts().at(-1);
    expect(receipt).toMatchObject({
      assignment: assignmentId,
      outcome: "completed",
      summary: "routed status done (recovered from worker status)",
    });
    expect(runtime.service.store.assignments.get(assignmentId)?.state).toBe("ended");
    expect(runtime.health().settlements).toEqual({
      directCompleted: 0,
      recoveredCompleted: 1,
      recoveredWithoutReport: 0,
    });
  });

  it("recovers a routed assignment through the silence-timeout probe", async () => {
    const harness = await startHarness({
      tokens: [
        { workerId: "worker-a", token: "secret-a" },
        { workerId: "worker-b", token: "secret-b" },
      ],
      launchTimeoutMs: 200,
      reconcileIntervalMs: 0,
    });
    const runtime = harness.runtime;
    const a = await attachWorker(harness, {
      token: "secret-a",
      mailbox: "mailbox:worker-a",
      worker: manifest("worker-a", "/ws-a", 1),
    });
    await attachWorker(harness, {
      token: "secret-b",
      mailbox: "mailbox:worker-b",
      worker: manifest("worker-b", "/ws-b", 1),
    });
    await waitFor(() => runtime.workers().filter((worker) => worker.registered).length === 2);

    const submitted = runtime.service.submit({
      id: "trigger-routed-silence",
      source: "local",
      thread: "local",
      actor: "local-user",
      bot: false,
      addressed: "mention",
      text: "silent routed recovery",
      at: AT,
      meta: { scope: "local", workspace: "/ws-a" },
    });
    const assign = await receiveOne(a.link);
    const assignmentId = assignmentIdOf(assign.envelope);
    await a.link.ack([assign.envelope.id]);

    const status = await receiveOne(a.link, 3_000);
    expect(status.envelope.type).toBe("gethen.task.status");
    await a.link.ack([status.envelope.id]);
    const data = status.envelope.data as Record<string, unknown>;
    const statusId = String(data.assignmentId);
    await a.link.send(
      newEnvelope({
        source: "agent:worker-a",
        type: "gethen.task.status.result",
        subject: statusId,
        time: AT,
        data: {
          assignmentId: statusId,
          state: { assignmentId: statusId, phase: "done", step: 1, seq: 1, lastEventAt: AT },
          report: { assignmentId: statusId, kind: "ready", summary: "silent routed done" },
        },
        correlationId: status.envelope.id,
        to: MASTER_MAILBOX,
      }),
    );

    const launch = await submitted;
    expect(launch.decision?.outcome).toBe("accept");
    const receipt = runtime.service.store.listReceipts().at(-1);
    expect(receipt).toMatchObject({
      assignment: assignmentId,
      outcome: "completed",
      summary: "silent routed done (recovered from worker status)",
    });
  });

  it("routes each assignment to a different worker and exposes both in the fleet view", async () => {
    const harness = await startHarness({
      tokens: [
        { workerId: "worker-a", token: "secret-a" },
        { workerId: "worker-b", token: "secret-b" },
      ],
    });
    const runtime = harness.runtime;
    const a = await attachWorker(harness, {
      token: "secret-a",
      mailbox: "mailbox:worker-a",
      worker: manifest("worker-a", "/ws-a", 2),
    });
    const b = await attachWorker(harness, {
      token: "secret-b",
      mailbox: "mailbox:worker-b",
      worker: manifest("worker-b", "/ws-b", 1),
    });
    await waitFor(() => runtime.workers().filter((worker) => worker.registered).length === 2);
    expect(runtime.workers().map((worker) => [worker.id, worker.health])).toEqual([
      ["worker-a", "online"],
      ["worker-b", "online"],
    ]);

    const triggerFor = (id: string, workspace: string): Trigger => ({
      id,
      source: "local",
      thread: "local",
      actor: "local-user",
      bot: false,
      addressed: "mention",
      text: `work in ${workspace}`,
      at: AT,
      meta: { scope: "local", workspace },
    });

    const first = runtime.service.submit(triggerFor("trigger-a", "/ws-a"));
    const assignA = await receiveOne(a.link);
    expect(assignA.envelope).toMatchObject({
      type: "gethen.task.assign",
      to: "mailbox:worker-a",
    });
    await a.link.ack([assignA.envelope.id]);
    await a.link.send(resultEnvelope(assignA.envelope, { summary: "a done" }));
    const firstSettled = await first;
    expect(firstSettled.decision?.outcome).toBe("accept");

    const second = runtime.service.submit(triggerFor("trigger-b", "/ws-b"));
    const assignB = await receiveOne(b.link);
    expect(assignB.envelope).toMatchObject({
      type: "gethen.task.assign",
      to: "mailbox:worker-b",
    });
    await b.link.ack([assignB.envelope.id]);
    await b.link.send(resultEnvelope(assignB.envelope, { summary: "b done" }));
    await second;

    const receipts = runtime.service.store.listReceipts();
    expect(receipts.map((receipt) => receipt.worker)).toEqual(["worker-a", "worker-b"]);
    expect(runtime.service.store.assignments.list().map((item) => item.worker)).toEqual([
      "worker-a",
      "worker-b",
    ]);
    expect(
      runtime.workers().map((worker) => ({
        id: worker.id,
        workspaces: worker.workspaces.map((workspace) => workspace.id),
        quota: worker.load.quota,
        running: worker.load.running,
      })),
    ).toEqual([
      { id: "worker-a", workspaces: ["/ws-a"], quota: 2, running: 0 },
      { id: "worker-b", workspaces: ["/ws-b"], quota: 1, running: 0 },
    ]);
  });
});

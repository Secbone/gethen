import { mkdtempSync, rmSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { newEnvelope, type ReceivedMessage } from "@gethen/mailbox";
import { createMailboxClientLink, type MailboxClientLink } from "@gethen/mailbox-link";
import type { Assignment, LaunchRequest, Ticket } from "@gethen/master";
import { createWsTransport, type TransportSession } from "@gethen/transport";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createMasterRuntime,
  type MasterRuntime,
  MasterRuntimeError,
  type MasterRuntimeOptions,
} from "./runtime";

const NOW = new Date("2026-11-17T10:00:00.000Z");
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

/** A state payload a worker may report in `gethen.task.progress`/`status.result`. */
const STATE = {
  phase: "thinking",
  step: 2,
  seq: 5,
  lastEventAt: AT,
  lastMessage: "planning",
};

interface Harness {
  runtime: MasterRuntime;
  url?: string;
}

let dir: string;
let sequence = 0;
const runtimes: MasterRuntime[] = [];
const servers: Server[] = [];
const sessions: TransportSession[] = [];
const links: MailboxClientLink[] = [];

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "gethen-runtime-tasks-"));
});

afterEach(async () => {
  for (const link of links.splice(0)) link.close();
  for (const session of sessions.splice(0)) {
    await session.close("test_cleanup").catch(() => undefined);
  }
  for (const runtime of runtimes.splice(0)) {
    await runtime.close().catch(() => undefined);
  }
  for (const server of servers.splice(0)) {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
  rmSync(dir, { recursive: true, force: true });
});

async function startHarness(
  options: { withServer?: boolean; tokens?: MasterRuntimeOptions["tokens"] } = {},
): Promise<Harness> {
  sequence += 1;
  const runtime = createMasterRuntime({
    dbPath: join(dir, `master-${sequence}.sqlite`),
    tokens: options.tokens ?? [{ workerId: WORKER, token: TOKEN }],
    workerId: WORKER,
    mailboxDbPath: join(dir, `mailbox-${sequence}.sqlite`),
    pollIntervalMs: 5,
    visibilityTimeoutMs: 1_000,
    now: () => NOW,
  });
  runtimes.push(runtime);
  if ((options.withServer ?? true) === false) return { runtime };

  const server = createServer((_request, response) => {
    response.statusCode = 404;
    response.end("not found");
  });
  server.on("upgrade", (request, socket, head) => {
    runtime.handleUpgrade?.(request, socket, head);
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : 0;
  return { runtime, url: `ws://127.0.0.1:${port}/gethen/worker` };
}

async function attachWorker(harness: Harness): Promise<MailboxClientLink> {
  if (harness.url === undefined) throw new Error("harness has no server url");
  const session = await createWsTransport({ heartbeat: false }).connect(
    { url: harness.url },
    { kind: "bearer", token: TOKEN },
  );
  sessions.push(session);
  const link = await createMailboxClientLink({
    session,
    mailbox: WORKER_MAILBOX,
    receiveTimeoutMs: 500,
    requestTimeoutMs: 2_000,
    visibilityTimeoutMs: 1_000,
    now: () => NOW,
  });
  links.push(link);
  return link;
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

function statusResult(request: ReceivedMessage, state: unknown) {
  return newEnvelope({
    source: `agent:${WORKER}`,
    type: "gethen.task.status.result",
    subject: "assignment-1",
    time: AT,
    data: { assignmentId: "assignment-1", state },
    correlationId: request.envelope.id,
    to: request.envelope.replyTo ?? MASTER_MAILBOX,
  });
}

describe("master-runtime tasks", () => {
  it("reports task control as unavailable in fake mode", async () => {
    const runtime = createMasterRuntime({ dbPath: join(dir, "fake.sqlite"), now: () => NOW });
    runtimes.push(runtime);

    expect(() => runtime.tasks.states()).toThrowError(MasterRuntimeError);
    expect(() => runtime.tasks.state("assignment-1")).toThrow(/RUNTIME_COMMS_UNAVAILABLE/);
    await expect(runtime.tasks.status("assignment-1")).rejects.toMatchObject({
      code: "RUNTIME_COMMS_UNAVAILABLE",
    });
    await expect(runtime.tasks.steer("assignment-1", "go")).rejects.toMatchObject({
      code: "RUNTIME_COMMS_UNAVAILABLE",
    });
  });

  it("asks the worker for live state through gethen.task.status", async () => {
    const harness = await startHarness();
    const link = await attachWorker(harness);

    const pendingStatus = harness.runtime.tasks.status("assignment-1", { timeoutMs: 3_000 });
    const request = await receiveOne(link);
    expect(request.envelope).toMatchObject({
      type: "gethen.task.status",
      to: WORKER_MAILBOX,
      subject: "assignment-1",
    });
    await link.ack([request.envelope.id]);
    await link.send(statusResult(request, STATE));

    await expect(pendingStatus).resolves.toMatchObject({
      phase: "thinking",
      step: 2,
      seq: 5,
      lastMessage: "planning",
    });

    // A worker that has no state for the assignment answers with `null`.
    const missingStatus = harness.runtime.tasks.status("assignment-2", { timeoutMs: 3_000 });
    const missingRequest = await receiveOne(link);
    await link.ack([missingRequest.envelope.id]);
    await link.send(statusResult(missingRequest, null));
    await expect(missingStatus).resolves.toBeNull();
  });

  it("sends steer and cancel control messages to the worker", async () => {
    const harness = await startHarness();
    const link = await attachWorker(harness);

    const steer = harness.runtime.tasks.steer("assignment-1", "focus on tests");
    const steerMessage = await receiveOne(link);
    expect(steerMessage.envelope).toMatchObject({
      type: "gethen.task.steer",
      to: WORKER_MAILBOX,
      subject: "assignment-1",
      data: { assignmentId: "assignment-1", text: "focus on tests" },
    });
    await link.ack([steerMessage.envelope.id]);
    await steer;

    const cancel = harness.runtime.tasks.cancel("assignment-1", "stop now");
    const cancelMessage = await receiveOne(link);
    expect(cancelMessage.envelope).toMatchObject({
      type: "gethen.task.cancel",
      to: WORKER_MAILBOX,
      subject: "assignment-1",
      data: { assignmentId: "assignment-1", reason: "stop now" },
    });
    await link.ack([cancelMessage.envelope.id]);
    await cancel;
  });

  it("exposes the last state from progress reports", async () => {
    const harness = await startHarness();
    const link = await attachWorker(harness);
    const launch = harness.runtime.bridge?.launchPort.launch(launchRequest());
    if (launch === undefined) throw new Error("expected a mailbox bridge");

    const assign = await receiveOne(link);
    await link.ack([assign.envelope.id]);
    await link.send(
      newEnvelope({
        source: `agent:${WORKER}`,
        type: "gethen.task.progress",
        subject: "assignment-1",
        time: AT,
        data: { assignmentId: "assignment-1", state: STATE },
        correlationId: "assignment-1",
        to: MASTER_MAILBOX,
      }),
    );

    const deadline = Date.now() + 3_000;
    while (harness.runtime.tasks.state("assignment-1") === undefined) {
      if (Date.now() > deadline) throw new Error("timed out waiting for the progress state");
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(harness.runtime.tasks.states()["assignment-1"]).toMatchObject({ phase: "thinking" });

    await link.send(
      newEnvelope({
        source: `agent:${WORKER}`,
        type: "gethen.task.result",
        subject: "assignment-1",
        time: AT,
        data: { assignmentId: "assignment-1", kind: "ready", summary: "done" },
        correlationId: "assignment-1",
        to: MASTER_MAILBOX,
      }),
    );
    await expect(launch).resolves.toMatchObject({ report: { kind: "ready" } });
    expect(harness.runtime.tasks.state("assignment-1")).toMatchObject({ phase: "done" });
  });

  it("rejects task control after the runtime closed", async () => {
    const harness = await startHarness({ withServer: false });
    await harness.runtime.close();

    expect(() => harness.runtime.tasks.states()).toThrow(/RUNTIME_CLOSED/);
    await expect(harness.runtime.tasks.status("assignment-1")).rejects.toMatchObject({
      code: "RUNTIME_CLOSED",
    });
    await expect(harness.runtime.tasks.cancel("assignment-1")).rejects.toMatchObject({
      code: "RUNTIME_CLOSED",
    });
  });
});

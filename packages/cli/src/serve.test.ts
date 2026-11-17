import { mkdtempSync, rmSync } from "node:fs";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createMailboxAgent, type MailboxAgent } from "@gethen/agent";
import { createMailboxClientLink, type MailboxClientLink } from "@gethen/mailbox-link";
import type { EscalationRecord } from "@gethen/master";
import { createWsTransport, type TransportSession } from "@gethen/transport";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  autoEscalationResolver,
  parseWorkerTokens,
  type ServeConfig,
  type ServeHandle,
  serveCommand,
  startServe,
  WORKER_PATH,
} from "./commands/serve";

type MailboxHarness = Parameters<typeof createMailboxAgent>[0]["harness"];
type EnvelopeLike = Parameters<MailboxClientLink["send"]>[0];

/** A live TaskState a scripted worker reports (matches bridge-mailbox). */
const TASK_STATE = {
  phase: "thinking",
  step: 1,
  seq: 0,
  lastEventAt: new Date("2026-11-17T10:00:00.000Z").toISOString(),
  lastMessage: "planning",
};

/** Build a worker->master mailbox envelope without importing @gethen/mailbox. */
function workerEnvelope(input: {
  type: string;
  data: unknown;
  subject?: string;
  correlationId?: string;
  to?: string;
}): EnvelopeLike {
  return {
    specversion: "1.0",
    id: crypto.randomUUID(),
    source: "agent:worker-1",
    type: input.type,
    time: new Date().toISOString(),
    datacontenttype: "application/json",
    data: input.data,
    ...(input.subject === undefined ? {} : { subject: input.subject }),
    ...(input.correlationId === undefined ? {} : { correlationId: input.correlationId }),
    ...(input.to === undefined ? {} : { to: input.to }),
  };
}

const handles: ServeHandle[] = [];
const agents: MailboxAgent[] = [];
const links: MailboxClientLink[] = [];
const sessions: TransportSession[] = [];
const dirs: string[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  for (const agent of agents.splice(0)) await agent.stop();
  for (const link of links.splice(0)) link.close();
  for (const session of sessions.splice(0)) {
    await session.close("test_cleanup").catch(() => undefined);
  }
  while (handles.length > 0) await handles.pop()?.close();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function escalationRecord(overrides: Partial<EscalationRecord> = {}): EscalationRecord {
  return {
    id: "escalation-1",
    ticket: "ticket-1",
    assignment: "assignment-1",
    session: "worker-1",
    kind: "decision",
    question: "Proceed?",
    options: [
      { id: "yes", label: "Yes" },
      { id: "no", label: "No" },
    ],
    status: "waiting",
    at: new Date("2026-11-16T00:00:00.000Z").toISOString(),
    ...overrides,
  };
}

async function serve(config: Partial<ServeConfig> = {}) {
  const dir = mkdtempSync(join(tmpdir(), "gethen-serve-"));
  dirs.push(dir);
  const handle = await startServe({ dbPath: join(dir, "master.sqlite"), port: 0, ...config });
  handles.push(handle);
  return handle;
}

/**
 * A scripted harness (no DSH). Observations arrive after start() resolved, and
 * the event/report are separated so the mailbox consumer flushes one outbox
 * envelope at a time.
 */
function readyHarness(): MailboxHarness {
  return {
    async start(task, sink) {
      let settled = false;
      setTimeout(() => {
        if (settled) return;
        sink.event({
          type: "message",
          at: new Date().toISOString(),
          data: { text: `harness ran ${task.assignmentId}` },
        });
      }, 10);
      setTimeout(() => {
        if (settled) return;
        settled = true;
        sink.report({ kind: "ready", summary: `harness finished ${task.assignmentId}` });
      }, 50);
      return {
        sendResolution: async () => undefined,
        cancel: async (reason?: string) => {
          if (settled) return;
          settled = true;
          sink.report({ kind: "failed", summary: reason ?? "cancelled" });
        },
      };
    },
  };
}

async function attachWorker(
  handle: ServeHandle,
  options: { workerId?: string; token: string },
): Promise<void> {
  const workerId = options.workerId ?? "worker-1";
  const session = await createWsTransport({ heartbeat: true }).connect(
    { url: `ws://127.0.0.1:${handle.port}${WORKER_PATH}` },
    { kind: "bearer", token: options.token },
  );
  sessions.push(session);
  const link = await createMailboxClientLink({ session, mailbox: `mailbox:${workerId}` });
  links.push(link);
  const agent = createMailboxAgent({
    workerId,
    mailbox: link,
    harness: readyHarness(),
    quota: 1,
    pollIntervalMs: 10,
  });
  agents.push(agent);
  agent.start();
}

/** Attach a raw mailbox link (no agent) so a test can script the worker side. */
async function attachRawWorker(handle: ServeHandle, token: string): Promise<MailboxClientLink> {
  const session = await createWsTransport({ heartbeat: false }).connect(
    { url: `ws://127.0.0.1:${handle.port}${WORKER_PATH}` },
    { kind: "bearer", token },
  );
  sessions.push(session);
  const link = await createMailboxClientLink({
    session,
    mailbox: "mailbox:worker-1",
    receiveTimeoutMs: 100,
    visibilityTimeoutMs: 2_000,
  });
  links.push(link);
  return link;
}

async function receiveEnvelope(
  link: MailboxClientLink,
  timeoutMs = 3_000,
): Promise<{ envelope: { id: string; type?: string; data?: unknown; replyTo?: string } }> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const messages = await link.receive({ limit: 1, timeoutMs: 100 });
    const [message] = messages;
    if (message !== undefined) {
      return message as unknown as {
        envelope: { id: string; type?: string; data?: unknown; replyTo?: string };
      };
    }
    if (Date.now() > deadline) {
      throw new Error("timed out waiting for a mailbox delivery");
    }
  }
}

function baseUrl(handle: ServeHandle): string {
  return `http://127.0.0.1:${handle.port}`;
}

async function getJson(handle: ServeHandle, path: string): Promise<Record<string, unknown>> {
  const response = await fetch(`${baseUrl(handle)}${path}`);
  return (await response.json()) as Record<string, unknown>;
}

async function postJson(
  handle: ServeHandle,
  path: string,
  body: unknown,
): Promise<Record<string, unknown>> {
  const response = await fetch(`${baseUrl(handle)}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return (await response.json()) as Record<string, unknown>;
}

/** Poll `/health` until `predicate` is true. */
async function waitForHealth(
  handle: ServeHandle,
  predicate: (health: Record<string, unknown>) => boolean,
  message: string,
): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 5_000;
  for (;;) {
    const health = await getJson(handle, "/health");
    if (predicate(health)) return health;
    if (Date.now() > deadline) {
      throw new Error(`${message}: ${JSON.stringify(health)}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Raw HTTP upgrade so the test can assert the transport-level rejection line. */
function upgradeStatus(port: number, path: string): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const socket = connect(port, "127.0.0.1");
    let response = "";
    let settled = false;
    const finish = (): void => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(response.split("\r\n")[0] ?? "");
    };
    socket.setTimeout(2_000, finish);
    socket.on("connect", () => {
      socket.write(
        `GET ${path} HTTP/1.1\r\n` +
          `Host: 127.0.0.1:${port}\r\n` +
          "Upgrade: websocket\r\n" +
          "Connection: Upgrade\r\n" +
          "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
          "Sec-WebSocket-Version: 13\r\n\r\n",
      );
    });
    socket.on("data", (chunk) => {
      response += chunk.toString("utf8");
      if (response.includes("\r\n\r\n")) finish();
    });
    socket.on("close", finish);
    socket.on("error", (error) => {
      if (settled) return;
      settled = true;
      reject(error);
    });
  });
}

describe("gethen serve", () => {
  it("falls back to the fake launch when no worker tokens are configured", async () => {
    const handle = await serve();
    expect(handle.launch).toBe("fake");

    const health = await getJson(handle, "/health");
    expect(health).toMatchObject({
      ok: true,
      tickets: 0,
      workers: 0,
      launch: "fake",
      mailbox: { pending: 0, leased: 0, dead: 0 },
    });

    const run = await postJson(handle, "/gethen/run", { text: "standalone smoke" });
    expect(run).toMatchObject({
      stored: "stored",
      outcome: "accept",
      report: "ready",
      receipt: true,
    });

    const after = await getJson(handle, "/health");
    expect(after).toMatchObject({ tickets: 1, open: 1, receipts: 1, workers: 0, launch: "fake" });
  });

  it("runs an assignment through a mailbox worker over the ws transport", async () => {
    const token = "test-worker-token";
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token }],
      workspace: "/tmp/gethen-test-workspace",
      mailboxPollIntervalMs: 10,
      mailboxVisibilityTimeoutMs: 2_000,
    });
    expect(handle.launch).toBe("ws");

    await attachWorker(handle, { token });
    await waitForHealth(handle, (health) => health.workers === 1, "worker did not attach");

    const run = await postJson(handle, "/gethen/run", { text: "mailbox smoke" });
    expect(run).toMatchObject({
      stored: "stored",
      outcome: "accept",
      report: "ready",
      receipt: true,
    });

    const health = await waitForHealth(
      handle,
      (current) => {
        const mailbox = current.mailbox as { pending: number; leased: number } | undefined;
        return mailbox !== undefined && mailbox.pending === 0 && mailbox.leased === 0;
      },
      "mailboxes did not drain",
    );
    expect(health).toMatchObject({ workers: 1, launch: "ws", tickets: 1, receipts: 1 });
  }, 15_000);

  it("reports the mailbox transport stack and rejects unauthenticated upgrades", async () => {
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token: "mailbox-token" }],
      mailboxPollIntervalMs: 10,
      mailboxVisibilityTimeoutMs: 2_000,
    });

    expect(handle.launch).toBe("ws");
    const health = await getJson(handle, "/health");
    expect(health).toMatchObject({ ok: true, launch: "ws", workers: 0, tickets: 0 });
    expect(health.mailbox).toEqual({ pending: 0, leased: 0, dead: 0 });
    expect(await upgradeStatus(handle.port, WORKER_PATH)).toBe("HTTP/1.1 401 Unauthorized");
  });

  it("auto-answers with the declared default or first option", async () => {
    const resolver = autoEscalationResolver();

    const first = await resolver(escalationRecord());
    expect(first).toMatchObject({ kind: "answer", option: "yes" });
    expect(first.text).toContain("auto");

    const preferred = await resolver(escalationRecord({ defaultOption: "no" }));
    expect(preferred.option).toBe("no");
  });

  it("rejects an unknown --escalation mode", async () => {
    const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);

    expect(await serveCommand(["--escalation", "bogus"])).toBe(1);

    expect(stderr.mock.calls.map((call) => String(call[0])).join("")).toContain("--escalation");
  });

  it("documents the mailbox transport without a --comms switch", async () => {
    const lines: string[] = [];

    expect(await serveCommand(["--help"], { write: (line) => lines.push(line) })).toBe(0);

    const output = lines.join("\n");
    expect(output).toContain("mailbox");
    expect(output).not.toContain("--comms");
  });

  it("rejects upgrades on other paths and without a bearer token", async () => {
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token: "secret" }],
    });

    expect(await upgradeStatus(handle.port, "/nope")).toBe("HTTP/1.1 404 Not Found");
    expect(await upgradeStatus(handle.port, WORKER_PATH)).toBe("HTTP/1.1 401 Unauthorized");
  });

  it("parses the worker token JSON and rejects malformed entries", () => {
    expect(parseWorkerTokens('[{"workerId":"w1","token":"t1"}]')).toEqual([
      { workerId: "w1", token: "t1" },
    ]);
    expect(() => parseWorkerTokens("not json")).toThrow(/invalid worker tokens JSON/);
    expect(() => parseWorkerTokens("{}")).toThrow(/JSON array/);
    expect(() => parseWorkerTokens('[{"workerId":"w1"}]')).toThrow(/token/);
  });

  it("rejects malformed requests", async () => {
    const handle = await serve();
    const bad = await fetch(`${baseUrl(handle)}/gethen/run`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(bad.status).toBe(400);
    const missing = await fetch(`${baseUrl(handle)}/nope`);
    expect(missing.status).toBe(404);
  });
  it("returns 409 for task routes when no mailbox transport is configured", async () => {
    const handle = await serve();
    const base = baseUrl(handle);

    const list = await fetch(`${base}/gethen/tasks`);
    expect(list.status).toBe(409);
    expect(await list.json()).toMatchObject({ code: "RUNTIME_COMMS_UNAVAILABLE" });

    const status = await fetch(`${base}/gethen/tasks/assignment-1`);
    expect(status.status).toBe(409);

    const steer = await fetch(`${base}/gethen/tasks/assignment-1/steer`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: "go" }),
    });
    expect(steer.status).toBe(409);
  });

  it("serves the task list and asks the worker for status", async () => {
    const token = "task-token";
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token }],
      mailboxPollIntervalMs: 10,
      mailboxVisibilityTimeoutMs: 2_000,
    });
    const link = await attachRawWorker(handle, token);

    expect(await getJson(handle, "/gethen/tasks")).toEqual({ tasks: {} });

    const statusResponse = fetch(`${baseUrl(handle)}/gethen/tasks/assignment-1`);
    const request = await receiveEnvelope(link);
    expect(request.envelope).toMatchObject({
      type: "gethen.task.status",
      subject: "assignment-1",
    });
    await link.ack([request.envelope.id]);
    await link.send(
      workerEnvelope({
        type: "gethen.task.status.result",
        data: { assignmentId: "assignment-1", state: TASK_STATE },
        correlationId: request.envelope.id,
        ...(request.envelope.replyTo === undefined ? {} : { to: request.envelope.replyTo }),
      }),
    );
    const status = await statusResponse;
    expect(status.status).toBe(200);
    expect(await status.json()).toMatchObject({
      assignmentId: "assignment-1",
      state: { phase: "thinking", step: 1 },
    });

    const unknownResponse = fetch(`${baseUrl(handle)}/gethen/tasks/missing`);
    const unknownRequest = await receiveEnvelope(link);
    await link.ack([unknownRequest.envelope.id]);
    await link.send(
      workerEnvelope({
        type: "gethen.task.status.result",
        data: { assignmentId: "missing", state: null },
        correlationId: unknownRequest.envelope.id,
        ...(unknownRequest.envelope.replyTo === undefined
          ? {}
          : { to: unknownRequest.envelope.replyTo }),
      }),
    );
    expect((await unknownResponse).status).toBe(404);
  });

  it("maps an unanswered status to 504 and sends steer/cancel", async () => {
    const token = "control-token";
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token }],
      mailboxPollIntervalMs: 10,
      mailboxVisibilityTimeoutMs: 2_000,
    });
    const link = await attachRawWorker(handle, token);

    const timeoutResponse = await fetch(`${baseUrl(handle)}/gethen/tasks/slow?timeoutMs=100`);
    expect(timeoutResponse.status).toBe(504);
    expect(await timeoutResponse.json()).toMatchObject({ code: "MAILBOX_STATUS_TIMEOUT" });
    const stale = await receiveEnvelope(link);
    await link.ack([stale.envelope.id]);

    const steerResponse = await fetch(`${baseUrl(handle)}/gethen/tasks/assignment-1/steer`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: "focus on tests" }),
    });
    expect(steerResponse.status).toBe(202);
    const steerMessage = await receiveEnvelope(link);
    expect(steerMessage.envelope).toMatchObject({
      type: "gethen.task.steer",
      data: { assignmentId: "assignment-1", text: "focus on tests" },
    });
    await link.ack([steerMessage.envelope.id]);

    const emptySteer = await fetch(`${baseUrl(handle)}/gethen/tasks/assignment-1/steer`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(emptySteer.status).toBe(400);

    const cancelResponse = await fetch(`${baseUrl(handle)}/gethen/tasks/assignment-1/cancel`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ reason: "stop" }),
    });
    expect(cancelResponse.status).toBe(202);
    const cancelMessage = await receiveEnvelope(link);
    expect(cancelMessage.envelope).toMatchObject({
      type: "gethen.task.cancel",
      data: { reason: "stop" },
    });
    await link.ack([cancelMessage.envelope.id]);
  });
});

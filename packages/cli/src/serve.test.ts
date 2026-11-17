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
  it("serves task event pages and validates the query", async () => {
    const fake = await serve();
    expect((await fetch(`${baseUrl(fake)}/gethen/tasks/a1/events`)).status).toBe(409);

    const token = "events-token";
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token }],
      mailboxPollIntervalMs: 10,
      mailboxVisibilityTimeoutMs: 2_000,
    });

    const empty = await fetch(`${baseUrl(handle)}/gethen/tasks/assignment-1/events`);
    expect(empty.status).toBe(200);
    expect(await empty.json()).toEqual({
      assignmentId: "assignment-1",
      events: [],
      count: 0,
      dropped: 0,
    });

    expect((await fetch(`${baseUrl(handle)}/gethen/tasks/a1/events?limit=0`)).status).toBe(400);
    expect((await fetch(`${baseUrl(handle)}/gethen/tasks/a1/events?afterSeq=-1`)).status).toBe(400);
  });

  it("serves the session directory and session history", async () => {
    const token = "sessions-token";
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token }],
      mailboxPollIntervalMs: 10,
      mailboxVisibilityTimeoutMs: 2_000,
    });
    const link = await attachRawWorker(handle, token);

    const listResponse = fetch(`${baseUrl(handle)}/gethen/sessions?timeoutMs=3000`);
    const listRequest = await receiveEnvelope(link);
    expect(listRequest.envelope).toMatchObject({ type: "gethen.session.list" });
    await link.ack([listRequest.envelope.id]);
    await link.send(
      workerEnvelope({
        type: "gethen.session.list.result",
        data: { sessions: [{ sessionId: "session-1", state: "idle", key: "k1" }] },
        correlationId: listRequest.envelope.id,
        ...(listRequest.envelope.replyTo === undefined ? {} : { to: listRequest.envelope.replyTo }),
      }),
    );
    const list = await listResponse;
    expect(list.status).toBe(200);
    expect(await list.json()).toEqual({
      sessions: [{ sessionId: "session-1", state: "idle", key: "k1" }],
    });

    const historyResponse = fetch(
      `${baseUrl(handle)}/gethen/sessions/session-1/history?limit=5&afterSeq=0`,
    );
    const historyRequest = await receiveEnvelope(link);
    expect(historyRequest.envelope).toMatchObject({
      type: "gethen.session.history",
      data: { sessionId: "session-1", limit: 5, afterSeq: 0 },
    });
    await link.ack([historyRequest.envelope.id]);
    await link.send(
      workerEnvelope({
        type: "gethen.session.history.result",
        data: {
          sessionId: "session-1",
          at: TASK_STATE.lastEventAt,
          events: [{ seq: 0, type: "message", at: TASK_STATE.lastEventAt }],
        },
        correlationId: historyRequest.envelope.id,
        ...(historyRequest.envelope.replyTo === undefined
          ? {}
          : { to: historyRequest.envelope.replyTo }),
      }),
    );
    const history = await historyResponse;
    expect(history.status).toBe(200);
    expect(await history.json()).toMatchObject({
      sessionId: "session-1",
      events: [{ seq: 0, type: "message" }],
    });
  });

  it("validates run session bindings and times out unanswered session lists", async () => {
    const fake = await serve();
    const post = (body: unknown) =>
      fetch(`${baseUrl(fake)}/gethen/run`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });

    expect((await post({ text: "fresh", session: { mode: "new" } })).status).toBe(200);
    expect((await post({ text: "bad", session: { mode: "reuse" } })).status).toBe(400);
    expect((await post({ text: "bad", session: { mode: "new", sessionId: "s1" } })).status).toBe(
      400,
    );
    expect((await post({ text: "bad", session: { mode: "fork" } })).status).toBe(400);
    expect((await post({ text: "bad", session: { mode: "reuse", key: "k1" } })).status).toBe(200);

    const token = "slow-session-token";
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token }],
      mailboxPollIntervalMs: 10,
      mailboxVisibilityTimeoutMs: 2_000,
    });
    const link = await attachRawWorker(handle, token);
    const slow = await fetch(`${baseUrl(handle)}/gethen/sessions?timeoutMs=100`);
    expect(slow.status).toBe(504);
    expect(await slow.json()).toMatchObject({ code: "MAILBOX_STATUS_TIMEOUT" });
    const stale = await receiveEnvelope(link);
    await link.ack([stale.envelope.id]);
  });
});

describe("task escalation answers", () => {
  it("answers a parked assignment over the task route and resumes the ticket", async () => {
    const token = "answer-route-token";
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token }],
      mailboxPollIntervalMs: 10,
      mailboxVisibilityTimeoutMs: 2_000,
    });
    const link = await attachRawWorker(handle, token);

    const runResponse = postJson(handle, "/gethen/run", { text: "fix the failing test" });
    const assign = await receiveEnvelope(link);
    const assignData = assign.envelope.data as { assignmentId?: string };
    const assignmentId = assignData.assignmentId;
    if (assignmentId === undefined) throw new Error("assign envelope has no assignmentId");
    await link.ack([assign.envelope.id]);
    await link.send(
      workerEnvelope({
        type: "gethen.task.ask",
        subject: assignmentId,
        correlationId: assign.envelope.id,
        data: {
          assignmentId,
          escalationId: "esc-1",
          kind: "decision",
          question: "Which branch?",
          options: [{ id: "main", label: "main" }],
          defaultOption: "main",
        },
        ...(assign.envelope.replyTo === undefined ? {} : { to: assign.envelope.replyTo }),
      }),
    );
    const run = await runResponse;
    const ticketId = String(run.ticket);

    const answered = await postJson(handle, `/gethen/tasks/${assignmentId}/answer`, {
      kind: "answer",
      text: "use main",
      option: "main",
    });
    expect(answered).toMatchObject({
      action: "answer",
      escalation: { id: "esc-1", status: "resolved" },
    });

    const answer = await receiveEnvelope(link);
    expect(answer.envelope).toMatchObject({
      type: "gethen.task.answer",
      data: {
        assignmentId,
        escalationId: "esc-1",
        resolution: { kind: "answer", text: "use main", option: "main" },
      },
    });
    await link.ack([answer.envelope.id]);

    expect(handle.service.store.tickets.get(ticketId)?.attention).toBeUndefined();
    expect(handle.service.store.assignments.get(assignmentId)?.state).toBe("running");
    expect((await getJson(handle, "/gethen/tickets")).tickets).toEqual([]);
  });
});

describe("ticket lifecycle routes", () => {
  it("defaults overview to needs-action and serves ack/close/archive", async () => {
    const handle = await serve();
    const run = await postJson(handle, "/gethen/run", { text: "fix the failing test" });
    const ticketId = String(run.ticket);

    const overview = await getJson(handle, "/gethen/overview");
    expect(overview.filter).toBe("needs-action");
    expect(overview.counts).toMatchObject({ tickets: 1, open: 1, needsAction: 1, archived: 0 });
    expect((overview.tickets as Array<{ id: string }>).map((ticket) => ticket.id)).toEqual([
      ticketId,
    ]);

    const ack = await postJson(handle, `/gethen/tickets/${ticketId}/ack`, {});
    expect(ack).toMatchObject({ ok: true, action: "ack" });
    expect((await getJson(handle, "/gethen/tickets")).tickets).toEqual([]);

    const all = await getJson(handle, "/gethen/tickets?filter=all");
    expect((all.tickets as Array<{ needsAction: boolean }>)[0]?.needsAction).toBe(false);

    const closed = await postJson(handle, `/gethen/tickets/${ticketId}/close`, {
      outcome: "completed",
      reason: "reviewed",
    });
    expect(closed.ticket).toMatchObject({ lifecycle: "closed", outcome: "completed" });

    const archived = await postJson(handle, `/gethen/tickets/${ticketId}/archive`, {});
    expect(archived.ticket).toMatchObject({ lifecycle: "archived" });
    expect((await getJson(handle, "/gethen/tickets?filter=all")).counts).toMatchObject({
      archived: 1,
      needsAction: 0,
    });
  });

  it("answers a report-only escalation and maps a second answer to 409", async () => {
    const handle = await serve();
    const run = await postJson(handle, "/gethen/run", { text: "fix the failing test" });
    const ticketId = String(run.ticket);
    handle.service.engine.report(ticketId, {
      kind: "waiting",
      summary: "a person must choose",
      question: "Which branch?",
      at: new Date().toISOString(),
    });

    const answered = await postJson(handle, `/gethen/tickets/${ticketId}/answer`, {
      text: "use main",
      option: "main",
    });
    expect(answered).toMatchObject({
      ok: true,
      action: "answer",
      delivered: false,
      escalation: {
        status: "resolved",
        resolution: { kind: "answer", text: "use main", option: "main" },
      },
      ticket: { id: ticketId, needsAction: false },
    });

    const again = await fetch(`${baseUrl(handle)}/gethen/tickets/${ticketId}/answer`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: "again" }),
    });
    expect(again.status).toBe(409);

    const empty = await fetch(`${baseUrl(handle)}/gethen/tickets/${ticketId}/answer`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: " " }),
    });
    expect(empty.status).toBe(400);
  });

  it("maps lifecycle conflicts to 409 and unknown tickets to 404", async () => {
    const handle = await serve();
    const run = await postJson(handle, "/gethen/run", { text: "fix the failing test" });
    const ticketId = String(run.ticket);

    const open = await fetch(`${baseUrl(handle)}/gethen/tickets/${ticketId}/archive`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });
    expect(open.status).toBe(409);
    const openBody = (await open.json()) as { error?: string };
    expect(openBody.error).toContain("close it before archiving");

    const missing = await fetch(`${baseUrl(handle)}/gethen/tickets/missing/close`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    });
    expect(missing.status).toBe(404);

    const invalid = await fetch(`${baseUrl(handle)}/gethen/tickets/${ticketId}/close`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ outcome: "stopped" }),
    });
    expect(invalid.status).toBe(400);
  });
});

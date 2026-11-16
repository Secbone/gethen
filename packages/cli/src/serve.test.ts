import { mkdtempSync, rmSync } from "node:fs";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { EscalationResolutionInput } from "@gethen/master";
import { createWorkerAgent, FakeExecutor, type WorkerAgent } from "@gethen/worker-agent";
import { afterEach, describe, expect, it } from "vitest";
import {
  parseWorkerTokens,
  type ServeConfig,
  type ServeHandle,
  startServe,
} from "./commands/serve";

const handles: ServeHandle[] = [];
const agents: WorkerAgent[] = [];
const dirs: string[] = [];

afterEach(async () => {
  for (const agent of agents.splice(0)) await agent.close();
  while (handles.length > 0) await handles.pop()?.close();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

async function serve(config: Partial<ServeConfig> = {}) {
  const dir = mkdtempSync(join(tmpdir(), "gethen-serve-"));
  dirs.push(dir);
  const handle = await startServe({ dbPath: join(dir, "master.sqlite"), port: 0, ...config });
  handles.push(handle);
  return handle;
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

/** Poll `/health` until the gateway has registered `expected` workers. */
async function waitForWorkers(handle: ServeHandle, expected: number): Promise<void> {
  const deadline = Date.now() + 5_000;
  for (;;) {
    const health = await getJson(handle, "/health");
    if (health.workers === expected) return;
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${expected} worker(s): ${JSON.stringify(health)}`);
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
    expect(health).toMatchObject({ ok: true, tickets: 0, workers: 0, launch: "fake" });

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

  it("runs an assignment through a real worker agent over ws", async () => {
    const token = "test-worker-token";
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token }],
      workspace: "/tmp/gethen-test-workspace",
      resolveEscalation: async (): Promise<EscalationResolutionInput> => ({
        kind: "answer",
        text: "test auto-answer",
        option: "yes",
      }),
    });
    expect(handle.launch).toBe("ws");

    const agent = createWorkerAgent({
      masterUrl: `ws://127.0.0.1:${handle.port}/gethen/worker`,
      token,
      workerId: "worker-1",
      machine: "test-machine",
      platform: "linux",
      runtimes: ["dsh"],
      workspaces: ["/tmp/gethen-test-workspace"],
      models: ["fake-model"],
      capabilities: {},
      quota: 1,
      executor: new FakeExecutor(),
      reconnect: { initialDelayMs: 10, maxDelayMs: 50, jitter: 0 },
    });
    agents.push(agent);
    await agent.whenConnected();
    await waitForWorkers(handle, 1);

    const run = await postJson(handle, "/gethen/run", { text: "ws smoke" });
    expect(run).toMatchObject({
      stored: "stored",
      outcome: "accept",
      report: "ready",
      receipt: true,
    });

    const health = await getJson(handle, "/health");
    expect(health).toMatchObject({ workers: 1, launch: "ws", tickets: 1, receipts: 1 });
  }, 15_000);

  it("rejects upgrades on other paths and without a bearer token", async () => {
    const handle = await serve({
      tokens: [{ workerId: "worker-1", token: "secret" }],
    });

    expect(await upgradeStatus(handle.port, "/nope")).toBe("HTTP/1.1 404 Not Found");
    expect(await upgradeStatus(handle.port, "/gethen/worker")).toBe("HTTP/1.1 401 Unauthorized");
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
});

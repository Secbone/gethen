#!/usr/bin/env node
// Deterministic R1-D/R3-E verification: real WS transport, fake executor.
// No network, no model, no real DSH: starts the CLI master (`startServe`) with
// in-memory worker tokens, connects a real worker-agent with the scripted
// FakeExecutor, triggers one run and asserts report/receipt/health.
//
// Usage: node scripts/verify-ws.mjs

import { mkdtempSync, rmSync } from "node:fs";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";
import { register } from "tsx/esm/api";

// Run the TypeScript sources directly; the repo has no build step for the CLI.
register();

const { startServe } = await import("../packages/cli/src/commands/serve.ts");
const { createWorkerAgent, FakeExecutor } = await import("../packages/worker-agent/src/index.ts");

const dir = mkdtempSync(join(tmpdir(), "gethen-verify-ws-"));
const token = "verify-ws-token";
const workerId = "verify-worker";
const WORKER_PATH = "/gethen/worker";

let handle;
let worker;

function check(condition, message) {
  if (!condition) throw new Error(message);
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, timeoutMs, message) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error(message);
    await delay(10);
  }
}

async function json(path, init) {
  const response = await fetch(`http://127.0.0.1:${handle.port}${path}`, init);
  const body = await response.json();
  return { status: response.status, body };
}

try {
  handle = await startServe({
    dbPath: join(dir, "master.sqlite"),
    port: 0,
    tokens: [{ workerId, token }],
    workspace: dir,
    // A headless verification answers escalations deterministically; the real
    // CLI serve without a resolver parks them for a person instead.
    resolveEscalation: async () => ({ kind: "answer", text: "verify-ws auto-answer" }),
  });

  worker = createWorkerAgent({
    masterUrl: `ws://127.0.0.1:${handle.port}${WORKER_PATH}`,
    token,
    workerId,
    machine: hostname(),
    platform: process.platform,
    runtimes: ["dsh"],
    workspaces: [dir],
    models: ["verify-model"],
    capabilities: {},
    quota: 1,
    executor: new FakeExecutor(),
    reconnect: { initialDelayMs: 100, maxDelayMs: 500, jitter: 0 },
    logger: { error: (message) => console.error(`verify-ws: worker: ${message}`) },
  });

  await worker.whenConnected();
  await waitFor(
    async () => (await json("/health")).body.workers === 1,
    5_000,
    "worker hello was not registered by the gateway",
  );
  console.log("verify-ws: worker connected over ws");

  const run = await json("/gethen/run", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ text: "verify ws end-to-end" }),
  });
  check(run.status === 200, `POST /gethen/run failed with HTTP ${run.status}`);
  check(run.body.report === "ready", `expected report "ready", got ${JSON.stringify(run.body)}`);
  check(run.body.outcome === "accept", `expected outcome "accept", got ${run.body.outcome}`);
  check(run.body.receipt === true, "expected a receipt for the assignment");

  const health = (await json("/health")).body;
  check(health.workers === 1, `expected workers=1, got ${health.workers}`);
  check(health.launch === "ws", `expected launch="ws", got ${health.launch}`);
  check(health.tickets === 1, `expected tickets=1, got ${health.tickets}`);
  check(health.receipts === 1, `expected receipts=1, got ${health.receipts}`);

  const tickets = handle.service.store.tickets.list();
  check(tickets.length === 1, `expected one ticket, got ${tickets.length}`);
  check(tickets[0]?.latestReport === "ready", "ticket did not record the ready report");
  check(
    handle.service.store.listReceipts().length === 1,
    "master store did not persist the assignment receipt",
  );

  console.log("verify-ws: report ready, receipt persisted, health workers=1 launch=ws");
  console.log("verify-ws: OK");
} catch (error) {
  console.error(`verify-ws: FAILED — ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
} finally {
  if (worker !== undefined) await worker.close().catch(() => undefined);
  if (handle !== undefined) await handle.close().catch(() => undefined);
  rmSync(dir, { recursive: true, force: true });
}

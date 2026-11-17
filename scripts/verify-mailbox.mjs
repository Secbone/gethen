#!/usr/bin/env node
// Deterministic C3 mailbox verification: real L1 ws transport + L2 mailbox
// stack between `startServe` (mailbox transport) and a worker-side mailbox
// consumer, with an inline FakeHarness so no DSH runtime or model is needed.
//
// Usage: node scripts/verify-mailbox.mjs
//
// Covers: mailbox attach/auth, gethen.task.assign -> harness -> result, ready
// report plus receipt persisted on the master, /health mailbox counters,
// and the mailboxes draining afterwards. Exits non-zero on any failure.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { register } from "tsx/esm/api";

// Run the TypeScript sources directly; the repo has no build step for the CLI.
register();

const { startServe } = await import("../packages/cli/src/commands/serve.ts");
const { createWsTransport } = await import("../packages/transport/src/index.ts");
const { createMailboxClientLink } = await import("../packages/mailbox-link/src/index.ts");
const { createMailboxAgent } = await import("../packages/agent/src/index.ts");

const dir = mkdtempSync(join(tmpdir(), "gethen-verify-mailbox-"));
const workerId = "verify-worker";
const token = "verify-mailbox-token";

let handle;
let session;
let link;
let agent;
let stopping = false;

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

/**
 * The harness port with no DSH behind it. Like a real harness it returns the
 * execution handle first and only then streams observations: one event, one
 * ready report (or a failed report when cancelled).
 */
const fakeHarness = {
  async start(task, sink) {
    let settled = false;
    // Observations arrive after start() resolved, like a real harness. The
    // event and the report are separated so the consumer flushes the progress
    // envelope before the terminal one (its outbox is single-flight per item).
    setTimeout(() => {
      if (settled) return;
      sink.event({
        type: "message",
        at: new Date().toISOString(),
        data: { text: `fake harness ran ${task.assignmentId}` },
      });
    }, 10);
    setTimeout(() => {
      if (settled) return;
      settled = true;
      sink.report({ kind: "ready", summary: `fake harness finished ${task.assignmentId}` });
    }, 50);
    return {
      sendResolution: async () => undefined,
      cancel: async (reason) => {
        if (settled) return;
        settled = true;
        sink.report({ kind: "failed", summary: reason ?? "cancelled" });
      },
    };
  },
  async close() {
    // nothing pooled
  },
};

try {
  handle = await startServe({
    dbPath: join(dir, "master.sqlite"),
    port: 0,
    tokens: [{ workerId, token }],
    workerId,
    mailboxDbPath: join(dir, "mailbox.sqlite"),
    mailboxPollIntervalMs: 10,
    mailboxVisibilityTimeoutMs: 2_000,
    workspace: dir,
  });

  session = await createWsTransport({ heartbeat: true }).connect(
    { url: `ws://127.0.0.1:${handle.port}/gethen/worker` },
    { kind: "bearer", token },
  );
  link = await createMailboxClientLink({ session, mailbox: `mailbox:${workerId}` });
  agent = createMailboxAgent({
    workerId,
    mailbox: link,
    harness: fakeHarness,
    quota: 1,
    pollIntervalMs: 10,
    onError: (error, context) => {
      // A pending receive is rejected when the link closes during shutdown.
      if (stopping) return;
      console.error(`verify-mailbox: agent ${context.phase} error: ${String(error)}`);
    },
  });
  agent.start();
  console.log("verify-mailbox: worker attached to the mailbox transport");

  const base = `http://127.0.0.1:${handle.port}`;
  const health = async () => (await fetch(`${base}/health`)).json();
  await waitFor(
    async () => {
      const current = await health();
      return current.workers === 1;
    },
    5_000,
    "worker did not attach to the mailbox transport",
  );

  const response = await fetch(`${base}/gethen/run`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ text: "verify mailbox end-to-end" }),
  });
  const run = await response.json();
  check(response.ok, `POST /gethen/run failed with HTTP ${response.status}`);
  check(run.report === "ready", `expected report "ready", got ${JSON.stringify(run)}`);
  check(run.outcome === "accept", `expected outcome "accept", got ${run.outcome}`);
  check(run.receipt === true, "expected a receipt for the assignment");
  console.log("verify-mailbox: run produced a ready report and a receipt");

  await waitFor(
    async () => {
      const counters = (await health()).mailbox;
      return counters !== undefined && counters.pending === 0 && counters.leased === 0;
    },
    5_000,
    "mailboxes did not drain after the report",
  );

  const finalHealth = await health();
  check(finalHealth.workers === 1, `expected workers=1, got ${finalHealth.workers}`);
  check(finalHealth.launch === "ws", `expected launch="ws", got ${finalHealth.launch}`);
  check(finalHealth.tickets === 1, `expected tickets=1, got ${finalHealth.tickets}`);
  check(finalHealth.receipts === 1, `expected receipts=1, got ${finalHealth.receipts}`);
  check(
    finalHealth.mailbox !== undefined &&
      finalHealth.mailbox.pending === 0 &&
      finalHealth.mailbox.leased === 0 &&
      finalHealth.mailbox.dead === 0,
    `expected an empty mailbox, got ${JSON.stringify(finalHealth.mailbox)}`,
  );

  const tickets = handle.service.store.tickets.list();
  check(tickets.length === 1, `expected one ticket, got ${tickets.length}`);
  check(tickets[0]?.latestReport === "ready", "ticket did not record the ready report");
  check(
    handle.service.store.listReceipts().length === 1,
    "master store did not persist the assignment receipt",
  );

  console.log("verify-mailbox: report ready, receipt persisted, mailboxes empty, health OK");
  console.log("verify-mailbox: OK");
} catch (error) {
  console.error(
    `verify-mailbox: FAILED — ${error instanceof Error ? error.message : String(error)}`,
  );
  process.exitCode = 1;
} finally {
  stopping = true;
  if (agent !== undefined) await agent.stop().catch(() => undefined);
  if (link !== undefined) link.close();
  if (session !== undefined) await session.close("verify_mailbox_done").catch(() => undefined);
  if (handle !== undefined) await handle.close().catch(() => undefined);
  rmSync(dir, { recursive: true, force: true });
}

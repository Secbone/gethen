#!/usr/bin/env node
// R5-B opt-in e2e: master (startServe, in-memory worker token) -> worker-agent
// (real WS) -> createSdkExecutor -> real `dsh --profile sdk --patch <overlay>`.
//
// This spawns a real DSH SDK runtime and costs model tokens, so it only runs
// with GETHEN_E2E=1:
//
//   GETHEN_E2E=1 node scripts/e2e-sdk.mjs
//
// The `sdk` profile is never edited: the run uses the overlay at
// scripts/e2e/sdk-profile.patch.yml (`dsh --profile sdk --patch <file>`), which
// routes the runtime at pi-ai's `opencode-go` provider and mounts the Gethen
// worker tool entry. Credentials resolve through the SDK runtime's normal
// credential seam (launch environment, `$DSH_HOME/.env`, ...).
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { hostname, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const E2E_DIR = process.env.GETHEN_E2E_DIR ?? "/tmp/gethen-e2e";
const OK_FILE = join(E2E_DIR, "ok.txt");
const PATCH_FILE = resolve(HERE, "e2e", "sdk-profile.patch.yml");
const WORKER_ENTRY = "file:///home/secbone/.local/share/gethen/dsh/worker.js";
const DSH_COMMAND = process.env.GETHEN_DSH ?? "dsh";
const PROVIDER = process.env.GETHEN_E2E_PROVIDER ?? "opencode-go";
const MODEL = process.env.GETHEN_E2E_MODEL ?? "deepseek-v4.1-flash";
const INSTRUCTION = "在 /tmp/gethen-e2e/ 下创建 ok.txt 写入 ok，然后报告 ready";
const MAX_RUN_MS = Number(process.env.GETHEN_E2E_TIMEOUT_MS ?? 15 * 60_000);
const MAX_EVENTS = 200;

if (process.env.GETHEN_E2E !== "1") {
  process.stdout.write(
    "e2e-sdk: skipped (set GETHEN_E2E=1 to run the real DSH SDK end-to-end check)\n",
  );
  process.exit(0);
}

/** A bounded, human-readable trace of the worker session for failure reports. */
const events = [];

function collectStrings(value, out = []) {
  if (typeof value === "string") out.push(value);
  else if (Array.isArray(value)) for (const item of value) collectStrings(item, out);
  else if (value !== null && typeof value === "object") {
    for (const item of Object.values(value)) collectStrings(item, out);
  }
  return out;
}

function summarize(notification) {
  const event = notification?.event ?? {};
  const text = collectStrings(event).join(" ").replace(/\s+/g, " ").trim();
  const line = `${String(event.type ?? "event")}: ${text.slice(0, 400)}`;
  events.push(line);
  if (events.length > MAX_EVENTS) events.shift();
  return line;
}

function printEvents(limit = 20) {
  const recent = events.slice(-limit);
  process.stdout.write(`e2e-sdk: last ${recent.length} worker session events\n`);
  for (const line of recent) process.stdout.write(`  ${line}\n`);
}

function fail(message) {
  process.stderr.write(`e2e-sdk: FAILED — ${message}\n`);
  if (events.length > 0) printEvents();
  process.exitCode = 1;
}

function check(condition, message) {
  if (!condition) throw new Error(message);
}

function delay(ms) {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
}

async function waitFor(predicate, timeoutMs, message) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error(message);
    await delay(100);
  }
}

async function json(port, path, init) {
  const response = await fetch(`http://127.0.0.1:${port}${path}`, init);
  const body = await response.json();
  return { status: response.status, body };
}

function preflight() {
  if (!existsSync(PATCH_FILE)) {
    throw new Error(
      `missing profile overlay ${PATCH_FILE}; the sdk profile cannot be patched without it`,
    );
  }
  if (!existsSync(new URL(WORKER_ENTRY))) {
    throw new Error(
      `missing worker entry ${WORKER_ENTRY}; run \`pnpm build:dsh\` to build the Gethen DSH entries`,
    );
  }
  const version = spawnSync(DSH_COMMAND, ["--version"], { encoding: "utf8" });
  if (version.status !== 0) {
    throw new Error(
      `cannot run ${DSH_COMMAND} (${version.error?.message ?? `exit ${version.status}`}); ` +
        "set GETHEN_DSH to the dsh binary",
    );
  }
  const config = spawnSync(
    DSH_COMMAND,
    ["--profile", "sdk", "--patch", PATCH_FILE, "--dump-config"],
    { encoding: "utf8" },
  );
  if (config.status !== 0) {
    throw new Error(
      `\`dsh --profile sdk --patch ${PATCH_FILE} --dump-config\` failed:\n` +
        `${(config.stderr || config.stdout || "").trim().slice(-2000)}`,
    );
  }
  process.stdout.write(
    `e2e-sdk: using ${DSH_COMMAND} --profile sdk --patch ${PATCH_FILE} ` +
      `(provider=${PROVIDER}, model=${MODEL})\n`,
  );
  process.stdout.write(
    "e2e-sdk: credentials resolve inside dsh; a missing OPENCODE_API_KEY fails initialize/request\n",
  );
}

async function main() {
  preflight();
  const { register } = await import("tsx/esm/api");
  register();
  const { startServe } = await import("../packages/cli/src/commands/serve.ts");
  const { createWorkerAgent } = await import("../packages/worker-agent/src/index.ts");
  const { createSdkExecutor } = await import("../packages/executor-sdk/src/index.ts");
  const { DshSdkClient } = await import("../packages/sdk-driver/src/index.ts");

  class E2eClient extends DshSdkClient {
    #prompted = false;
    constructor() {
      super();
      this.onStderr((text) => process.stderr.write(`e2e-sdk: dsh stderr: ${text}`));
    }
    start(options) {
      process.stderr.write(`e2e-sdk: starting ${DSH_COMMAND} in ${String(options.cwd)}\n`);
      return super.start(options);
    }
    async initialize(options) {
      try {
        const result = await super.initialize(options);
        process.stderr.write(
          `e2e-sdk: initialized ${options.provider}/${options.model} (${result.serverInfo.name})\n`,
        );
        return result;
      } catch (error) {
        process.stderr.write(`e2e-sdk: initialize failed: ${String(error?.message ?? error)}\n`);
        throw error;
      }
    }
    async prompt(sessionId, text) {
      try {
        const result = await super.prompt(sessionId, text);
        process.stderr.write(`e2e-sdk: prompted ${sessionId} (${text.length} chars)\n`);
        return result;
      } catch (error) {
        process.stderr.write(`e2e-sdk: prompt failed: ${String(error?.message ?? error)}\n`);
        throw error;
      }
    }

    onEvent(listener) {
      return super.onEvent((notification) => {
        const line = summarize(notification);
        if (process.env.GETHEN_E2E_TRACE === "1") process.stdout.write(`e2e-sdk: ${line}\n`);
        listener(notification);
      });
    }
  }

  const stateDir = mkdtempSync(join(tmpdir(), "gethen-e2e-sdk-"));
  const token = "e2e-sdk-token";
  const workerId = "e2e-sdk-worker";
  let handle;
  let worker;
  let executor;
  try {
    rmSync(E2E_DIR, { recursive: true, force: true });
    mkdirSync(E2E_DIR, { recursive: true });

    handle = await startServe({
      dbPath: join(stateDir, "master.sqlite"),
      port: 0,
      tokens: [{ workerId, token }],
      workspace: E2E_DIR,
      // Answer permission escalations (and any decision) automatically so the
      // check does not need a person; option ids come from the recorded options.
      resolveEscalation: async (record) => ({
        kind: "answer",
        ...(record.options[0] === undefined ? {} : { option: record.options[0].id }),
        text: "e2e auto-answer",
      }),
    });

    executor = createSdkExecutor({
      provider: PROVIDER,
      model: MODEL,
      dshCommand: DSH_COMMAND,
      dshArgs: ["--profile", "sdk", "--patch", PATCH_FILE],
      requestTimeoutMs: MAX_RUN_MS,
      clientFactory: () => new E2eClient(),
    });

    worker = createWorkerAgent({
      masterUrl: `ws://127.0.0.1:${handle.port}/gethen/worker`,
      token,
      workerId,
      machine: hostname(),
      platform: process.platform,
      runtimes: ["dsh"],
      workspaces: [E2E_DIR],
      models: [MODEL],
      capabilities: {},
      quota: 1,
      executor,
      reconnect: { initialDelayMs: 100, maxDelayMs: 500, jitter: 0 },
      logger: { error: (message) => process.stderr.write(`e2e-sdk: worker: ${message}\n`) },
    });

    await worker.whenConnected();
    await waitFor(
      async () => (await json(handle.port, "/health")).body.workers === 1,
      10_000,
      "worker hello was not registered by the gateway",
    );
    process.stdout.write("e2e-sdk: worker connected, dispatching assignment\n");

    const progress = setInterval(() => {
      const last = events.at(-1);
      process.stdout.write(`e2e-sdk: still running${last === undefined ? "" : ` — ${last}`}\n`);
    }, 20_000);
    progress.unref?.();

    const run = await json(handle.port, "/gethen/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: INSTRUCTION }),
      signal: AbortSignal.timeout(MAX_RUN_MS),
    });
    clearInterval(progress);

    check(run.status === 200, `POST /gethen/run failed with HTTP ${run.status}`);
    if (run.body.report !== "ready") {
      const receipts = handle.service.store.listReceipts();
      const escalations = handle.service.store.listEscalations();
      process.stderr.write(
        `e2e-sdk: receipts=${JSON.stringify(receipts.map((row) => ({ outcome: row.outcome, summary: row.summary })))}\n`,
      );
      process.stderr.write(
        `e2e-sdk: escalations=${JSON.stringify(escalations.map((row) => ({ kind: row.kind, status: row.status, question: row.question })))}\n`,
      );
    }
    check(
      run.body.report === "ready",
      `expected report "ready", got ${JSON.stringify(run.body)} (events above)`,
    );
    check(run.body.outcome === "accept", `expected outcome "accept", got ${run.body.outcome}`);
    check(run.body.receipt === true, "expected a persisted receipt for the assignment");

    const health = (await json(handle.port, "/health")).body;
    check(health.workers === 1, `expected workers=1, got ${health.workers}`);
    check(health.launch === "ws", `expected launch="ws", got ${health.launch}`);
    check(health.tickets === 1, `expected tickets=1, got ${health.tickets}`);
    check(health.receipts === 1, `expected receipts=1, got ${health.receipts}`);

    check(existsSync(OK_FILE), `worker did not create ${OK_FILE}`);
    const content = readFileSync(OK_FILE, "utf8").trim();
    check(content === "ok", `expected ${OK_FILE} to contain "ok", got ${JSON.stringify(content)}`);

    const tickets = handle.service.store.tickets.list();
    check(tickets[0]?.latestReport === "ready", "ticket did not record the ready report");

    process.stdout.write(
      `e2e-sdk: OK — report ready, receipt persisted, workers=1, ` +
        `${OK_FILE} contains ${JSON.stringify(content)}\n`,
    );
    printEvents(20);
  } finally {
    if (worker !== undefined) await worker.close().catch(() => undefined);
    if (executor !== undefined) await executor.close().catch(() => undefined);
    if (handle !== undefined) await handle.close().catch(() => undefined);
    rmSync(stateDir, { recursive: true, force: true });
  }
}

try {
  await main();
} catch (error) {
  fail(error instanceof Error ? (error.stack ?? error.message) : String(error));
}

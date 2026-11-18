import { existsSync, readFileSync } from "node:fs";
import { isIP } from "node:net";
import { homedir, hostname } from "node:os";
import { join } from "node:path";
import { agentMailboxName, createMailboxAgent, type MailboxAgent } from "@gethen/agent";
import { createSdkHarnessExecutor } from "@gethen/executor-sdk";
import { createMailboxClientLink, type MailboxClientLink } from "@gethen/mailbox-link";
import { createWsTransport, type TransportSession } from "@gethen/transport";
import {
  parseWorkerManifest,
  type WorkerCapabilities,
  type WorkerManifest,
} from "@gethen/worker-bridge";
import { WORKER_PATH } from "./serve";
import { workerTokenCommand } from "./worker-token";

export interface WorkerConfig {
  masterUrl: string;
  token: string;
  workerId: string;
  machine?: string;
  provider: string;
  model: string;
  workspaces: string[];
  quota?: number;
  dshCommand?: string;
  dshArgs?: string[];
  /** hostname → IP overrides for private DNS/split-horizon setups. */
  dnsOverrides?: Record<string, string>;
  /** Master-side workspace path → local path (cross-machine workers). */
  workspaceMap?: Record<string, string>;
  /** Per-JSON-RPC timeout for the DSH SDK session (default 10 minutes). */
  requestTimeoutMs?: number;
  /** Process model: shared workspace runtime (default) or one runtime per assignment. */
  isolation?: "shared" | "process";
  /** Hard cap for one execution in ms (0 disables, default 3600000). */
  maxExecutionMs?: number;
  /** Directory for durable worker state (session key registry). */
  stateDir?: string;
  /** Cost class advertised to the Master router (default "mid"). */
  costClass?: "low" | "mid" | "high";
  /** Declared DSH capability overrides; these are filter hints, not permissions. */
  capabilities?: Partial<WorkerCapabilities>;
}

export interface WorkerConfigOptions {
  /** Default workspace when none is configured; defaults to `process.cwd()`. */
  cwd?: string;
}

/** Conservative defaults matching the documented DSH SDK profile. */
export const DEFAULT_WORKER_CAPABILITIES: WorkerCapabilities = {
  promptInjection: "system",
  skills: "filesystem",
  mcp: false,
  permissions: "approval",
  streaming: true,
  steer: true,
  fork: false,
};

interface WorkerConfigFile {
  masterUrl?: string;
  token?: string;
  workerId?: string;
  machine?: string;
  provider?: string;
  model?: string;
  workspaces?: string[];
  quota?: number;
  dshCommand?: string;
  dshArgs?: string[];
  /** hostname → IP overrides for private DNS/split-horizon setups. */
  dnsOverrides?: Record<string, string>;
  /** Master-side workspace path → local path (cross-machine workers). */
  workspaceMap?: Record<string, string>;
  /** Per-JSON-RPC timeout for the DSH SDK session (default 10 minutes). */
  requestTimeoutMs?: number;
  /** Process model: shared workspace runtime (default) or one runtime per assignment. */
  isolation?: "shared" | "process";
  /** Hard cap for one execution in ms (0 disables, default 3600000). */
  maxExecutionMs?: number;
  /** Directory for durable worker state (session key registry). */
  stateDir?: string;
  /** Cost class advertised to the Master router (default "mid"). */
  costClass?: "low" | "mid" | "high";
  /** Declared DSH capability overrides; these are filter hints, not permissions. */
  capabilities?: Partial<WorkerCapabilities>;
}

/**
 * Build the worker daemon config from `--config <file>` plus simple flags.
 * Flags win over the file. Throws with the missing fields listed, so a bad
 * unit file fails at startup instead of connecting with half a config.
 */
export function parseWorkerConfig(argv: string[], options: WorkerConfigOptions = {}): WorkerConfig {
  const file = configFile(argv);

  const missing: string[] = [];
  const masterUrl = requireString(
    flagValue(argv, "--master-url") ?? file.masterUrl,
    "masterUrl (--master-url)",
    missing,
  );
  const token = requireString(flagValue(argv, "--token") ?? file.token, "token (--token)", missing);
  const workerId = requireString(
    flagValue(argv, "--worker-id") ?? file.workerId,
    "workerId (--worker-id)",
    missing,
  );
  const provider = requireString(
    flagValue(argv, "--provider") ?? file.provider,
    "provider (--provider)",
    missing,
  );
  const model = requireString(flagValue(argv, "--model") ?? file.model, "model (--model)", missing);
  if (missing.length > 0) {
    throw new Error(`worker config is missing: ${missing.join(", ")}`);
  }

  const flagWorkspaces = flagValues(argv, "--workspace");
  const workspaces = flagWorkspaces.length > 0 ? flagWorkspaces : (file.workspaces ?? []);
  const flagDshArgs = dshArgsFromFlags(argv);
  const dshArgs = flagDshArgs.length > 0 ? flagDshArgs : file.dshArgs;
  const quota = positiveInteger(flagValue(argv, "--quota") ?? file.quota, "quota");
  const machine = flagValue(argv, "--machine") ?? file.machine;
  const dnsOverrides = parseDnsOverrides(flagValue(argv, "--dns-overrides"), file.dnsOverrides);
  const workspaceMap = parseWorkspaceMap(flagValue(argv, "--workspace-map"), file.workspaceMap);
  const requestTimeoutMs = positiveInteger(
    flagValue(argv, "--request-timeout-ms") ?? file.requestTimeoutMs,
    "requestTimeoutMs",
  );
  const isolation = parseIsolation(flagValue(argv, "--isolation") ?? file.isolation);
  const stateDir = flagValue(argv, "--state-dir") ?? file.stateDir;

  const costClass = parseCostClass(flagValue(argv, "--cost-class") ?? file.costClass);
  const capabilities = parseCapabilities(file.capabilities);

  return {
    masterUrl: normalizeMasterUrl(masterUrl),
    token,
    workerId,
    provider,
    model,
    workspaces: workspaces.length > 0 ? workspaces : [options.cwd ?? process.cwd()],
    ...(machine === undefined ? {} : { machine }),
    ...(quota === undefined ? {} : { quota }),
    ...(file.dshCommand === undefined ? {} : { dshCommand: file.dshCommand }),
    ...(dshArgs === undefined ? {} : { dshArgs }),
    ...(dnsOverrides === undefined ? {} : { dnsOverrides }),
    ...(workspaceMap === undefined ? {} : { workspaceMap }),
    ...(requestTimeoutMs === undefined ? {} : { requestTimeoutMs }),
    ...(isolation === undefined ? {} : { isolation }),
    ...(stateDir === undefined ? {} : { stateDir }),
    ...(costClass === undefined ? {} : { costClass }),
    ...(capabilities === undefined ? {} : { capabilities }),
  };
}

/** Validate the advertised cost class; an unknown value fails at startup. */
function parseCostClass(value: string | undefined): "low" | "mid" | "high" | undefined {
  if (value === undefined) return undefined;
  if (value !== "low" && value !== "mid" && value !== "high") {
    throw new Error('worker config: costClass must be "low", "mid" or "high"');
  }
  return value;
}

/** Validate declared capability overrides (they are merged over the defaults). */
function parseCapabilities(
  value: Partial<WorkerCapabilities> | undefined,
): Partial<WorkerCapabilities> | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("worker config: capabilities must be an object");
  }
  const merged = { ...DEFAULT_WORKER_CAPABILITIES, ...value };
  if (!["system", "prefix", "none"].includes(merged.promptInjection)) {
    throw new Error('worker config: capabilities.promptInjection must be "system"|"prefix"|"none"');
  }
  if (!["filesystem", "mcp", "none"].includes(merged.skills)) {
    throw new Error('worker config: capabilities.skills must be "filesystem"|"mcp"|"none"');
  }
  if (!["sandbox", "approval", "none"].includes(merged.permissions)) {
    throw new Error('worker config: capabilities.permissions must be "sandbox"|"approval"|"none"');
  }
  for (const key of ["mcp", "streaming", "steer", "fork"] as const) {
    if (typeof merged[key] !== "boolean") {
      throw new Error(`worker config: capabilities.${key} must be a boolean`);
    }
  }
  return value;
}

/** Parse `{"host":"ip"}` from a flag or the config file; invalid values fail fast. */
function parseDnsOverrides(
  flag: string | undefined,
  file: Record<string, string> | undefined,
): Record<string, string> | undefined {
  const raw = flag === undefined ? file : (JSON.parse(flag) as Record<string, string>);
  if (raw === undefined) return undefined;
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Error('worker config: dnsOverrides must be a {"host":"ip"} object');
  }
  const entries = Object.entries(raw);
  for (const [host, address] of entries) {
    if (typeof host !== "string" || host.length === 0) {
      throw new Error("worker config: dnsOverrides has an empty hostname");
    }
    if (typeof address !== "string" || netIsIP(address) === 0) {
      throw new Error(`worker config: dnsOverrides["${host}"] must be an IP address`);
    }
  }
  return entries.length === 0 ? undefined : Object.fromEntries(entries);
}

/** Validate the executor isolation mode. */
function parseIsolation(value: string | undefined): "shared" | "process" | undefined {
  if (value === undefined) return undefined;
  if (value !== "shared" && value !== "process") {
    throw new Error('worker config: isolation must be "shared" or "process"');
  }
  return value;
}

/** Parse `{"masterPath":"localPath"}` from a flag or the config file. */
function parseWorkspaceMap(
  flag: string | undefined,
  file: Record<string, string> | undefined,
): Record<string, string> | undefined {
  const raw = flag === undefined ? file : (JSON.parse(flag) as Record<string, string>);
  if (raw === undefined) return undefined;
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    throw new Error('worker config: workspaceMap must be a {"masterPath":"localPath"} object');
  }
  const entries = Object.entries(raw);
  for (const [from, to] of entries) {
    if (
      typeof from !== "string" ||
      from.length === 0 ||
      typeof to !== "string" ||
      to.length === 0
    ) {
      throw new Error("worker config: workspaceMap keys and values must be non-empty strings");
    }
  }
  return entries.length === 0 ? undefined : Object.fromEntries(entries);
}

function netIsIP(value: string): number {
  return isIP(value);
}

/**
 * Accept both the documented full URL (`wss://master/gethen/worker`) and a
 * plain master origin (`https://master:8443`), which is expanded to the worker
 * endpoint. Any explicit path is kept as-is.
 */
export function normalizeMasterUrl(input: string): string {
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new Error(`worker config: masterUrl "${input}" is not a valid URL`);
  }
  if (!["ws:", "wss:", "http:", "https:"].includes(url.protocol)) {
    throw new Error(
      `worker config: masterUrl must be a ws(s) or http(s) URL, got "${url.protocol}"`,
    );
  }
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  if (url.pathname === "/" || url.pathname === "") {
    url.pathname = WORKER_PATH;
  }
  return url.toString();
}

/** `gethen worker --config <file>` (or simple flags): run the outbound worker daemon. */
export async function workerCommand(argv: string[]): Promise<number> {
  // `add|ls|revoke` manage worker tokens; without a subcommand, the flags below
  // start the daemon.
  if (argv[0] === "add" || argv[0] === "ls" || argv[0] === "revoke") {
    return workerTokenCommand(argv);
  }
  if (argv.includes("-h") || argv.includes("--help")) {
    process.stdout.write(WORKER_USAGE);
    return 0;
  }
  let config: WorkerConfig;
  try {
    config = parseWorkerConfig(argv);
  } catch (error) {
    process.stderr.write(`gethen worker: ${messageOf(error)}\n`);
    return 1;
  }
  return runMailboxWorker(config);
}

/** Master mailbox the worker replies to (matches the L2 mailbox default). */
const MASTER_MAILBOX = "mailbox:master";
const RECONNECT_INITIAL_MS = 500;
const RECONNECT_MAX_MS = 30_000;

/**
 * The registration reported in the mailbox `hello`: the Master forms its
 * selectable pool from these declarations (workspace paths, model, capability
 * hints, quota and health). The declarations are not permissions; the DSH
 * sandbox/approval policy on this machine stays authoritative.
 */
export function workerManifest(
  config: WorkerConfig,
  at = new Date().toISOString(),
): WorkerManifest {
  const workspaces = new Set<string>([
    ...Object.keys(config.workspaceMap ?? {}),
    ...config.workspaces,
  ]);
  const manifest = {
    id: config.workerId,
    machine: config.machine ?? hostname(),
    runtime: "dsh" as const,
    workspaces: [...workspaces].map((id) => ({ id })),
    capabilities: { ...DEFAULT_WORKER_CAPABILITIES, ...config.capabilities },
    models: [config.model],
    costClass: config.costClass ?? ("mid" as const),
    load: { running: 0, quota: config.quota ?? 1 },
    health: { state: "online" as const, lastHeartbeatAt: at },
  };
  const parsed = parseWorkerManifest(manifest);
  if (!parsed.ok) throw new Error(`worker registration is invalid: ${parsed.reason}`);
  return parsed.value;
}

/**
 * The worker daemon: dial the master's L1 ws transport, attach the L2 mailbox
 * link for `mailbox:<workerId>` and let the mailbox agent drive the DSH SDK
 * harness. Disconnects reconnect with exponential backoff and re-attach the
 * same link; SIGTERM/SIGINT drain the agent, then close link, session and
 * harness.
 */
async function runMailboxWorker(config: WorkerConfig): Promise<number> {
  const workerId = config.workerId;
  const mailbox = agentMailboxName(workerId);
  const transport = createWsTransport({ heartbeat: true });
  const endpoint = {
    url: config.masterUrl,
    ...(config.dnsOverrides === undefined ? {} : { resolve: config.dnsOverrides }),
  };
  const connect = (): Promise<TransportSession> =>
    transport.connect(endpoint, { kind: "bearer", token: config.token });

  let session: TransportSession;
  try {
    session = await connect();
  } catch (error) {
    process.stderr.write(
      `gethen worker: connect to ${config.masterUrl} failed: ${messageOf(error)}\n`,
    );
    return 1;
  }

  const link: MailboxClientLink = await createMailboxClientLink({
    session,
    mailbox,
    worker: workerManifest(config),
  });

  /** Map the master-side workspace to a local directory that exists. */
  function resolveWorkspace(workspace: string): string {
    const mapped = config.workspaceMap?.[workspace] ?? workspace;
    if (existsSync(mapped)) return mapped;
    const configured = config.workspaces.find((candidate) => existsSync(candidate));
    const fallback = configured ?? process.cwd();
    process.stderr.write(
      `worker: workspace "${workspace}" -> "${mapped}" does not exist; using "${fallback}"\n`,
    );
    return fallback;
  }

  const baseHarness = createSdkHarnessExecutor({
    provider: config.provider,
    model: config.model,
    requestTimeoutMs: config.requestTimeoutMs ?? 600_000,
    ...(config.isolation === undefined ? {} : { isolation: config.isolation }),
    ...(config.maxExecutionMs === undefined ? {} : { maxExecutionMs: config.maxExecutionMs }),
    cwdFor: resolveWorkspace,
    // Durable key -> sessionId registry: a restarted worker must still answer
    // session.mode "reuse" by key (it used to forget every key).
    sessionKeysFile: join(
      config.stateDir ?? join(homedir(), ".gethen", "worker"),
      `${workerId}.sessions.json`,
    ),
    ...(config.dshCommand === undefined ? {} : { dshCommand: config.dshCommand }),
    ...(config.dshArgs === undefined ? {} : { dshArgs: config.dshArgs }),
  });
  const harness = {
    async start(
      task: Parameters<typeof baseHarness.start>[0],
      sink: Parameters<typeof baseHarness.start>[1],
    ) {
      process.stderr.write(
        `worker: assign ${task.assignmentId} workspace=${task.workspace} cwd=${resolveWorkspace(task.workspace)}\n`,
      );
      let execution: Awaited<ReturnType<typeof baseHarness.start>>;
      try {
        execution = await baseHarness.start(task, {
          event: (event) => sink.event(event),
          escalation: (escalation) => {
            process.stderr.write(`worker: escalation ${escalation.kind}: ${escalation.question}\n`);
            sink.escalation(escalation);
          },
          report: (report) => {
            process.stderr.write(`worker: report ${report.kind}: ${report.summary}\n`);
            sink.report(report);
          },
        });
      } catch (error) {
        process.stderr.write(`worker: harness start failed: ${messageOf(error)}\n`);
        throw error;
      }
      return execution;
    },
    sessions: () => baseHarness.sessions(),
    sessionHistory: (
      sessionId: string,
      options?: Parameters<typeof baseHarness.sessionHistory>[1],
    ) => baseHarness.sessionHistory(sessionId, options),
    close: () => baseHarness.close(),
  };
  let lastReceiveErrorAt = 0;
  const agent: MailboxAgent = createMailboxAgent({
    workerId,
    mailbox: link,
    harness,
    quota: config.quota ?? 1,
    mailboxName: mailbox,
    masterMailbox: MASTER_MAILBOX,
    onError: (error, context) => {
      const text = messageOf(error);
      // A flapping transport emits the same "disconnected" error hundreds of
      // times while reconnecting; keep the log readable.
      if (context.phase === "receive" && text.includes("disconnected")) {
        const now = Date.now();
        if (now - lastReceiveErrorAt < 10_000) return;
        lastReceiveErrorAt = now;
      }
      process.stderr.write(`gethen worker: mailbox ${context.phase} error: ${text}\n`);
    },
  });
  agent.start();
  process.stdout.write(
    `worker ${workerId} connected to ${config.masterUrl} (mailbox: ${mailbox})\n`,
  );

  let currentSession = session;
  let stopping = false;
  let reconnecting = false;
  let reconnectPromise: Promise<void> | undefined;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let attempt = 0;

  function watch(target: TransportSession): void {
    target.onState((state) => {
      if (state !== "closed" || stopping || target !== currentSession) return;
      scheduleReconnect();
    });
  }

  function scheduleReconnect(): void {
    if (stopping || reconnecting || reconnectTimer !== undefined) return;
    const base = Math.min(RECONNECT_MAX_MS, RECONNECT_INITIAL_MS * 2 ** attempt);
    attempt += 1;
    const delay = Math.round(base * (0.8 + Math.random() * 0.4));
    process.stderr.write(`gethen worker: mailbox link closed; reconnecting in ${delay}ms\n`);
    const timer = setTimeout(() => {
      reconnectTimer = undefined;
      reconnectPromise = reconnectOnce().finally(() => {
        reconnectPromise = undefined;
      });
    }, delay);
    timer.unref();
    reconnectTimer = timer;
  }

  async function reconnectOnce(): Promise<void> {
    if (stopping || reconnecting) return;
    reconnecting = true;
    let failed = false;
    try {
      const next = await connect();
      if (stopping) {
        await next.close("worker_stopped").catch(() => undefined);
        return;
      }
      await link.attach(next);
      currentSession = next;
      watch(next);
      attempt = 0;
      process.stdout.write(`worker ${workerId} reconnected (mailbox: ${mailbox})\n`);
    } catch (error) {
      failed = true;
      process.stderr.write(`gethen worker: reconnect failed: ${messageOf(error)}\n`);
    } finally {
      reconnecting = false;
    }
    // Retry after the flag is cleared: scheduling from inside `catch` was a
    // no-op because `reconnecting` was still true, which stopped the loop
    // after a single transient failure (e.g. a 502 while the master restarts).
    if (failed && !stopping) scheduleReconnect();
  }

  watch(session);
  await waitForSignal();
  stopping = true;
  if (reconnectTimer !== undefined) {
    clearTimeout(reconnectTimer);
    reconnectTimer = undefined;
  }
  if (reconnectPromise !== undefined) await reconnectPromise.catch(() => undefined);

  await agent.stop();
  link.close();
  await currentSession.close("worker_stopped").catch((error: unknown) => {
    process.stderr.write(`gethen worker: closing mailbox session failed: ${messageOf(error)}\n`);
  });
  await harness.close();
  process.stdout.write(`worker ${workerId} stopped\n`);
  return 0;
}

const WORKER_USAGE = `Usage:
  gethen worker add --worker-id <id> [--name <name>] [--tokens-file <file>] [--force]
  gethen worker ls [--json]
  gethen worker revoke --id <id>

  gethen worker --config <file>
  gethen worker --master-url <url> --token <token> --worker-id <id> \\
                --provider <provider> --model <model> [--workspace <dir>]...
                [--dsh-args <arg>]... [--state-dir <dir>]

Without a subcommand the worker daemon starts: it dials the master's ws
transport, attaches to mailbox:<worker-id> and executes dispatched assignments
through the DSH SDK harness (dsh --profile sdk). add/ls/revoke manage worker
tokens persisted in the master sqlite (--db, default
$GETHEN_STATE_DIR/master.sqlite or ~/.gethen/master.sqlite).

--dsh-args adds arguments for "dsh --profile sdk" and is repeatable or
comma-separated, e.g.
  --dsh-args "--patch,/tmp/overlay.yml"   or
  --dsh-args --patch --dsh-args /tmp/overlay.yml
--state-dir keeps durable worker state (session key registry) there; default
~/.gethen/worker, one file per worker id.
A daemon config file is JSON:
  { "masterUrl": "wss://master/gethen/worker", "token": "...", "workerId": "...",
    "provider": "...", "model": "...", "workspaces": ["/srv/project"],
    "machine": "...", "quota": 1, "dshCommand": "dsh", "dshArgs": ["--profile", "sdk"] }
Flags win over the file. SIGINT/SIGTERM shut the worker down gracefully.
`;

function waitForSignal(): Promise<void> {
  return new Promise<void>((resolve) => {
    // The reconnect timer is unref'd, so the daemon keeps itself alive with one
    // ref'd interval; without it a failed reconnect could let the process exit.
    const keepAlive = setInterval(() => undefined, 60_000);
    const shutdown = (): void => {
      clearInterval(keepAlive);
      process.off("SIGINT", shutdown);
      process.off("SIGTERM", shutdown);
      resolve();
    };
    process.once("SIGINT", shutdown);
    process.once("SIGTERM", shutdown);
  });
}

function configFile(argv: string[]): WorkerConfigFile {
  const path = flagValue(argv, "--config");
  if (path === undefined) return {};
  let raw: string;
  try {
    raw = readFileSync(path, "utf8");
  } catch (error) {
    throw new Error(`cannot read --config ${path}: ${messageOf(error)}`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`--config ${path} is not valid JSON: ${messageOf(error)}`);
  }
  if (!isRecord(parsed)) throw new Error(`--config ${path} must be a JSON object`);

  const file: WorkerConfigFile = {};
  const masterUrl = optionalString(parsed, "masterUrl");
  const token = optionalString(parsed, "token");
  const workerId = optionalString(parsed, "workerId");
  const machine = optionalString(parsed, "machine");
  const provider = optionalString(parsed, "provider");
  const model = optionalString(parsed, "model");
  const dshCommand = optionalString(parsed, "dshCommand");
  const workspaces = optionalStringArray(parsed, "workspaces");
  const dshArgs = optionalStringArray(parsed, "dshArgs");
  const quota = optionalNumber(parsed, "quota");
  const dnsOverrides = parsed.dnsOverrides;
  const workspaceMap = parsed.workspaceMap;
  const requestTimeoutMs = optionalNumber(parsed, "requestTimeoutMs");
  const isolation = optionalString(parsed, "isolation");
  const costClass = optionalString(parsed, "costClass");
  const capabilities = parsed.capabilities;
  if (masterUrl !== undefined) file.masterUrl = masterUrl;
  if (token !== undefined) file.token = token;
  if (workerId !== undefined) file.workerId = workerId;
  if (machine !== undefined) file.machine = machine;
  if (provider !== undefined) file.provider = provider;
  if (model !== undefined) file.model = model;
  if (dshCommand !== undefined) file.dshCommand = dshCommand;
  if (workspaces !== undefined) file.workspaces = workspaces;
  if (dshArgs !== undefined) file.dshArgs = dshArgs;
  if (quota !== undefined) file.quota = quota;
  if (dnsOverrides !== undefined) file.dnsOverrides = dnsOverrides as Record<string, string>;
  if (workspaceMap !== undefined) file.workspaceMap = workspaceMap as Record<string, string>;
  if (requestTimeoutMs !== undefined) file.requestTimeoutMs = requestTimeoutMs;
  if (isolation !== undefined) file.isolation = isolation as "shared" | "process";
  if (costClass !== undefined) file.costClass = costClass as WorkerConfigFile["costClass"];
  if (capabilities !== undefined) {
    file.capabilities = capabilities as Partial<WorkerCapabilities>;
  }
  return file;
}

function flagValue(argv: string[], name: string): string | undefined {
  const values = flagValues(argv, name);
  return values.at(-1);
}

function flagValues(argv: string[], name: string): string[] {
  const values: string[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] !== name) continue;
    const value = argv[index + 1];
    if (value !== undefined) {
      values.push(value);
      index += 1;
    }
  }
  return values;
}

/**
 * `--dsh-args` arguments for the SDK runtime. Each occurrence may carry one
 * argument or a comma-separated list, so both `--dsh-args --profile --dsh-args
 * sdk` and `--dsh-args "--profile,sdk,--patch,/tmp/overlay.yml"` work.
 */
function dshArgsFromFlags(argv: string[]): string[] {
  const args: string[] = [];
  for (const value of flagValues(argv, "--dsh-args")) {
    for (const part of value.split(",")) {
      const trimmed = part.trim();
      if (trimmed.length > 0) args.push(trimmed);
    }
  }
  return args;
}

function requireString(value: string | undefined, label: string, missing: string[]): string {
  if (value === undefined || value.trim().length === 0) {
    missing.push(label);
    return "";
  }
  return value.trim();
}

function positiveInteger(value: string | number | undefined, label: string): number | undefined {
  if (value === undefined) return undefined;
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`worker config: ${label} must be a positive integer`);
  }
  return parsed;
}

function optionalString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw new Error(`worker config: "${key}" must be a string`);
  return value;
}

function optionalStringArray(record: Record<string, unknown>, key: string): string[] | undefined {
  const value = record[key];
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) throw new Error(`worker config: "${key}" must be an array of strings`);
  const items: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") {
      throw new Error(`worker config: "${key}" must be an array of strings`);
    }
    items.push(item);
  }
  return items;
}

function optionalNumber(record: Record<string, unknown>, key: string): number | undefined {
  const value = record[key];
  if (value === undefined) return undefined;
  if (typeof value !== "number") throw new Error(`worker config: "${key}" must be a number`);
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

import { readFileSync } from "node:fs";
import { hostname } from "node:os";
import { createSdkExecutor } from "@gethen/executor-sdk";
import { createWorkerAgent, type WorkerAgent } from "@gethen/worker-agent";
import { WORKER_PATH } from "./serve";

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
}

export interface WorkerConfigOptions {
  /** Default workspace when none is configured; defaults to `process.cwd()`. */
  cwd?: string;
}

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
  const quota = positiveInteger(flagValue(argv, "--quota") ?? file.quota, "quota");
  const machine = flagValue(argv, "--machine") ?? file.machine;

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
    ...(file.dshArgs === undefined ? {} : { dshArgs: file.dshArgs }),
  };
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

  const executor = createSdkExecutor({
    provider: config.provider,
    model: config.model,
    cwdFor: (workspace) => workspace,
    ...(config.dshCommand === undefined ? {} : { dshCommand: config.dshCommand }),
    ...(config.dshArgs === undefined ? {} : { dshArgs: config.dshArgs }),
  });
  const agent: WorkerAgent = createWorkerAgent({
    masterUrl: config.masterUrl,
    token: config.token,
    workerId: config.workerId,
    machine: config.machine ?? hostname(),
    platform: process.platform,
    runtimes: ["dsh"],
    workspaces: [...config.workspaces],
    models: [config.model],
    // Advertise nothing extra: the Master must not assume approvals/tools are
    // pre-granted. Escalations are negotiated per assignment through the SDK.
    capabilities: {},
    quota: config.quota ?? 1,
    executor,
    logger: { error: (message) => process.stderr.write(`gethen worker: ${message}\n`) },
  });

  process.stdout.write(`worker ${config.workerId} connecting to ${config.masterUrl}\n`);
  await waitForSignal();
  await agent.close();
  await executor.close();
  process.stdout.write(`worker ${config.workerId} stopped\n`);
  return 0;
}

const WORKER_USAGE = `Usage: gethen worker --config <file>
   or: gethen worker --master-url <url> --token <token> --worker-id <id> \\
         --provider <provider> --model <model> [--workspace <dir>]...

Connects out to a master and executes dispatched assignments through the DSH
SDK executor. A config file is JSON:
  { "masterUrl": "wss://master/gethen/worker", "token": "...", "workerId": "...",
    "provider": "...", "model": "...", "workspaces": ["/srv/project"],
    "machine": "...", "quota": 1, "dshCommand": "dsh", "dshArgs": ["--profile", "sdk"] }
Flags win over the file. SIGINT/SIGTERM shut the worker down gracefully.
`;

function waitForSignal(): Promise<void> {
  return new Promise<void>((resolve) => {
    // The agent's reconnect timer is unref'd, so the daemon keeps itself alive
    // with one ref'd interval; without it a failed first connect would let the
    // process exit instead of retrying (systemd would restart-loop).
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

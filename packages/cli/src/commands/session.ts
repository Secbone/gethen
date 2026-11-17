import type { HarnessSessionHistory, HarnessSessionSummary } from "@gethen/master-runtime";
import { masterEndpoint } from "./run";

export type SessionAction = "list" | "history";

export interface SessionArgs {
  action: SessionAction;
  master: string;
  /** Print the master's raw JSON response. */
  json: boolean;
  /** How long to wait for the worker's reply (list/history). */
  timeoutMs?: number;
  /** `history`: forward page size and cursor. */
  limit?: number;
  afterSeq?: number;
  /** `history`: which session to read. */
  sessionId?: string;
}

export interface SessionOptions {
  /** Environment for the `GETHEN_MASTER_URL` fallback (tests). */
  readonly env?: NodeJS.ProcessEnv;
  /** Sink for stdout (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Sink for errors (tests). Defaults to stderr. */
  readonly error?: (line: string) => void;
}

const FLAGS_WITH_VALUE = new Set(["--master", "--timeout-ms", "--limit", "--after-seq"]);

/** Parse `gethen session list|history <sessionId> … --master <url>`. */
export function parseSessionArgs(
  argv: string[],
  env: NodeJS.ProcessEnv = process.env,
): SessionArgs {
  const action = argv[0];
  if (action !== "list" && action !== "history") {
    throw new Error('expected a subcommand: "list" or "history"');
  }
  const master = flagValue(argv, "--master") ?? env.GETHEN_MASTER_URL;
  if (master === undefined || master.trim().length === 0) {
    throw new Error("--master <url> is required (or set GETHEN_MASTER_URL)");
  }
  const json = argv.includes("--json");
  const timeoutMs = positiveInteger(flagValue(argv, "--timeout-ms"), "--timeout-ms");

  if (action === "list") {
    return {
      action,
      master: master.trim(),
      json,
      ...(timeoutMs === undefined ? {} : { timeoutMs }),
    };
  }

  const sessionId = positionalArgs(argv.slice(1))[0]?.trim();
  if (sessionId === undefined || sessionId.length === 0) {
    throw new Error("a session id is required for history");
  }
  const limit = positiveInteger(flagValue(argv, "--limit"), "--limit");
  const afterSeq = nonNegativeInteger(flagValue(argv, "--after-seq"), "--after-seq");
  return {
    action,
    master: master.trim(),
    json,
    sessionId,
    ...(timeoutMs === undefined ? {} : { timeoutMs }),
    ...(limit === undefined ? {} : { limit }),
    ...(afterSeq === undefined ? {} : { afterSeq }),
  };
}

/** The route `gethen session` calls on the master for one parsed command. */
export function sessionPath(args: SessionArgs): string {
  const query = new URLSearchParams();
  if (args.timeoutMs !== undefined) query.set("timeoutMs", String(args.timeoutMs));
  if (args.action === "list") {
    const suffix = query.size === 0 ? "" : `?${query.toString()}`;
    return `/gethen/sessions${suffix}`;
  }
  const sessionId = encodeURIComponent(args.sessionId ?? "");
  if (args.limit !== undefined) query.set("limit", String(args.limit));
  if (args.afterSeq !== undefined) query.set("afterSeq", String(args.afterSeq));
  const suffix = query.size === 0 ? "" : `?${query.toString()}`;
  return `/gethen/sessions/${sessionId}/history${suffix}`;
}

/** `gethen session list|history … --master <url>` */
export async function sessionCommand(
  argv: string[],
  options: SessionOptions = {},
): Promise<number> {
  const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
  const writeError = options.error ?? ((line: string) => process.stderr.write(`${line}\n`));

  if (argv.includes("-h") || argv.includes("--help")) {
    write(SESSION_USAGE);
    return 0;
  }
  if (argv.length === 0) {
    writeError(SESSION_USAGE);
    return 1;
  }

  let args: SessionArgs;
  let endpoint: string;
  try {
    args = parseSessionArgs(argv, options.env ?? process.env);
    endpoint = masterEndpoint(args.master, sessionPath(args));
  } catch (error) {
    writeError(`gethen session: ${messageOf(error)}`);
    return 1;
  }

  let response: Response;
  try {
    response = await fetch(endpoint, { method: "GET" });
  } catch (error) {
    writeError(`gethen session: cannot reach master at ${endpoint}: ${messageOf(error)}`);
    return 1;
  }
  const raw = await response.text();
  if (!response.ok) {
    writeError(`gethen session: ${httpError(response.status, raw)}`);
    return 1;
  }
  if (args.json) {
    write(raw.trim());
    return 0;
  }

  if (args.action === "list") {
    let body: { sessions?: HarnessSessionSummary[] };
    try {
      body = JSON.parse(raw) as { sessions?: HarnessSessionSummary[] };
    } catch {
      writeError(`gethen session: master returned invalid JSON: ${truncate(raw)}`);
      return 1;
    }
    renderSessions(body.sessions ?? [], write);
    return 0;
  }

  let history: HarnessSessionHistory;
  try {
    history = JSON.parse(raw) as HarnessSessionHistory;
  } catch {
    writeError(`gethen session: master returned invalid JSON: ${truncate(raw)}`);
    return 1;
  }
  renderHistory(history, write);
  return 0;
}

const SESSION_USAGE = `Usage:
  gethen session list --master <url> [--timeout-ms <n>] [--json]
  gethen session history <sessionId> --master <url> [--limit <n>]
                         [--after-seq <n>] [--timeout-ms <n>] [--json]

Talks to a running \`gethen serve\`: list asks the Worker for its harness session
directory; history reads one session's event log (404/504 when the Worker has no
session or does not answer in time). --master may be an origin or an origin with
a base path; GETHEN_MASTER_URL is the fallback. Exit code is non-zero for
transport/HTTP/validation errors.
`;

function renderSessions(
  sessions: readonly HarnessSessionSummary[],
  write: (line: string) => void,
): void {
  write(`sessions: ${sessions.length}`);
  if (sessions.length === 0) {
    write("(no sessions)");
    return;
  }
  for (const session of sessions) {
    const state = session.state ?? (session.activeAssignmentId === undefined ? "idle" : "running");
    const key = session.key ?? "-";
    const workspace = session.workspace ?? "-";
    const lastUsed = session.lastUsedAt ?? "-";
    write(
      `${session.sessionId}  state=${state}  key=${key}  workspace=${workspace}  last=${lastUsed}`,
    );
  }
}

function renderHistory(history: HarnessSessionHistory, write: (line: string) => void): void {
  const events = history.events ?? [];
  write(`session: ${history.sessionId}`);
  write(`events: ${events.length}`);
  for (const event of events) {
    write(`#${event.seq} ${event.type} ${event.at}`);
    if (event.data !== undefined) write(`  ${truncate(JSON.stringify(event.data), 300)}`);
  }
}

function httpError(status: number, raw: string): string {
  const parsed = safeJson(raw);
  if (typeof parsed === "object" && parsed !== null) {
    const error = (parsed as { error?: unknown }).error;
    if (typeof error === "string" && error.length > 0) return `HTTP ${status}: ${error}`;
  }
  const text = raw.trim();
  return text.length > 0 ? `HTTP ${status}: ${truncate(text)}` : `HTTP ${status}`;
}

function safeJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    return undefined;
  }
}

function flagValue(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
}

/** Positional words, skipping flags and the values they consume. */
function positionalArgs(argv: string[]): string[] {
  const words: string[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === undefined) continue;
    if (FLAGS_WITH_VALUE.has(arg)) {
      index += 1;
      continue;
    }
    if (arg.startsWith("-")) continue;
    words.push(arg);
  }
  return words;
}

function positiveInteger(value: string | undefined, label: string): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
  return parsed;
}

function nonNegativeInteger(value: string | undefined, label: string): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${label} must be a non-negative integer`);
  }
  return parsed;
}

function truncate(value: string, limit = 500): string {
  return value.length <= limit ? value : `${value.slice(0, limit)}…`;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

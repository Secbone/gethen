import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { type PruneDshSessionsResult, pruneDshSessions } from "@gethen/dsh-sessions";
import { SqliteStore } from "@gethen/master";
import type { HarnessSessionHistory, HarnessSessionSummary } from "@gethen/master-runtime";
import { defaultMasterDbPath } from "../paths";
import { masterEndpoint } from "./run";

export type SessionAction = "list" | "history" | "prune";

interface SessionRemoteBase {
  master: string;
  /** Print the master's raw JSON response. */
  json: boolean;
  /** How long to wait for the worker's reply (list/history). */
  timeoutMs?: number;
}

export interface SessionListArgs extends SessionRemoteBase {
  action: "list";
}

export interface SessionHistoryArgs extends SessionRemoteBase {
  action: "history";
  /** `history`: forward page size and cursor. */
  limit?: number;
  afterSeq?: number;
  /** `history`: which session to read. */
  sessionId: string;
}

/**
 * Local cleanup. This never talks to the master HTTP API: it removes only
 * session directories under this machine's DSH home.
 */
export interface SessionPruneArgs {
  action: "prune";
  olderThanDays: number;
  /** Delete only with `--yes`; otherwise this is a dry run. */
  dryRun: boolean;
  json: boolean;
  /** Explicit `--yes` (the destructive opt-in). */
  yes: boolean;
  dshHome?: string;
  stateDir?: string;
  dbPath?: string;
}

export type SessionArgs = SessionListArgs | SessionHistoryArgs | SessionPruneArgs;

export interface SessionOptions {
  /** Environment for `GETHEN_MASTER_URL`/`DSH_HOME`/`GETHEN_STATE_DIR` (tests). */
  readonly env?: NodeJS.ProcessEnv;
  /** Sink for stdout (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Sink for errors (tests). Defaults to stderr. */
  readonly error?: (line: string) => void;
  /** Home directory for defaults (tests). Defaults to the OS home. */
  readonly home?: string;
  /** Clock used by `prune` (tests). Defaults to now. */
  readonly now?: Date;
}

const FLAGS_WITH_VALUE = new Set([
  "--master",
  "--timeout-ms",
  "--limit",
  "--after-seq",
  "--older-than",
  "--dsh-home",
  "--state-dir",
  "--db",
]);

/**
 * Parse `gethen session list|history <sessionId> … --master <url>` and
 * `gethen session prune --older-than <days> [--dry-run] [--yes] [--json]`.
 * `list`/`history` require a master; `prune` is local and must not.
 */
export function parseSessionArgs(
  argv: string[],
  env: NodeJS.ProcessEnv = process.env,
): SessionArgs {
  const action = argv[0];
  if (action === "prune") return parsePruneArgs(argv);
  if (action !== "list" && action !== "history") {
    throw new Error('expected a subcommand: "list", "history" or "prune"');
  }
  const master = flagValue(argv, "--master") ?? env.GETHEN_MASTER_URL;
  if (master === undefined || master.trim().length === 0) {
    throw new Error("--master <url> is required (or set GETHEN_MASTER_URL)");
  }
  const json = argv.includes("--json");
  const timeoutMs = positiveInteger(flagValue(argv, "--timeout-ms"), "--timeout-ms");
  const base = {
    master: master.trim(),
    json,
    ...(timeoutMs === undefined ? {} : { timeoutMs }),
  };

  if (action === "list") {
    return { action, ...base };
  }

  const sessionId = positionalArgs(argv.slice(1))[0]?.trim();
  if (sessionId === undefined || sessionId.length === 0) {
    throw new Error("a session id is required for history");
  }
  const limit = positiveInteger(flagValue(argv, "--limit"), "--limit");
  const afterSeq = nonNegativeInteger(flagValue(argv, "--after-seq"), "--after-seq");
  return {
    action,
    ...base,
    sessionId,
    ...(limit === undefined ? {} : { limit }),
    ...(afterSeq === undefined ? {} : { afterSeq }),
  };
}

function parsePruneArgs(argv: string[]): SessionPruneArgs {
  const olderThanDays = positiveNumber(flagValue(argv, "--older-than"), "--older-than");
  if (olderThanDays === undefined) {
    throw new Error("--older-than <days> is required for prune");
  }
  const dshHome = flagValue(argv, "--dsh-home");
  const stateDir = flagValue(argv, "--state-dir");
  const dbPath = flagValue(argv, "--db");
  return {
    action: "prune",
    olderThanDays,
    // Deletion requires the explicit --yes opt-in; --dry-run always wins.
    dryRun: argv.includes("--dry-run") || !argv.includes("--yes"),
    yes: argv.includes("--yes"),
    json: argv.includes("--json"),
    ...(dshHome === undefined ? {} : { dshHome }),
    ...(stateDir === undefined ? {} : { stateDir }),
    ...(dbPath === undefined ? {} : { dbPath }),
  };
}

/** The route `gethen session` calls on the master for one parsed command. */
export function sessionPath(args: SessionArgs): string {
  if (args.action === "prune") {
    throw new Error("session prune is local and does not call the master");
  }
  const query = new URLSearchParams();
  if (args.timeoutMs !== undefined) query.set("timeoutMs", String(args.timeoutMs));
  if (args.action === "list") {
    const suffix = query.size === 0 ? "" : `?${query.toString()}`;
    return `/gethen/sessions${suffix}`;
  }
  const sessionId = encodeURIComponent(args.sessionId);
  if (args.limit !== undefined) query.set("limit", String(args.limit));
  if (args.afterSeq !== undefined) query.set("afterSeq", String(args.afterSeq));
  const suffix = query.size === 0 ? "" : `?${query.toString()}`;
  return `/gethen/sessions/${sessionId}/history${suffix}`;
}

/** `gethen session list|history|prune …` */
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
  try {
    args = parseSessionArgs(argv, options.env ?? process.env);
  } catch (error) {
    writeError(`gethen session: ${messageOf(error)}`);
    return 1;
  }
  if (args.action === "prune") {
    return sessionPrune(args, options, write, writeError);
  }

  let endpoint: string;
  try {
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

/**
 * `gethen session prune` runs entirely on this machine: it protects sessions
 * of active (running/waiting) Assignments when the master sqlite is available,
 * every durable key-registry mapping, and sessions with an open DSH turn.
 */
async function sessionPrune(
  args: SessionPruneArgs,
  options: SessionOptions,
  write: (line: string) => void,
  writeError: (line: string) => void,
): Promise<number> {
  const env = options.env ?? process.env;
  const home = options.home ?? homedir();
  const dshHome = args.dshHome ?? env.DSH_HOME ?? join(home, ".dsh");
  const stateDir = args.stateDir ?? join(home, ".gethen", "worker");
  const dbPath = args.dbPath ?? defaultMasterDbPath(env, home);

  let activeSessionIds: string[] = [];
  let dbUsed: string | undefined;
  if (existsSync(dbPath)) {
    try {
      activeSessionIds = activeAssignmentSessionIds(dbPath);
      dbUsed = dbPath;
    } catch (error) {
      writeError(`gethen session prune: cannot read ${dbPath}: ${messageOf(error)}`);
      writeError("refusing to prune without knowing the active Assignments");
      return 1;
    }
  }

  let result: PruneDshSessionsResult;
  try {
    result = await pruneDshSessions({
      home: dshHome,
      stateDir,
      olderThanDays: args.olderThanDays,
      dryRun: args.dryRun,
      protectSessionIds: activeSessionIds,
      ...(options.now === undefined ? {} : { now: options.now }),
    });
  } catch (error) {
    writeError(`gethen session prune: ${messageOf(error)}`);
    return 1;
  }

  if (args.json) {
    write(
      JSON.stringify(
        {
          ...result,
          db: dbUsed ?? null,
          activeAssignments: activeSessionIds.length,
        },
        null,
        2,
      ),
    );
    return 0;
  }
  renderPrune(result, dbUsed, activeSessionIds.length, write, options.now ?? new Date());
  return 0;
}

/** Session ids of Assignments that may still be resumed (running/waiting). */
function activeAssignmentSessionIds(dbPath: string): string[] {
  const store = new SqliteStore(dbPath);
  try {
    const ids = new Set<string>();
    for (const assignment of store.assignments.list()) {
      if (assignment.state !== "running" && assignment.state !== "waiting") continue;
      ids.add(assignment.session?.sessionId ?? assignment.id);
    }
    return [...ids];
  } finally {
    store.close();
  }
}

const SESSION_USAGE = `Usage:
  gethen session list --master <url> [--timeout-ms <n>] [--json]
  gethen session history <sessionId> --master <url> [--limit <n>]
                         [--after-seq <n>] [--timeout-ms <n>] [--json]
  gethen session prune --older-than <days> [--dry-run] [--yes] [--json]
                       [--dsh-home <dir>] [--state-dir <dir>] [--db <file>]

list/history ask a running \`gethen serve\` about a Worker's harness session
directory/history. prune is local: it removes DSH session directories under
$DSH_HOME (default ~/.dsh) whose newest mtime is older than --older-than days.
It never removes sessions of running/waiting Assignments (read from the master
sqlite when present), sessions referenced by a key registry under --state-dir
(default ~/.gethen/worker), or sessions with an open DSH turn.

Safety: prune is a DRY RUN unless --yes is passed. --dry-run always wins.
Exit code is non-zero for transport/HTTP/validation errors; prune errors refuse
rather than guess when the master database cannot be read.
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

function renderPrune(
  result: PruneDshSessionsResult,
  dbPath: string | undefined,
  activeAssignments: number,
  write: (line: string) => void,
  now: Date,
): void {
  write(
    result.dryRun
      ? "session prune: dry run (pass --yes to delete)"
      : `session prune: deleted ${result.deleted.length} session(s)`,
  );
  write(`dsh home: ${result.dshHome}`);
  write(
    dbPath === undefined
      ? "master db: not found (only key-registry and open-turn protection applied)"
      : `master db: ${dbPath} (${activeAssignments} active assignment session(s) protected)`,
  );
  write(`older than: ${result.olderThanDays}d (cutoff ${result.cutoff})`);
  write(
    `scanned: ${result.scanned}  candidates: ${result.candidates.length}  ` +
      `protected: ${result.skipped.filter((entry) => entry.reason !== "delete_failed").length}`,
  );
  if (result.dryRun) {
    write(`would free: ${formatBytes(result.bytesEligible)}`);
  } else {
    write(
      `deleted: ${result.deleted.length}  freed: ${formatBytes(result.bytesFreed)}  ` +
        `metadata records: ${result.removedRecords}`,
    );
  }
  const listed = result.dryRun ? result.candidates : result.deleted;
  for (const entry of listed.slice(0, 20)) {
    const age = Math.max(0, (now.getTime() - Date.parse(entry.lastModifiedAt)) / 86_400_000);
    write(
      `  - ${entry.sessionId}  project=${entry.project}  age=${age.toFixed(1)}d  ` +
        `size=${formatBytes(entry.bytes)}`,
    );
  }
  if (listed.length > 20) write(`  … ${listed.length - 20} more`);
  for (const entry of result.skipped) {
    if (entry.reason === "delete_failed" || entry.reason === "outside_home") {
      write(`  skipped ${entry.sessionId}: ${entry.reason}${entry.detail ?? ""}`);
    }
  }
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unit]}`;
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

function positiveNumber(value: string | undefined, label: string): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive number`);
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

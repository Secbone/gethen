import { createReadStream, existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { createZstdDecompress } from "node:zlib";
import type {
  JsonObject,
  JsonValue,
  SessionConfig,
  SessionEvent,
  SessionHistoryOptions,
  SessionListOptions,
  SessionSummary,
} from "./types";

const SESSION_FILE = "session.v4.jsonl.zstd";
const DEFAULT_LIST_LIMIT = 100;
const DEFAULT_HISTORY_LIMIT = 200;
const DEFAULT_MAX_EVENTS = 5000;

export interface DshSessionStoreOptions {
  /** DSH home; defaults to `$DSH_HOME` or `~/.dsh`. */
  home?: string;
}

/**
 * Read-only view of the local DSH session store.
 *
 * DSH keeps a small per-session metadata record under
 * `storages/session_projcache/sessions/*.json` (cwd, model, sandbox, title,
 * turn state) and the full event log under
 * `sessions/<project>/<sessionId>/session.v4.jsonl.zstd`. This class exposes
 * both so a worker can decide whether a session is worth reusing (config, last
 * activity, state, history) and the master can inspect it.
 */
export class DshSessionStore {
  readonly home: string;
  private readonly projcacheDir: string;
  private readonly sessionsDir: string;

  constructor(options: DshSessionStoreOptions = {}) {
    this.home = options.home ?? process.env.DSH_HOME ?? join(homedir(), ".dsh");
    this.projcacheDir = join(this.home, "storages", "session_projcache", "sessions");
    this.sessionsDir = join(this.home, "sessions");
  }

  /** Sessions known to DSH, newest activity first. */
  async list(options: SessionListOptions = {}): Promise<SessionSummary[]> {
    const limit = Math.max(1, options.limit ?? DEFAULT_LIST_LIMIT);
    const workspace = options.workspace === undefined ? undefined : normalizeCwd(options.workspace);
    const candidates: SessionSummary[] = [];
    let files: string[] = [];
    try {
      files = readdirSync(this.projcacheDir).filter((file) => file.endsWith(".json"));
    } catch {
      return [];
    }
    for (const file of files) {
      const sessionId = file.slice(0, -".json".length);
      const summary = this.readSummary(sessionId);
      if (summary === undefined) continue;
      if (workspace !== undefined && normalizeCwd(summary.config.cwd) !== workspace) continue;
      candidates.push(summary);
    }
    candidates.sort((left, right) => {
      const leftAt = left.lastActivityAt ?? "";
      const rightAt = right.lastActivityAt ?? "";
      return rightAt.localeCompare(leftAt);
    });
    return candidates.slice(0, limit);
  }

  /** One session's config + state (undefined when DSH has no record). */
  async describe(sessionId: string): Promise<SessionSummary | undefined> {
    return this.readSummary(sessionId);
  }

  /** Normalized event history, newest tail or a forward page after `afterSeq`. */
  async history(sessionId: string, options: SessionHistoryOptions = {}): Promise<SessionEvent[]> {
    const limit = Math.max(1, options.limit ?? DEFAULT_HISTORY_LIMIT);
    const maxEvents = Math.max(1, options.maxEvents ?? DEFAULT_MAX_EVENTS);
    const path = this.logPath(sessionId);
    if (path === undefined) return [];
    const events: SessionEvent[] = [];
    const stream = createReadStream(path).pipe(createZstdDecompress());
    const lines = createInterface({ input: stream, crlfDelay: Number.POSITIVE_INFINITY });
    try {
      for await (const line of lines) {
        if (line.length === 0) continue;
        const parsed = safeParse(line);
        if (parsed === undefined) continue;
        const event = normalizeEvent(parsed);
        if (event === undefined) continue;
        events.push(event);
        if (events.length > maxEvents) events.shift();
      }
    } finally {
      lines.close();
      stream.destroy();
    }
    const filtered =
      options.afterSeq === undefined
        ? events
        : events.filter((event) => event.seq > (options.afterSeq as number));
    return options.afterSeq === undefined ? filtered.slice(-limit) : filtered.slice(0, limit);
  }

  /** Approximate last persisted sequence for a session (cheap, no decompression). */
  async lastSeq(sessionId: string): Promise<number | undefined> {
    const summary = this.readSummary(sessionId);
    return summary?.eventCount;
  }

  private readSummary(sessionId: string): SessionSummary | undefined {
    const recordPath = join(this.projcacheDir, `${sessionId}.json`);
    if (!existsSync(recordPath)) return undefined;
    let parsed: unknown;
    try {
      parsed = JSON.parse(readFileSync(recordPath, "utf8"));
    } catch {
      return undefined;
    }
    if (!isRecord(parsed) || !isRecord(parsed.record)) return undefined;
    const record = parsed.record;
    const identity = isRecord(record.identity) ? record.identity : undefined;
    if (identity === undefined || typeof identity.cwd !== "string") return undefined;
    const rows = isRecord(record.rows) ? record.rows : {};
    const createdAtMs =
      typeof identity.createdAt === "number"
        ? identity.createdAt
        : Date.parse(new Date(0).toISOString());
    const turnRow = isRecord(rows["turnBoundary"]) ? rows["turnBoundary"] : undefined;
    const turnVal = isRecord(turnRow?.val) ? turnRow.val : undefined;
    const modelSelection = rowValue(rows, "modelSelection") as
      | { lastUsed?: { provider?: unknown; model?: unknown } }
      | undefined;
    const tokenUsage = rowValue(rows, "tokenUsage") as JsonObject | undefined;
    const title = rowValue(rows, "title");
    const sandboxMode = rowValue(rows, "sandboxMode");
    const approvalState = rowValue(rows, "approvalState") as { policy?: unknown } | undefined;
    const eventSeq = turnRow !== undefined && typeof turnRow.seq === "number" ? turnRow.seq : 0;
    const openTurn =
      turnVal !== undefined &&
      turnVal.openTurnStartSeq !== null &&
      turnVal.openTurnStartSeq !== undefined;
    const log = this.logPath(sessionId);
    let lastActivityMs = Number.NaN;
    let sizeBytes: number | undefined;
    if (log !== undefined) {
      try {
        const stat = statSync(log);
        lastActivityMs = stat.mtimeMs;
        sizeBytes = stat.size;
      } catch {
        // ignore
      }
    }
    let projcacheMs = Number.NaN;
    try {
      projcacheMs = statSync(recordPath).mtimeMs;
    } catch {
      // ignore
    }
    const lastActivity = Number.isFinite(Math.max(lastActivityMs, projcacheMs))
      ? new Date(Math.max(lastActivityMs, projcacheMs)).toISOString()
      : undefined;
    const config: SessionConfig = {
      cwd: identity.cwd,
      createdAt: new Date(Number.isFinite(createdAtMs) ? createdAtMs : 0).toISOString(),
    };
    if (typeof title === "string") config.title = title;
    const lastUsed = isRecord(modelSelection) ? modelSelection.lastUsed : undefined;
    if (
      isRecord(lastUsed) &&
      typeof lastUsed.provider === "string" &&
      typeof lastUsed.model === "string"
    ) {
      config.model = { provider: lastUsed.provider, model: lastUsed.model };
    }
    if (typeof sandboxMode === "string") config.sandboxMode = sandboxMode;
    if (isRecord(approvalState) && typeof approvalState.policy === "string") {
      config.approvalPolicy = approvalState.policy;
    }
    const summary: SessionSummary = {
      sessionId,
      config,
      state: openTurn ? "running" : log === undefined ? "unknown" : "idle",
      hasLog: log !== undefined,
    };
    if (lastActivity !== undefined) summary.lastActivityAt = lastActivity;
    if (eventSeq > 0) summary.eventCount = eventSeq;
    if (tokenUsage !== undefined) summary.tokenUsage = tokenUsage;
    if (sizeBytes !== undefined) summary.sizeBytes = sizeBytes;
    return summary;
  }

  private logPath(sessionId: string): string | undefined {
    let projects: string[] = [];
    try {
      projects = readdirSync(this.sessionsDir);
    } catch {
      return undefined;
    }
    for (const project of projects) {
      const candidate = join(this.sessionsDir, project, sessionId, SESSION_FILE);
      if (existsSync(candidate)) return candidate;
    }
    return undefined;
  }
}

function rowValue(rows: Record<string, unknown>, name: string): unknown {
  const row = rows[name];
  if (!isRecord(row)) return undefined;
  return row.val;
}

function normalizeEvent(record: Record<string, unknown>): SessionEvent | undefined {
  if (typeof record.type !== "string" || typeof record.seq !== "number") return undefined;
  const atMs = typeof record.time === "number" ? record.time : Date.now();
  const event: SessionEvent = {
    seq: record.seq,
    type: record.type,
    at: new Date(atMs).toISOString(),
  };
  if (record.data !== undefined) event.data = record.data as JsonValue;
  return event;
}

function safeParse(text: string): Record<string, unknown> | undefined {
  try {
    const parsed: unknown = JSON.parse(text);
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function isRecord(value: unknown): value is Record<string, any> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeCwd(cwd: string): string {
  return cwd.length > 1 && cwd.endsWith("/") ? cwd.slice(0, -1) : cwd;
}

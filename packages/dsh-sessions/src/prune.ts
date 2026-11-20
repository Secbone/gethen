/**
 * Safe local cleanup for the DSH session store.
 *
 * The pruner only removes session directories whose newest mtime is older than
 * the caller's cutoff, and never removes a session that is still live:
 *
 * - session ids passed as `protectSessionIds` (running/waiting Assignments);
 * - every session id mapped by a durable worker key registry (a later
 *   `session.mode: "reuse"` must still resolve);
 * - sessions whose DSH metadata reports an open turn (`running`).
 *
 * The corresponding projcache metadata record is removed with the directory so
 * `gethen session list` does not keep reporting a deleted log. Empty project
 * directories are cleaned up as a best effort.
 */
import { existsSync, readdirSync, rmdirSync, rmSync } from "node:fs";
import { join, resolve } from "node:path";
import {
  isWithin,
  resolveDshHome,
  type SessionDirectoryEntry,
  scanSessionDirectories,
  scanSessionKeyRegistries,
} from "./stats";
import { DshSessionStore } from "./store";

export type PruneSkipReason =
  | "active_assignment"
  | "key_registry"
  | "running_session"
  | "delete_failed"
  | "outside_home";

/** One session directory the pruner considered or removed. */
export interface PrunedSession {
  sessionId: string;
  project: string;
  path: string;
  bytes: number;
  /** RFC 3339 timestamp of the newest observed activity. */
  lastModifiedAt: string;
}

export interface PruneSkip {
  sessionId: string;
  reason: PruneSkipReason;
  detail?: string;
}

export interface PruneDshSessionsOptions {
  /** DSH home; defaults to `$DSH_HOME` or `~/.dsh`. */
  home?: string;
  /** Delete sessions whose newest mtime is older than this many days. */
  olderThanDays: number;
  now?: Date;
  /** Report candidates without touching the filesystem. */
  dryRun?: boolean;
  /** Session ids that must never be removed (running/waiting Assignments). */
  protectSessionIds?: Iterable<string>;
  /** Worker state dir containing `*.sessions.json` key registries. */
  stateDir?: string;
  /** Protect sessions whose DSH record reports an open turn. Default true. */
  protectRunning?: boolean;
}

export interface PruneDshSessionsResult {
  dshHome: string;
  olderThanDays: number;
  /** RFC 3339 cutoff; sessions modified at/after it are kept. */
  cutoff: string;
  dryRun: boolean;
  scanned: number;
  candidates: PrunedSession[];
  deleted: PrunedSession[];
  skipped: PruneSkip[];
  /** Bytes actually freed (always 0 for a dry run). */
  bytesFreed: number;
  /** Bytes the candidate set would free. */
  bytesEligible: number;
  /** Projcache records removed alongside their session directory. */
  removedRecords: number;
}

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * Remove old local DSH session directories. Pure filesystem work, best effort:
 * a single unremovable session is reported in `skipped` instead of failing the
 * whole prune.
 */
export async function pruneDshSessions(
  options: PruneDshSessionsOptions,
): Promise<PruneDshSessionsResult> {
  if (!Number.isFinite(options.olderThanDays) || options.olderThanDays <= 0) {
    throw new Error("olderThanDays must be a positive number");
  }
  const home = resolveDshHome(options.home);
  const root = join(home, "sessions");
  const now = options.now ?? new Date();
  const cutoffMs = now.getTime() - options.olderThanDays * DAY_MS;
  const scan = scanSessionDirectories(home);

  const protectedIds = new Map<string, PruneSkipReason>();
  for (const sessionId of options.protectSessionIds ?? []) {
    if (sessionId.length > 0) protectedIds.set(sessionId, "active_assignment");
  }
  if (options.stateDir !== undefined) {
    for (const sessionId of scanSessionKeyRegistries(options.stateDir).sessionIds) {
      if (!protectedIds.has(sessionId)) protectedIds.set(sessionId, "key_registry");
    }
  }
  if (options.protectRunning !== false) {
    try {
      const live = await new DshSessionStore({ home }).list({ limit: 1_000_000 });
      for (const summary of live) {
        if (summary.state === "running" && !protectedIds.has(summary.sessionId)) {
          protectedIds.set(summary.sessionId, "running_session");
        }
      }
    } catch {
      // A missing/corrupt projcache must not make the prune unsafe; the caller's
      // explicit active set and the key registry still protect known sessions.
    }
  }

  const candidates: PrunedSession[] = [];
  const skipped: PruneSkip[] = [];
  for (const entry of scan.sessions) {
    const reason = protectedIds.get(entry.sessionId);
    if (reason !== undefined) {
      skipped.push({ sessionId: entry.sessionId, reason });
      continue;
    }
    if (entry.lastModifiedMs >= cutoffMs) continue;
    candidates.push(toPrunedSession(entry));
  }

  const bytesEligible = candidates.reduce((sum, entry) => sum + entry.bytes, 0);
  const result: PruneDshSessionsResult = {
    dshHome: home,
    olderThanDays: options.olderThanDays,
    cutoff: new Date(cutoffMs).toISOString(),
    dryRun: options.dryRun === true,
    scanned: scan.sessions.length,
    candidates,
    deleted: [],
    skipped,
    bytesFreed: 0,
    bytesEligible,
    removedRecords: 0,
  };
  if (options.dryRun === true) return result;

  const projcacheDir = join(home, "storages", "session_projcache", "sessions");
  for (const entry of candidates) {
    const target = resolve(entry.path);
    if (!isWithin(root, target)) {
      skipped.push({ sessionId: entry.sessionId, reason: "outside_home", detail: target });
      continue;
    }
    try {
      rmSync(target, { recursive: true, force: true });
      result.deleted.push(entry);
      result.bytesFreed += entry.bytes;
      if (removeRecord(projcacheDir, entry.sessionId)) result.removedRecords += 1;
      removeEmptyProject(join(root, entry.project));
    } catch (error) {
      skipped.push({
        sessionId: entry.sessionId,
        reason: "delete_failed",
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return result;
}

function toPrunedSession(entry: SessionDirectoryEntry): PrunedSession {
  return {
    sessionId: entry.sessionId,
    project: entry.project,
    path: entry.path,
    bytes: entry.bytes,
    lastModifiedAt: new Date(entry.lastModifiedMs).toISOString(),
  };
}

/**
 * Delete the projcache record that belongs to a removed session. Only a direct
 * `<id>.json` under the known projcache directory is eligible, so a crafted id
 * cannot escape the store.
 */
function removeRecord(projcacheDir: string, sessionId: string): boolean {
  const record = join(projcacheDir, `${sessionId}.json`);
  if (!isWithin(projcacheDir, record) || !existsSync(record)) return false;
  try {
    rmSync(record, { force: true });
    return true;
  } catch {
    return false;
  }
}

/** Remove an empty project directory left behind by pruning; ignore failures. */
function removeEmptyProject(projectPath: string): void {
  try {
    if (readdirSync(projectPath).length === 0) rmdirSync(projectPath);
  } catch {
    // Missing/non-empty/unreadable: leave it for the next prune.
  }
}

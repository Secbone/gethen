/**
 * Size/age/key-registry observation for the local DSH session store.
 *
 * DSH keeps one directory per session under `$DSH_HOME/sessions/<project>/<id>/`
 * and one metadata record per session under
 * `$DSH_HOME/storages/session_projcache/sessions/<id>.json`. The scanner here
 * is intentionally filesystem-only (no decompression) so it stays cheap enough
 * for a Worker `hello`/status refresh and for the Master aggregate.
 */
import { lstatSync, readdirSync, readFileSync, type Stats } from "node:fs";
import { homedir } from "node:os";
import { join, resolve, sep } from "node:path";

const DAY_MS = 24 * 60 * 60 * 1000;

/** One session directory discovered on disk. */
export interface SessionDirectoryEntry {
  sessionId: string;
  project: string;
  path: string;
  /** Sum of file sizes below the session directory. */
  bytes: number;
  /** Newest mtime of the directory or any file below it (epoch ms). */
  lastModifiedMs: number;
}

/** Result of scanning `$DSH_HOME/sessions`. */
export interface SessionDirectoryScan {
  home: string;
  sessions: SessionDirectoryEntry[];
  totalBytes: number;
  /** Epoch ms of the least-recently-modified session, when any exist. */
  oldestMs?: number;
  /** Count of unreadable directories/files encountered (best effort scan). */
  unreadable: number;
}

/** Session-store observation reported by a Worker and aggregated by the Master. */
export interface DshSessionStats {
  /** Number of session directories under the local DSH home. */
  sessions: number;
  /** Total size of those directories, in bytes. */
  totalBytes: number;
  /** Age in days of the least recently modified session (absent when none). */
  oldestDays?: number;
  /** Entries in the durable `key -> sessionId` registries under the state dir. */
  keyRegistryEntries: number;
  /** When the observation was taken (RFC 3339); absent on a zero snapshot. */
  observedAt?: string;
}

/** A key-registry entry (one durable session key mapping). */
export interface SessionKeyRegistryEntry {
  file: string;
  key: string;
  sessionId: string;
}

export interface SessionKeyRegistryScan {
  files: string[];
  entries: SessionKeyRegistryEntry[];
  /** Distinct session ids referenced by a registry (safe to reuse). */
  sessionIds: Set<string>;
}

/** `$DSH_HOME` (or `~/.dsh`) for the current machine. */
export function resolveDshHome(home?: string): string {
  return home ?? process.env.DSH_HOME ?? join(homedir(), ".dsh");
}

/** The session-directory root for a DSH home. */
export function sessionsRoot(home?: string): string {
  return join(resolveDshHome(home), "sessions");
}

/**
 * Scan the session directories without reading logs. A session is one
 * immediate child directory of each project directory. Symlinks are measured
 * but never followed, so a prune can never walk outside the DSH home.
 */
export function scanSessionDirectories(home?: string): SessionDirectoryScan {
  const dshHome = resolveDshHome(home);
  const root = join(dshHome, "sessions");
  const sessions: SessionDirectoryEntry[] = [];
  let unreadable = 0;
  let projectNames: string[] = [];
  try {
    projectNames = readdirSync(root);
  } catch {
    return { home: dshHome, sessions, totalBytes: 0, unreadable: 0 };
  }
  for (const project of projectNames) {
    const projectPath = join(root, project);
    let sessionNames: string[] = [];
    try {
      if (!lstatSync(projectPath).isDirectory()) continue;
      sessionNames = readdirSync(projectPath);
    } catch {
      unreadable += 1;
      continue;
    }
    for (const sessionId of sessionNames) {
      const sessionPath = join(projectPath, sessionId);
      try {
        // Only directories are sessions; stray files in a project dir are ignored.
        if (!lstatSync(sessionPath).isDirectory()) continue;
      } catch {
        unreadable += 1;
        continue;
      }
      const measured = measureTree(sessionPath);
      if (measured === undefined) {
        unreadable += 1;
        continue;
      }
      sessions.push({
        sessionId,
        project,
        path: sessionPath,
        bytes: measured.bytes,
        lastModifiedMs: measured.lastModifiedMs,
      });
    }
  }
  sessions.sort((left, right) => left.lastModifiedMs - right.lastModifiedMs);
  const totalBytes = sessions.reduce((sum, entry) => sum + entry.bytes, 0);
  const oldestMs = sessions.length === 0 ? undefined : sessions[0]?.lastModifiedMs;
  return {
    home: dshHome,
    sessions,
    totalBytes,
    ...(oldestMs === undefined ? {} : { oldestMs }),
    unreadable,
  };
}

interface TreeMeasurement {
  bytes: number;
  lastModifiedMs: number;
}

/** Size and newest mtime of one tree; `undefined` when even the root is unreadable. */
function measureTree(path: string): TreeMeasurement | undefined {
  let latest: number;
  try {
    latest = lstatSync(path).mtimeMs;
  } catch {
    return undefined;
  }
  let bytes = 0;
  walk(
    path,
    (stat, isDirectory) => {
      if (!isDirectory) bytes += stat.size;
      if (stat.mtimeMs > latest) latest = stat.mtimeMs;
    },
    () => undefined,
  );
  return { bytes, lastModifiedMs: latest };
}

/**
 * Depth-first walk that never follows symlinks. Callbacks receive the lstat
 * result; failures are reported once so callers can count a degraded scan.
 */
function walk(
  path: string,
  visit: (stat: Stats, isDirectory: boolean) => void,
  onError: () => void,
): void {
  let entries: string[];
  try {
    entries = readdirSync(path);
  } catch {
    onError();
    return;
  }
  for (const name of entries) {
    const child = join(path, name);
    let stat: Stats;
    try {
      stat = lstatSync(child);
    } catch {
      onError();
      continue;
    }
    const isDirectory = stat.isDirectory();
    visit(stat, isDirectory);
    if (isDirectory && !stat.isSymbolicLink()) walk(child, visit, onError);
  }
}

/**
 * Collect the observation a Worker reports (and the Master aggregates):
 * session directory count/size/oldest age plus key-registry entries.
 * Best effort: a missing/unreadable store reports zeros instead of throwing.
 */
export function collectSessionStats(
  options: {
    home?: string;
    /** Directory holding `*.sessions.json` key registries (worker state dir). */
    stateDir?: string;
    now?: Date;
  } = {},
): DshSessionStats {
  const scan = scanSessionDirectories(options.home);
  const now = (options.now ?? new Date()).getTime();
  const oldestDays =
    scan.oldestMs === undefined
      ? undefined
      : Math.max(0, Math.round(((now - scan.oldestMs) / DAY_MS) * 10) / 10);
  const keyRegistryEntries =
    options.stateDir === undefined ? 0 : scanSessionKeyRegistries(options.stateDir).entries.length;
  return {
    sessions: scan.sessions.length,
    totalBytes: scan.totalBytes,
    ...(oldestDays === undefined ? {} : { oldestDays }),
    keyRegistryEntries,
    observedAt: new Date(now).toISOString(),
  };
}

/** Every `*.sessions.json` file in a worker state directory, sorted. */
export function sessionKeyRegistryFiles(stateDir: string): string[] {
  try {
    return readdirSync(stateDir)
      .filter((name) => name.endsWith(".sessions.json"))
      .sort()
      .map((name) => join(stateDir, name));
  } catch {
    return [];
  }
}

/** Parse one registry file; a missing/corrupt file is an empty map. */
export function readSessionKeyRegistry(file: string): Map<string, string> {
  const keys = new Map<string, string>();
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(file, "utf8"));
  } catch {
    return keys;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return keys;
  for (const [key, sessionId] of Object.entries(parsed)) {
    if (typeof sessionId === "string" && sessionId.length > 0) keys.set(key, sessionId);
  }
  return keys;
}

/** Read every key registry under a worker state directory. */
export function scanSessionKeyRegistries(stateDir: string): SessionKeyRegistryScan {
  const files = sessionKeyRegistryFiles(stateDir);
  const entries: SessionKeyRegistryEntry[] = [];
  const sessionIds = new Set<string>();
  for (const file of files) {
    for (const [key, sessionId] of readSessionKeyRegistry(file)) {
      entries.push({ file, key, sessionId });
      sessionIds.add(sessionId);
    }
  }
  return { files, entries, sessionIds };
}

/** True when `target` is `root` or one of its descendants (symlink-free lexical check). */
export function isWithin(root: string, target: string): boolean {
  const normalizedRoot = resolve(root);
  const normalizedTarget = resolve(target);
  return (
    normalizedTarget === normalizedRoot || normalizedTarget.startsWith(`${normalizedRoot}${sep}`)
  );
}

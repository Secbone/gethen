/**
 * DSH session-store aggregation for `gethen_status` and `/health`.
 *
 * The Master reports its *own* local DSH session count and the latest values
 * each Worker sampled. It never opens a Worker filesystem: Workers send the
 * observation in their `hello`/manifest and refresh it over the same link.
 */
import { collectSessionStats, type DshSessionStats } from "@gethen/dsh-sessions";
import type { WorkerSessionStats } from "@gethen/worker-bridge";
import type { WorkerSnapshot } from "./fleet";

/** One side's session observation (Master local or a Worker). */
export type SessionStats = DshSessionStats;

/**
 * Aggregated view. `master` is this host's own DSH home; `workers` are the
 * latest Worker-reported values. The totals intentionally add both: on a
 * single-machine deployment the same DSH home may be counted twice, which is
 * why the split is always kept explicit.
 */
export interface SessionsStatus {
  master: SessionStats;
  totalSessions: number;
  totalBytes: number;
  /** Age of the oldest session across the Master and reporting Workers. */
  oldestDays?: number;
  keyRegistryEntries: number;
  /** Number of Workers that reported an observation. */
  reportingWorkers: number;
  workers: Array<{ worker: string } & WorkerSessionStats>;
}

/** A zeroed observation for hosts/tests that have no DSH store. */
export function emptySessionStats(): SessionStats {
  return { sessions: 0, totalBytes: 0, keyRegistryEntries: 0 };
}

/** A zeroed aggregate (fake mode, closed runtime, or missing DSH home). */
export function emptySessionsStatus(): SessionsStatus {
  return {
    master: emptySessionStats(),
    totalSessions: 0,
    totalBytes: 0,
    keyRegistryEntries: 0,
    reportingWorkers: 0,
    workers: [],
  };
}

/**
 * Sample the Master's local DSH home. `stateDir` is omitted on purpose: the
 * Master has no Worker key registry; Worker entries are reported separately.
 */
export function localSessionStats(options: { home?: string; now?: Date } = {}): SessionStats {
  return collectSessionStats(options);
}

/** Combine the Master observation with the reporting Workers' latest values. */
export function aggregateSessions(
  master: SessionStats,
  workers: readonly WorkerSnapshot[],
): SessionsStatus {
  const reported: Array<{ worker: string } & WorkerSessionStats> = [];
  for (const snapshot of workers) {
    if (snapshot.sessions === undefined) continue;
    reported.push({ worker: snapshot.id, ...snapshot.sessions });
  }
  reported.sort((left, right) => left.worker.localeCompare(right.worker));
  let totalSessions = master.sessions;
  let totalBytes = master.totalBytes;
  let keyRegistryEntries = master.keyRegistryEntries;
  let oldestDays = master.oldestDays;
  for (const entry of reported) {
    totalSessions += entry.sessions;
    totalBytes += entry.totalBytes;
    keyRegistryEntries += entry.keyRegistryEntries;
    if (entry.oldestDays !== undefined) {
      oldestDays =
        oldestDays === undefined ? entry.oldestDays : Math.max(oldestDays, entry.oldestDays);
    }
  }
  return {
    master,
    totalSessions,
    totalBytes,
    ...(oldestDays === undefined ? {} : { oldestDays }),
    keyRegistryEntries,
    reportingWorkers: reported.length,
    workers: reported,
  };
}

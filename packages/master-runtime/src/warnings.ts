/**
 * Operator warnings derived from live health counters. Warnings never change
 * execution; they surface degrades early on `/health`, `gethen_status` and the
 * Master's status tools.
 */
import type { BridgeSettlementMetrics } from "@gethen/bridge-mailbox";
import type { SchedulingMetrics, WorkerSnapshot } from "./fleet";

export type RuntimeWarningCode =
  | "recovered_without_report"
  | "mailbox_dead"
  | "worker_offline"
  | "queue_timeout"
  | "worker_version_mismatch";

export interface RuntimeWarning {
  code: RuntimeWarningCode;
  message: string;
  count: number;
}

/** Warn when a counter reaches the threshold; every threshold defaults to 1. */
export interface WarningThresholds {
  recoveredWithoutReport?: number;
  mailboxDead?: number;
  offlineWorkers?: number;
  queueTimeouts?: number;
  /** Threshold for build-commit skew; defaults to 1 (warn on any skew). */
  workerVersionMismatch?: number;
}

export interface WarningInput {
  settlements: BridgeSettlementMetrics;
  mailbox: { dead: number };
  workers: readonly WorkerSnapshot[];
  scheduling: SchedulingMetrics;
  /** Master's expected build commit; `unknown` disables expected-only checks. */
  expectedBuildCommit?: string;
}

function threshold(value: number | undefined): number {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : 1;
}

/** Build the warning list for one health snapshot. */
export function runtimeWarnings(
  input: WarningInput,
  thresholds: WarningThresholds = {},
): RuntimeWarning[] {
  const warnings: RuntimeWarning[] = [];
  const recovered = threshold(thresholds.recoveredWithoutReport);
  if (input.settlements.recoveredWithoutReport >= recovered) {
    warnings.push({
      code: "recovered_without_report",
      count: input.settlements.recoveredWithoutReport,
      message:
        "a Worker status recovery had no retained report content; " +
        "receipts may contain a placeholder summary",
    });
  }
  const dead = threshold(thresholds.mailboxDead);
  if (input.mailbox.dead >= dead) {
    warnings.push({
      code: "mailbox_dead",
      count: input.mailbox.dead,
      message: `${input.mailbox.dead} mailbox message(s) are dead-lettered; inspect or replay them`,
    });
  }
  const offline = threshold(thresholds.offlineWorkers);
  const offlineCount = input.workers.filter(
    (worker) => worker.registered && worker.health === "offline",
  ).length;
  if (offlineCount >= offline) {
    warnings.push({
      code: "worker_offline",
      count: offlineCount,
      message: `${offlineCount} registered Worker(s) are offline`,
    });
  }
  const timeouts = threshold(thresholds.queueTimeouts);
  if (input.scheduling.queueTimeouts >= timeouts) {
    warnings.push({
      code: "queue_timeout",
      count: input.scheduling.queueTimeouts,
      message: "at least one dispatch hit ROUTER_QUEUE_TIMEOUT; check Worker capacity",
    });
  }
  const expected = input.expectedBuildCommit?.trim() ?? "unknown";
  const commits = input.workers
    .filter((worker) => worker.registered)
    .map((worker) => worker.build?.commit ?? "unknown");
  const unique = [...new Set(commits)];
  const skew =
    unique.length > 1 || (expected !== "unknown" && unique.some((commit) => commit !== expected));
  const versionThreshold = threshold(thresholds.workerVersionMismatch);
  if (skew && commits.length >= versionThreshold) {
    warnings.push({
      code: "worker_version_mismatch",
      count: unique.length,
      message:
        `Worker build commits ${JSON.stringify(unique)} do not match expected "${expected}"; ` +
        "upgrade stale Workers or set routing.rejectStaleWorkers",
    });
  }
  return warnings;
}

/**
 * Aggregates of Assignment receipts, the evidence used by the panel, the canary
 * monitor and evolution reports. Pure: no IO, no clock.
 */

export type ReceiptOutcome = "completed" | "failed" | "aborted";

export interface ReceiptUsage {
  inputTokens: number;
  outputTokens: number;
  costUsd?: number;
}

/** The subset of an Assignment receipt that evidence math needs. */
export interface ReceiptLike {
  worker: string;
  packDigest: string;
  outcome: ReceiptOutcome;
  at: string;
  usage?: ReceiptUsage;
}

export interface WorkerReceiptSummary {
  total: number;
  completed: number;
  successRate: number;
}

export interface ReceiptSummary {
  total: number;
  completed: number;
  failed: number;
  aborted: number;
  successRate: number;
  byWorker: Record<string, WorkerReceiptSummary>;
}

/** Counts receipts by outcome, overall and per worker. */
export function summarizeReceipts(receipts: readonly ReceiptLike[]): ReceiptSummary {
  const byWorker: Record<string, WorkerReceiptSummary> = {};
  let completed = 0;
  let failed = 0;
  let aborted = 0;
  for (const receipt of receipts) {
    if (receipt.outcome === "completed") completed += 1;
    else if (receipt.outcome === "failed") failed += 1;
    else aborted += 1;

    const group = byWorker[receipt.worker] ?? { total: 0, completed: 0, successRate: 0 };
    group.total += 1;
    if (receipt.outcome === "completed") group.completed += 1;
    group.successRate = rate(group.completed, group.total);
    byWorker[receipt.worker] = group;
  }
  return {
    total: receipts.length,
    completed,
    failed,
    aborted,
    successRate: rate(completed, receipts.length),
    byWorker,
  };
}

function rate(part: number, total: number): number {
  return total === 0 ? 0 : part / total;
}

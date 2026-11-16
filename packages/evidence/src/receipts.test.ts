import { describe, expect, it } from "vitest";

import { type ReceiptLike, summarizeReceipts } from "./receipts";

const AT = "2026-11-16T06:00:00.000Z";

function receipt(worker: string, outcome: ReceiptLike["outcome"]): ReceiptLike {
  return { worker, packDigest: "sha256:a", outcome, at: AT };
}

describe("summarizeReceipts", () => {
  it("returns zeroed totals for no receipts", () => {
    expect(summarizeReceipts([])).toEqual({
      total: 0,
      completed: 0,
      failed: 0,
      aborted: 0,
      successRate: 0,
      byWorker: {},
    });
  });

  it("counts outcomes and derives the overall success rate", () => {
    const summary = summarizeReceipts([
      receipt("worker-a", "completed"),
      receipt("worker-a", "completed"),
      receipt("worker-a", "failed"),
      receipt("worker-b", "aborted"),
    ]);
    expect(summary).toMatchObject({ total: 4, completed: 2, failed: 1, aborted: 1 });
    expect(summary.successRate).toBe(0.5);
    expect(summarizeReceipts([receipt("worker-a", "failed")]).successRate).toBe(0);
    expect(summarizeReceipts([receipt("worker-a", "aborted")]).successRate).toBe(0);
  });

  it("groups totals and success rates per worker", () => {
    const summary = summarizeReceipts([
      receipt("worker-a", "completed"),
      receipt("worker-a", "failed"),
      receipt("worker-b", "completed"),
      receipt("worker-b", "completed"),
      receipt("worker-b", "aborted"),
    ]);
    expect(summary.byWorker).toEqual({
      "worker-a": { total: 2, completed: 1, successRate: 0.5 },
      "worker-b": { total: 3, completed: 2, successRate: 2 / 3 },
    });
  });

  it("ignores usage and pack digests when aggregating", () => {
    const summary = summarizeReceipts([
      {
        worker: "worker-a",
        packDigest: "sha256:a",
        outcome: "completed",
        at: AT,
        usage: { inputTokens: 10, outputTokens: 20, costUsd: 0.5 },
      },
      {
        worker: "worker-a",
        packDigest: "sha256:b",
        outcome: "completed",
        at: AT,
        usage: { inputTokens: 1, outputTokens: 2 },
      },
    ]);
    expect(summary).toEqual({
      total: 2,
      completed: 2,
      failed: 0,
      aborted: 0,
      successRate: 1,
      byWorker: { "worker-a": { total: 2, completed: 2, successRate: 1 } },
    });
  });
});

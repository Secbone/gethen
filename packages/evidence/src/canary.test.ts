import { describe, expect, it } from "vitest";

import { createCanaryMonitor } from "./canary";
import type { ReceiptLike } from "./receipts";

const AT = "2026-11-16T06:00:00.000Z";

function receipts(
  outcome: ReceiptLike["outcome"],
  count: number,
  worker = "worker-a",
): ReceiptLike[] {
  return Array.from({ length: count }, () => ({
    worker,
    packDigest: outcome === "completed" ? "sha256:stable" : "sha256:canary",
    outcome,
    at: AT,
  }));
}

describe("CanaryMonitor", () => {
  it("does not roll back before the canary has enough samples", () => {
    const monitor = createCanaryMonitor({
      minSamples: 3,
      maxFailureRate: 0,
      maxFailureRateDelta: 0,
    });
    const assessment = monitor.assess(receipts("completed", 10), receipts("failed", 2));
    expect(assessment.rollback).toBe(false);
    expect(assessment.samples).toBe(2);
    expect(assessment.canaryFailureRate).toBe(1);
    expect(assessment).not.toHaveProperty("reason");
  });

  it("rolls back when the canary failure rate crosses the ceiling", () => {
    const monitor = createCanaryMonitor({
      minSamples: 2,
      maxFailureRate: 0.25,
      maxFailureRateDelta: 1,
    });
    const assessment = monitor.assess(receipts("completed", 10), [
      ...receipts("completed", 5),
      ...receipts("failed", 5),
    ]);
    expect(assessment).toMatchObject({
      rollback: true,
      reason: "failure-ceiling",
      stableFailureRate: 0,
      canaryFailureRate: 0.5,
      samples: 10,
    });
  });

  it("rolls back when the canary regresses against the stable baseline", () => {
    const monitor = createCanaryMonitor({
      minSamples: 2,
      maxFailureRate: 0.9,
      maxFailureRateDelta: 0.1,
    });
    const assessment = monitor.assess(receipts("completed", 10), [
      ...receipts("completed", 7),
      ...receipts("failed", 3),
    ]);
    expect(assessment).toMatchObject({
      rollback: true,
      reason: "regression",
      stableFailureRate: 0,
      canaryFailureRate: 0.3,
    });
  });

  it("keeps a healthy canary", () => {
    const monitor = createCanaryMonitor({ minSamples: 5 });
    const assessment = monitor.assess(receipts("completed", 10), receipts("completed", 10));
    expect(assessment).toEqual({
      rollback: false,
      stableFailureRate: 0,
      canaryFailureRate: 0,
      samples: 10,
    });
  });

  it("counts aborted receipts as failures and treats an empty stable side as zero rate", () => {
    const monitor = createCanaryMonitor({
      minSamples: 1,
      maxFailureRate: 0.5,
      maxFailureRateDelta: 0.5,
    });
    const assessment = monitor.assess([], [...receipts("completed", 1), ...receipts("aborted", 2)]);
    expect(assessment).toMatchObject({
      rollback: true,
      reason: "failure-ceiling",
      stableFailureRate: 0,
      canaryFailureRate: 2 / 3,
    });
  });

  it("rejects an invalid policy", () => {
    expect(() => createCanaryMonitor({ minSamples: -1 })).toThrow("minSamples");
    expect(() => createCanaryMonitor({ maxFailureRate: 2 })).toThrow("maxFailureRate");
    expect(() => createCanaryMonitor({ maxFailureRateDelta: Number.NaN })).toThrow(
      "maxFailureRateDelta",
    );
  });
});

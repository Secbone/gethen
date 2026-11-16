/**
 * Canary assessment: decide whether a candidate pack should be rolled back
 * from the receipts produced under it and the stable pack it is compared with.
 *
 * Failure means any outcome other than `completed` (`failed` and `aborted`).
 * The sample gate looks at candidate receipts only; stable receipts are just
 * the baseline rate, and an empty baseline counts as a zero failure rate.
 */
import type { ReceiptLike } from "./receipts";

export interface CanaryPolicy {
  minSamples: number;
  maxFailureRate: number;
  maxFailureRateDelta: number;
}

export const DEFAULT_CANARY_POLICY: CanaryPolicy = {
  minSamples: 20,
  maxFailureRate: 0.25,
  maxFailureRateDelta: 0.1,
};

export type CanaryRollbackReason = "failure-ceiling" | "regression";

export interface CanaryAssessment {
  rollback: boolean;
  reason?: CanaryRollbackReason;
  stableFailureRate: number;
  canaryFailureRate: number;
  samples: number;
}

export interface CanaryMonitor {
  assess(stable: readonly ReceiptLike[], canary: readonly ReceiptLike[]): CanaryAssessment;
}

/** Builds a monitor with policy overrides merged onto `DEFAULT_CANARY_POLICY`. */
export function createCanaryMonitor(policy: Partial<CanaryPolicy> = {}): CanaryMonitor {
  const resolved = resolvePolicy(policy);
  return {
    assess: (stable, canary) => assessCanary(stable, canary, resolved),
  };
}

/** Pure assessment with an explicit policy (defaults to `DEFAULT_CANARY_POLICY`). */
export function assessCanary(
  stable: readonly ReceiptLike[],
  canary: readonly ReceiptLike[],
  policy: CanaryPolicy = DEFAULT_CANARY_POLICY,
): CanaryAssessment {
  const stableFailureRate = failureRate(stable);
  const canaryFailureRate = failureRate(canary);
  const samples = canary.length;
  const rates = { stableFailureRate, canaryFailureRate, samples };

  if (samples < policy.minSamples) return { rollback: false, ...rates };
  if (canaryFailureRate > policy.maxFailureRate) {
    return { rollback: true, reason: "failure-ceiling", ...rates };
  }
  if (canaryFailureRate - stableFailureRate > policy.maxFailureRateDelta) {
    return { rollback: true, reason: "regression", ...rates };
  }
  return { rollback: false, ...rates };
}

function failureRate(receipts: readonly ReceiptLike[]): number {
  if (receipts.length === 0) return 0;
  let failures = 0;
  for (const receipt of receipts) {
    if (receipt.outcome !== "completed") failures += 1;
  }
  return failures / receipts.length;
}

function resolvePolicy(policy: Partial<CanaryPolicy>): CanaryPolicy {
  const resolved: CanaryPolicy = { ...DEFAULT_CANARY_POLICY, ...policy };
  if (!Number.isSafeInteger(resolved.minSamples) || resolved.minSamples < 0) {
    throw new Error("canary minSamples must be a non-negative integer");
  }
  if (
    !Number.isFinite(resolved.maxFailureRate) ||
    resolved.maxFailureRate < 0 ||
    resolved.maxFailureRate > 1
  ) {
    throw new Error("canary maxFailureRate must be within [0, 1]");
  }
  if (
    !Number.isFinite(resolved.maxFailureRateDelta) ||
    resolved.maxFailureRateDelta < 0 ||
    resolved.maxFailureRateDelta > 1
  ) {
    throw new Error("canary maxFailureRateDelta must be within [0, 1]");
  }
  return resolved;
}

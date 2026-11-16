/**
 * Eval gate: compares candidate signals against a baseline per protocol metric.
 *
 * Signals are aggregated into one number per metric as the sample-weighted mean
 * of `value` (weight = `n`), which keeps noisy workers from dominating.
 *
 * A metric `threshold` is an absolute bar the candidate must clear in the
 * metric's improvement direction: `up` requires candidate >= threshold, `down`
 * requires candidate <= threshold. Without a threshold only the delta sign is
 * gated. Missing or non-finite signals fail the gate instead of being skipped.
 */

export type MetricDirection = "up" | "down";

export interface Metric {
  name: string;
  direction: MetricDirection;
  threshold?: number;
}

/** One worker's observation of one metric; `n` is the sample count. */
export interface Signal {
  worker: string;
  metric: string;
  value: number;
  n: number;
}

export interface EvalProtocol {
  id: string;
  version: number;
  metrics: Metric[];
}

export interface MetricDelta {
  metric: string;
  baseline: number;
  candidate: number;
  delta: number;
  ok: boolean;
}

export interface GateResult {
  passed: boolean;
  deltas: MetricDelta[];
  reasons: string[];
}

/** True only when every protocol metric improves or holds its direction. */
export function evaluateGate(
  protocol: EvalProtocol,
  baseline: readonly Signal[],
  candidate: readonly Signal[],
): GateResult {
  const deltas: MetricDelta[] = [];
  const reasons: string[] = [];
  for (const metric of protocol.metrics) {
    const base = aggregate(baseline, metric.name);
    const cand = aggregate(candidate, metric.name);
    if (base.kind === "missing") {
      reasons.push(`missing-baseline:${metric.name}`);
      continue;
    }
    if (base.kind === "invalid") {
      reasons.push(`invalid-baseline:${metric.name}`);
      continue;
    }
    if (cand.kind === "missing") {
      reasons.push(`missing-candidate:${metric.name}`);
      continue;
    }
    if (cand.kind === "invalid") {
      reasons.push(`invalid-candidate:${metric.name}`);
      continue;
    }

    const delta = cand.value - base.value;
    const directionOk = metric.direction === "up" ? delta >= 0 : delta <= 0;
    if (!directionOk) {
      reasons.push(`direction:${metric.name}`);
      deltas.push({
        metric: metric.name,
        baseline: base.value,
        candidate: cand.value,
        delta,
        ok: false,
      });
      continue;
    }
    if (metric.threshold !== undefined) {
      const thresholdOk =
        metric.direction === "up" ? cand.value >= metric.threshold : cand.value <= metric.threshold;
      if (!thresholdOk) {
        reasons.push(`threshold:${metric.name}`);
        deltas.push({
          metric: metric.name,
          baseline: base.value,
          candidate: cand.value,
          delta,
          ok: false,
        });
        continue;
      }
    }
    deltas.push({
      metric: metric.name,
      baseline: base.value,
      candidate: cand.value,
      delta,
      ok: true,
    });
  }
  return { passed: reasons.length === 0, deltas, reasons };
}

type Aggregated = { kind: "ok"; value: number } | { kind: "missing" } | { kind: "invalid" };

/** Weighted mean of all valid signals for `metric`; distinguishes absent vs invalid. */
function aggregate(signals: readonly Signal[], metric: string): Aggregated {
  let matches = 0;
  let weighted = 0;
  let weight = 0;
  for (const signal of signals) {
    if (signal.metric !== metric) continue;
    matches += 1;
    if (!Number.isFinite(signal.value) || !Number.isFinite(signal.n) || signal.n <= 0) continue;
    weighted += signal.value * signal.n;
    weight += signal.n;
  }
  if (matches === 0) return { kind: "missing" };
  if (!Number.isFinite(weight) || weight <= 0 || !Number.isFinite(weighted)) {
    return { kind: "invalid" };
  }
  return { kind: "ok", value: weighted / weight };
}

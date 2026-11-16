import { describe, expect, it } from "vitest";

import {
  type EvalProtocol,
  evaluateGate,
  type Metric,
  type MetricDelta,
  type Signal,
} from "./eval";

function protocol(...metrics: Metric[]): EvalProtocol {
  return { id: "eval-1", version: 1, metrics };
}

function signal(metric: string, value: number, n = 1, worker = "worker-a"): Signal {
  return { worker, metric, value, n };
}

function delta(result: ReturnType<typeof evaluateGate>, metric: string): MetricDelta {
  const found = result.deltas.find((item) => item.metric === metric);
  if (found === undefined) throw new Error(`no delta for ${metric}`);
  return found;
}

describe("evaluateGate", () => {
  it("passes an empty protocol without deltas", () => {
    expect(evaluateGate(protocol(), [], [])).toEqual({ passed: true, deltas: [], reasons: [] });
  });

  it("passes when an up metric improves", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up" }),
      [signal("successRate", 0.5)],
      [signal("successRate", 0.6)],
    );
    expect(result.passed).toBe(true);
    expect(result.reasons).toEqual([]);
    expect(result.deltas).toHaveLength(1);
    const metric = delta(result, "successRate");
    expect(metric).toMatchObject({
      metric: "successRate",
      baseline: 0.5,
      candidate: 0.6,
      ok: true,
    });
    expect(metric.delta).toBeCloseTo(0.1);
  });

  it("passes when a metric holds its direction (delta zero)", () => {
    const up = evaluateGate(
      protocol({ name: "successRate", direction: "up" }),
      [signal("successRate", 0.5)],
      [signal("successRate", 0.5)],
    );
    expect(up.passed).toBe(true);
    expect(delta(up, "successRate").delta).toBe(0);

    const down = evaluateGate(
      protocol({ name: "costUsd", direction: "down" }),
      [signal("costUsd", 0.5)],
      [signal("costUsd", 0.5)],
    );
    expect(down.passed).toBe(true);
    expect(delta(down, "costUsd").ok).toBe(true);
  });

  it("fails when an up metric regresses", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up" }),
      [signal("successRate", 0.7)],
      [signal("successRate", 0.6)],
    );
    expect(result.passed).toBe(false);
    expect(result.reasons).toEqual(["direction:successRate"]);
    const metric = delta(result, "successRate");
    expect(metric).toMatchObject({ ok: false });
    expect(metric.delta).toBeCloseTo(-0.1);
  });

  it("passes when a down metric falls", () => {
    const result = evaluateGate(
      protocol({ name: "costUsd", direction: "down" }),
      [signal("costUsd", 0.5)],
      [signal("costUsd", 0.4)],
    );
    expect(result.passed).toBe(true);
    const metric = delta(result, "costUsd");
    expect(metric).toMatchObject({ ok: true });
    expect(metric.delta).toBeCloseTo(-0.1);
  });

  it("fails when a down metric rises", () => {
    const result = evaluateGate(
      protocol({ name: "costUsd", direction: "down" }),
      [signal("costUsd", 0.5)],
      [signal("costUsd", 0.6)],
    );
    expect(result.passed).toBe(false);
    expect(result.reasons).toEqual(["direction:costUsd"]);
  });

  it("enforces an absolute threshold in the up direction", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up", threshold: 0.9 }),
      [signal("successRate", 0.5)],
      [signal("successRate", 0.8)],
    );
    expect(result.passed).toBe(false);
    expect(result.reasons).toEqual(["threshold:successRate"]);
    expect(delta(result, "successRate").ok).toBe(false);
  });

  it("accepts an up candidate exactly at the threshold", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up", threshold: 0.9 }),
      [signal("successRate", 0.5)],
      [signal("successRate", 0.9)],
    );
    expect(result.passed).toBe(true);
    expect(result.reasons).toEqual([]);
  });

  it("enforces an absolute threshold in the down direction", () => {
    const result = evaluateGate(
      protocol({ name: "costUsd", direction: "down", threshold: 0.2 }),
      [signal("costUsd", 0.5)],
      [signal("costUsd", 0.3)],
    );
    expect(result.passed).toBe(false);
    expect(result.reasons).toEqual(["threshold:costUsd"]);
  });

  it("fails when the baseline signal is missing", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up" }),
      [],
      [signal("successRate", 0.9)],
    );
    expect(result).toEqual({
      passed: false,
      deltas: [],
      reasons: ["missing-baseline:successRate"],
    });
  });

  it("fails when the candidate signal is missing", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up" }),
      [signal("successRate", 0.5)],
      [],
    );
    expect(result).toEqual({
      passed: false,
      deltas: [],
      reasons: ["missing-candidate:successRate"],
    });
  });

  it("fails on non-finite baseline values", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up" }),
      [signal("successRate", Number.NaN)],
      [signal("successRate", 0.9)],
    );
    expect(result.passed).toBe(false);
    expect(result.reasons).toEqual(["invalid-baseline:successRate"]);
  });

  it("fails on candidate samples with no weight", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up" }),
      [signal("successRate", 0.5)],
      [signal("successRate", 0.9, 0)],
    );
    expect(result.passed).toBe(false);
    expect(result.reasons).toEqual(["invalid-candidate:successRate"]);
  });

  it("aggregates multiple worker signals as a sample-weighted mean", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up" }),
      [signal("successRate", 0.4, 10, "worker-a"), signal("successRate", 0.8, 30, "worker-b")],
      [signal("successRate", 0.8, 40, "worker-a")],
    );
    expect(result.passed).toBe(true);
    const metric = delta(result, "successRate");
    expect(metric.baseline).toBeCloseTo(0.7);
    expect(metric.candidate).toBeCloseTo(0.8);
    expect(metric.delta).toBeCloseTo(0.1);
    expect(metric.ok).toBe(true);
  });

  it("only fails the metrics that regress", () => {
    const result = evaluateGate(
      protocol({ name: "successRate", direction: "up" }, { name: "costUsd", direction: "down" }),
      [signal("successRate", 0.5), signal("costUsd", 0.4)],
      [signal("successRate", 0.6), signal("costUsd", 0.5)],
    );
    expect(result.passed).toBe(false);
    expect(result.reasons).toEqual(["direction:costUsd"]);
    expect(result.deltas).toHaveLength(2);
    expect(delta(result, "successRate").ok).toBe(true);
    expect(delta(result, "costUsd").ok).toBe(false);
  });
});

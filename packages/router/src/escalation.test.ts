import type { Escalation, Resolution } from "@gethen/worker-bridge";
import { describe, expect, it } from "vitest";
import { DEFAULT_RESOLUTION_POLICY, gateResolution } from "./escalation";

const escalation: Escalation = {
  id: "e1",
  ticket: "t1",
  assignment: "a1",
  session: "s1",
  kind: "decision",
  question: "merge or rebase?",
  options: [
    { id: "merge", label: "merge" },
    { id: "rebase", label: "rebase" },
  ],
  context: { summary: "branch diverged" },
  blocking: true,
  at: "2026-11-16T00:00:00.000Z",
};

function answer(overrides: Partial<Extract<Resolution, { kind: "answer" }>> = {}): Resolution {
  return {
    id: "r1",
    escalation: "e1",
    kind: "answer",
    option: "rebase",
    text: "rebase onto main",
    confidence: 0.8,
    basis: ["memory"],
    at: "2026-11-16T00:00:01.000Z",
    ...overrides,
  };
}

describe("gateResolution", () => {
  it("accepts a confident answer", () => {
    expect(gateResolution(escalation, answer())).toMatchObject({ action: "accept" });
  });

  it("sends low-confidence answers to a person", () => {
    expect(gateResolution(escalation, answer({ confidence: 0.3 }))).toEqual({
      action: "escalate-human",
      reason: "low-confidence",
    });
  });

  it("always sends permission and scope questions to a person", () => {
    for (const kind of ["permission", "scope"] as const) {
      expect(gateResolution({ ...escalation, kind }, answer({ confidence: 0.99 }))).toEqual({
        action: "escalate-human",
        reason: `policy-requires-human:${kind}`,
      });
    }
  });

  it("rejects a resolution for another escalation", () => {
    expect(gateResolution(escalation, answer({ escalation: "e2" }))).toEqual({
      action: "escalate-human",
      reason: "escalation-mismatch",
    });
  });

  it("treats an explicit uncertainty or drop as human work", () => {
    expect(
      gateResolution(escalation, {
        id: "r2",
        escalation: "e1",
        kind: "escalate",
        to: "human",
        question: "which one?",
        at: "2026-11-16T00:00:01.000Z",
      }),
    ).toEqual({ action: "escalate-human", reason: "master-uncertain" });
    expect(
      gateResolution(escalation, {
        id: "r3",
        escalation: "e1",
        kind: "drop",
        reason: "not worth it",
        at: "2026-11-16T00:00:01.000Z",
      }),
    ).toEqual({ action: "escalate-human", reason: "drop-requires-human" });
  });

  it("accepts a reroute without a person", () => {
    expect(
      gateResolution(escalation, {
        id: "r4",
        escalation: "e1",
        kind: "reroute",
        worker: "w2",
        reason: "needs a different toolchain",
        at: "2026-11-16T00:00:01.000Z",
      }),
    ).toMatchObject({ action: "accept", reason: "reroute" });
  });

  it("honors a stricter policy", () => {
    expect(
      gateResolution(escalation, answer({ confidence: 0.8 }), {
        ...DEFAULT_RESOLUTION_POLICY,
        minConfidence: 0.9,
      }),
    ).toEqual({ action: "escalate-human", reason: "low-confidence" });
  });
});

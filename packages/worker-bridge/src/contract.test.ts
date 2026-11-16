import { describe, expect, it } from "vitest";
import type { DispatchRequest, Escalation, Resolution } from "./types";
import {
  assertDispatch,
  assertEscalation,
  assertResolution,
  BridgeContractError,
} from "./validate";

const dispatch: DispatchRequest = {
  ticket: "t1",
  assignment: "a1",
  worker: "w1",
  workspace: "gethen",
  prompt: "do the work",
  pack: { id: "p" },
};

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

describe("assertDispatch", () => {
  it("accepts a complete request", () => {
    expect(() => assertDispatch(dispatch)).not.toThrow();
  });

  it("rejects a missing prompt", () => {
    expect(() => assertDispatch({ ...dispatch, prompt: "  " })).toThrow(BridgeContractError);
  });
});

describe("assertEscalation", () => {
  it("accepts a valid escalation", () => {
    expect(() => assertEscalation(escalation)).not.toThrow();
  });

  it("rejects duplicate option ids", () => {
    expect(() =>
      assertEscalation({
        ...escalation,
        options: [
          { id: "merge", label: "merge" },
          { id: "merge", label: "again" },
        ],
      }),
    ).toThrow(/duplicate escalation option/);
  });

  it("rejects a default that is not an option", () => {
    expect(() => assertEscalation({ ...escalation, defaultOption: "squash" })).toThrow(
      /defaultOption is not an option/,
    );
  });
});

describe("assertResolution", () => {
  const answer: Resolution = {
    id: "r1",
    escalation: "e1",
    kind: "answer",
    option: "rebase",
    text: "rebase onto main",
    confidence: 0.8,
    basis: ["memory"],
    at: "2026-11-16T00:00:01.000Z",
  };

  it("accepts a valid answer for the escalation", () => {
    expect(() => assertResolution(answer, escalation)).not.toThrow();
  });

  it("rejects an answer for another escalation", () => {
    expect(() => assertResolution({ ...answer, escalation: "e2" }, escalation)).toThrow(
      /expected e1/,
    );
  });

  it("rejects out-of-range confidence and empty basis", () => {
    expect(() => assertResolution({ ...answer, confidence: 1.5 })).toThrow(/within \[0, 1\]/);
    expect(() => assertResolution({ ...answer, basis: [] })).toThrow(/requires a basis/);
  });

  it("requires a question when escalating to a person", () => {
    expect(() =>
      assertResolution({
        id: "r2",
        escalation: "e1",
        kind: "escalate",
        to: "human",
        question: "",
        at: "2026-11-16T00:00:01.000Z",
      }),
    ).toThrow(/requires question/);
  });
});

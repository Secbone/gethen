import { describe, expect, it } from "vitest";
import {
  AssignmentTransitionError,
  canTransition,
  stateAfterEscalation,
  stateAfterResolution,
  transition,
} from "./assignment";

describe("assignment lifecycle", () => {
  it("allows the documented transitions", () => {
    expect(canTransition("running", "waiting")).toBe(true);
    expect(canTransition("waiting", "running")).toBe(true);
    expect(canTransition("waiting", "parked")).toBe(true);
    expect(canTransition("parked", "running")).toBe(true);
    expect(canTransition("running", "ended")).toBe(true);
    expect(canTransition("parked", "waiting")).toBe(false);
    expect(canTransition("ended", "running")).toBe(false);
  });

  it("rejects an invalid move", () => {
    expect(() => transition("ended", "running")).toThrow(AssignmentTransitionError);
    expect(transition("running", "waiting")).toBe("waiting");
  });

  it("waits only for a blocking escalation", () => {
    expect(stateAfterEscalation({ blocking: true })).toBe("waiting");
    expect(stateAfterEscalation({ blocking: false })).toBe("running");
  });

  it("parks when the Master defers to a person", () => {
    expect(
      stateAfterResolution({
        action: "accept",
        resolution: {} as never,
        reason: "confident-answer",
      }),
    ).toBe("running");
    expect(stateAfterResolution({ action: "escalate-human", reason: "low-confidence" })).toBe(
      "parked",
    );
  });
});

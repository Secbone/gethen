import type { Escalation } from "@gethen/worker-bridge";
import type { ResolutionVerdict } from "./escalation";

/**
 * Assignment lifecycle. A blocking escalation ends the Worker's turn instead of
 * holding a model turn open, so `waiting` and `parked` are durable states.
 */
export type AssignmentState = "running" | "waiting" | "parked" | "ended";

const TRANSITIONS: Record<AssignmentState, readonly AssignmentState[]> = {
  running: ["waiting", "ended"],
  waiting: ["running", "parked", "ended"],
  parked: ["running", "ended"],
  ended: [],
};

export class AssignmentTransitionError extends Error {
  constructor(from: AssignmentState, to: AssignmentState) {
    super(`cannot move an assignment from ${from} to ${to}`);
    this.name = "AssignmentTransitionError";
  }
}

export function canTransition(from: AssignmentState, to: AssignmentState): boolean {
  return TRANSITIONS[from].includes(to);
}

export function transition(from: AssignmentState, to: AssignmentState): AssignmentState {
  if (!canTransition(from, to)) throw new AssignmentTransitionError(from, to);
  return to;
}

/** `waiting` while the Master decides; `running` for a non-blocking question. */
export function stateAfterEscalation(escalation: Pick<Escalation, "blocking">): AssignmentState {
  return escalation.blocking ? "waiting" : "running";
}

/** A rejected resolution parks the Assignment until a person answers. */
export function stateAfterResolution(verdict: ResolutionVerdict): AssignmentState {
  return verdict.action === "accept" ? "running" : "parked";
}

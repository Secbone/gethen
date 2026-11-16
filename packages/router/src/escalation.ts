import type { Escalation, EscalationKind, Resolution } from "@gethen/worker-bridge";

export interface ResolutionPolicy {
  /** Minimum confidence for a Master answer to stand without a person. */
  minConfidence: number;
  /** Question kinds that always go to a person, whatever the confidence. */
  alwaysHumanKinds: readonly EscalationKind[];
}

export const DEFAULT_RESOLUTION_POLICY: ResolutionPolicy = {
  minConfidence: 0.6,
  alwaysHumanKinds: ["permission", "scope"],
};

export type ResolutionVerdict =
  | { action: "accept"; resolution: Resolution; reason: string }
  | { action: "escalate-human"; reason: string };

/**
 * Decide whether a Master resolution may stand. The escalation always blocks,
 * so a rejected resolution means the Assignment waits for a person.
 */
export function gateResolution(
  escalation: Escalation,
  proposed: Resolution,
  policy: ResolutionPolicy = DEFAULT_RESOLUTION_POLICY,
): ResolutionVerdict {
  if (proposed.escalation !== escalation.id)
    return { action: "escalate-human", reason: "escalation-mismatch" };
  switch (proposed.kind) {
    case "escalate":
      return { action: "escalate-human", reason: "master-uncertain" };
    case "drop":
      // Abandoning authorized work is a person's decision in this version.
      return { action: "escalate-human", reason: "drop-requires-human" };
    case "reroute":
      return { action: "accept", resolution: proposed, reason: "reroute" };
    case "answer":
      if (policy.alwaysHumanKinds.includes(escalation.kind))
        return { action: "escalate-human", reason: `policy-requires-human:${escalation.kind}` };
      if (!Number.isFinite(proposed.confidence) || proposed.confidence < policy.minConfidence)
        return { action: "escalate-human", reason: "low-confidence" };
      return { action: "accept", resolution: proposed, reason: "confident-answer" };
    default:
      return { action: "escalate-human", reason: "unknown-resolution" };
  }
}

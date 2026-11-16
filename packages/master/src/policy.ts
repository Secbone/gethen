/**
 * P1 single-operator Policy layer: Trigger admission and Mandate revalidation.
 * Pure functions; callers own persistence, the clock and identity.
 *
 * Design notes (docs/design/p1-master-research.md §1.3):
 * - Default deny. Only an allowlisted person's explicit request (mention/direct
 *   or a follow-up on a Thread that already has Tickets) originates explicit
 *   work; only an allowlisted bot originates inferred work.
 * - `acceptMode` narrows inferred work only. `off` refuses it; `observe` and
 *   `auto` admit it as `inferred` — whether inferred work is auto-accepted is a
 *   later engine decision, matching the reference gate semantics.
 * - A Policy change never revokes a live Mandate. It only makes revalidation
 *   fail once an enabled rule or the Mandate's source disappears.
 */
import type {
  AcceptMode,
  Mandate,
  Policy,
  PolicyRule,
  Reason,
  SourceId,
  SourcePolicy,
  Strength,
  Ticket,
  Trigger,
  TriggerFacts,
} from "./model";

/** Input accepted by {@link createPolicy}; missing collections default to empty. */
export interface CreatePolicyInput {
  version: number;
  acceptMode: AcceptMode;
  rules?: PolicyRule[];
  sources?: Record<SourceId, SourcePolicy>;
}

/** Result of gating one Trigger against the current Policy. */
export interface GateResult {
  admissible: boolean;
  strength: Strength;
  /** Why the Trigger was refused; empty when it was admitted. */
  reasons: Reason[];
}

const ACCEPT_MODES: readonly AcceptMode[] = ["off", "observe", "auto"];

/**
 * Conservative default: no Source is admitted and inferred work is off, so
 * nothing can become a Ticket until a real Policy is written. Defined here
 * rather than imported from `store` to keep the policy layer dependency-free.
 */
export const EMPTY_POLICY: Policy = Object.freeze<Policy>({
  version: 1,
  acceptMode: "off",
  rules: [],
  sources: {},
});

function copySources(sources: Record<SourceId, SourcePolicy>): Record<SourceId, SourcePolicy> {
  const copied: Record<SourceId, SourcePolicy> = {};
  for (const [id, source] of Object.entries(sources)) {
    copied[id] = { allowed: [...source.allowed], bots: [...source.bots] };
  }
  return copied;
}

/**
 * Builds a Policy snapshot, validating the fields the store would otherwise
 * accept blindly. The snapshot is shallow-frozen; `rules` and `sources` are
 * copied so later mutation of the input cannot silently change the Policy.
 */
export function createPolicy(input: CreatePolicyInput): Policy {
  if (!Number.isSafeInteger(input.version) || input.version < 1) {
    throw new Error("policy.version must be a positive integer");
  }
  if (!ACCEPT_MODES.includes(input.acceptMode)) {
    throw new Error("policy.acceptMode must be one of off, observe, auto");
  }

  const rules = [...(input.rules ?? [])];
  const seen = new Set<string>();
  for (const rule of rules) {
    if (seen.has(rule.id)) {
      throw new Error(`policy.rules must have unique ids (duplicate: ${rule.id})`);
    }
    seen.add(rule.id);
  }

  return Object.freeze<Policy>({
    version: input.version,
    acceptMode: input.acceptMode,
    rules,
    sources: copySources(input.sources ?? {}),
  });
}

function rejection(trigger: Trigger, facts: TriggerFacts, code: string): GateResult {
  return {
    admissible: false,
    strength: "not_admitted",
    reasons: [
      {
        code,
        values: { source: facts.source, sender: facts.sender },
        triggerId: trigger.id,
      },
    ],
  };
}

function admission(strength: Strength): GateResult {
  return { admissible: true, strength, reasons: [] };
}

/**
 * The deterministic first half of an Eval: may this Trigger originate work, and
 * with what strength? Anything not explicitly admitted is refused by default.
 */
export function gateTrigger(trigger: Trigger, facts: TriggerFacts, policy: Policy): GateResult {
  const source = policy.sources[facts.source];
  if (source === undefined) return rejection(trigger, facts, "not_admitted");

  if (facts.bot) {
    if (!source.bots.includes(facts.sender)) return rejection(trigger, facts, "not_admitted");
    if (policy.acceptMode === "off") return rejection(trigger, facts, "accept_mode_off");
    return admission("inferred");
  }

  if (!source.allowed.includes(facts.sender)) return rejection(trigger, facts, "not_admitted");
  if (facts.addressed !== "none" || facts.followup) return admission("explicit");
  return rejection(trigger, facts, "not_admitted");
}

/**
 * Re-check a Mandate against the current Policy. A Policy bump alone does not
 * invalidate it; it stays valid while its source exists and every rule it
 * matched is still enabled.
 */
export function mandateValid(mandate: Mandate, policy: Policy): boolean {
  if (mandate.withdrawnAt !== undefined) return false;
  if (mandate.policyVersion === policy.version) return true;

  const source = policy.sources[mandate.basis.source];
  if (source === undefined) return false;
  return mandate.matchedRules.every((id) =>
    policy.rules.some((rule) => rule.id === id && rule.enabled),
  );
}

/**
 * Whether an ended Ticket may resume. `stop` is final; beyond it the Mandate
 * must still satisfy the current Policy. An open Ticket has nothing to resume
 * but is validated the same way by the caller.
 */
export function canResume(ticket: Ticket, mandate: Mandate, policy: Policy): boolean {
  if (ticket.outcome === "stopped") return false;
  if (!mandateValid(mandate, policy)) return false;
  return true;
}

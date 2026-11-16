/**
 * Fleet-level scheduling: decide whether a task can REUSE an idle resident
 * slot, must START a new slot on an eligible runtime, has to QUEUE because
 * every candidate is at capacity, or has to PARK because policy excludes it.
 *
 * This is the multi-node layer above `routeWorkers` (the single-worker
 * primitive in `./routing`); it consumes a `FleetSnapshot` from `@gethen/fleet`
 * and never talks to a Worker directly.
 *
 * Hard filters (a candidate that fails one is never returned):
 * - node health must be `online` (degraded/offline are rejected);
 * - runtime must be installed on the node and in `idle` or `busy` state;
 * - `isEligible` must pass (project label, model, GPU, secrets, free quota);
 * - locality policy: a required locality must match, and cloud nodes are
 *   excluded from `locality: "local"` tasks (unless `requireLocality` is off).
 *
 * Soft ranking for START: project affinity, receipt evidence, load headroom and
 * cost class. REUSE ranks idle resident slots by evidence and then load, as the
 * design asks. Everything is deterministic: candidates are walked in node id /
 * runtime kind / runtime name / slot id order and ties break by those ids, so
 * the same snapshot always yields the same decision.
 */
import {
  type FleetSnapshot,
  findSlots,
  isEligible,
  type Node,
  type NodeId,
  type Runtime,
  type Slot,
} from "@gethen/fleet";

import type { EvidenceCount } from "./routing";

/** What a task needs before the fleet may schedule it. */
export interface TaskRequirement {
  /** Task kind, e.g. `code-change`; used for evidence lookup. */
  kind: string;
  /** Workspace the task belongs to; also enables REUSE and resident slots. */
  workspace?: string;
  /** Project id; nodes without the label are excluded, matching profiles preferred. */
  project?: string;
  model?: string;
  /** Required node locality. Cloud nodes never satisfy `local`. */
  locality?: "local" | "cloud";
  gpu?: boolean;
  secrets?: string[];
  /** Slots the task needs; defaults to 1. */
  slots?: number;
}

/** Receipt history for one node or runtime; optional per-kind breakdown. */
export interface ScheduleEvidenceEntry extends EvidenceCount {
  byKind?: Record<string, EvidenceCount>;
}

/** Evidence the scheduler may consult; everything is optional. */
export interface ScheduleEvidence {
  byNode?: Record<string, ScheduleEvidenceEntry>;
  byRuntime?: Record<string, ScheduleEvidenceEntry>;
}

export interface ScheduleOptions {
  /** Operator pin; honored only for candidates that pass the hard filters. */
  preferNode?: NodeId;
  /**
   * When true (default), a task's `locality` is a hard filter. When false it
   * becomes a preference: a mismatched-locality node is still a candidate but
   * loses the locality cost score.
   */
  requireLocality?: boolean;
}

export interface ScheduleWeights {
  affinity: number;
  evidence: number;
  load: number;
  cost: number;
}

export const DEFAULT_SCHEDULE_WEIGHTS: ScheduleWeights = {
  affinity: 0.35,
  evidence: 0.3,
  load: 0.25,
  cost: 0.1,
};

export type ScheduleDecision =
  | { action: "reuse"; node: NodeId; runtime: string; slot: string; reason: string }
  | {
      action: "start";
      node: NodeId;
      runtime: string;
      workspace: string;
      kind: "resident" | "ephemeral";
      reason: string;
    }
  | { action: "queue"; reason: string }
  | { action: "park"; reason: string };

/** Why a task could not be scheduled; every PARK carries one of these codes. */
export type ScheduleParkReason =
  | "no-nodes"
  | "locality-mismatch"
  | "project-not-found"
  | "offline"
  | "no-runtime"
  | "gpu-unavailable"
  | "model-unavailable"
  | "secrets-unavailable"
  | "not-eligible";

interface Candidate {
  node: Node;
  runtime: Runtime;
  evidence: number;
  load: number;
}

interface RankedCandidate extends Candidate {
  score: number;
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(1, Math.max(0, value));
}

function round(value: number): number {
  return Math.round(value * 1e6) / 1e6;
}

/** A runtime can take new slots only while it is serving. */
function serving(runtime: Runtime): boolean {
  return runtime.state === "idle" || runtime.state === "busy";
}

function localityAllowed(
  node: Node,
  requirement: TaskRequirement,
  options: ScheduleOptions,
): boolean {
  if (requirement.locality === undefined) return true;
  if (options.requireLocality === false) return true;
  return node.labels.locality === requirement.locality;
}

function headroomOf(runtime: Runtime): number {
  if (runtime.slots.quota <= 0) return 0;
  return clamp01((runtime.slots.quota - runtime.slots.running) / runtime.slots.quota);
}

function affinityOf(node: Node, runtime: Runtime, requirement: TaskRequirement): number {
  const project = requirement.project;
  if (project === undefined) return 0;
  if (runtime.profile === project) return 1;
  return node.labels.projects.includes(project) ? 0.5 : 0;
}

/** Pooled success rate across the node's and the runtime's receipt history. */
function evidenceRate(
  evidence: ScheduleEvidence | undefined,
  node: Node,
  runtime: Runtime,
  kind: string,
): number {
  if (evidence === undefined) return 0;
  const nodeEntry = evidence.byNode?.[node.id];
  const runtimeEntry = evidence.byRuntime?.[runtime.profile];
  const counts = [
    nodeEntry?.byKind?.[kind] ?? nodeEntry,
    runtimeEntry?.byKind?.[kind] ?? runtimeEntry,
  ].filter((entry): entry is EvidenceCount => entry !== undefined);
  let attempts = 0;
  let successes = 0;
  for (const entry of counts) {
    attempts += entry.attempts;
    successes += entry.successes;
  }
  return attempts > 0 ? clamp01(successes / attempts) : 0;
}

/** Local nodes are the cheap class; a relaxed locality match dominates. */
function costOf(node: Node, requirement: TaskRequirement, options: ScheduleOptions): number {
  if (requirement.locality !== undefined && options.requireLocality === false) {
    return node.labels.locality === requirement.locality ? 1 : 0;
  }
  return node.labels.locality === "local" ? 1 : 0;
}

function eligibilityFor(requirement: TaskRequirement, slots: number) {
  return {
    project: requirement.project,
    model: requirement.model,
    secrets: requirement.secrets,
    gpu: requirement.gpu,
    slots,
  };
}

/**
 * Every (node, runtime) pair that passes the hard filters with the given slot
 * requirement, in deterministic order. `slots: 0` is capacity-blind; it is how
 * QUEUE (candidates exist, all full) is distinguished from PARK (policy).
 */
function candidateRuntimes(
  snapshot: FleetSnapshot,
  requirement: TaskRequirement,
  options: ScheduleOptions,
  evidence: ScheduleEvidence | undefined,
  slots: number,
): Candidate[] {
  const nodes = [...snapshot.nodes].sort((left, right) => left.id.localeCompare(right.id));
  const runtimes = [...snapshot.runtimes].sort(
    (left, right) =>
      left.node.localeCompare(right.node) ||
      left.kind.localeCompare(right.kind) ||
      left.profile.localeCompare(right.profile),
  );
  const candidates: Candidate[] = [];
  for (const node of nodes) {
    if (!localityAllowed(node, requirement, options)) continue;
    for (const runtime of runtimes) {
      if (runtime.node !== node.id) continue;
      if (!isEligible(node, runtime, eligibilityFor(requirement, slots))) continue;
      candidates.push({
        node,
        runtime,
        evidence: evidenceRate(evidence, node, runtime, requirement.kind),
        load: headroomOf(runtime),
      });
    }
  }
  return candidates;
}

/** Eligible candidates exist but all are at capacity. */
function hasFullCandidates(
  snapshot: FleetSnapshot,
  requirement: TaskRequirement,
  options: ScheduleOptions,
  evidence: ScheduleEvidence | undefined,
  required: number,
): boolean {
  return (
    candidateRuntimes(snapshot, requirement, options, evidence, 0).length > 0 &&
    candidateRuntimes(snapshot, requirement, options, evidence, required).length === 0
  );
}

/** Preferred node sorts first without overriding the hard filters. */
function preferOrder(left: Node, right: Node, preferNode: NodeId | undefined): number {
  if (preferNode === undefined) return 0;
  const leftPinned = left.id === preferNode;
  const rightPinned = right.id === preferNode;
  if (leftPinned === rightPinned) return 0;
  return leftPinned ? -1 : 1;
}

function scoreOf(
  candidate: Candidate,
  requirement: TaskRequirement,
  options: ScheduleOptions,
): number {
  const weights = DEFAULT_SCHEDULE_WEIGHTS;
  return round(
    weights.affinity * affinityOf(candidate.node, candidate.runtime, requirement) +
      weights.evidence * candidate.evidence +
      weights.load * candidate.load +
      weights.cost * costOf(candidate.node, requirement, options),
  );
}

/**
 * Judge one task against the fleet snapshot.
 *
 * Order of decisions:
 * 1. REUSE — an idle resident slot in the requested workspace on an eligible
 *    runtime (ranked by evidence, then load, then ids);
 * 2. START — the best-scoring eligible runtime with free quota;
 * 3. QUEUE — candidates exist but every one is at capacity;
 * 4. PARK — policy excludes the task (no node, locality, project, runtime or
 *    label mismatch).
 */
export function scheduleTask(
  snapshot: FleetSnapshot,
  requirement: TaskRequirement,
  evidence?: ScheduleEvidence,
  options: ScheduleOptions = {},
): ScheduleDecision {
  const required = requirement.slots ?? 1;

  if (requirement.workspace !== undefined) {
    const reused = bestReuse(snapshot, requirement, evidence, options);
    if (reused !== undefined) return reused;
  }

  const sized = candidateRuntimes(snapshot, requirement, options, evidence, required);
  if (sized.length > 0) return bestStart(sized, requirement, options);

  if (hasFullCandidates(snapshot, requirement, options, evidence, required)) {
    return { action: "queue", reason: "at-capacity" };
  }

  return { action: "park", reason: parkReason(snapshot, requirement, options) };
}

/** REUSE: idle resident slots in the task's workspace, best first. */
function bestReuse(
  snapshot: FleetSnapshot,
  requirement: TaskRequirement,
  evidence: ScheduleEvidence | undefined,
  options: ScheduleOptions,
): ScheduleDecision | undefined {
  const slots = findSlots(snapshot, {
    workspace: requirement.workspace,
    kind: "resident",
    state: "idle",
  });
  const nodesById = new Map(snapshot.nodes.map((node) => [node.id, node]));
  const candidates: { slot: Slot; node: Node; runtime: Runtime; evidence: number; load: number }[] =
    [];
  for (const slot of slots) {
    const node = nodesById.get(slot.node);
    if (node === undefined) continue;
    if (!localityAllowed(node, requirement, options)) continue;
    const runtime = snapshot.runtimes
      .filter((item) => item.node === slot.node && item.profile === slot.runtime)
      .sort((left, right) => left.kind.localeCompare(right.kind))[0];
    if (runtime === undefined || !serving(runtime)) continue;
    if (!isEligible(node, runtime, eligibilityFor(requirement, 0))) continue;
    candidates.push({
      slot,
      node,
      runtime,
      evidence: evidenceRate(evidence, node, runtime, requirement.kind),
      load: headroomOf(runtime),
    });
  }
  candidates.sort((left, right) => {
    const pinned = preferOrder(left.node, right.node, options.preferNode);
    if (pinned !== 0) return pinned;
    if (right.evidence !== left.evidence) return right.evidence - left.evidence;
    if (right.load !== left.load) return right.load - left.load;
    return (
      left.node.id.localeCompare(right.node.id) ||
      left.runtime.profile.localeCompare(right.runtime.profile) ||
      left.slot.id.localeCompare(right.slot.id)
    );
  });
  const best = candidates[0];
  if (best === undefined) return undefined;
  return {
    action: "reuse",
    node: best.node.id,
    runtime: best.runtime.profile,
    slot: best.slot.id,
    reason: "reuse-idle-slot",
  };
}

function bestStart(
  candidates: Candidate[],
  requirement: TaskRequirement,
  options: ScheduleOptions,
): ScheduleDecision {
  const pinned = options.preferNode;
  const pool =
    pinned === undefined
      ? candidates
      : candidates.filter((candidate) => candidate.node.id === pinned);
  const ranked: RankedCandidate[] = (pool.length > 0 ? pool : candidates)
    .map((candidate) => ({ ...candidate, score: scoreOf(candidate, requirement, options) }))
    .sort(
      (left, right) =>
        right.score - left.score ||
        left.node.id.localeCompare(right.node.id) ||
        left.runtime.kind.localeCompare(right.runtime.kind) ||
        left.runtime.profile.localeCompare(right.runtime.profile),
    );
  const best = ranked[0];
  if (best === undefined) {
    // Unreachable: callers only call this with a non-empty candidate list.
    return { action: "park", reason: "not-eligible" };
  }
  const explicitWorkspace = requirement.workspace !== undefined;
  return {
    action: "start",
    node: best.node.id,
    runtime: best.runtime.profile,
    workspace: requirement.workspace ?? requirement.project ?? "",
    kind: explicitWorkspace ? "resident" : "ephemeral",
    reason: "start-free-capacity",
  };
}

/** The most specific policy reason why nothing can be scheduled. */
function parkReason(
  snapshot: FleetSnapshot,
  requirement: TaskRequirement,
  options: ScheduleOptions,
): ScheduleParkReason {
  if (snapshot.nodes.length === 0) return "no-nodes";
  const nodes = snapshot.nodes.filter((node) => localityAllowed(node, requirement, options));
  if (nodes.length === 0) return "locality-mismatch";

  const project = requirement.project;
  if (project !== undefined && !nodes.some((node) => node.labels.projects.includes(project))) {
    return "project-not-found";
  }

  const online = nodes.filter((node) => node.health === "online");
  if (online.length === 0) return "offline";
  const nodesById = new Map(online.map((node) => [node.id, node]));
  const runtimes = snapshot.runtimes.filter(
    (runtime) => nodesById.get(runtime.node)?.labels.runtimes.includes(runtime.kind) === true,
  );
  if (!runtimes.some(serving)) return "no-runtime";

  if (requirement.gpu === true && !online.some((node) => node.labels.gpu === true)) {
    return "gpu-unavailable";
  }

  const model = requirement.model;
  if (model !== undefined && !runtimes.some((runtime) => runtime.models.includes(model))) {
    return "model-unavailable";
  }

  const secrets = requirement.secrets;
  if (
    secrets !== undefined &&
    !online.some((node) =>
      secrets.every((secret) => node.labels.secrets?.includes(secret) === true),
    )
  ) {
    return "secrets-unavailable";
  }

  return "not-eligible";
}

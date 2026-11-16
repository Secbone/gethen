/**
 * Read-side helpers over a `FleetSnapshot`: slot lookup, node load and
 * eligibility of a runtime for a unit of work. Pure; no IO, no clock.
 */
import type {
  FleetSnapshot,
  Node,
  NodeId,
  Runtime,
  RuntimeKind,
  Slot,
  SlotKind,
  SlotState,
} from "./model";

export interface SlotQuery {
  workspace?: string;
  kind?: SlotKind;
  state?: SlotState;
}

/** Slots matching every given filter, in deterministic (node, runtime, id) order. */
export function findSlots(snapshot: FleetSnapshot, query: SlotQuery = {}): Slot[] {
  return snapshot.slots
    .filter(
      (slot) =>
        (query.workspace === undefined || slot.workspace === query.workspace) &&
        (query.kind === undefined || slot.kind === query.kind) &&
        (query.state === undefined || slot.state === query.state),
    )
    .sort(
      (left, right) =>
        left.node.localeCompare(right.node) ||
        left.runtime.localeCompare(right.runtime) ||
        left.id.localeCompare(right.id),
    );
}

export interface NodeLoad {
  node: NodeId;
  /** Runtimes reported on the node. */
  runtimes: number;
  /** Sum of running slots across the node's runtimes. */
  running: number;
  /** Sum of slot quotas across the node's runtimes. */
  quota: number;
  /** `running / quota`, or 0 when the node reports no quota. */
  utilization: number;
}

/** Aggregate execution load of one node; a node absent from the snapshot is empty. */
export function nodeLoad(snapshot: FleetSnapshot, nodeId: NodeId): NodeLoad {
  const runtimes = snapshot.runtimes.filter((runtime) => runtime.node === nodeId);
  let running = 0;
  let quota = 0;
  for (const runtime of runtimes) {
    running += runtime.slots.running;
    quota += runtime.slots.quota;
  }
  return {
    node: nodeId,
    runtimes: runtimes.length,
    running,
    quota,
    utilization: quota > 0 ? running / quota : 0,
  };
}

export interface EligibilityRequirement {
  /** Project id the work belongs to; must be in the node's project labels. */
  project?: string;
  /** Runtime kind the work needs. */
  kind?: RuntimeKind;
  /** Model that must be available in the runtime. */
  model?: string;
  /** Secret names the work needs; all must be available on the node. */
  secrets?: string[];
  /** The work needs a GPU. */
  gpu?: boolean;
  /** Free slots required; defaults to 1. */
  slots?: number;
}

/**
 * Whether `runtime` on `node` may accept a unit of work with `requirement`.
 * Fails closed: an offline/degraded node, a runtime that is not idle/busy, a
 * full quota or a missing label/model/secret all make it ineligible.
 */
export function isEligible(
  node: Node,
  runtime: Runtime,
  requirement: EligibilityRequirement = {},
): boolean {
  if (runtime.node !== node.id) return false;
  if (node.health !== "online") return false;
  if (runtime.state !== "idle" && runtime.state !== "busy") return false;
  if (!node.labels.runtimes.includes(runtime.kind)) return false;
  if (requirement.kind !== undefined && requirement.kind !== runtime.kind) return false;
  if (requirement.project !== undefined && !node.labels.projects.includes(requirement.project)) {
    return false;
  }
  if (requirement.model !== undefined && !runtime.models.includes(requirement.model)) return false;
  if (requirement.gpu === true && node.labels.gpu !== true) return false;
  if (
    requirement.secrets !== undefined &&
    !requirement.secrets.every((secret) => node.labels.secrets?.includes(secret) === true)
  ) {
    return false;
  }
  const required = requirement.slots ?? 1;
  return runtime.slots.quota - runtime.slots.running >= required;
}

/**
 * Fleet resource model: nodes (Worker machines), the runtimes they host, and
 * the execution slots inside those runtimes. Pure data; no IO, no clock.
 *
 * Identity rules the reconciler relies on:
 * - A runtime is identified by `(node, kind, profile)`; `profile` is its name.
 * - A slot is identified by its id, but desired state matches *resident* slots
 *   by `(node, runtime profile, workspace)` because the Worker assigns slot ids.
 * - A node is identified by its id, which is also its sort/display name.
 */

export type NodeId = string;

/** Execution runtime a node can host. DSH is the first executor. */
export type RuntimeKind = "dsh" | "opencode" | "pi" | "codex";

export type Locality = "local" | "cloud";

/** Whether the Master currently trusts the node to take work. */
export type NodeHealth = "online" | "degraded" | "offline";

export interface NodeLabels {
  locality: Locality;
  /** Project ids this node is allowed to work on. */
  projects: string[];
  /** Runtime kinds installed on the node. */
  runtimes: RuntimeKind[];
  gpu?: boolean;
  /** Secret names available to workloads on this node. */
  secrets?: string[];
}

export interface NodeResources {
  cpu: number;
  memMb: number;
  diskMb: number;
}

/** A Worker machine, as reported by its heartbeat. */
export interface Node {
  id: NodeId;
  labels: NodeLabels;
  resources: NodeResources;
  health: NodeHealth;
  /** RFC 3339 timestamp of the last heartbeat the Master accepted. */
  lastHeartbeatAt: string;
}

export type RuntimeState = "stopped" | "starting" | "idle" | "busy" | "draining" | "failed";

export interface RuntimeSlots {
  /** Slots currently executing work. */
  running: number;
  /** Maximum concurrent slots the runtime is configured for. */
  quota: number;
}

/** One long-lived execution process (for DSH: one profile) on a node. */
export interface Runtime {
  node: NodeId;
  kind: RuntimeKind;
  /** Runtime name within the node (for DSH: the profile). */
  profile: string;
  state: RuntimeState;
  models: string[];
  slots: RuntimeSlots;
}

export type SlotKind = "resident" | "ephemeral";
export type SlotState = "idle" | "busy" | "draining" | "closed";

/** A concurrency slot: one session's worth of execution capacity. */
export interface Slot {
  id: string;
  node: NodeId;
  /** Profile name of the runtime that owns the slot. */
  runtime: string;
  workspace: string;
  kind: SlotKind;
  state: SlotState;
  sessionId?: string;
  /** Lifetime hint for ephemeral slots; Workers enforce expiry. */
  ttlMs?: number;
}

/** One runtime a node should be running. */
export interface DesiredRuntime {
  kind: RuntimeKind;
  /** Runtime name within the node (for DSH: the profile). */
  profile: string;
  models?: string[];
  /** Desired concurrency quota; informational for the R4 reconciler. */
  quota?: number;
}

/** One resident slot a node should keep open. */
export interface DesiredSlot {
  /** Profile name of the runtime that should host the slot. */
  runtime: string;
  workspace: string;
  /** Capability Pack id handed to the Worker with `slot.create`. */
  pack: string;
}

export interface DesiredNode {
  runtimes: DesiredRuntime[];
  /** Resident slots expected on the node; ephemeral slots are never desired. */
  slots: DesiredSlot[];
}

/** The fleet's expected state, one entry per desired node. */
export interface DesiredState {
  nodes: Record<NodeId, DesiredNode>;
}

/** The fleet's observed state, as reported by Workers and the Master's registry. */
export interface FleetSnapshot {
  nodes: Node[];
  runtimes: Runtime[];
  slots: Slot[];
}

/** Stable key for a runtime within a node. */
export function runtimeKey(kind: RuntimeKind, profile: string): string {
  return `${kind}\u0000${profile}`;
}

/** Stable key for the resident-slot identity the reconciler matches on. */
export function residentSlotKey(runtime: string, workspace: string): string {
  return `${runtime}\u0000${workspace}`;
}

/** A slot still exists (and may hold a session) unless it is closed. */
export function isLiveSlot(slot: Slot): boolean {
  return slot.state !== "closed";
}

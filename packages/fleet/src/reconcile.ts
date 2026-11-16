/**
 * Desired-state reconciler (Kubernetes-controller style): compares the fleet's
 * expected state with an observed snapshot and returns the lifecycle
 * `ControlOp`s a Worker must execute. Pure; no IO, no clock, no randomness.
 *
 * Rules (per node, node ids ascending):
 * - runtime missing / stopped / failed → `runtime.start`;
 * - desired runtime starting, idle or busy → no action;
 * - runtime not desired and live → `runtime.stop` (graceful, `drain: true`);
 * - runtime not desired and draining with no running slot → `runtime.stop`
 *   finishes the transition (`drain-complete`);
 * - desired resident slot missing (no live idle/busy resident with the same
 *   runtime profile and workspace) → `slot.create`;
 * - desired resident slot kept idle/busy → no action;
 * - undesired slot idle → `slot.close`; busy → `slot.drain` (two-phase stop);
 * - draining slot → `slot.close` (`drain-complete`);
 * - ephemeral slots are never part of desired state → closed/drained as expired.
 *
 * Ordering is deterministic: nodes by id, runtimes by profile then kind, slot
 * creations by runtime then workspace, extra slots by runtime/workspace/id.
 * The same inputs therefore always produce the same plan (idempotent), and a
 * converged fleet produces an empty plan.
 */
import type { ControlOp } from "@gethen/worker-protocol";

import type {
  DesiredRuntime,
  DesiredSlot,
  DesiredState,
  FleetSnapshot,
  NodeId,
  Runtime,
  Slot,
} from "./model";
import { isLiveSlot, residentSlotKey, runtimeKey } from "./model";

/** One control op plus why the reconciler wants it. */
export interface Action {
  /**
   * Node the op is addressed to. `ControlOp` itself carries no node field
   * because a Worker only ever receives ops over its own connection.
   */
  node: NodeId;
  op: ControlOp;
  reason: string;
}

/** Runtime name (`profile`) encoded in a runtime key. */
function profileOf(key: string): string {
  return key.slice(key.indexOf("\u0000") + 1);
}

/** Stable ordering for runtime entries: name first, kind as the tie-break. */
function compareRuntimeKeys(left: string, right: string): number {
  return profileOf(left).localeCompare(profileOf(right)) || left.localeCompare(right);
}

function compareSlots(left: Slot, right: Slot): number {
  return (
    left.runtime.localeCompare(right.runtime) ||
    left.workspace.localeCompare(right.workspace) ||
    left.id.localeCompare(right.id)
  );
}

function planRuntimes(
  nodeId: NodeId,
  desiredRuntimes: readonly DesiredRuntime[],
  actual: FleetSnapshot,
  actions: Action[],
): void {
  const desired = new Map<string, DesiredRuntime>();
  for (const runtime of desiredRuntimes)
    desired.set(runtimeKey(runtime.kind, runtime.profile), runtime);
  const current = new Map<string, Runtime>();
  for (const runtime of actual.runtimes) {
    if (runtime.node === nodeId) current.set(runtimeKey(runtime.kind, runtime.profile), runtime);
  }

  const keys = [...new Set([...desired.keys(), ...current.keys()])].sort(compareRuntimeKeys);
  for (const key of keys) {
    const wanted = desired.get(key);
    const running = current.get(key);

    if (wanted !== undefined) {
      if (running === undefined) {
        actions.push({
          node: nodeId,
          op: { op: "runtime.start", name: wanted.profile },
          reason: "runtime-missing",
        });
      } else if (running.state === "stopped") {
        actions.push({
          node: nodeId,
          op: { op: "runtime.start", name: wanted.profile },
          reason: "runtime-stopped",
        });
      } else if (running.state === "failed") {
        actions.push({
          node: nodeId,
          op: { op: "runtime.start", name: wanted.profile },
          reason: "runtime-failed",
        });
      }
      // starting/idle/busy are healthy; draining is transient and finishes
      // on its own, so a desired runtime in those states needs no action.
      continue;
    }

    if (running === undefined || running.state === "stopped") continue;
    if (running.state === "draining") {
      if (running.slots.running === 0) {
        actions.push({
          node: nodeId,
          op: { op: "runtime.stop", name: running.profile },
          reason: "drain-complete",
        });
      }
      continue;
    }
    actions.push({
      node: nodeId,
      op: { op: "runtime.stop", name: running.profile, drain: true },
      reason: "runtime-extra",
    });
  }
}

function planSlots(
  nodeId: NodeId,
  desiredSlots: readonly DesiredSlot[],
  actual: FleetSnapshot,
  actions: Action[],
): void {
  const live = actual.slots
    .filter((slot) => slot.node === nodeId && isLiveSlot(slot))
    .sort(compareSlots);

  const residents = new Map<string, Slot[]>();
  for (const slot of live) {
    if (slot.kind !== "resident") continue;
    const key = residentSlotKey(slot.runtime, slot.workspace);
    const group = residents.get(key);
    if (group === undefined) residents.set(key, [slot]);
    else group.push(slot);
  }

  const kept = new Set<string>();
  const wanted = new Map<string, DesiredSlot>();
  for (const slot of desiredSlots) wanted.set(residentSlotKey(slot.runtime, slot.workspace), slot);

  for (const slot of [...wanted.values()].sort(
    (left, right) =>
      left.runtime.localeCompare(right.runtime) || left.workspace.localeCompare(right.workspace),
  )) {
    const candidates = residents.get(residentSlotKey(slot.runtime, slot.workspace)) ?? [];
    const usable = candidates.find(
      (candidate) => candidate.state === "idle" || candidate.state === "busy",
    );
    if (usable !== undefined) {
      kept.add(usable.id);
      continue;
    }
    actions.push({
      node: nodeId,
      op: {
        op: "slot.create",
        workspace: slot.workspace,
        pack: slot.pack,
        kind: "resident",
      },
      reason: "slot-missing",
    });
  }

  for (const slot of live) {
    if (kept.has(slot.id)) continue;
    const reason = slot.kind === "ephemeral" ? "slot-expired" : "slot-extra";
    if (slot.state === "draining") {
      actions.push({
        node: nodeId,
        op: { op: "slot.close", slot: slot.id },
        reason: "drain-complete",
      });
    } else if (slot.state === "busy") {
      // Drain first, close on the next pass once the session has ended.
      actions.push({
        node: nodeId,
        op: { op: "slot.drain", slot: slot.id },
        reason,
      });
    } else {
      actions.push({
        node: nodeId,
        op: { op: "slot.close", slot: slot.id },
        reason,
      });
    }
  }
}

/**
 * Plan the lifecycle ops that bring `actual` closer to `desired`.
 * Deterministic and idempotent: converged input yields `[]`.
 */
export function planReconcile(desired: DesiredState, actual: FleetSnapshot): Action[] {
  const actions: Action[] = [];
  const knownNodes = new Set(actual.nodes.map((node) => node.id));
  const nodeIds = [
    ...new Set([...Object.keys(desired.nodes), ...actual.nodes.map((node) => node.id)]),
  ].sort();

  for (const nodeId of nodeIds) {
    // A node that never registered cannot be acted on; the router reports it
    // separately instead of the reconciler inventing work for an unknown host.
    if (!knownNodes.has(nodeId)) continue;
    const wanted = desired.nodes[nodeId];
    planRuntimes(nodeId, wanted?.runtimes ?? [], actual, actions);
    planSlots(nodeId, wanted?.slots ?? [], actual, actions);
  }

  return actions;
}

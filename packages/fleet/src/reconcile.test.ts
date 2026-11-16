import { describe, expect, it } from "vitest";

import type {
  DesiredNode,
  DesiredRuntime,
  DesiredSlot,
  DesiredState,
  FleetSnapshot,
  Node,
  Runtime,
  Slot,
} from "./model";
import { planReconcile } from "./reconcile";

const AT = "2026-11-16T12:00:00.000Z";

function node(id: string): Node {
  return {
    id,
    labels: { locality: "local", projects: ["gethen"], runtimes: ["dsh"] },
    resources: { cpu: 8, memMb: 16384, diskMb: 65536 },
    health: "online",
    lastHeartbeatAt: AT,
  };
}

function runtime(nodeId: string, profile: string, overrides: Partial<Runtime> = {}): Runtime {
  return {
    node: nodeId,
    kind: "dsh",
    profile,
    state: "idle",
    models: ["deepseek-v4"],
    slots: { running: 0, quota: 2 },
    ...overrides,
  };
}

function slot(id: string, nodeId: string, profile: string, overrides: Partial<Slot> = {}): Slot {
  return {
    id,
    node: nodeId,
    runtime: profile,
    workspace: "ws-gethen",
    kind: "resident",
    state: "idle",
    ...overrides,
  };
}

function snapshot(parts: Partial<FleetSnapshot> = {}): FleetSnapshot {
  return { nodes: [], runtimes: [], slots: [], ...parts };
}

function desiredRuntime(profile: string, overrides: Partial<DesiredRuntime> = {}): DesiredRuntime {
  return { kind: "dsh", profile, ...overrides };
}

function desiredSlot(profile: string, overrides: Partial<DesiredSlot> = {}): DesiredSlot {
  return { runtime: profile, workspace: "ws-gethen", pack: "pack-gethen", ...overrides };
}

function desired(nodes: Record<string, DesiredNode> = {}): DesiredState {
  return { nodes };
}

function wantedNode(
  runtimes: DesiredRuntime[] = [desiredRuntime("main")],
  slots: DesiredSlot[] = [desiredSlot("main")],
): DesiredNode {
  return { runtimes, slots };
}

describe("planReconcile", () => {
  it("returns an empty plan for a converged fleet", () => {
    const actual = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
      slots: [slot("slot-1", "node-a", "main")],
    });

    expect(planReconcile(desired({ "node-a": wantedNode() }), actual)).toEqual([]);
  });

  it("starts a desired runtime that is missing", () => {
    const actual = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
    });

    expect(
      planReconcile(
        desired({ "node-a": wantedNode([desiredRuntime("main"), desiredRuntime("worker")], []) }),
        actual,
      ),
    ).toEqual([
      { node: "node-a", op: { op: "runtime.start", name: "worker" }, reason: "runtime-missing" },
    ]);
  });

  it("starts a desired runtime that stopped or failed", () => {
    const actual = snapshot({
      nodes: [node("node-a")],
      runtimes: [
        runtime("node-a", "main", { state: "stopped" }),
        runtime("node-a", "worker", { state: "failed" }),
      ],
    });

    expect(
      planReconcile(
        desired({
          "node-a": wantedNode([desiredRuntime("main"), desiredRuntime("worker")], []),
        }),
        actual,
      ),
    ).toEqual([
      { node: "node-a", op: { op: "runtime.start", name: "main" }, reason: "runtime-stopped" },
      { node: "node-a", op: { op: "runtime.start", name: "worker" }, reason: "runtime-failed" },
    ]);
  });

  it("leaves healthy and transient runtime states alone", () => {
    for (const state of ["starting", "idle", "busy", "draining"] as const) {
      const actual = snapshot({
        nodes: [node("node-a")],
        runtimes: [runtime("node-a", "main", { state })],
      });

      expect(
        planReconcile(desired({ "node-a": wantedNode([desiredRuntime("main")], []) }), actual),
      ).toEqual([]);
    }
  });

  it("stops a live runtime that is not desired", () => {
    const actual = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main", { state: "busy" })],
    });

    expect(planReconcile(desired({ "node-a": wantedNode([], []) }), actual)).toEqual([
      {
        node: "node-a",
        op: { op: "runtime.stop", name: "main", drain: true },
        reason: "runtime-extra",
      },
    ]);
    expect(
      planReconcile(
        desired({ "node-a": wantedNode([], []) }),
        snapshot({
          nodes: [node("node-a")],
          runtimes: [runtime("node-a", "main", { state: "stopped" })],
        }),
      ),
    ).toEqual([]);
  });

  it("finishes the stop of an extra draining runtime only when it is idle", () => {
    const withWork = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main", { state: "draining", slots: { running: 1, quota: 2 } })],
    });
    const drained = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main", { state: "draining" })],
    });

    expect(planReconcile(desired({ "node-a": wantedNode([], []) }), withWork)).toEqual([]);
    expect(planReconcile(desired({ "node-a": wantedNode([], []) }), drained)).toEqual([
      {
        node: "node-a",
        op: { op: "runtime.stop", name: "main" },
        reason: "drain-complete",
      },
    ]);
  });

  it("creates a desired resident slot that is missing", () => {
    const actual = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
    });

    expect(planReconcile(desired({ "node-a": wantedNode() }), actual)).toEqual([
      {
        node: "node-a",
        op: {
          op: "slot.create",
          workspace: "ws-gethen",
          pack: "pack-gethen",
          kind: "resident",
        },
        reason: "slot-missing",
      },
    ]);
  });

  it("keeps a desired resident slot while it is idle or busy", () => {
    for (const state of ["idle", "busy"] as const) {
      const actual = snapshot({
        nodes: [node("node-a")],
        runtimes: [runtime("node-a", "main")],
        slots: [slot("slot-1", "node-a", "main", { state })],
      });

      expect(planReconcile(desired({ "node-a": wantedNode() }), actual)).toEqual([]);
    }
  });

  it("closes or drains a resident slot that is no longer desired", () => {
    const closePlan = (state: "idle" | "busy" | "draining") =>
      planReconcile(
        desired({ "node-a": wantedNode([desiredRuntime("main")], []) }),
        snapshot({
          nodes: [node("node-a")],
          runtimes: [runtime("node-a", "main")],
          slots: [slot("slot-1", "node-a", "main", { state })],
        }),
      );

    expect(closePlan("idle")).toEqual([
      { node: "node-a", op: { op: "slot.close", slot: "slot-1" }, reason: "slot-extra" },
    ]);
    expect(closePlan("busy")).toEqual([
      { node: "node-a", op: { op: "slot.drain", slot: "slot-1" }, reason: "slot-extra" },
    ]);
    expect(closePlan("draining")).toEqual([
      { node: "node-a", op: { op: "slot.close", slot: "slot-1" }, reason: "drain-complete" },
    ]);
  });

  it("treats ephemeral slots as expired", () => {
    const plan = (state: "idle" | "busy") =>
      planReconcile(
        desired({ "node-a": wantedNode([desiredRuntime("main")], []) }),
        snapshot({
          nodes: [node("node-a")],
          runtimes: [runtime("node-a", "main")],
          slots: [slot("slot-1", "node-a", "main", { kind: "ephemeral", state, ttlMs: 60_000 })],
        }),
      );

    expect(plan("idle")).toEqual([
      { node: "node-a", op: { op: "slot.close", slot: "slot-1" }, reason: "slot-expired" },
    ]);
    expect(plan("busy")).toEqual([
      { node: "node-a", op: { op: "slot.drain", slot: "slot-1" }, reason: "slot-expired" },
    ]);
  });

  it("replaces a desired resident slot that is draining without closing it twice", () => {
    const actual = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
      slots: [slot("slot-1", "node-a", "main", { state: "draining" })],
    });

    expect(planReconcile(desired({ "node-a": wantedNode() }), actual)).toEqual([
      {
        node: "node-a",
        op: {
          op: "slot.create",
          workspace: "ws-gethen",
          pack: "pack-gethen",
          kind: "resident",
        },
        reason: "slot-missing",
      },
      { node: "node-a", op: { op: "slot.close", slot: "slot-1" }, reason: "drain-complete" },
    ]);
  });

  it("orders actions by node id and then runtime name, deterministically", () => {
    const actual = snapshot({ nodes: [node("node-b"), node("node-a")] });
    const state = desired({
      "node-b": wantedNode([desiredRuntime("main")], []),
      "node-a": wantedNode(
        [
          desiredRuntime("zeta"),
          desiredRuntime("alpha", { kind: "codex" }),
          desiredRuntime("main"),
        ],
        [],
      ),
    });

    expect(planReconcile(state, actual)).toEqual([
      { node: "node-a", op: { op: "runtime.start", name: "alpha" }, reason: "runtime-missing" },
      { node: "node-a", op: { op: "runtime.start", name: "main" }, reason: "runtime-missing" },
      { node: "node-a", op: { op: "runtime.start", name: "zeta" }, reason: "runtime-missing" },
      { node: "node-b", op: { op: "runtime.start", name: "main" }, reason: "runtime-missing" },
    ]);
    expect(planReconcile(state, actual)).toEqual(planReconcile(state, actual));
  });

  it("does not invent work for a desired node that never registered", () => {
    expect(planReconcile(desired({ "node-ghost": wantedNode() }), snapshot())).toEqual([]);
  });

  it("tears down a registered node that is no longer desired", () => {
    const actual = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
      slots: [slot("slot-1", "node-a", "main")],
    });

    expect(planReconcile(desired(), actual)).toEqual([
      {
        node: "node-a",
        op: { op: "runtime.stop", name: "main", drain: true },
        reason: "runtime-extra",
      },
      { node: "node-a", op: { op: "slot.close", slot: "slot-1" }, reason: "slot-extra" },
    ]);
  });

  it("returns an empty plan when there is nothing desired or observed", () => {
    expect(planReconcile(desired(), snapshot())).toEqual([]);
  });

  it("deduplicates duplicate desired slots", () => {
    const actual = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
    });

    expect(
      planReconcile(
        desired({
          "node-a": wantedNode(
            [desiredRuntime("main")],
            [desiredSlot("main"), desiredSlot("main")],
          ),
        }),
        actual,
      ),
    ).toHaveLength(1);
  });
});

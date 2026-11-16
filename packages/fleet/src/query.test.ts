import { describe, expect, it } from "vitest";

import type { FleetSnapshot, Node, Runtime, Slot } from "./model";
import { findSlots, isEligible, nodeLoad } from "./query";

const AT = "2026-11-16T12:00:00.000Z";

function node(overrides: Partial<Node> = {}): Node {
  return {
    id: "node-a",
    labels: {
      locality: "local",
      projects: ["gethen"],
      runtimes: ["dsh"],
      gpu: true,
      secrets: ["github-token"],
    },
    resources: { cpu: 8, memMb: 16384, diskMb: 65536 },
    health: "online",
    lastHeartbeatAt: AT,
    ...overrides,
  };
}

function runtime(overrides: Partial<Runtime> = {}): Runtime {
  return {
    node: "node-a",
    kind: "dsh",
    profile: "main",
    state: "idle",
    models: ["deepseek-v4"],
    slots: { running: 1, quota: 2 },
    ...overrides,
  };
}

function slot(id: string, overrides: Partial<Slot> = {}): Slot {
  return {
    id,
    node: "node-a",
    runtime: "main",
    workspace: "ws-gethen",
    kind: "resident",
    state: "idle",
    ...overrides,
  };
}

function snapshot(overrides: Partial<FleetSnapshot> = {}): FleetSnapshot {
  return { nodes: [], runtimes: [], slots: [], ...overrides };
}

describe("findSlots", () => {
  it("returns every slot in deterministic (node, runtime, id) order", () => {
    const fleet = snapshot({
      slots: [
        slot("slot-b", { node: "node-b" }),
        slot("slot-a"),
        slot("slot-a2", { runtime: "worker" }),
      ],
    });

    expect(findSlots(fleet).map((item) => item.id)).toEqual(["slot-a", "slot-a2", "slot-b"]);
  });

  it("filters by workspace", () => {
    const fleet = snapshot({
      slots: [slot("slot-a"), slot("slot-b", { workspace: "ws-other" })],
    });

    expect(findSlots(fleet, { workspace: "ws-other" }).map((item) => item.id)).toEqual(["slot-b"]);
  });

  it("filters by kind and state together", () => {
    const fleet = snapshot({
      slots: [
        slot("slot-a"),
        slot("slot-b", { state: "busy" }),
        slot("slot-c", { kind: "ephemeral", state: "busy" }),
      ],
    });

    expect(findSlots(fleet, { kind: "resident", state: "busy" }).map((item) => item.id)).toEqual([
      "slot-b",
    ]);
  });

  it("returns an empty list when nothing matches", () => {
    expect(findSlots(snapshot({ slots: [slot("slot-a")] }), { state: "closed" })).toEqual([]);
  });
});

describe("nodeLoad", () => {
  it("aggregates running slots and quota across the node's runtimes", () => {
    const fleet = snapshot({
      runtimes: [
        runtime(),
        runtime({ profile: "worker", state: "busy", slots: { running: 2, quota: 2 } }),
        runtime({ node: "node-b", slots: { running: 3, quota: 4 } }),
      ],
    });

    expect(nodeLoad(fleet, "node-a")).toEqual({
      node: "node-a",
      runtimes: 2,
      running: 3,
      quota: 4,
      utilization: 0.75,
    });
  });

  it("reports zero load for a node absent from the snapshot", () => {
    expect(nodeLoad(snapshot(), "node-a")).toEqual({
      node: "node-a",
      runtimes: 0,
      running: 0,
      quota: 0,
      utilization: 0,
    });
  });

  it("avoids dividing by zero when no runtime reports quota", () => {
    const fleet = snapshot({ runtimes: [runtime({ slots: { running: 0, quota: 0 } })] });

    expect(nodeLoad(fleet, "node-a").utilization).toBe(0);
  });
});

describe("isEligible", () => {
  it("admits an idle runtime on a healthy node with a free slot and matching labels", () => {
    expect(isEligible(node(), runtime(), { project: "gethen", model: "deepseek-v4" })).toBe(true);
  });

  it("rejects nodes that are offline or degraded", () => {
    expect(isEligible(node({ health: "offline" }), runtime())).toBe(false);
    expect(isEligible(node({ health: "degraded" }), runtime())).toBe(false);
  });

  it("rejects a runtime from another node or in a non-serving state", () => {
    expect(isEligible(node(), runtime({ node: "node-b" }))).toBe(false);
    for (const state of ["stopped", "starting", "draining", "failed"] as const) {
      expect(isEligible(node(), runtime({ state }))).toBe(false);
    }
  });

  it("rejects a runtime kind the node does not install", () => {
    expect(isEligible(node(), runtime({ kind: "codex" }))).toBe(false);
    expect(isEligible(node(), runtime(), { kind: "codex" })).toBe(false);
  });

  it("rejects a project the node is not labelled for", () => {
    expect(isEligible(node(), runtime(), { project: "other" })).toBe(false);
  });

  it("rejects a model the runtime does not offer", () => {
    expect(isEligible(node(), runtime(), { model: "gpt-5" })).toBe(false);
  });

  it("requires a GPU only when the requirement asks for one", () => {
    const cpuNode = node({ labels: { ...node().labels, gpu: false } });

    expect(isEligible(cpuNode, runtime(), { gpu: true })).toBe(false);
    expect(isEligible(cpuNode, runtime())).toBe(true);
  });

  it("requires every requested secret to exist on the node", () => {
    expect(isEligible(node(), runtime(), { secrets: ["github-token"] })).toBe(true);
    expect(isEligible(node(), runtime(), { secrets: ["github-token", "missing"] })).toBe(false);
    expect(
      isEligible(node({ labels: { ...node().labels, secrets: undefined } }), runtime(), {
        secrets: ["github-token"],
      }),
    ).toBe(false);
  });

  it("rejects when quota minus running slots cannot cover the requirement", () => {
    const full = runtime({ slots: { running: 2, quota: 2 } });

    expect(isEligible(node(), full)).toBe(false);
    expect(isEligible(node(), runtime({ slots: { running: 1, quota: 3 } }), { slots: 2 })).toBe(
      true,
    );
    expect(isEligible(node(), runtime({ slots: { running: 1, quota: 3 } }), { slots: 3 })).toBe(
      false,
    );
  });
});

import type { FleetSnapshot, Node, Runtime, Slot } from "@gethen/fleet";
import { describe, expect, it } from "vitest";

import { type ScheduleEvidence, scheduleTask, type TaskRequirement } from "./schedule";

const AT = "2026-11-16T12:00:00.000Z";

function node(id: string, overrides: Partial<Node> = {}): Node {
  return {
    id,
    labels: { locality: "local", projects: ["gethen"], runtimes: ["dsh"] },
    resources: { cpu: 8, memMb: 16384, diskMb: 65536 },
    health: "online",
    lastHeartbeatAt: AT,
    ...overrides,
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

function requirement(overrides: Partial<TaskRequirement> = {}): TaskRequirement {
  return { kind: "code-change", workspace: "ws-gethen", ...overrides };
}

describe("scheduleTask", () => {
  it("parks an empty fleet", () => {
    expect(scheduleTask(snapshot(), requirement())).toEqual({
      action: "park",
      reason: "no-nodes",
    });
  });

  it("reuses an idle resident slot instead of starting a new one", () => {
    const fleet = snapshot({
      nodes: [node("node-a"), node("node-b")],
      runtimes: [runtime("node-a", "main"), runtime("node-b", "main")],
      slots: [slot("slot-1", "node-a", "main")],
    });

    expect(scheduleTask(fleet, requirement())).toEqual({
      action: "reuse",
      node: "node-a",
      runtime: "main",
      slot: "slot-1",
      reason: "reuse-idle-slot",
    });
  });

  it("ranks reusable slots by receipt evidence", () => {
    const fleet = snapshot({
      nodes: [node("node-a"), node("node-b")],
      runtimes: [runtime("node-a", "main"), runtime("node-b", "main")],
      slots: [slot("slot-1", "node-a", "main"), slot("slot-2", "node-b", "main")],
    });
    const evidence: ScheduleEvidence = {
      byNode: {
        "node-a": { attempts: 10, successes: 1 },
        "node-b": { attempts: 10, successes: 9 },
      },
    };

    expect(scheduleTask(fleet, requirement(), evidence)).toMatchObject({
      action: "reuse",
      node: "node-b",
      slot: "slot-2",
    });
  });

  it("is deterministic and input-order independent", () => {
    const parts = {
      nodes: [node("node-a"), node("node-b")],
      runtimes: [runtime("node-a", "main"), runtime("node-b", "main")],
      slots: [slot("slot-b", "node-b", "main"), slot("slot-a", "node-a", "main")],
    };
    const first = scheduleTask(snapshot(parts), requirement());
    const second = scheduleTask(
      snapshot({
        nodes: [...parts.nodes].reverse(),
        runtimes: [...parts.runtimes].reverse(),
        slots: [...parts.slots].reverse(),
      }),
      requirement(),
    );

    expect(first).toEqual({
      action: "reuse",
      node: "node-a",
      runtime: "main",
      slot: "slot-a",
      reason: "reuse-idle-slot",
    });
    expect(second).toEqual(first);
    expect(scheduleTask(snapshot(parts), requirement())).toEqual(first);
  });

  it("starts a resident slot when nothing can be reused", () => {
    const fleet = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
    });

    expect(scheduleTask(fleet, requirement())).toEqual({
      action: "start",
      node: "node-a",
      runtime: "main",
      workspace: "ws-gethen",
      kind: "resident",
      reason: "start-free-capacity",
    });
  });

  it("starts an ephemeral slot without a workspace and falls back to the project", () => {
    const fleet = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
    });

    expect(scheduleTask(fleet, requirement({ workspace: undefined, project: "gethen" }))).toEqual({
      action: "start",
      node: "node-a",
      runtime: "main",
      workspace: "gethen",
      kind: "ephemeral",
      reason: "start-free-capacity",
    });
  });

  it("prefers the runtime whose profile matches the project", () => {
    const fleet = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "other"), runtime("node-a", "gethen")],
    });

    expect(scheduleTask(fleet, requirement({ project: "gethen" }))).toMatchObject({
      action: "start",
      runtime: "gethen",
    });
  });

  it("prefers the node with better receipt evidence", () => {
    const fleet = snapshot({
      nodes: [node("node-a"), node("node-b")],
      runtimes: [runtime("node-a", "main"), runtime("node-b", "main")],
    });
    const evidence: ScheduleEvidence = {
      byNode: {
        "node-a": { attempts: 10, successes: 1 },
        "node-b": { attempts: 10, successes: 9 },
      },
    };

    expect(scheduleTask(fleet, requirement(), evidence)).toMatchObject({
      action: "start",
      node: "node-b",
    });
  });

  it("prefers the node with more load headroom", () => {
    const fleet = snapshot({
      nodes: [node("node-a"), node("node-b")],
      runtimes: [
        runtime("node-a", "main", { slots: { running: 2, quota: 3 } }),
        runtime("node-b", "main", { slots: { running: 0, quota: 3 } }),
      ],
    });

    expect(scheduleTask(fleet, requirement())).toMatchObject({ action: "start", node: "node-b" });
  });

  it("prefers the local (cheap) node when everything else ties", () => {
    const fleet = snapshot({
      nodes: [
        node("node-a", { labels: { locality: "local", projects: ["gethen"], runtimes: ["dsh"] } }),
        node("node-b", { labels: { locality: "cloud", projects: ["gethen"], runtimes: ["dsh"] } }),
      ],
      runtimes: [runtime("node-a", "main"), runtime("node-b", "main")],
    });

    expect(scheduleTask(fleet, requirement())).toMatchObject({ action: "start", node: "node-a" });
  });

  it("queues when every candidate is at capacity", () => {
    const fleet = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main", { slots: { running: 2, quota: 2 } })],
    });

    expect(scheduleTask(fleet, requirement())).toEqual({ action: "queue", reason: "at-capacity" });
  });

  it("parks when a local-only task can only match cloud nodes", () => {
    const fleet = snapshot({
      nodes: [
        node("node-cloud", {
          labels: { locality: "cloud", projects: ["gethen"], runtimes: ["dsh"] },
        }),
      ],
      runtimes: [runtime("node-cloud", "main")],
    });

    expect(scheduleTask(fleet, requirement({ locality: "local" }))).toEqual({
      action: "park",
      reason: "locality-mismatch",
    });
  });

  it("treats locality as a hard filter, not a preference", () => {
    const fleet = snapshot({
      nodes: [
        node("node-cloud", {
          labels: { locality: "cloud", projects: ["gethen"], runtimes: ["dsh"] },
        }),
        node("node-local"),
      ],
      runtimes: [runtime("node-cloud", "main"), runtime("node-local", "main")],
    });
    const evidence: ScheduleEvidence = {
      byNode: { "node-cloud": { attempts: 10, successes: 10 } },
    };

    expect(scheduleTask(fleet, requirement({ locality: "local" }), evidence)).toMatchObject({
      action: "start",
      node: "node-local",
    });
  });

  it("relaxes locality when requireLocality is false", () => {
    const fleet = snapshot({
      nodes: [
        node("node-cloud", {
          labels: { locality: "cloud", projects: ["gethen"], runtimes: ["dsh"] },
        }),
      ],
      runtimes: [runtime("node-cloud", "main")],
    });

    expect(
      scheduleTask(fleet, requirement({ locality: "local" }), undefined, {
        requireLocality: false,
      }),
    ).toMatchObject({ action: "start", node: "node-cloud" });
  });

  it("parks when the project label is missing", () => {
    const fleet = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
    });

    expect(scheduleTask(fleet, requirement({ project: "other" }))).toEqual({
      action: "park",
      reason: "project-not-found",
    });
  });

  it("parks when the runtime kind is not installed on any node", () => {
    const fleet = snapshot({
      nodes: [
        node("node-a", { labels: { locality: "local", projects: ["gethen"], runtimes: [] } }),
      ],
      runtimes: [runtime("node-a", "main")],
    });

    expect(scheduleTask(fleet, requirement())).toEqual({ action: "park", reason: "no-runtime" });
  });

  it("parks with the specific label reason that failed", () => {
    const fleet = snapshot({
      nodes: [node("node-a")],
      runtimes: [runtime("node-a", "main")],
    });

    expect(scheduleTask(fleet, requirement({ gpu: true }))).toEqual({
      action: "park",
      reason: "gpu-unavailable",
    });
    expect(scheduleTask(fleet, requirement({ model: "gpt-5" }))).toEqual({
      action: "park",
      reason: "model-unavailable",
    });
    expect(scheduleTask(fleet, requirement({ secrets: ["missing"] }))).toEqual({
      action: "park",
      reason: "secrets-unavailable",
    });
  });

  it("parks when every node is offline", () => {
    const fleet = snapshot({
      nodes: [node("node-a", { health: "offline" })],
      runtimes: [runtime("node-a", "main")],
    });

    expect(scheduleTask(fleet, requirement())).toEqual({ action: "park", reason: "offline" });
  });

  it("honors an operator pin that passes the hard filters and ignores one that does not", () => {
    const fleet = snapshot({
      nodes: [node("node-a"), node("node-b")],
      runtimes: [runtime("node-a", "main"), runtime("node-b", "main")],
    });
    const evidence: ScheduleEvidence = {
      byNode: { "node-a": { attempts: 10, successes: 10 } },
    };

    expect(scheduleTask(fleet, requirement(), evidence, { preferNode: "node-b" })).toMatchObject({
      action: "start",
      node: "node-b",
    });
    expect(
      scheduleTask(fleet, requirement(), evidence, { preferNode: "node-missing" }),
    ).toMatchObject({ action: "start", node: "node-a" });
  });
});

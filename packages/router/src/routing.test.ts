import type { WorkerManifest } from "@gethen/worker-bridge";
import { describe, expect, it } from "vitest";
import { routeWorkers } from "./routing";

const base: WorkerManifest = {
  id: "w1",
  machine: "local",
  runtime: "dsh",
  workspaces: [{ id: "gethen", repo: "org/gethen" }],
  capabilities: {
    promptInjection: "system",
    skills: "filesystem",
    mcp: true,
    permissions: "approval",
    streaming: true,
    steer: true,
    fork: false,
  },
  models: ["opencode-go/deepseek-v4.1-flash"],
  costClass: "mid",
  load: { running: 0, quota: 2 },
  health: { state: "online", lastHeartbeatAt: "2026-11-16T00:00:00.000Z" },
};

function worker(overrides: Partial<WorkerManifest>): WorkerManifest {
  return {
    ...base,
    ...overrides,
    workspaces: overrides.workspaces ?? base.workspaces,
    capabilities: overrides.capabilities ?? base.capabilities,
    load: overrides.load ?? base.load,
    health: overrides.health ?? base.health,
  };
}

describe("routeWorkers hard filters", () => {
  it("rejects offline and at-capacity workers", () => {
    const offline = worker({
      id: "off",
      health: { state: "offline", lastHeartbeatAt: base.health.lastHeartbeatAt },
    });
    const full = worker({ id: "full", load: { running: 2, quota: 2 } });
    const result = routeWorkers([offline, full], { kind: "code-change" });
    expect(result.decision).toBeUndefined();
    expect(result.rejected).toEqual([
      { worker: "off", reason: "offline" },
      { worker: "full", reason: "at-capacity" },
    ]);
  });

  it("rejects workspace, model and capability mismatches", () => {
    expect(routeWorkers([base], { kind: "k", workspace: "other" }).rejected[0]?.reason).toBe(
      "workspace-missing:other",
    );
    expect(routeWorkers([base], { kind: "k", model: "other/model" }).rejected[0]?.reason).toBe(
      "model-missing:other/model",
    );
    expect(
      routeWorkers([base], { kind: "k", capabilities: { fork: true } }).rejected[0]?.reason,
    ).toBe("capability-mismatch:fork");
  });
});

describe("routeWorkers scoring", () => {
  it("prefers the worker with workspace affinity", () => {
    const nearby = worker({ id: "nearby" });
    const far = worker({ id: "far", workspaces: [{ id: "elsewhere" }] });
    const result = routeWorkers([far, nearby], { kind: "k", workspace: "gethen" });
    expect(result.decision?.worker).toBe("nearby");
    expect(result.decision?.reasons.map((reason) => reason.code)).toContain("workspace-affinity");
  });

  it("prefers a worker with better receipt evidence for the task kind", () => {
    const proven = worker({ id: "proven", workspaces: [{ id: "elsewhere" }] });
    const fresh = worker({ id: "fresh", workspaces: [{ id: "elsewhere" }] });
    const result = routeWorkers(
      [fresh, proven],
      { kind: "code-change" },
      {
        proven: {
          attempts: 4,
          successes: 4,
          byKind: { "code-change": { attempts: 2, successes: 2 } },
        },
      },
    );
    expect(result.decision?.worker).toBe("proven");
    expect(result.decision?.reasons.map((reason) => reason.code)).toContain("evidence");
  });

  it("honors an operator pin only when the worker is eligible", () => {
    const pinned = worker({ id: "pinned", workspaces: [{ id: "elsewhere" }] });
    const other = worker({ id: "other", workspaces: [{ id: "elsewhere" }] });
    expect(
      routeWorkers([pinned, other], { kind: "k" }, {}, { prefer: "pinned" }).decision?.worker,
    ).toBe("pinned");
    const offline = worker({
      id: "pinned",
      health: { state: "offline", lastHeartbeatAt: base.health.lastHeartbeatAt },
    });
    expect(
      routeWorkers([offline, other], { kind: "k" }, {}, { prefer: "pinned" }).decision?.worker,
    ).toBe("other");
  });

  it("penalizes a degraded worker and breaks ties by id", () => {
    const degraded = worker({
      id: "a",
      health: { state: "degraded", lastHeartbeatAt: base.health.lastHeartbeatAt },
    });
    const healthy = worker({ id: "b" });
    expect(routeWorkers([degraded, healthy], { kind: "k" }).decision?.worker).toBe("b");
    const first = worker({ id: "aaa" });
    const second = worker({ id: "zzz" });
    expect(routeWorkers([second, first], { kind: "k" }).decision?.worker).toBe("aaa");
  });

  it("keeps every eligible worker as a candidate", () => {
    const result = routeWorkers([worker({ id: "a" }), worker({ id: "b" })], { kind: "k" });
    expect(result.candidates.map((candidate) => candidate.worker).sort()).toEqual(["a", "b"]);
    expect(result.rejected).toEqual([]);
  });
});

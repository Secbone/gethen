import { describe, expect, it } from "vitest";

import {
  type FleetSnapshot,
  isLiveSlot,
  residentSlotKey,
  runtimeKey,
  type Slot,
  type SlotState,
} from "./model";

const AT = "2026-11-16T12:00:00.000Z";

function slot(overrides: Partial<Slot> = {}): Slot {
  return {
    id: "slot-1",
    node: "node-a",
    runtime: "main",
    workspace: "ws-gethen",
    kind: "resident",
    state: "idle",
    ...overrides,
  };
}

describe("fleet model", () => {
  it("constructs a snapshot of nodes, runtimes and slots", () => {
    const snapshot: FleetSnapshot = {
      nodes: [
        {
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
        },
      ],
      runtimes: [
        {
          node: "node-a",
          kind: "dsh",
          profile: "main",
          state: "idle",
          models: ["deepseek-v4"],
          slots: { running: 1, quota: 2 },
        },
      ],
      slots: [slot({ sessionId: "session-1", ttlMs: 60_000 })],
    };

    expect(snapshot.nodes[0]?.labels).toMatchObject({ locality: "local", gpu: true });
    expect(snapshot.runtimes[0]?.slots).toEqual({ running: 1, quota: 2 });
    expect(snapshot.slots[0]?.sessionId).toBe("session-1");
    expect(snapshot.slots[0]?.kind).toBe("resident");
  });

  it("keys runtimes by kind and profile", () => {
    expect(runtimeKey("dsh", "main")).toBe(runtimeKey("dsh", "main"));
    expect(runtimeKey("dsh", "main")).not.toBe(runtimeKey("codex", "main"));
    expect(runtimeKey("dsh", "main")).not.toBe(runtimeKey("dsh", "worker"));
  });

  it("keys resident slots by runtime profile and workspace", () => {
    expect(residentSlotKey("main", "ws-a")).toBe(residentSlotKey("main", "ws-a"));
    expect(residentSlotKey("main", "ws-a")).not.toBe(residentSlotKey("main", "ws-b"));
    expect(residentSlotKey("main", "ws-a")).not.toBe(residentSlotKey("worker", "ws-a"));
  });

  it("treats every slot state except closed as live", () => {
    const states: SlotState[] = ["idle", "busy", "draining"];
    for (const state of states) expect(isLiveSlot(slot({ state }))).toBe(true);
    expect(isLiveSlot(slot({ state: "closed" }))).toBe(false);
  });
});

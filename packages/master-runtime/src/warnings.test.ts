import { describe, expect, it } from "vitest";
import type { WorkerSnapshot } from "./fleet";
import { emptySchedulingMetrics } from "./fleet";
import { runtimeWarnings } from "./warnings";

function worker(id: string, registered: boolean, health: WorkerSnapshot["health"]): WorkerSnapshot {
  return {
    id,
    registered,
    connected: health !== "offline",
    health,
    workspaces: [],
    models: [],
    load: { running: 0, quota: 1 },
    active: 0,
  };
}

const healthy = {
  settlements: {
    directCompleted: 1,
    recoveredCompleted: 0,
    recoveredWithoutReport: 0,
    lateCompleted: 0,
  },
  mailbox: { dead: 0 },
  workers: [worker("w1", true, "online")],
  scheduling: emptySchedulingMetrics(),
};

describe("runtimeWarnings", () => {
  it("is empty for healthy counters", () => {
    expect(runtimeWarnings(healthy)).toEqual([]);
  });

  it("reports every degraded counter", () => {
    const warnings = runtimeWarnings({
      settlements: {
        directCompleted: 0,
        recoveredCompleted: 2,
        recoveredWithoutReport: 1,
        lateCompleted: 0,
      },
      mailbox: { dead: 2 },
      workers: [worker("w1", true, "offline"), worker("w2", true, "online")],
      scheduling: { ...emptySchedulingMetrics(), queueTimeouts: 1 },
    });
    expect(warnings.map((warning) => warning.code)).toEqual([
      "recovered_without_report",
      "mailbox_dead",
      "worker_offline",
      "queue_timeout",
    ]);
    expect(warnings[1]).toMatchObject({ count: 2 });
  });

  it("honours configured thresholds and ignores unregistered workers", () => {
    expect(
      runtimeWarnings(
        {
          ...healthy,
          mailbox: { dead: 1 },
          workers: [worker("configured", false, "offline"), worker("w1", true, "online")],
        },
        { mailboxDead: 2 },
      ),
    ).toEqual([]);
  });
});

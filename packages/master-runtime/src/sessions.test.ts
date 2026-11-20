import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { WorkerSessionStats } from "@gethen/worker-bridge";
import { afterEach, describe, expect, it } from "vitest";
import type { WorkerSnapshot } from "./fleet";
import {
  aggregateSessions,
  emptySessionStats,
  emptySessionsStatus,
  localSessionStats,
} from "./sessions";

const AT = "2026-11-20T00:00:00.000Z";
const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function snapshot(id: string, sessions?: WorkerSessionStats): WorkerSnapshot {
  return {
    id,
    registered: true,
    connected: true,
    health: "online",
    workspaces: [],
    models: [],
    load: { running: 0, quota: 1 },
    active: 0,
    ...(sessions === undefined ? {} : { sessions }),
  };
}

function stats(overrides: Partial<WorkerSessionStats> = {}): WorkerSessionStats {
  return {
    sessions: 2,
    totalBytes: 1024,
    keyRegistryEntries: 1,
    observedAt: AT,
    ...overrides,
  };
}

describe("session aggregation", () => {
  it("adds the Master observation and every reporting Worker", () => {
    const aggregate = aggregateSessions(
      { sessions: 3, totalBytes: 300, oldestDays: 5, keyRegistryEntries: 0 },
      [
        snapshot("worker-b", stats({ sessions: 2, totalBytes: 2000, oldestDays: 40 })),
        snapshot("worker-a", stats({ sessions: 1, totalBytes: 1000, oldestDays: 12 })),
        snapshot("worker-c"),
      ],
    );
    expect(aggregate.totalSessions).toBe(6);
    expect(aggregate.totalBytes).toBe(3300);
    expect(aggregate.oldestDays).toBe(40);
    expect(aggregate.keyRegistryEntries).toBe(2);
    expect(aggregate.reportingWorkers).toBe(2);
    expect(aggregate.workers.map((entry) => entry.worker)).toEqual(["worker-a", "worker-b"]);
  });

  it("keeps only the Master side when no Worker reported stats", () => {
    const aggregate = aggregateSessions(
      { sessions: 1, totalBytes: 10, oldestDays: 2, keyRegistryEntries: 0 },
      [snapshot("worker-a")],
    );
    expect(aggregate).toMatchObject({
      totalSessions: 1,
      totalBytes: 10,
      oldestDays: 2,
      reportingWorkers: 0,
      workers: [],
    });
  });

  it("has a zero snapshot for hosts without a DSH store", () => {
    expect(emptySessionsStatus()).toEqual({
      master: { sessions: 0, totalBytes: 0, keyRegistryEntries: 0 },
      totalSessions: 0,
      totalBytes: 0,
      keyRegistryEntries: 0,
      reportingWorkers: 0,
      workers: [],
    });
    expect(emptySessionStats()).toEqual({ sessions: 0, totalBytes: 0, keyRegistryEntries: 0 });
  });

  it("samples a local DSH home for the Master side", () => {
    const root = mkdtempSync(join(tmpdir(), "gethen-sessions-"));
    roots.push(root);
    const home = join(root, "dsh");
    const dir = join(home, "sessions", "--work--", "session-a");
    mkdirSync(dir, { recursive: true });
    const log = join(dir, "session.v4.jsonl.zstd");
    writeFileSync(log, "x".repeat(64));
    const at = new Date("2026-11-20T00:00:00.000Z");
    const mtime = new Date(at.getTime() - 10 * 86_400_000);
    utimesSync(log, mtime, mtime);
    utimesSync(dir, mtime, mtime);
    const sampled = localSessionStats({ home, now: at });
    expect(sampled.sessions).toBe(1);
    expect(sampled.totalBytes).toBe(64);
    expect(sampled.oldestDays).toBe(10);
    // The Master has no Worker key registry, so that counter stays zero.
    expect(sampled.keyRegistryEntries).toBe(0);
  });
});

import { describe, expect, it } from "vitest";
import { parseWorkerManifest } from "./manifest";
import type { WorkerManifest } from "./types";

const AT = "2026-11-20T00:00:00.000Z";

function manifest(overrides: Partial<WorkerManifest> = {}): WorkerManifest {
  return {
    id: "worker-a",
    machine: "build-1",
    runtime: "dsh",
    workspaces: [{ id: "gethen" }],
    capabilities: {
      promptInjection: "system",
      skills: "filesystem",
      mcp: false,
      permissions: "approval",
      streaming: true,
      steer: true,
      fork: false,
    },
    models: ["deepseek-v4.1-flash"],
    costClass: "mid",
    load: { running: 0, quota: 1 },
    health: { state: "online", lastHeartbeatAt: AT },
    ...overrides,
  };
}

describe("parseWorkerManifest sessions", () => {
  it("accepts and preserves a local session observation", () => {
    const parsed = parseWorkerManifest(
      manifest({
        sessions: {
          sessions: 4,
          totalBytes: 8192,
          oldestDays: 12.5,
          keyRegistryEntries: 2,
          observedAt: AT,
        },
      }),
    );
    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.value.sessions).toEqual({
        sessions: 4,
        totalBytes: 8192,
        oldestDays: 12.5,
        keyRegistryEntries: 2,
        observedAt: AT,
      });
    }
  });

  it("rejects negative counts, bad ages and a bad timestamp", () => {
    for (const sessions of [
      { sessions: -1, totalBytes: 0, keyRegistryEntries: 0, observedAt: AT },
      { sessions: 0, totalBytes: 0, keyRegistryEntries: 0, observedAt: AT, oldestDays: -1 },
      { sessions: 0, totalBytes: 0, keyRegistryEntries: 0, observedAt: "yesterday" },
      "not-an-object",
    ]) {
      expect(parseWorkerManifest(manifest({ sessions: sessions as never }))).toMatchObject({
        ok: false,
        reason: expect.stringContaining("sessions"),
      });
    }
  });
});

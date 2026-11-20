import { existsSync, mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { pruneDshSessions } from "./prune";

const NOW = new Date("2026-11-20T00:00:00.000Z");
const DAY_MS = 86_400_000;
const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

interface Fixture {
  home: string;
  stateDir: string;
  projcache: string;
}

function fixture(): Fixture {
  const root = mkdtempSync(join(tmpdir(), "gethen-dsh-prune-"));
  roots.push(root);
  const home = join(root, "dsh");
  const stateDir = join(root, "state");
  const projcache = join(home, "storages", "session_projcache", "sessions");
  mkdirSync(projcache, { recursive: true });
  mkdirSync(stateDir, { recursive: true });
  return { home, stateDir, projcache };
}

function addSession(
  fixture: Fixture,
  sessionId: string,
  ageDays: number,
  options: { openTurn?: boolean; bytes?: number } = {},
): string {
  const dir = join(fixture.home, "sessions", "--work-project--", sessionId);
  mkdirSync(dir, { recursive: true });
  const log = join(dir, "session.v4.jsonl.zstd");
  writeFileSync(log, "x".repeat(options.bytes ?? 1024));
  const mtime = new Date(NOW.getTime() - ageDays * DAY_MS);
  utimesSync(log, mtime, mtime);
  utimesSync(dir, mtime, mtime);
  writeFileSync(
    join(fixture.projcache, `${sessionId}.json`),
    JSON.stringify({
      version: 7,
      record: {
        identity: { formatVersion: 4, createdAt: mtime.getTime(), cwd: "/work" },
        rows: {
          turnBoundary: {
            ver: 2,
            seq: 1,
            val: { openTurnStartSeq: options.openTurn === true ? 1 : null },
          },
        },
      },
    }),
  );
  return dir;
}

describe("pruneDshSessions", () => {
  it("reports old candidates without deleting on a dry run", async () => {
    const f = fixture();
    const old = addSession(f, "old-session", 40);
    const fresh = addSession(f, "fresh-session", 2);
    const result = await pruneDshSessions({
      home: f.home,
      stateDir: f.stateDir,
      olderThanDays: 30,
      now: NOW,
      dryRun: true,
    });
    expect(result.dryRun).toBe(true);
    expect(result.candidates.map((entry) => entry.sessionId)).toEqual(["old-session"]);
    expect(result.bytesEligible).toBe(1024);
    expect(result.deleted).toEqual([]);
    expect(result.bytesFreed).toBe(0);
    expect(existsSync(old)).toBe(true);
    expect(existsSync(fresh)).toBe(true);
  });

  it("deletes only unprotected old sessions and removes their metadata", async () => {
    const f = fixture();
    const removable = addSession(f, "old-removable", 40, { bytes: 2048 });
    const keyed = addSession(f, "old-keyed", 40);
    const running = addSession(f, "old-running", 40, { openTurn: true });
    const active = addSession(f, "old-active", 40);
    const fresh = addSession(f, "fresh-session", 2);
    writeFileSync(
      join(f.stateDir, "worker-1.sessions.json"),
      JSON.stringify({ "project-x": "old-keyed" }),
    );

    const result = await pruneDshSessions({
      home: f.home,
      stateDir: f.stateDir,
      olderThanDays: 30,
      now: NOW,
      protectSessionIds: ["old-active"],
    });
    expect(result.deleted.map((entry) => entry.sessionId)).toEqual(["old-removable"]);
    expect(result.bytesFreed).toBe(2048);
    expect(result.removedRecords).toBe(1);
    expect(result.skipped).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ sessionId: "old-keyed", reason: "key_registry" }),
        expect.objectContaining({ sessionId: "old-running", reason: "running_session" }),
        expect.objectContaining({ sessionId: "old-active", reason: "active_assignment" }),
      ]),
    );
    expect(existsSync(removable)).toBe(false);
    expect(existsSync(join(f.projcache, "old-removable.json"))).toBe(false);
    expect(existsSync(keyed)).toBe(true);
    expect(existsSync(running)).toBe(true);
    expect(existsSync(active)).toBe(true);
    expect(existsSync(fresh)).toBe(true);
  });

  it("keeps sessions at or newer than the cutoff", async () => {
    const f = fixture();
    addSession(f, "cutoff-session", 30);
    const result = await pruneDshSessions({
      home: f.home,
      olderThanDays: 30,
      now: NOW,
      dryRun: true,
    });
    expect(result.candidates).toEqual([]);
    expect(result.scanned).toBe(1);
  });

  it("rejects a non-positive threshold", async () => {
    await expect(pruneDshSessions({ olderThanDays: 0 })).rejects.toThrow(/positive/);
    await expect(pruneDshSessions({ olderThanDays: Number.NaN })).rejects.toThrow(/positive/);
  });
});

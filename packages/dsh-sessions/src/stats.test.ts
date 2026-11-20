import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { collectSessionStats, scanSessionDirectories } from "./stats";

const NOW = new Date("2026-11-20T00:00:00.000Z");
const DAY_MS = 86_400_000;
const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function fixture(): { root: string; home: string; stateDir: string } {
  const root = mkdtempSync(join(tmpdir(), "gethen-dsh-stats-"));
  roots.push(root);
  const home = join(root, "dsh");
  const stateDir = join(root, "state");
  mkdirSync(home, { recursive: true });
  mkdirSync(stateDir, { recursive: true });
  return { root, home, stateDir };
}

function addSession(
  home: string,
  sessionId: string,
  ageDays: number,
  bytes: number,
  project = "--work-project--",
): string {
  const dir = join(home, "sessions", project, sessionId);
  mkdirSync(dir, { recursive: true });
  const log = join(dir, "session.v4.jsonl.zstd");
  writeFileSync(log, "x".repeat(bytes));
  const mtime = new Date(NOW.getTime() - ageDays * DAY_MS);
  utimesSync(log, mtime, mtime);
  utimesSync(dir, mtime, mtime);
  return dir;
}

describe("session stats", () => {
  it("reports session count, bytes, oldest age and key-registry entries", () => {
    const { home, stateDir } = fixture();
    addSession(home, "session-old", 40, 1000);
    addSession(home, "session-new", 2, 2000, "--other-project--");
    writeFileSync(
      join(stateDir, "worker-a.sessions.json"),
      JSON.stringify({ "project-a": "session-old", "project-b": "session-new" }),
    );
    // Unrelated state files must not be counted as registries.
    writeFileSync(join(stateDir, "worker-a.json"), JSON.stringify({ x: "y" }));

    const stats = collectSessionStats({ home, stateDir, now: NOW });
    expect(stats.sessions).toBe(2);
    expect(stats.totalBytes).toBe(3000);
    expect(stats.oldestDays).toBe(40);
    expect(stats.keyRegistryEntries).toBe(2);
    expect(stats.observedAt).toBe(NOW.toISOString());
  });

  it("reports zeros for a missing DSH home instead of throwing", () => {
    const { root } = fixture();
    const stats = collectSessionStats({
      home: join(root, "missing"),
      stateDir: join(root, "missing-state"),
      now: NOW,
    });
    expect(stats).toMatchObject({ sessions: 0, totalBytes: 0, keyRegistryEntries: 0 });
    expect(stats.oldestDays).toBeUndefined();
  });

  it("scans session directories newest activity last and ignores stray files", () => {
    const { home } = fixture();
    addSession(home, "session-old", 40, 10);
    addSession(home, "session-new", 2, 20);
    writeFileSync(join(home, "sessions", "--work-project--", "not-a-session.json"), "x");
    const scan = scanSessionDirectories(home);
    expect(scan.sessions.map((entry) => entry.sessionId)).toEqual(["session-old", "session-new"]);
    expect(scan.totalBytes).toBe(30);
    expect(scan.unreadable).toBe(0);
  });
});

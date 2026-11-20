import { existsSync, mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { parseSessionArgs, sessionCommand } from "./commands/session";

interface StubResult {
  status: number;
  body: unknown;
}

interface Captured {
  method: string | undefined;
  url: string | undefined;
}

const servers: Server[] = [];

afterEach(async () => {
  while (servers.length > 0) {
    const server = servers.pop();
    if (server !== undefined) await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

function capture(): { lines: string[]; write: (line: string) => void } {
  const lines: string[] = [];
  return { lines, write: (line: string) => lines.push(line) };
}

async function stubMaster(handler: () => StubResult): Promise<{
  base: string;
  captured: Captured[];
}> {
  const captured: Captured[] = [];
  const server = createServer((request, response) => {
    request.resume();
    request.on("end", () => {
      captured.push({ method: request.method, url: request.url });
      const result = handler();
      const body = JSON.stringify(result.body);
      response.writeHead(result.status, {
        "content-type": "application/json",
        "content-length": Buffer.byteLength(body),
      });
      response.end(body);
    });
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : 0;
  return { base: `http://127.0.0.1:${port}`, captured };
}

describe("gethen session", () => {
  it("parses list/history and rejects bad invocations", () => {
    expect(parseSessionArgs(["list", "--master", "http://m:1", "--json"])).toMatchObject({
      action: "list",
      master: "http://m:1",
      json: true,
    });
    expect(
      parseSessionArgs([
        "history",
        "session-1",
        "--master",
        "http://m:1",
        "--limit",
        "5",
        "--after-seq",
        "0",
        "--timeout-ms",
        "250",
      ]),
    ).toMatchObject({
      action: "history",
      sessionId: "session-1",
      limit: 5,
      afterSeq: 0,
      timeoutMs: 250,
    });
    const fromEnv = parseSessionArgs(["list"], { GETHEN_MASTER_URL: "http://m:1" });
    expect(fromEnv.action).toBe("list");
    if (fromEnv.action === "list") expect(fromEnv.master).toBe("http://m:1");

    expect(() => parseSessionArgs(["list"])).toThrow(/--master/);
    expect(() => parseSessionArgs(["history", "--master", "http://m:1"])).toThrow(/session id/);
    expect(() => parseSessionArgs(["bogus", "--master", "http://m:1"])).toThrow(/subcommand/);
    expect(parseSessionArgs(["prune", "--older-than", "30"])).toMatchObject({
      action: "prune",
      olderThanDays: 30,
      dryRun: true,
      yes: false,
      json: false,
    });
    const yes = parseSessionArgs(["prune", "--older-than", "0.5", "--yes"]);
    expect(yes.action === "prune" ? yes.dryRun : undefined).toBe(false);
    const both = parseSessionArgs(["prune", "--older-than", "30", "--yes", "--dry-run"]);
    expect(both.action === "prune" ? both.dryRun : undefined).toBe(true);
    expect(() => parseSessionArgs(["prune"])).toThrow(/older-than/);
    expect(() => parseSessionArgs(["prune", "--older-than", "0"])).toThrow(/positive number/);
    expect(() =>
      parseSessionArgs(["history", "s1", "--master", "http://m:1", "--limit", "0"]),
    ).toThrow(/positive integer/);
    expect(() =>
      parseSessionArgs(["history", "s1", "--master", "http://m:1", "--after-seq", "-1"]),
    ).toThrow(/non-negative integer/);
  });

  it("lists sessions and reads history from the master", async () => {
    const at = "2026-11-17T10:00:00.000Z";
    const sessionsBody = {
      sessions: [
        {
          sessionId: "session-1",
          state: "idle",
          key: "project-a",
          workspace: "/ws",
          lastUsedAt: at,
        },
      ],
    };
    const listStub = await stubMaster(() => ({ status: 200, body: sessionsBody }));

    const listOut = capture();
    expect(
      await sessionCommand(["list", "--master", listStub.base, "--timeout-ms", "250"], {
        write: listOut.write,
      }),
    ).toBe(0);
    expect(listStub.captured[0]).toMatchObject({
      method: "GET",
      url: "/gethen/sessions?timeoutMs=250",
    });
    const listText = listOut.lines.join("\n");
    expect(listText).toContain("sessions: 1");
    expect(listText).toContain("session-1");
    expect(listText).toContain("state=idle");
    expect(listText).toContain("key=project-a");

    const listJson = capture();
    expect(
      await sessionCommand(["list", "--master", listStub.base, "--json"], {
        write: listJson.write,
      }),
    ).toBe(0);
    expect(JSON.parse(listJson.lines.join("\n"))).toEqual(sessionsBody);

    const historyBody = {
      sessionId: "session-1",
      at,
      events: [{ seq: 0, type: "message", at, data: { text: "hi" } }],
    };
    const historyStub = await stubMaster(() => ({ status: 200, body: historyBody }));
    const historyOut = capture();
    expect(
      await sessionCommand(
        ["history", "session-1", "--master", historyStub.base, "--limit", "5", "--after-seq", "0"],
        { write: historyOut.write },
      ),
    ).toBe(0);
    expect(historyStub.captured[0]?.url).toBe(
      "/gethen/sessions/session-1/history?limit=5&afterSeq=0",
    );
    const historyText = historyOut.lines.join("\n");
    expect(historyText).toContain("session: session-1");
    expect(historyText).toContain("#0 message");
  });

  it("exits non-zero on HTTP and transport errors", async () => {
    const failing = await stubMaster(() => ({
      status: 504,
      body: { error: "worker did not answer", code: "MAILBOX_STATUS_TIMEOUT" },
    }));
    const httpErrors = capture();
    expect(
      await sessionCommand(["list", "--master", failing.base], { error: httpErrors.write }),
    ).toBe(1);
    expect(httpErrors.lines.join("\n")).toContain("504");
    expect(httpErrors.lines.join("\n")).toContain("worker did not answer");

    const badMaster = capture();
    expect(
      await sessionCommand(["list", "--master", "ftp://example.com"], { error: badMaster.write }),
    ).toBe(1);
    expect(badMaster.lines.join("\n")).toContain("http(s)");

    const unreachable = capture();
    expect(
      await sessionCommand(["list", "--master", "http://127.0.0.1:1"], {
        error: unreachable.write,
      }),
    ).toBe(1);
    expect(unreachable.lines.join("\n")).toContain("cannot reach master");
  });
});

const DAY_MS = 86_400_000;

interface PruneFixture {
  root: string;
  dshHome: string;
  stateDir: string;
  db: string;
}

/** A throwaway DSH home plus an empty worker state dir and a missing master db. */
function pruneFixture(): PruneFixture {
  const root = mkdtempSync(join(tmpdir(), "gethen-session-prune-"));
  const dshHome = join(root, "dsh");
  const stateDir = join(root, "state");
  mkdirSync(dshHome, { recursive: true });
  mkdirSync(stateDir, { recursive: true });
  return { root, dshHome, stateDir, db: join(root, "master.sqlite") };
}

function writeSession(
  fixture: PruneFixture,
  sessionId: string,
  ageDays: number,
  now: Date,
  options: { openTurn?: boolean } = {},
): string {
  const dir = join(fixture.dshHome, "sessions", "--work-project--", sessionId);
  mkdirSync(dir, { recursive: true });
  const log = join(dir, "session.v4.jsonl.zstd");
  writeFileSync(log, "x".repeat(4096));
  const mtime = new Date(now.getTime() - ageDays * DAY_MS);
  utimesSync(log, mtime, mtime);
  utimesSync(dir, mtime, mtime);
  if (options.openTurn === true) {
    const projcache = join(fixture.dshHome, "storages", "session_projcache", "sessions");
    mkdirSync(projcache, { recursive: true });
    writeFileSync(
      join(projcache, `${sessionId}.json`),
      JSON.stringify({
        version: 7,
        record: {
          identity: { formatVersion: 4, createdAt: mtime.getTime(), cwd: "/work/project" },
          rows: {
            turnBoundary: { ver: 2, seq: 1, val: { openTurnStartSeq: 1 } },
          },
        },
      }),
    );
  }
  return dir;
}

describe("gethen session prune", () => {
  const NOW = new Date("2026-11-20T00:00:00.000Z");

  it("is a dry run unless --yes is passed and never deletes candidates", async () => {
    const fixture = pruneFixture();
    try {
      const old = writeSession(fixture, "old-session", 40, NOW);
      const fresh = writeSession(fixture, "fresh-session", 1, NOW);
      const out = capture();
      const errors = capture();
      expect(
        await sessionCommand(
          [
            "prune",
            "--older-than",
            "30",
            "--dsh-home",
            fixture.dshHome,
            "--state-dir",
            fixture.stateDir,
            "--db",
            fixture.db,
          ],
          { write: out.write, error: errors.write, now: NOW },
        ),
      ).toBe(0);
      const text = out.lines.join("\n");
      expect(text).toContain("dry run");
      expect(text).toContain("would free");
      expect(text).toContain("old-session");
      expect(existsSync(old)).toBe(true);
      expect(existsSync(fresh)).toBe(true);
      expect(errors.lines).toEqual([]);
    } finally {
      rmSync(fixture.root, { recursive: true, force: true });
    }
  });

  it("deletes old sessions but skips key-registry mappings and open turns", async () => {
    const fixture = pruneFixture();
    try {
      const removable = writeSession(fixture, "old-removable", 40, NOW);
      const keyed = writeSession(fixture, "old-keyed", 40, NOW);
      const running = writeSession(fixture, "old-running", 40, NOW, { openTurn: true });
      const fresh = writeSession(fixture, "fresh-session", 1, NOW);
      writeFileSync(
        join(fixture.stateDir, "worker-1.sessions.json"),
        JSON.stringify({ "project-x": "old-keyed" }),
      );

      const out = capture();
      const errors = capture();
      expect(
        await sessionCommand(
          [
            "prune",
            "--older-than",
            "30",
            "--yes",
            "--json",
            "--dsh-home",
            fixture.dshHome,
            "--state-dir",
            fixture.stateDir,
            "--db",
            fixture.db,
          ],
          { write: out.write, error: errors.write, now: NOW },
        ),
      ).toBe(0);
      expect(errors.lines).toEqual([]);
      const body = JSON.parse(out.lines.join("\n")) as {
        dryRun: boolean;
        deleted: Array<{ sessionId: string }>;
        skipped: Array<{ sessionId: string; reason: string }>;
        bytesFreed: number;
      };
      expect(body.dryRun).toBe(false);
      expect(body.deleted.map((entry) => entry.sessionId)).toEqual(["old-removable"]);
      expect(body.bytesFreed).toBe(4096);
      expect(body.skipped).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ sessionId: "old-keyed", reason: "key_registry" }),
          expect.objectContaining({ sessionId: "old-running", reason: "running_session" }),
        ]),
      );
      expect(existsSync(removable)).toBe(false);
      expect(existsSync(keyed)).toBe(true);
      expect(existsSync(running)).toBe(true);
      expect(existsSync(fresh)).toBe(true);
    } finally {
      rmSync(fixture.root, { recursive: true, force: true });
    }
  });

  it("refuses to prune when an existing master db cannot be read", async () => {
    const fixture = pruneFixture();
    try {
      writeFileSync(fixture.db, "not sqlite");
      const out = capture();
      const errors = capture();
      expect(
        await sessionCommand(
          [
            "prune",
            "--older-than",
            "30",
            "--yes",
            "--dsh-home",
            fixture.dshHome,
            "--state-dir",
            fixture.stateDir,
            "--db",
            fixture.db,
          ],
          { write: out.write, error: errors.write, now: NOW },
        ),
      ).toBe(1);
      expect(errors.lines.join("\n")).toContain("cannot read");
    } finally {
      rmSync(fixture.root, { recursive: true, force: true });
    }
  });
});

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createFakeLaunch, MasterService, type Policy, type Trigger } from "@gethen/master";
import { afterEach, describe, expect, it } from "vitest";
import { readStatus, type StatusSummary, statusCommand } from "./commands/status";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const POLICY: Policy = {
  version: 1,
  acceptMode: "auto",
  rules: [],
  sources: { local: { allowed: ["local-user"], bots: [] } },
};

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "gethen-status-"));
  dirs.push(dir);
  return dir;
}

function trigger(text: string): Trigger {
  return {
    id: `trigger-${crypto.randomUUID()}`,
    source: "local",
    thread: "local",
    actor: "local-user",
    bot: false,
    addressed: "mention",
    text,
    at: new Date().toISOString(),
    meta: { scope: "local" },
  };
}

/** Populate a master database through the real engine + fake launch, then close it. */
async function populatedDb(): Promise<string> {
  const dir = tempDir();
  const dbPath = join(dir, "master.sqlite");
  const service = new MasterService({ dbPath, policy: POLICY, launch: createFakeLaunch() });
  service.start();
  try {
    await service.submit(trigger("status smoke"));
  } finally {
    service.stop();
  }
  return dbPath;
}

describe("gethen status", () => {
  it("reads counts and the newest tickets from a closed master database", async () => {
    const dbPath = await populatedDb();

    const summary = readStatus(dbPath);
    expect(summary).toMatchObject({
      db: dbPath,
      tickets: 1,
      open: 1,
      receipts: 1,
      escalations: 0,
    });
    expect(summary.recent).toHaveLength(1);
    expect(summary.recent[0]).toMatchObject({
      stage: "person/review",
      latestReport: "ready",
    });
    expect(summary.recent[0]?.assignments).toHaveLength(1);
  });

  it("prints the summary as JSON", async () => {
    const dbPath = await populatedDb();
    const lines: string[] = [];

    expect(statusCommand(["--db", dbPath, "--json"], { write: (line) => lines.push(line) })).toBe(
      0,
    );

    expect(lines).toHaveLength(1);
    const summary = JSON.parse(lines[0] ?? "{}") as StatusSummary;
    expect(summary.tickets).toBe(1);
    expect(summary.open).toBe(1);
    expect(summary.receipts).toBe(1);
    expect(summary.recent[0]?.stage).toBe("person/review");
    expect(summary.recent[0]?.latestReport).toBe("ready");
  });

  it("prints a text table with counts and recent tickets", async () => {
    const dbPath = await populatedDb();
    const lines: string[] = [];

    expect(statusCommand(["--db", dbPath], { write: (line) => lines.push(line) })).toBe(0);

    const output = lines.join("\n");
    expect(output).toContain(`db: ${dbPath}`);
    expect(output).toContain("tickets: 1");
    expect(output).toContain("person/review");
    expect(output).toContain("ID");
    expect(output).toContain("assignment ids are included in --json output");
  });

  it("resolves the default database from GETHEN_STATE_DIR", async () => {
    const dbPath = await populatedDb();
    const lines: string[] = [];

    expect(
      statusCommand(["--json"], {
        env: { GETHEN_STATE_DIR: dirname(dbPath) },
        write: (line) => lines.push(line),
      }),
    ).toBe(0);

    const summary = JSON.parse(lines[0] ?? "{}") as StatusSummary;
    expect(summary.db).toBe(dbPath);
    expect(summary.tickets).toBe(1);
  });

  it("fails clearly when the database file does not exist", () => {
    const dir = tempDir();
    const lines: string[] = [];

    const code = statusCommand(["--db", join(dir, "missing.sqlite")], {
      error: (line) => lines.push(line),
    });

    expect(code).toBe(1);
    expect(lines.join("\n")).toContain("no master database at");
  });
});

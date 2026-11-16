import {
  existsSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteStore } from "@gethen/master";
import { hashWorkerToken, type WorkerTokenRecord } from "@gethen/transport-ws";
import { afterEach, describe, expect, it, vi } from "vitest";
import { workerCommand } from "./commands/worker";
import {
  type WorkerListEntry,
  type WorkerTokenOptions,
  workerAdd,
  workerList,
  workerRevoke,
} from "./commands/worker-token";

const dirs: string[] = [];

afterEach(() => {
  vi.restoreAllMocks();
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

const AT = "2026-11-16T12:00:00.000Z";

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "gethen-worker-token-"));
  dirs.push(dir);
  return dir;
}

/** An existing master sqlite (commands require the database to exist). */
function tempDb(): { dir: string; dbPath: string } {
  const dir = tempDir();
  const dbPath = join(dir, "master.sqlite");
  new SqliteStore(dbPath).close();
  return { dir, dbPath };
}

function capture(): { lines: string[]; write: (line: string) => void } {
  const lines: string[] = [];
  return { lines, write: (line: string) => lines.push(line) };
}

function tokens(prefix = "token"): () => string {
  let index = 0;
  return () => {
    index += 1;
    return `${prefix}-${index}`;
  };
}

/** Swallow the token printout in tests that only care about side effects. */
const silent = (): void => undefined;

function testOptions(overrides: Partial<WorkerTokenOptions> = {}): WorkerTokenOptions {
  return { now: () => new Date(AT), randomToken: tokens(), ...overrides };
}

function readTokens(path: string): WorkerTokenRecord[] {
  return JSON.parse(readFileSync(path, "utf8")) as WorkerTokenRecord[];
}

describe("gethen worker add/ls/revoke", () => {
  it("add persists the token hash and prints the token exactly once", () => {
    const { dbPath } = tempDb();
    const out = capture();

    const code = workerAdd(["--worker-id", "w1", "--name", "Box 1", "--db", dbPath], {
      ...testOptions(),
      randomToken: () => "token-one",
      write: out.write,
    });

    expect(code).toBe(0);
    const output = out.lines.join("\n");
    expect(output).toContain("worker w1 created");
    expect(output).toContain("token: token-one");
    expect(output).toContain("shown only once");
    expect(output.split("token-one")).toHaveLength(2); // appears exactly once

    const store = new SqliteStore(dbPath);
    try {
      const record = store.findWorkerByTokenHash(hashWorkerToken("token-one"));
      expect(record).toMatchObject({ id: "w1", name: "Box 1" });
      expect(record?.createdAt).toBe(AT);
      expect(record?.lastSeenAt).toBeUndefined();
      expect(record?.revokedAt).toBeUndefined();
    } finally {
      store.close();
    }
  });

  it("rejects a duplicate worker id and --force replaces its token", () => {
    const { dbPath } = tempDb();
    expect(
      workerAdd(["--worker-id", "w1", "--db", dbPath], {
        ...testOptions(),
        randomToken: () => "old-token",
        write: silent,
      }),
    ).toBe(0);

    const duplicate = capture();
    expect(
      workerAdd(["--worker-id", "w1", "--db", dbPath], {
        ...testOptions(),
        randomToken: () => "new-token",
        error: duplicate.write,
      }),
    ).toBe(1);
    expect(duplicate.lines.join("\n")).toContain("already exists");

    const replaced = capture();
    expect(
      workerAdd(["--worker-id", "w1", "--force", "--db", dbPath], {
        ...testOptions(),
        randomToken: () => "new-token",
        write: replaced.write,
      }),
    ).toBe(0);
    expect(replaced.lines.join("\n")).toContain("replaced");

    const store = new SqliteStore(dbPath);
    try {
      expect(store.findWorkerByTokenHash(hashWorkerToken("old-token"))).toBeUndefined();
      expect(store.findWorkerByTokenHash(hashWorkerToken("new-token"))?.id).toBe("w1");
    } finally {
      store.close();
    }
  });

  it("merges --tokens-file atomically and rotates one worker's token", () => {
    const { dir, dbPath } = tempDb();
    const tokensPath = join(dir, "worker-tokens.json");
    writeFileSync(tokensPath, `${JSON.stringify([{ workerId: "w0", token: "seed" }], null, 2)}\n`, {
      mode: 0o600,
    });

    expect(
      workerAdd(["--worker-id", "w1", "--tokens-file", tokensPath, "--db", dbPath], {
        ...testOptions(),
        randomToken: () => "token-a",
        write: silent,
      }),
    ).toBe(0);
    expect(readTokens(tokensPath)).toEqual([
      { workerId: "w0", token: "seed" },
      { workerId: "w1", token: "token-a" },
    ]);

    expect(
      workerAdd(["--worker-id", "w1", "--force", "--tokens-file", tokensPath, "--db", dbPath], {
        ...testOptions(),
        randomToken: () => "token-b",
        write: silent,
      }),
    ).toBe(0);
    expect(readTokens(tokensPath).filter((record) => record.workerId === "w1")).toEqual([
      { workerId: "w1", token: "token-b" },
    ]);
    // The file is replaced atomically with a 0600 temp file, no leftovers.
    expect(readdirSync(dir).filter((name) => name.includes(".tmp-"))).toEqual([]);
    expect(statSync(tokensPath).mode & 0o077).toBe(0);
  });

  it("lists workers as a text table and as JSON", () => {
    const { dbPath } = tempDb();
    const nextToken = tokens("ls");
    expect(
      workerAdd(["--worker-id", "w1", "--name", "Box 1", "--db", dbPath], {
        ...testOptions(),
        randomToken: nextToken,
        write: silent,
      }),
    ).toBe(0);
    expect(
      workerAdd(["--worker-id", "w2", "--name", "Box 2", "--db", dbPath], {
        ...testOptions(),
        randomToken: nextToken,
        write: silent,
      }),
    ).toBe(0);

    const text = capture();
    expect(workerList(["--db", dbPath], { ...testOptions(), write: text.write })).toBe(0);
    const output = text.lines.join("\n");
    expect(output).toContain("ID");
    expect(output).toContain("STATUS");
    expect(output).toContain("w1");
    expect(output).toContain("Box 2");
    expect(output).toContain("active");

    const json = capture();
    expect(workerList(["--db", dbPath, "--json"], { write: json.write })).toBe(0);
    const entries = JSON.parse(json.lines.join("\n")) as WorkerListEntry[];
    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({
      id: "w1",
      name: "Box 1",
      createdAt: AT,
      lastSeenAt: null,
      revoked: false,
      revokedAt: null,
    });
  });

  it("revokes idempotently and errors for unknown ids or missing flags", () => {
    const { dbPath } = tempDb();
    expect(
      workerAdd(["--worker-id", "w1", "--db", dbPath], { ...testOptions(), write: silent }),
    ).toBe(0);

    const missing = capture();
    expect(workerRevoke(["--db", dbPath], { ...testOptions(), error: missing.write })).toBe(1);
    expect(missing.lines.join("\n")).toContain("--id");

    const unknown = capture();
    expect(
      workerRevoke(["--id", "nope", "--db", dbPath], { ...testOptions(), error: unknown.write }),
    ).toBe(1);
    expect(unknown.lines.join("\n")).toContain("not found");

    const first = capture();
    expect(
      workerRevoke(["--id", "w1", "--db", dbPath], { ...testOptions(), write: first.write }),
    ).toBe(0);
    expect(first.lines.join("\n")).toContain("worker w1 revoked");
    const store = new SqliteStore(dbPath);
    try {
      expect(store.getWorker("w1")?.revokedAt).toBe(AT);
    } finally {
      store.close();
    }

    const again = capture();
    expect(
      workerRevoke(["--id", "w1", "--db", dbPath], { ...testOptions(), write: again.write }),
    ).toBe(0);
    expect(again.lines.join("\n")).toContain("already revoked");
  });

  it("fails clearly when the master database does not exist", () => {
    const { dir } = tempDb();
    const missing = join(dir, "missing.sqlite");
    const addErrors = capture();
    expect(
      workerAdd(["--worker-id", "w1", "--db", missing], {
        ...testOptions(),
        error: addErrors.write,
      }),
    ).toBe(1);
    expect(addErrors.lines.join("\n")).toContain("no master database");

    const listErrors = capture();
    expect(workerList(["--db", missing], { ...testOptions(), error: listErrors.write })).toBe(1);
    expect(listErrors.lines.join("\n")).toContain("no master database");
    expect(existsSync(missing)).toBe(false);
  });

  it("uses GETHEN_STATE_DIR for the default database", () => {
    const dir = tempDir();
    new SqliteStore(join(dir, "master.sqlite")).close();
    const out = capture();

    expect(
      workerList(["--json"], {
        ...testOptions(),
        env: { GETHEN_STATE_DIR: dir },
        write: out.write,
      }),
    ).toBe(0);
    expect(JSON.parse(out.lines.join("\n"))).toEqual([]);
  });

  it("prints subcommand help and dispatches through gethen worker", async () => {
    const addHelp = capture();
    expect(workerAdd(["--help"], { write: addHelp.write })).toBe(0);
    expect(addHelp.lines.join("\n")).toContain("--tokens-file");

    const lsHelp = capture();
    expect(workerList(["--help"], { write: lsHelp.write })).toBe(0);
    expect(lsHelp.lines.join("\n")).toContain("--json");

    const revokeHelp = capture();
    expect(workerRevoke(["--help"], { write: revokeHelp.write })).toBe(0);
    expect(revokeHelp.lines.join("\n")).toContain("--id");

    // The daemon path is only bypassed for the three subcommands.
    const { dbPath } = tempDb();
    const stdout = vi.spyOn(process.stdout, "write").mockReturnValue(true);
    expect(await workerCommand(["ls", "--db", dbPath, "--json"])).toBe(0);
    const output = stdout.mock.calls.map((call) => String(call[0])).join("");
    expect(output.trim()).toBe("[]");
  });
});

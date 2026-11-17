import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import { DshSessionStore } from "./store";

const CREATED = 1_790_000_000_000;

function fixture(
  options: {
    sessionId?: string;
    cwd?: string;
    openTurn?: boolean;
    withLog?: boolean;
    title?: string;
  } = {},
) {
  const sessionId = options.sessionId ?? "session-a";
  const cwd = options.cwd ?? "/work/project";
  const home = mkdtempSync(join(tmpdir(), "gethen-dsh-sessions-"));
  const projcache = join(home, "storages", "session_projcache", "sessions");
  mkdirSync(projcache, { recursive: true });
  writeFileSync(
    join(projcache, `${sessionId}.json`),
    JSON.stringify({
      version: 7,
      record: {
        identity: { formatVersion: 4, createdAt: CREATED, cwd },
        rows: {
          turnBoundary: {
            ver: 2,
            seq: 42,
            val: { openTurnStartSeq: options.openTurn ? 40 : null },
          },
          modelSelection: {
            ver: 3,
            seq: 42,
            val: { lastUsed: { provider: "opencode-go", model: "deepseek-v4.1-flash" } },
          },
          sandboxMode: { ver: 1, seq: 42, val: "workspace-write" },
          approvalState: { ver: 1, seq: 42, val: { policy: "never" } },
          title: { ver: 1, seq: 42, val: options.title ?? "fixture task" },
          tokenUsage: { ver: 1, seq: 42, val: { inputTokens: 10, outputTokens: 20 } },
        },
      },
    }),
  );
  if (options.withLog !== false) {
    const dir = join(home, "sessions", "--work-project--", sessionId);
    mkdirSync(dir, { recursive: true });
    const lines = [
      { type: "session", version: 4, id: sessionId, createdAt: CREATED, cwd },
      { type: "user/message", seq: 0, time: CREATED + 1, data: { text: "hello" } },
      { type: "assistant/message", seq: 1, time: CREATED + 2, data: { text: "hi" } },
      { type: "tool/call", seq: 2, time: CREATED + 3, data: { name: "bash" } },
      { type: "turn/end", seq: 3, time: CREATED + 4, data: { turn: 1 } },
    ];
    writeFileSync(
      join(dir, "session.v4.jsonl.zstd"),
      zstdCompressSync(Buffer.from(lines.map((line) => JSON.stringify(line)).join("\n"))),
    );
  }
  return { home, sessionId };
}

describe("DshSessionStore", () => {
  it("lists sessions with config, state and event metadata", async () => {
    const { home } = fixture();
    const store = new DshSessionStore({ home });
    const sessions = await store.list();
    expect(sessions).toHaveLength(1);
    expect(sessions[0]).toMatchObject({
      sessionId: "session-a",
      state: "idle",
      hasLog: true,
      eventCount: 42,
      config: {
        cwd: "/work/project",
        title: "fixture task",
        model: { provider: "opencode-go", model: "deepseek-v4.1-flash" },
        sandboxMode: "workspace-write",
        approvalPolicy: "never",
      },
    });
    expect(sessions[0]?.lastActivityAt).toBeDefined();
  });

  it("marks an open turn as running and filters by workspace", async () => {
    const { home } = fixture({ openTurn: true, title: "running task" });
    const store = new DshSessionStore({ home });
    expect((await store.list())[0]?.state).toBe("running");
    expect(await store.list({ workspace: "/work/project" })).toHaveLength(1);
    expect(await store.list({ workspace: "/other" })).toHaveLength(0);
  });

  it("returns normalized history tail, forward pages and type-safe errors", async () => {
    const { home, sessionId } = fixture();
    const store = new DshSessionStore({ home });
    const tail = await store.history(sessionId, { limit: 2 });
    expect(tail.map((event) => event.type)).toEqual(["tool/call", "turn/end"]);
    const forward = await store.history(sessionId, { afterSeq: 0, limit: 10 });
    expect(forward.map((event) => event.seq)).toEqual([1, 2, 3]);
    expect(await store.history("missing")).toEqual([]);
  });

  it("describes unknown sessions and tolerates a missing log", async () => {
    const { home, sessionId } = fixture({ withLog: false });
    const store = new DshSessionStore({ home });
    const summary = await store.describe(sessionId);
    expect(summary).toMatchObject({ sessionId, hasLog: false, state: "unknown" });
    expect(await store.describe("nope")).toBeUndefined();
  });
});

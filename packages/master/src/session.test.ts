import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";

import { Engine, type LaunchEffectPayload } from "./engine";
import { MemoryRepositories } from "./memory";
import { type Assignment, type Policy, parseSessionSpec, type Ticket, type Trigger } from "./model";
import { SqliteStore } from "./store";

const START = "2026-11-17T09:00:00.000Z";

function policy(): Policy {
  return {
    version: 1,
    acceptMode: "auto",
    rules: [
      {
        id: "fix-bug",
        name: "Fix a bug",
        instruction: "Reproduce the reported bug and fix it",
        enabled: true,
      },
    ],
    sources: { github: { allowed: ["alice"], bots: ["dependabot"] } },
  };
}

function trigger(id: string, overrides: Partial<Trigger> = {}): Trigger {
  return {
    id,
    source: "github",
    thread: "thread-1",
    actor: "alice",
    bot: false,
    addressed: "mention",
    text: `request ${id}`,
    at: START,
    ...overrides,
  };
}

function assignment(overrides: Partial<Assignment> = {}): Assignment {
  return {
    id: "asg-1",
    ticket: "tkt-1",
    worker: "worker-a",
    workspace: "/tmp/ws",
    packDigest: "sha256:pack",
    state: "pending",
    operationId: "op-1",
    ...overrides,
  };
}

function tempPath(name: string): string {
  return join(mkdtempSync(join(tmpdir(), "gethen-session-")), name);
}

function setup() {
  const repos = new MemoryRepositories(policy());
  let now = new Date(START);
  let counter = 0;
  const engine = new Engine({
    repos,
    clock: { now: () => new Date(now) },
    ids: { next: (prefix) => `${prefix}-${++counter}` },
  });
  return {
    repos,
    engine,
    at: () => now.toISOString(),
    receive(id: string, overrides: Partial<Trigger> = {}) {
      return engine.receive(trigger(id, overrides));
    },
    advance(ms: number) {
      now = new Date(now.getTime() + ms);
    },
  };
}

type Context = ReturnType<typeof setup>;

function onlyTicket(ctx: Context): Ticket {
  const ticket = ctx.repos.tickets.list()[0];
  if (!ticket) throw new Error("expected one ticket");
  return ticket;
}

function lastAssignment(ctx: Context, ticket: Ticket): Assignment {
  const id = ticket.assignments.at(-1);
  const value = id === undefined ? undefined : ctx.repos.assignments.get(id);
  if (!value) throw new Error("expected one assignment");
  return value;
}

function accept(ctx: Context, triggerId = "t1") {
  ctx.receive(triggerId);
  ctx.engine.evaluateDue();
  return ctx.engine.decide(triggerId, "accept");
}

describe("parseSessionSpec", () => {
  it("defaults to a new session when the spec is absent or blank", () => {
    expect(parseSessionSpec(undefined)).toEqual({ ok: true, value: { mode: "new" } });
    expect(parseSessionSpec(null)).toEqual({ ok: true, value: { mode: "new" } });
    expect(parseSessionSpec("")).toEqual({ ok: true, value: { mode: "new" } });
  });

  it("parses JSON text from meta and already-parsed objects", () => {
    expect(parseSessionSpec('{"mode":"reuse","sessionId":"session-1"}')).toEqual({
      ok: true,
      value: { mode: "reuse", sessionId: "session-1" },
    });
    expect(parseSessionSpec({ mode: "new" })).toEqual({ ok: true, value: { mode: "new" } });
    expect(parseSessionSpec({ mode: "reuse", key: "project-main" })).toEqual({
      ok: true,
      value: { mode: "reuse", key: "project-main" },
    });
  });

  it("ignores unknown extra fields", () => {
    expect(parseSessionSpec({ mode: "new", future: true })).toEqual({
      ok: true,
      value: { mode: "new" },
    });
  });

  it('rejects "new" with a sessionId but accepts a key for later reuse', () => {
    expect(parseSessionSpec({ mode: "new", sessionId: "session-1" }).ok).toBe(false);
    expect(parseSessionSpec({ mode: "new", key: "project-main" })).toMatchObject({
      ok: true,
      value: { mode: "new", key: "project-main" },
    });
  });

  it('requires exactly one of sessionId or key for "reuse"', () => {
    expect(parseSessionSpec({ mode: "reuse" }).ok).toBe(false);
    expect(parseSessionSpec({ mode: "reuse", sessionId: "s", key: "k" }).ok).toBe(false);
  });

  it("rejects malformed JSON, non-objects and unknown modes", () => {
    expect(parseSessionSpec("{not json").ok).toBe(false);
    expect(parseSessionSpec("42").ok).toBe(false);
    expect(parseSessionSpec([]).ok).toBe(false);
    expect(parseSessionSpec({}).ok).toBe(false);
    expect(parseSessionSpec({ mode: "maybe" }).ok).toBe(false);
  });

  it("rejects blank or non-string session ids and keys", () => {
    expect(parseSessionSpec({ mode: "reuse", sessionId: "" }).ok).toBe(false);
    expect(parseSessionSpec({ mode: "reuse", sessionId: 3 }).ok).toBe(false);
    expect(parseSessionSpec({ mode: "reuse", key: "" }).ok).toBe(false);
  });
});

describe("engine session propagation", () => {
  it("copies a valid meta.session onto the Assignment and the launch effect", () => {
    const ctx = setup();
    ctx.receive("t1", { meta: { session: '{"mode":"reuse","key":"project-main"}' } });
    ctx.engine.evaluateDue();

    const decision = ctx.engine.decide("t1", "accept");
    expect(decision.outcome).toBe("accept");

    const ticket = onlyTicket(ctx);
    expect(lastAssignment(ctx, ticket).session).toEqual({
      mode: "reuse",
      key: "project-main",
    });
    const launch = ctx.engine.drain().find((effect) => effect.kind === "launch");
    if (launch === undefined) throw new Error("expected a launch effect");
    expect((launch.payload as LaunchEffectPayload).assignment.session).toEqual({
      mode: "reuse",
      key: "project-main",
    });
  });

  it("defaults the Assignment to a new session when meta names none", () => {
    const ctx = setup();
    accept(ctx);
    expect(lastAssignment(ctx, onlyTicket(ctx)).session).toEqual({ mode: "new" });
  });

  it("ignores work with an invalid session spec instead of accepting it", () => {
    const ctx = setup();
    ctx.receive("t1", { meta: { session: '{"mode":"reuse"}' } });
    ctx.engine.evaluateDue();

    const decision = ctx.engine.decide("t1", "accept");
    expect(decision.outcome).toBe("ignore");
    expect(decision.reasons[0]).toMatchObject({ code: "invalid_session_spec" });
    expect(ctx.repos.tickets.list()).toHaveLength(0);
    expect(ctx.engine.drain()).toEqual([]);
  });

  it("retries with a fresh session even when the first Assignment reused one", () => {
    const ctx = setup();
    ctx.receive("t1", { meta: { session: '{"mode":"reuse","sessionId":"session-1"}' } });
    ctx.engine.evaluateDue();
    ctx.engine.decide("t1", "accept");

    const ticket = onlyTicket(ctx);
    ctx.engine.drain();
    const retried = ctx.engine.retryTicket(ticket.id);
    expect(retried.session).toEqual({ mode: "new" });
    expect(ticket.assignments.at(-1)).toBe(retried.id);
  });
});

describe("assignment session persistence", () => {
  it("round-trips the session spec across a store restart", () => {
    const path = tempPath("state.db");
    const spec = { mode: "reuse" as const, sessionId: "session-9" };
    const first = new SqliteStore(path);
    try {
      first.assignments.save(assignment({ session: spec }));
    } finally {
      first.close();
    }

    const second = new SqliteStore(path);
    try {
      expect(second.assignments.get("asg-1")?.session).toEqual(spec);
    } finally {
      second.close();
    }
  });

  it("migrates a database whose assignments table predates session_json", () => {
    const path = tempPath("legacy.db");
    const legacy = new DatabaseSync(path);
    try {
      legacy.exec(`
        CREATE TABLE assignments (
          id TEXT PRIMARY KEY,
          ticket TEXT NOT NULL,
          worker TEXT NOT NULL,
          workspace TEXT NOT NULL,
          pack_digest TEXT NOT NULL,
          state TEXT NOT NULL,
          operation_id TEXT NOT NULL,
          started_at TEXT,
          ended_at TEXT
        );
        INSERT INTO assignments VALUES (
          'asg-old', 'tkt-old', 'worker-old', '/tmp/ws',
          'sha256:pack', 'ended', 'op-old', '${START}', '${START}'
        );
      `);
    } finally {
      legacy.close();
    }

    const store = new SqliteStore(path);
    try {
      const migrated = store.assignments.get("asg-old");
      if (migrated === undefined) throw new Error("expected the legacy assignment");
      expect(migrated.session).toBeUndefined();
      expect(migrated.worker).toBe("worker-old");

      store.assignments.save({ ...migrated, session: { mode: "reuse", key: "project-main" } });
      expect(store.assignments.get("asg-old")?.session).toEqual({
        mode: "reuse",
        key: "project-main",
      });
    } finally {
      store.close();
    }

    // Reopening runs the ALTER again; the duplicate-column fallback must hold.
    const reopened = new SqliteStore(path);
    try {
      expect(reopened.assignments.get("asg-old")?.session).toEqual({
        mode: "reuse",
        key: "project-main",
      });
    } finally {
      reopened.close();
    }
  });
});

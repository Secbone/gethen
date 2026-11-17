import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import { FileLock, withFileLock } from "./lock";
import type {
  Assignment,
  AssignmentReceipt,
  Decision,
  EscalationRecord,
  Eval,
  Input,
  Mandate,
  Policy,
  Thread,
  Ticket,
  Trigger,
  WorkerRecord,
} from "./model";
import { OutboxRunner } from "./outbox";
import { DEFAULT_POLICY, SqliteStore } from "./store";

const AT = "2026-11-16T06:00:00.000Z";
const LATER = "2026-11-16T06:05:00.000Z";
const NEVER = "9999-12-31T23:59:59.000Z";

const openStores: SqliteStore[] = [];

afterEach(() => {
  for (const store of openStores.splice(0)) store.close();
});

function tempPath(name: string): string {
  return join(mkdtempSync(join(tmpdir(), "gethen-master-")), name);
}

function openStore(path: string = tempPath("state.db")): SqliteStore {
  const store = new SqliteStore(path);
  openStores.push(store);
  return store;
}

function plus(base: string, milliseconds: number): string {
  return new Date(Date.parse(base) + milliseconds).toISOString();
}

function defined<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("expected a value");
  return value;
}

describe("SqliteStore repositories", () => {
  it("round-trips every aggregate, including JSON columns and optional fields", () => {
    const store = openStore();
    const trigger: Trigger = {
      id: "trg-1",
      source: "local",
      thread: "thr-1",
      actor: "local-user",
      bot: false,
      addressed: "direct",
      text: "hello",
      at: AT,
      meta: { channel: "test" },
    };
    const thread: Thread = {
      id: "thr-1",
      source: "local",
      triggers: ["trg-1"],
      pending: ["trg-2"],
      subscriptions: ["tkt-1"],
      activeEval: "ev-1",
      expired: 2,
    };
    const evaluation: Eval = {
      id: "ev-1",
      thread: "thr-1",
      triggers: ["trg-1"],
      policyVersion: 1,
      startedAt: AT,
      deadlineAt: LATER,
      deliveredAt: LATER,
    };
    const decision: Decision = {
      id: "dec-1",
      eval: "ev-1",
      outcome: "accept",
      strength: "explicit",
      reasons: [{ code: "addressed", values: { score: 1 }, triggerId: "trg-1", text: "direct" }],
      at: AT,
      dismissedAt: LATER,
    };
    const mandate: Mandate = {
      id: "man-1",
      ticket: "tkt-1",
      policyVersion: 1,
      strength: "explicit",
      scope: "local",
      basis: { via: "addressed", source: "local", sender: "local-user", scope: "chat" },
      matchedRules: ["rule-1"],
      createdAt: AT,
      withdrawnAt: LATER,
    };
    const ticket: Ticket = {
      id: "tkt-1",
      mandate: "man-1",
      source: "local",
      threads: ["thr-1"],
      subscriptions: ["thr-1", "thr-2"],
      channels: ["ch-1"],
      inputs: ["inp-1"],
      assignments: ["asg-1"],
      openedAt: AT,
      endedAt: LATER,
      outcome: "completed",
      latestReport: "ready",
      attention: {
        question: "Which branch?",
        options: [{ id: "main", label: "main" }],
        askedAt: AT,
      },
      delivery: { error: "timeout", at: AT, attempt: 2 },
      ack: { at: LATER, stage: "person/review", fingerprint: "person/review|ready||" },
      archivedAt: LATER,
    };
    const assignment: Assignment = {
      id: "asg-1",
      ticket: "tkt-1",
      worker: "worker-a",
      workspace: "/tmp/workspace",
      packDigest: "sha256:abc",
      state: "ended",
      operationId: "op-1",
      startedAt: AT,
      endedAt: LATER,
    };
    const input: Input = {
      id: "inp-1",
      ticket: "tkt-1",
      kind: "trigger",
      text: "hello",
      trigger: "trg-1",
      deliveredAt: LATER,
      readAt: LATER,
      fromPerson: true,
    };
    const receipt: AssignmentReceipt = {
      assignment: "asg-1",
      ticket: "tkt-1",
      worker: "worker-a",
      workspace: "/tmp/workspace",
      packDigest: "sha256:abc",
      at: AT,
      outcome: "completed",
      summary: "done",
      events: 3,
      usage: { inputTokens: 10, outputTokens: 20, costUsd: 0.5 },
    };

    store.threads.save(thread);
    store.triggers.save(trigger);
    store.evals.save(evaluation);
    store.decisions.save(decision);
    store.mandates.save(mandate);
    store.tickets.save(ticket);
    store.assignments.save(assignment);
    store.inputs.save(input);
    store.append(receipt);

    expect(store.threads.get("thr-1")).toEqual(thread);
    expect(store.triggers.get("trg-1")).toEqual(trigger);
    expect(store.evals.get("ev-1")).toEqual(evaluation);
    expect(store.decisions.get("dec-1")).toEqual(decision);
    expect(store.mandates.get("man-1")).toEqual(mandate);
    expect(store.tickets.get("tkt-1")).toEqual(ticket);
    expect(store.assignments.get("asg-1")).toEqual(assignment);
    expect(store.inputs.get("inp-1")).toEqual(input);
    expect(store.listReceipts()).toEqual([receipt]);

    expect(store.threads.list()).toEqual([thread]);
    expect(store.triggers.list()).toEqual([trigger]);
    expect(store.evals.list()).toEqual([evaluation]);
    expect(store.decisions.list()).toEqual([decision]);
    expect(store.mandates.list()).toEqual([mandate]);
    expect(store.tickets.list()).toEqual([ticket]);
    expect(store.assignments.list()).toEqual([assignment]);
    expect(store.inputs.list()).toEqual([input]);
  });

  it("does not invent optional keys and upserts by id", () => {
    const store = openStore();
    const minimal: Ticket = {
      id: "tkt-2",
      mandate: "man-1",
      source: "local",
      threads: [],
      subscriptions: [],
      channels: [],
      inputs: [],
      assignments: [],
      openedAt: AT,
    };
    store.tickets.save(minimal);
    const read = defined(store.tickets.get("tkt-2"));
    expect(read).toEqual(minimal);
    expect(read).not.toHaveProperty("endedAt");
    expect(read).not.toHaveProperty("latestReport");
    expect(read).not.toHaveProperty("attention");
    expect(read).not.toHaveProperty("ack");
    expect(read).not.toHaveProperty("archivedAt");

    const trigger: Trigger = {
      id: "trg-1",
      source: "local",
      thread: "thr-1",
      actor: "a",
      bot: true,
      addressed: "none",
      text: "before",
      at: AT,
    };
    store.triggers.save(trigger);
    store.triggers.save({ ...trigger, text: "after" });
    expect(store.triggers.list()).toHaveLength(1);
    expect(defined(store.triggers.get("trg-1")).text).toBe("after");
  });
});

describe("SqliteStore transactions", () => {
  it("rolls back state and outbox effects together, and commits on success", () => {
    const store = openStore();
    const thread: Thread = {
      id: "thr-1",
      source: "local",
      triggers: [],
      pending: [],
      subscriptions: [],
    };
    expect(() =>
      store.transaction(() => {
        store.threads.save(thread);
        store.enqueue([{ kind: "deliver", key: "k-1", payload: { n: 1 }, dueAt: AT }], AT);
        throw new Error("boom");
      }),
    ).toThrow("boom");
    expect(store.threads.get("thr-1")).toBeUndefined();
    expect(store.claimDue(NEVER, 10)).toEqual([]);

    store.transaction(() => {
      store.threads.save(thread);
      store.enqueue([{ kind: "deliver", key: "k-1", payload: { n: 1 }, dueAt: AT }], AT);
    });
    expect(store.threads.get("thr-1")).toEqual(thread);
    expect(store.claimDue(LATER, 10)).toHaveLength(1);
  });

  it("supports nested transactions as savepoints", () => {
    const store = openStore();
    const outer: Thread = {
      id: "outer",
      source: "local",
      triggers: [],
      pending: [],
      subscriptions: [],
    };
    const inner: Thread = {
      id: "inner",
      source: "local",
      triggers: [],
      pending: [],
      subscriptions: [],
    };
    store.transaction(() => {
      store.threads.save(outer);
      expect(() =>
        store.transaction(() => {
          store.threads.save(inner);
          throw new Error("inner boom");
        }),
      ).toThrow("inner boom");
    });
    expect(store.threads.get("outer")).toEqual(outer);
    expect(store.threads.get("inner")).toBeUndefined();
  });
});

describe("SqliteStore effects", () => {
  it("claims only due effects, in order, and never twice", () => {
    const store = openStore();
    store.enqueue(
      [
        { kind: "a", key: "k-1", payload: { n: 1 }, dueAt: "2026-11-16T06:00:02.000Z" },
        { kind: "a", key: "k-2", payload: { n: 2 }, dueAt: "2026-11-16T06:00:01.000Z" },
        { kind: "a", key: "k-3", payload: { n: 3 }, dueAt: "2026-11-16T07:00:00.000Z" },
      ],
      AT,
    );

    const first = store.claimDue("2026-11-16T06:00:05.000Z", 10);
    expect(first.map((effect) => effect.payload)).toEqual([{ n: 2 }, { n: 1 }]);
    for (const effect of first) {
      expect(effect.status).toBe("running");
      expect(effect.attempt).toBe(0);
    }
    expect(store.claimDue("2026-11-16T06:00:05.000Z", 10)).toEqual([]);
    expect(store.claimDue("2026-11-16T07:00:00.000Z", 10).map((effect) => effect.payload)).toEqual([
      { n: 3 },
    ]);
  });

  it("honours the claim limit", () => {
    const store = openStore();
    store.enqueue(
      [
        { kind: "a", key: "k-1", payload: 1, dueAt: AT },
        { kind: "a", key: "k-2", payload: 2, dueAt: AT },
      ],
      AT,
    );
    expect(store.claimDue(LATER, 1)).toHaveLength(1);
    expect(store.claimDue(LATER, 1)).toHaveLength(1);
    expect(store.claimDue(LATER, 1)).toHaveLength(0);
  });

  it("marks completed effects done", () => {
    const store = openStore();
    store.enqueue([{ kind: "a", key: "k-1", payload: null, dueAt: AT }], AT);
    const claimed = store.claimDue(LATER, 1);
    store.complete(defined(claimed[0]).id);
    expect(store.claimDue(NEVER, 10)).toEqual([]);
  });

  it("retries with backoff and fails after more than five attempts", () => {
    const store = openStore();
    const [id] = store.enqueue([{ kind: "a", key: "k-1", payload: null, dueAt: AT }], AT);
    const effectId = defined(id);

    expect(store.claimDue(AT, 1)).toHaveLength(1);
    store.fail(effectId, 5_000, AT);
    expect(store.claimDue(plus(AT, 4_999), 1)).toEqual([]);
    let clock = plus(AT, 5_000);

    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const claimed = store.claimDue(clock, 1);
      expect(claimed).toHaveLength(1);
      expect(defined(claimed[0]).attempt).toBe(attempt);
      store.fail(effectId, 0, clock);
      clock = plus(clock, 1);
    }
    // The sixth failure exceeds MAX_EFFECT_ATTEMPTS and retires the effect.
    expect(store.claimDue(NEVER, 10)).toEqual([]);
  });

  it("resets running effects left behind by a crash", () => {
    const store = openStore();
    store.enqueue([{ kind: "a", key: "k-1", payload: null, dueAt: AT }], AT);
    expect(store.claimDue(AT, 1)).toHaveLength(1);

    expect(store.resetStale(plus(AT, 30_000), 60_000)).toBe(0);
    expect(store.resetStale(plus(AT, 61_000), 60_000)).toBe(1);
    const reclaimed = store.claimDue(plus(AT, 61_000), 1);
    expect(reclaimed).toHaveLength(1);
    expect(defined(reclaimed[0]).attempt).toBe(0);
  });
});

describe("SqliteStore policy and operations", () => {
  it("stores a versioned policy and reloads it after reopen", () => {
    const path = tempPath("state.db");
    const first = openStore(path);
    expect(first.policy.current()).toEqual(DEFAULT_POLICY);

    const policy: Policy = {
      version: 2,
      acceptMode: "auto",
      rules: [{ id: "rule-1", name: "allow", instruction: "allow local work", enabled: true }],
      sources: { local: { allowed: ["local-user"], bots: [] } },
    };
    first.setPolicy(policy);
    expect(first.policy.current()).toEqual(policy);
    first.close();

    const second = openStore(path);
    expect(second.policy.current()).toEqual(policy);
  });

  it("rejects a non-positive policy version", () => {
    const store = openStore();
    expect(() => store.setPolicy({ ...DEFAULT_POLICY, version: 0 })).toThrow("policy.version");
  });

  it("records operations idempotently and rejects hash conflicts", () => {
    const store = openStore();
    expect(store.findOperation("op-1")).toBeUndefined();
    expect(store.recordOperation("op-1", "launch", "hash-a", { accepted: true }).result).toEqual({
      accepted: true,
    });

    const found = defined(store.findOperation("op-1"));
    expect(found).toMatchObject({
      id: "op-1",
      kind: "launch",
      requestHash: "hash-a",
      result: { accepted: true },
    });
    // Same id and hash is a replay: the original result wins.
    expect(store.recordOperation("op-1", "launch", "hash-a", { accepted: false }).result).toEqual({
      accepted: true,
    });
    expect(() => store.recordOperation("op-1", "launch", "hash-b", {})).toThrow(
      /OPERATION_CONFLICT/,
    );

    expect(store.runOperation("op-2", "launch", "hash-c", () => 42)).toBe(42);
    expect(store.runOperation("op-2", "launch", "hash-c", () => 99)).toBe(42);
    expect(() => store.runOperation("op-2", "launch", "hash-d", () => 1)).toThrow(
      /OPERATION_CONFLICT/,
    );
  });

  it("rejects use after close and closes idempotently", () => {
    const store = openStore();
    store.close();
    store.close();
    expect(() => store.threads.list()).toThrow(/closed/);
  });
});

describe("OutboxRunner", () => {
  it("runs due effects, completes successes and backs off failures", async () => {
    const store = openStore();
    store.enqueue(
      [
        { kind: "deliver", key: "k-1", payload: { n: 1 }, dueAt: AT },
        { kind: "launch", key: "k-2", payload: { n: 2 }, dueAt: AT },
      ],
      AT,
    );
    const delivered: unknown[] = [];
    const errors: unknown[] = [];
    const runner = new OutboxRunner({
      store,
      handlers: {
        deliver: async (payload) => {
          delivered.push(payload);
        },
        launch: async () => {
          throw new Error("worker down");
        },
      },
      now: () => new Date(AT),
      onError: (error) => {
        errors.push(error);
      },
    });

    expect(await runner.runDue()).toBe(2);
    expect(delivered).toEqual([{ n: 1 }]);
    expect(errors).toHaveLength(1);
    expect(store.claimDue(plus(AT, 1_999), 10)).toEqual([]);
    const retried = store.claimDue(plus(AT, 2_000), 10);
    expect(retried).toHaveLength(1);
    expect(defined(retried[0]).kind).toBe("launch");
    expect(defined(retried[0]).attempt).toBe(1);
  });

  it("treats a missing handler as a retryable failure", async () => {
    const store = openStore();
    store.enqueue([{ kind: "missing", key: "k-1", payload: null, dueAt: AT }], AT);
    const runner = new OutboxRunner({ store, handlers: {}, now: () => new Date(AT) });
    expect(await runner.runDue()).toBe(1);
    expect(store.claimDue(plus(AT, 1_999), 10)).toEqual([]);
    expect(store.claimDue(plus(AT, 2_000), 10)).toHaveLength(1);
  });

  it("starts and stops a polling timer once", () => {
    vi.useFakeTimers();
    try {
      const store = openStore();
      const runner = new OutboxRunner({ store, handlers: {} });
      expect(runner.started).toBe(false);
      runner.start(1_000);
      expect(runner.started).toBe(true);
      runner.start(1_000);
      runner.stop();
      expect(runner.started).toBe(false);
      runner.stop();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("FileLock", () => {
  it("takes over a lock whose recorded process is dead", () => {
    const path = tempPath("master.lock");
    const child = spawnSync(process.execPath, ["-e", "process.stdout.write(String(process.pid))"], {
      encoding: "utf8",
    });
    const deadPid = Number(child.stdout);
    expect(Number.isSafeInteger(deadPid)).toBe(true);
    const dead = JSON.stringify({ pid: deadPid, nonce: "dead" });
    writeFileSync(path, dead);
    // A crash while recovering must not wedge the lock either.
    writeFileSync(`${path}.recovery`, dead);

    const lock = new FileLock(path);
    expect(lock.locked).toBe(false);
    lock.acquire();
    expect(lock.locked).toBe(true);
    const owner = JSON.parse(readFileSync(path, "utf8")) as { pid: number; nonce: string };
    expect(owner.pid).toBe(process.pid);
    expect(owner.nonce).not.toBe("dead");
    lock.release();
    expect(lock.locked).toBe(false);
    expect(existsSync(path)).toBe(false);
    expect(existsSync(`${path}.recovery`)).toBe(false);
  });

  it("refuses a live writer and releases after a failed critical section", () => {
    const path = tempPath("master.lock");
    writeFileSync(path, JSON.stringify({ pid: process.pid, nonce: "other" }));
    expect(() => new FileLock(path).acquire()).toThrow(/FILE_LOCKED/);
    unlinkSync(path);

    const lock = new FileLock(path);
    lock.acquire();
    expect(() => lock.acquire()).toThrow(/FILE_LOCKED/);
    lock.release();
    expect(lock.locked).toBe(false);

    expect(() =>
      withFileLock(path, () => {
        throw new Error("failed mutation");
      }),
    ).toThrow("failed mutation");
    expect(existsSync(path)).toBe(false);
    expect(withFileLock(path, () => "ok")).toBe("ok");
    expect(existsSync(path)).toBe(false);
  });
});

describe("SqliteStore escalations", () => {
  const escalation: EscalationRecord = {
    id: "esc-1",
    ticket: "tkt-1",
    assignment: "asg-1",
    session: "session-1",
    kind: "decision",
    question: "Merge or rebase?",
    options: [
      { id: "merge", label: "Merge" },
      { id: "rebase", label: "Rebase" },
    ],
    defaultOption: "rebase",
    status: "waiting",
    at: AT,
  };

  it("round-trips an escalation, including its resolution", () => {
    const store = openStore();
    store.saveEscalation(escalation);
    expect(store.getEscalation("esc-1")).toEqual(escalation);

    const resolved: EscalationRecord = {
      ...escalation,
      status: "resolved",
      resolution: { kind: "answer", option: "rebase", text: "Rebase on main.", at: LATER },
    };
    store.saveEscalation(resolved);
    expect(store.listEscalations()).toEqual([resolved]);
    expect(store.getEscalation("missing")).toBeUndefined();
  });

  it("replaces a record by id instead of duplicating it", () => {
    const store = openStore();
    store.saveEscalation(escalation);
    store.saveEscalation({ ...escalation, status: "parked" });

    expect(store.listEscalations()).toHaveLength(1);
    expect(defined(store.getEscalation("esc-1")).status).toBe("parked");
  });

  it("omits optional fields and lists escalations oldest first", () => {
    const store = openStore();
    const minimal: EscalationRecord = {
      id: "esc-2",
      ticket: "tkt-1",
      assignment: "asg-1",
      session: "session-2",
      kind: "blocked",
      question: "Which environment?",
      options: [],
      status: "parked",
      at: LATER,
    };
    store.saveEscalation(escalation);
    store.saveEscalation(minimal);

    const read = defined(store.getEscalation("esc-2"));
    expect(read).toEqual(minimal);
    expect(read).not.toHaveProperty("defaultOption");
    expect(read).not.toHaveProperty("resolution");
    expect(store.listEscalations().map((item) => item.id)).toEqual(["esc-1", "esc-2"]);
  });

  it("keeps escalations across close and reopen", () => {
    const path = tempPath("escalations.db");
    const first = openStore(path);
    first.saveEscalation(escalation);
    first.close();

    const reopened = openStore(path);
    expect(reopened.getEscalation("esc-1")).toEqual(escalation);
  });
});

describe("SqliteStore workers", () => {
  const worker: WorkerRecord = {
    id: "wrk-1",
    name: "worker-one",
    tokenHash: "sha256:token-one",
    createdAt: AT,
  };

  it("persists a worker and finds it by id and token hash", () => {
    const store = openStore();
    store.saveWorker(worker);
    expect(store.getWorker("wrk-1")).toEqual(worker);
    expect(store.findWorkerByTokenHash("sha256:token-one")).toEqual(worker);
    expect(store.findWorkerByTokenHash("sha256:missing")).toBeUndefined();
    expect(store.getWorker("wrk-missing")).toBeUndefined();
  });

  it("lists workers ordered by createdAt and then name", () => {
    const store = openStore();
    store.saveWorker({ id: "wrk-late", name: "aaa", tokenHash: "sha256:late", createdAt: LATER });
    store.saveWorker({ id: "wrk-beta", name: "beta", tokenHash: "sha256:beta", createdAt: AT });
    store.saveWorker({ id: "wrk-alpha", name: "alpha", tokenHash: "sha256:alpha", createdAt: AT });
    expect(store.listWorkers().map((item) => item.id)).toEqual([
      "wrk-alpha",
      "wrk-beta",
      "wrk-late",
    ]);
  });

  it("upserts by id instead of duplicating rows", () => {
    const store = openStore();
    store.saveWorker(worker);
    const updated: WorkerRecord = { ...worker, name: "renamed", lastSeenAt: LATER };
    store.saveWorker(updated);
    expect(store.listWorkers()).toEqual([updated]);
    expect(store.getWorker("wrk-1")).toEqual(updated);
  });

  it("rejects a token hash already owned by another worker", () => {
    const store = openStore();
    store.saveWorker(worker);
    expect(() =>
      store.saveWorker({
        id: "wrk-2",
        name: "worker-two",
        tokenHash: worker.tokenHash,
        createdAt: AT,
      }),
    ).toThrow(/UNIQUE/);
    expect(store.listWorkers()).toEqual([worker]);
  });

  it("touches last_seen_at and ignores unknown ids", () => {
    const store = openStore();
    store.saveWorker(worker);
    store.touchWorkerSeen("wrk-1", LATER);
    expect(store.getWorker("wrk-1")).toEqual({ ...worker, lastSeenAt: LATER });
    expect(() => store.touchWorkerSeen("wrk-missing", LATER)).not.toThrow();
    expect(store.listWorkers()).toHaveLength(1);
    expect(() => store.touchWorkerSeen("wrk-1", "not-a-time")).toThrow(/RFC 3339/);
  });

  it("revokes a worker but still finds it by token hash", () => {
    const store = openStore();
    store.saveWorker(worker);
    store.revokeWorker("wrk-1", LATER);
    const found = defined(store.findWorkerByTokenHash(worker.tokenHash));
    expect(found.revokedAt).toBe(LATER);
    expect(found).toEqual({ ...worker, revokedAt: LATER });
    expect(found).not.toHaveProperty("lastSeenAt");
    expect(() => store.revokeWorker("wrk-missing", LATER)).not.toThrow();
  });

  it("keeps workers across reopen and accepts an existing schema", () => {
    const path = tempPath("workers.db");
    const first = openStore(path);
    first.saveWorker(worker);
    first.close();

    const reopened = openStore(path);
    expect(reopened.getWorker("wrk-1")).toEqual(worker);
    reopened.saveWorker({
      id: "wrk-2",
      name: "worker-two",
      tokenHash: "sha256:token-two",
      createdAt: LATER,
    });
    expect(reopened.listWorkers()).toHaveLength(2);
    reopened.close();

    // Re-running the schema on an existing database is a no-op.
    const third = new SqliteStore(path);
    try {
      expect(third.listWorkers().map((item) => item.id)).toEqual(["wrk-1", "wrk-2"]);
    } finally {
      third.close();
    }
  });
});

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { type Envelope, newEnvelope } from "./envelope";
import type { MailboxStore } from "./mailbox";
import { MemoryMailboxStore } from "./store-memory";
import { SqliteMailboxStore } from "./store-sqlite";

const AT = "2026-11-17T00:00:00.000Z";
const T1 = "2026-11-17T00:00:01.000Z";
const T2 = "2026-11-17T00:00:02.000Z";

interface StoreFixture {
  store: MailboxStore;
  close: () => void;
}

const fixtures: Array<{ name: string; create: () => StoreFixture }> = [
  {
    name: "memory",
    create: () => ({ store: new MemoryMailboxStore(), close: () => {} }),
  },
  {
    name: "sqlite",
    create: () => {
      const path = join(mkdtempSync(join(tmpdir(), "gethen-mailbox-")), "mailbox.db");
      const store = new SqliteMailboxStore(path);
      return { store, close: () => store.close() };
    },
  },
];

function envelope(id: string, overrides: Partial<Envelope> = {}): Envelope {
  return newEnvelope({
    id,
    source: "agent:router-1",
    type: "gethen.task.assign",
    time: AT,
    data: { id },
    ...overrides,
  });
}

function plus(base: string, milliseconds: number): string {
  return new Date(Date.parse(base) + milliseconds).toISOString();
}

function defined<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("expected a value");
  return value;
}

describe.each(fixtures)("MailboxStore contract ($name)", ({ create }) => {
  let store: MailboxStore;
  let close: () => void;

  beforeEach(() => {
    const fixture = create();
    store = fixture.store;
    close = fixture.close;
  });

  afterEach(() => {
    close();
  });

  it("delivers ready messages FIFO per mailbox and isolates mailboxes", () => {
    store.enqueue("agent:a", envelope("m1"), { now: AT });
    store.enqueue("agent:a", envelope("m2"), { now: T1 });
    store.enqueue("agent:b", envelope("m3"), { now: AT });

    const first = store.receive("agent:a", { limit: 10, visibilityTimeoutMs: 1_000, now: T1 });
    expect(first.map((message) => message.envelope.id)).toEqual(["m1", "m2"]);
    expect(first.every((message) => message.attempts === 1)).toBe(true);
    expect(first.every((message) => message.leaseUntil === plus(T1, 1_000))).toBe(true);
    expect(store.receive("agent:b", { now: T1 }).map((message) => message.envelope.id)).toEqual([
      "m3",
    ]);
    expect(store.receive("agent:a", { now: T1 })).toEqual([]);
  });

  it("orders by priority first and arrival second", () => {
    store.enqueue("agent:a", envelope("low", { priority: 0 }), { now: AT });
    store.enqueue("agent:a", envelope("high-early", { priority: 9 }), { now: T1 });
    store.enqueue("agent:a", envelope("high-late", { priority: 9 }), { now: T2 });
    store.enqueue("agent:a", envelope("mid", { priority: 5 }), { now: T2 });

    const received = store.receive("agent:a", { limit: 10, now: T2 });
    expect(received.map((message) => message.envelope.id)).toEqual([
      "high-early",
      "high-late",
      "mid",
      "low",
    ]);
  });

  it("dedupes a repeated envelope id while it is still tracked", () => {
    expect(
      store.enqueue("agent:a", envelope("env-1", { data: { n: 1 } }), { now: AT }).duplicate,
    ).toBe(false);
    expect(
      store.enqueue("agent:a", envelope("env-1", { data: { n: 2 } }), { now: T1 }).duplicate,
    ).toBe(true);
    const received = store.receive("agent:a", { now: T1 });
    expect(received).toHaveLength(1);
    expect(received[0]?.envelope.data).toEqual({ n: 1 });
  });

  it("suppresses acked ids for the dedupe window, then purges them", () => {
    store.enqueue("agent:a", envelope("env-1"), { now: AT });
    const received = defined(store.receive("agent:a", { now: AT })[0]);
    expect(store.ack("agent:a", [received.envelope.id], { dedupeWindowMs: 1_000, now: AT })).toBe(
      1,
    );

    expect(store.enqueue("agent:a", envelope("env-1"), { now: plus(AT, 500) }).duplicate).toBe(
      true,
    );
    expect(store.purge({ now: plus(AT, 500) })).toEqual({ purged: 0 });
    expect(store.purge({ now: plus(AT, 1_000) })).toEqual({ purged: 1 });
    expect(store.enqueue("agent:a", envelope("env-1"), { now: plus(AT, 1_001) }).duplicate).toBe(
      false,
    );
    expect(store.receive("agent:a", { now: plus(AT, 1_001) })).toHaveLength(1);
  });

  it("hides acked messages and only acks leased ids", () => {
    store.enqueue("agent:a", envelope("env-1"), { now: AT });
    expect(store.ack("agent:a", ["env-1"], { now: AT })).toBe(0);
    const received = defined(store.receive("agent:a", { now: AT })[0]);
    expect(store.ack("agent:a", [received.envelope.id], { now: AT })).toBe(1);
    expect(store.receive("agent:a", { now: AT })).toEqual([]);
    expect(store.stats("agent:a", { now: AT })).toMatchObject({ ready: 0, leased: 0, pending: 0 });
  });

  it("redelivers after the lease expires and counts attempts", () => {
    store.enqueue("agent:a", envelope("env-1"), { now: AT });
    expect(store.receive("agent:a", { visibilityTimeoutMs: 1_000, now: AT })).toHaveLength(1);
    expect(store.receive("agent:a", { visibilityTimeoutMs: 1_000, now: plus(AT, 999) })).toEqual(
      [],
    );
    const redelivered = store.receive("agent:a", {
      visibilityTimeoutMs: 1_000,
      now: plus(AT, 1_000),
    });
    expect(redelivered).toHaveLength(1);
    expect(redelivered[0]?.attempts).toBe(2);
  });

  it("nack requeue delays redelivery without losing the attempt count", () => {
    store.enqueue("agent:a", envelope("env-1"), { now: AT });
    const received = defined(store.receive("agent:a", { now: AT })[0]);
    expect(
      store.nack("agent:a", [received.envelope.id], { action: "requeue", delayMs: 5_000, now: AT }),
    ).toBe(1);
    expect(store.receive("agent:a", { now: plus(AT, 4_999) })).toEqual([]);
    const redelivered = store.receive("agent:a", { now: plus(AT, 5_000) });
    expect(redelivered.map((message) => message.attempts)).toEqual([2]);
  });

  it("dead-letters explicit nacks and expired leases past maxAttempts", () => {
    store.enqueue("agent:a", envelope("dead-explicit"), { now: AT });
    store.enqueue("agent:a", envelope("dead-lease"), { now: AT });
    const first = store.receive("agent:a", { limit: 10, maxAttempts: 1, now: AT });
    expect(first.map((message) => message.envelope.id)).toEqual(["dead-explicit", "dead-lease"]);
    expect(store.nack("agent:a", ["dead-explicit"], { action: "dead", now: AT })).toBe(1);
    // The lease expires and the single allowed attempt is used up.
    expect(store.receive("agent:a", { maxAttempts: 1, now: plus(AT, 30_000) })).toEqual([]);

    const dead = store.deadLetters("agent:a");
    expect(dead.map((item) => item.envelope.id)).toEqual(["dead-explicit", "dead-lease"]);
    expect(dead[0]?.attempts).toBe(1);
  });

  it("renews leases to extend visibility", () => {
    store.enqueue("agent:a", envelope("env-1"), { now: AT });
    const received = defined(store.receive("agent:a", { visibilityTimeoutMs: 1_000, now: AT })[0]);
    expect(
      store.renewLeases("agent:a", [received.envelope.id], 5_000, { now: plus(AT, 500) }),
    ).toBe(1);
    expect(store.receive("agent:a", { now: plus(AT, 2_000) })).toEqual([]);
    expect(store.receive("agent:a", { now: plus(AT, 5_500) })).toHaveLength(1);
  });

  it("expires TTL messages and reports purged rows", () => {
    store.enqueue("agent:a", envelope("ttl-receive", { ttlMs: 1_000 }), { now: AT });
    expect(store.receive("agent:a", { now: plus(AT, 1_000) })).toEqual([]);
    store.enqueue("agent:a", envelope("ttl-purge", { ttlMs: 1_000 }), { now: AT });
    expect(store.purge({ now: plus(AT, 1_001) })).toEqual({ purged: 1 });
    expect(store.stats("agent:a", { now: plus(AT, 1_001) }).total).toBe(0);
  });

  it("reports queue depth, oldest ready age and dead letters", () => {
    store.enqueue("agent:a", envelope("m1"), { now: AT });
    store.enqueue("agent:a", envelope("m2"), { now: T1 });
    const received = defined(store.receive("agent:a", { limit: 1, now: T1 })[0]);
    expect(received.envelope.id).toBe("m1");

    const stats = store.stats("agent:a", { now: plus(T1, 5_000) });
    expect(stats).toMatchObject({
      ready: 1,
      leased: 1,
      done: 0,
      dead: 0,
      pending: 2,
      total: 2,
      oldestReadyAt: T1,
      oldestReadyAgeMs: 5_000,
    });

    store.nack("agent:a", [received.envelope.id], { action: "dead", now: T1 });
    expect(store.stats("agent:a", { now: T1 })).toMatchObject({
      ready: 1,
      leased: 0,
      dead: 1,
      pending: 1,
      total: 2,
    });
  });

  it("persists subscription cursors per (topic, name)", () => {
    expect(store.getSubscription("builds", "consumer")).toBeUndefined();
    store.saveSubscription({ topic: "builds", name: "consumer", cursor: "env-1", createdAt: AT });
    expect(store.getSubscription("builds", "consumer")).toEqual({
      topic: "builds",
      name: "consumer",
      cursor: "env-1",
      createdAt: AT,
    });

    store.saveSubscription({ topic: "builds", name: "consumer", cursor: "env-2", createdAt: T1 });
    expect(store.getSubscription("builds", "consumer")).toMatchObject({
      cursor: "env-2",
      createdAt: AT,
    });
    store.saveSubscription({ topic: "builds", name: "audit", createdAt: T2 });
    store.saveSubscription({ topic: "other", name: "consumer", createdAt: T2 });

    expect(store.listSubscriptions("builds").map((record) => record.name)).toEqual([
      "audit",
      "consumer",
    ]);
    expect(store.listSubscriptions()).toHaveLength(3);
  });
});

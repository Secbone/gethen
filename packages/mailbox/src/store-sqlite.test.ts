import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { type Envelope, newEnvelope } from "./envelope";
import { SqliteMailboxStore } from "./store-sqlite";

const AT = "2026-11-17T00:00:00.000Z";

function envelope(id: string): Envelope {
  return newEnvelope({
    id,
    source: "agent:router-1",
    type: "gethen.task.assign",
    time: AT,
    data: { id },
  });
}

function plus(base: string, milliseconds: number): string {
  return new Date(Date.parse(base) + milliseconds).toISOString();
}

describe("SqliteMailboxStore durability", () => {
  it("recovers leases, dedupe state and subscription cursors after reopen", () => {
    const path = join(mkdtempSync(join(tmpdir(), "gethen-mailbox-")), "durable.db");
    const first = new SqliteMailboxStore(path);
    first.enqueue("agent:a", envelope("env-1"), { now: AT });
    expect(first.receive("agent:a", { visibilityTimeoutMs: 1_000, now: AT })).toHaveLength(1);

    first.enqueue("agent:a", envelope("env-2"), { now: AT });
    const second = first.receive("agent:a", { now: AT });
    expect(second.map((message) => message.envelope.id)).toEqual(["env-2"]);
    first.ack("agent:a", [second[0]?.envelope.id ?? ""], { dedupeWindowMs: 60_000, now: AT });
    first.saveSubscription({ topic: "builds", name: "consumer", cursor: "env-1", createdAt: AT });
    first.close();

    const reopened = new SqliteMailboxStore(path);
    expect(reopened.getSubscription("builds", "consumer")?.cursor).toBe("env-1");
    // The acked id is still inside its dedupe window after the restart.
    expect(reopened.enqueue("agent:a", envelope("env-2"), { now: plus(AT, 1_000) }).duplicate).toBe(
      true,
    );
    // env-1's lease expired while the process was down: redelivery, attempts kept.
    const redelivered = reopened.receive("agent:a", {
      visibilityTimeoutMs: 1_000,
      now: plus(AT, 2_000),
    });
    expect(redelivered.map((message) => message.envelope.id)).toEqual(["env-1"]);
    expect(redelivered[0]?.attempts).toBe(2);
    reopened.close();
  });

  it("keeps dead letters and their stats across reopen", () => {
    const path = join(mkdtempSync(join(tmpdir(), "gethen-mailbox-")), "dead.db");
    const first = new SqliteMailboxStore(path);
    first.enqueue("agent:a", envelope("env-1"), { now: AT });
    first.receive("agent:a", { now: AT });
    first.nack("agent:a", ["env-1"], { action: "dead", now: AT });
    expect(first.stats("agent:a", { now: AT })).toMatchObject({ ready: 0, dead: 1, total: 1 });
    first.close();

    const reopened = new SqliteMailboxStore(path);
    expect(reopened.deadLetters("agent:a").map((item) => item.envelope.id)).toEqual(["env-1"]);
    expect(reopened.receive("agent:a", { now: plus(AT, 60_000) })).toEqual([]);
    reopened.close();
  });
});

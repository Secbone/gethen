import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { Engine } from "./engine";
import type { Policy, Trigger } from "./model";
import { OutboxRunner } from "./outbox";
import { SqliteStore } from "./store";

const policy: Policy = {
  version: 1,
  acceptMode: "auto",
  rules: [],
  sources: { local: { allowed: ["alice"], bots: [] } },
};

const trigger: Trigger = {
  id: "t1",
  source: "local",
  thread: "th1",
  actor: "alice",
  bot: false,
  addressed: "mention",
  text: "fix the failing test",
  at: "2026-11-16T00:00:00.000Z",
};

function deterministicClock(start = Date.parse("2026-11-16T00:00:00.000Z")) {
  let now = start;
  return {
    now: () => {
      now += 1_000;
      return new Date(now);
    },
  };
}

function deterministicIds() {
  let n = 0;
  return { next: (prefix: string) => `${prefix}-${++n}` };
}

describe("master end to end", () => {
  it("turns one trigger into a ticket, a persisted launch effect and a receipt", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-master-"));
    const store = new SqliteStore(join(dir, "master.sqlite"));
    store.setPolicy(policy);
    const engine = new Engine({
      repos: store,
      clock: deterministicClock(),
      ids: deterministicIds(),
    });

    expect(engine.receive(trigger)).toBe("stored");
    expect(engine.receive(trigger)).toBe("duplicate");
    expect(engine.evaluateDue()).toBe(1);

    const candidate = engine.candidate(trigger.id);
    expect(candidate?.eval.thread).toBe("th1");

    const decision = engine.decide(trigger.id, "accept", [{ code: "router-accepted" }]);
    expect(decision.outcome).toBe("accept");

    const effects = engine.drain();
    expect(effects).toHaveLength(1);
    store.enqueue(effects);

    const launched: unknown[] = [];
    const outbox = new OutboxRunner({
      store,
      handlers: {
        launch: async (payload) => {
          const value = payload as { ticket: { id: string }; assignment: { id: string } };
          engine.assignmentStarted(value.ticket.id, value.assignment.id);
          launched.push(value.assignment.id);
        },
      },
    });
    expect(await outbox.runDue()).toBe(1);
    expect(launched).toHaveLength(1);

    const ticket = store.tickets.list()[0];
    expect(ticket).toBeDefined();
    expect(ticket?.assignments).toHaveLength(1);
    expect(store.assignments.list()[0]?.state).toBe("running");

    store.close();
  });

  it("keeps tickets and policy after a restart", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-master-restart-"));
    const path = join(dir, "master.sqlite");
    const first = new SqliteStore(path);
    first.setPolicy(policy);
    const engine = new Engine({
      repos: first,
      clock: deterministicClock(),
      ids: deterministicIds(),
    });
    engine.receive(trigger);
    engine.evaluateDue();
    engine.decide(trigger.id, "accept", []);
    first.enqueue(engine.drain());
    first.close();

    const second = new SqliteStore(path);
    expect(second.tickets.list()).toHaveLength(1);
    expect(second.policy.current().version).toBe(1);
    second.close();
  });
});

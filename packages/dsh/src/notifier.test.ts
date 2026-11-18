import type { NotificationRow } from "@gethen/master-runtime";
import { describe, expect, it } from "vitest";
import { type AgentLike, GethenNotifier, type NotificationSource } from "./notifier";

function row(overrides: Partial<NotificationRow> = {}): NotificationRow {
  return {
    id: overrides.id ?? "n1",
    kind: overrides.kind ?? "assignment.result",
    ticket: overrides.ticket ?? "ticket-1",
    assignment: overrides.assignment ?? "assignment-1",
    data: overrides.data ?? { report: "ready", summary: "done", late: false },
    createdAt: "2026-11-17T00:00:00.000Z",
  };
}

class FakeSource implements NotificationSource {
  pendingRows: NotificationRow[] = [];
  delivered: string[] = [];
  private readonly listeners = new Set<(row: NotificationRow) => void>();
  pending() {
    return [...this.pendingRows];
  }
  subscribe(listener: (row: NotificationRow) => void) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  markDelivered(id: string) {
    this.delivered.push(id);
  }
  publish(row: NotificationRow) {
    for (const listener of this.listeners) listener(row);
  }
}

function fakeAgent(): AgentLike & { steered: string[]; followed: string[] } {
  const steered: string[] = [];
  const followed: string[] = [];
  return {
    session: { id: "session-master" },
    status: "idle",
    steered,
    followed,
    steer(message) {
      steered.push(message.content[0]?.text ?? "");
    },
    followup(message) {
      followed.push(message.content[0]?.text ?? "");
    },
  };
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 30));
}

describe("GethenNotifier", () => {
  it("steers the bound Master agent when a worker result arrives", async () => {
    const source = new FakeSource();
    const notifier = new GethenNotifier({ notifications: source, batchWindowMs: 1 });
    notifier.start();
    const agent = fakeAgent();
    notifier.bind(agent);
    source.publish(row());
    await settle();
    expect(agent.steered).toHaveLength(1);
    expect(agent.steered[0]).toContain("assignment-1 finished: ready");
    expect(source.delivered).toEqual(["n1"]);
  });

  it("flushes notifications queued before the agent is bound", async () => {
    const source = new FakeSource();
    source.pendingRows = [row({ id: "early" })];
    const notifier = new GethenNotifier({ notifications: source, batchWindowMs: 1 });
    notifier.start();
    const agent = fakeAgent();
    notifier.bind(agent);
    await settle();
    expect(agent.steered).toHaveLength(1);
    expect(source.delivered).toEqual(["early"]);
  });

  it("batches a burst into one wake message", async () => {
    const source = new FakeSource();
    const notifier = new GethenNotifier({ notifications: source, batchWindowMs: 5 });
    notifier.start();
    const agent = fakeAgent();
    notifier.bind(agent);
    source.publish(row({ id: "a" }));
    source.publish(row({ id: "b", assignment: "assignment-2" }));
    await settle();
    expect(agent.steered).toHaveLength(1);
    expect(agent.steered[0]).toContain("assignment-1");
    expect(agent.steered[0]).toContain("assignment-2");
    expect(source.delivered.sort()).toEqual(["a", "b"]);
  });

  it("uses followup mode when configured", async () => {
    const source = new FakeSource();
    const notifier = new GethenNotifier({
      notifications: source,
      mode: "followup",
      batchWindowMs: 1,
    });
    notifier.start();
    const agent = fakeAgent();
    notifier.bind(agent);
    source.publish(row());
    await settle();
    expect(agent.followed).toHaveLength(1);
    expect(agent.steered).toHaveLength(0);
  });

  it("does not wake or acknowledge when wake is off", async () => {
    const source = new FakeSource();
    const notifier = new GethenNotifier({ notifications: source, mode: "off" });
    notifier.start();
    const agent = fakeAgent();
    notifier.bind(agent);
    source.publish(row());
    await settle();
    expect(agent.steered).toHaveLength(0);
    expect(source.delivered).toEqual([]);
  });

  it("formats an escalation as a decision request", async () => {
    const source = new FakeSource();
    const notifier = new GethenNotifier({ notifications: source, batchWindowMs: 1 });
    notifier.start();
    const agent = fakeAgent();
    notifier.bind(agent);
    source.publish(
      row({ id: "e", kind: "escalation.parked", data: { question: "merge?", kind: "decision" } }),
    );
    await settle();
    expect(agent.steered[0]).toContain("escalation needs an answer");
    expect(agent.steered[0]).toContain("gethen_task");
  });
});

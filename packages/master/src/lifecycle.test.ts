import { describe, expect, it } from "vitest";
import {
  acknowledgeTicket,
  archiveCandidates,
  archiveTicket,
  DEFAULT_ARCHIVE_POLICY,
  needsAction,
  stageForTicket,
  staleCandidates,
  ticketFingerprint,
  ticketLifecycle,
} from "./lifecycle";
import { type Assignment, DAY_MS, type Ticket } from "./model";

const AT = "2026-11-17T10:00:00.000Z";

function ticket(overrides: Partial<Ticket> = {}): Ticket {
  return {
    id: "ticket-1",
    mandate: "mandate-1",
    source: "local",
    threads: ["thread-1"],
    subscriptions: ["thread-1"],
    channels: [],
    inputs: [],
    assignments: ["assignment-1"],
    openedAt: AT,
    ...overrides,
  };
}

function assignment(overrides: Partial<Assignment> = {}): Assignment {
  return {
    id: "assignment-1",
    ticket: "ticket-1",
    worker: "worker-1",
    workspace: "/ws",
    packDigest: "sha256:x",
    state: "running",
    operationId: "launch:assignment-1",
    ...overrides,
  };
}

describe("ticket lifecycle", () => {
  it("derives open, closed and archived from the stored terminal fields", () => {
    expect(ticketLifecycle(ticket())).toBe("open");
    expect(ticketLifecycle(ticket({ endedAt: AT, outcome: "completed" }))).toBe("closed");
    expect(ticketLifecycle(ticket({ endedAt: AT, archivedAt: AT }))).toBe("archived");
  });

  it("labels a Ticket with a ready report as needing a person", () => {
    const ready = ticket({ latestReport: "ready" });
    expect(stageForTicket(ready, [assignment()])).toBe("person/review");
    expect(needsAction(ready, [assignment()])).toBe(true);
  });

  it("acknowledges the current fingerprint only", () => {
    const ready = ticket({ latestReport: "ready" });
    const acknowledged = acknowledgeTicket(ready, AT, [assignment({ state: "ended" })]);
    expect(acknowledged.ack).toMatchObject({ at: AT, stage: "person/review" });
    expect(needsAction(acknowledged, [assignment({ state: "ended" })])).toBe(false);

    const reported = { ...acknowledged, latestReport: "blocked" as const };
    expect(needsAction(reported, [assignment({ state: "ended" })])).toBe(true);
  });

  it("asks again when a question or delivery failure changes the facts", () => {
    const ready = ticket({ latestReport: "ready" });
    const acknowledged = acknowledgeTicket(ready, AT, [assignment({ state: "ended" })]);
    const withQuestion = {
      ...acknowledged,
      attention: { question: "Merge or rebase?", askedAt: AT },
    };
    expect(needsAction(withQuestion, [assignment({ state: "ended" })])).toBe(true);

    const withDelivery = {
      ...acknowledged,
      delivery: { error: "offline", at: AT, attempt: 1 },
    };
    expect(needsAction(withDelivery, [assignment({ state: "ended" })])).toBe(true);
  });

  it("never asks a person to act on a terminal Ticket", () => {
    const closed = ticket({ latestReport: "ready", endedAt: AT, outcome: "completed" });
    expect(needsAction(closed, [assignment({ state: "ended" })])).toBe(false);
    expect(needsAction(archiveTicket(closed, AT), [assignment({ state: "ended" })])).toBe(false);
  });

  it("keeps the fingerprint stable for the same facts", () => {
    const ready = ticket({ latestReport: "ready" });
    expect(ticketFingerprint(ready, "person/review")).toBe(
      ticketFingerprint({ ...ready }, "person/review"),
    );
  });
});

describe("archive policy", () => {
  const now = new Date("2026-11-30T00:00:00.000Z");

  it("retains ended Tickets per outcome", () => {
    const completed = ticket({
      id: "completed",
      endedAt: new Date("2026-11-20T00:00:00.000Z").toISOString(),
      outcome: "completed",
    });
    const merged = ticket({
      id: "merged",
      endedAt: new Date("2026-11-20T00:00:00.000Z").toISOString(),
      outcome: "merged",
    });
    const dropped = ticket({
      id: "dropped",
      endedAt: new Date("2026-11-20T00:00:00.000Z").toISOString(),
      outcome: "dropped",
    });
    expect(archiveCandidates([completed, merged, dropped], now).map((row) => row.id)).toEqual([
      "completed",
      "dropped",
    ]);
  });

  it("ignores open, archived and not-yet-due Tickets", () => {
    const open = ticket({ id: "open" });
    const archived = ticket({
      id: "archived",
      endedAt: "2026-01-01T00:00:00.000Z",
      outcome: "completed",
      archivedAt: AT,
    });
    const fresh = ticket({
      id: "fresh",
      endedAt: new Date(now.getTime() - DAY_MS).toISOString(),
      outcome: "completed",
    });
    expect(archiveCandidates([open, archived, fresh], now)).toEqual([]);
  });

  it("uses the closed outcome for legacy rows without one", () => {
    const legacy = ticket({
      endedAt: new Date(now.getTime() - 8 * DAY_MS).toISOString(),
    });
    expect(archiveCandidates([legacy], now)).toHaveLength(1);
  });

  it("honours an explicit retention policy", () => {
    const ended = ticket({ endedAt: now.toISOString(), outcome: "completed" });
    const policy = {
      retainMs: { ...DEFAULT_ARCHIVE_POLICY.retainMs, completed: 0 },
    };
    expect(archiveCandidates([ended], now, policy)).toHaveLength(1);
  });
});

describe("stale policy", () => {
  const now = new Date("2026-11-30T00:00:00.000Z");
  const old = new Date(now.getTime() - 8 * DAY_MS).toISOString();

  it("closes a ready report after the review window", () => {
    const ready = ticket({ latestReport: "ready", openedAt: old });
    const ended = assignment({ state: "ended", endedAt: old });
    expect(staleCandidates([ready], [ended], now)).toEqual([
      { ticket: ready, outcome: "completed" },
    ]);
  });

  it("drops a failed report after the failure window", () => {
    const failed = ticket({ latestReport: "failed", openedAt: old });
    const ended = assignment({ state: "ended", endedAt: old });
    expect(staleCandidates([failed], [ended], now)).toEqual([
      { ticket: failed, outcome: "dropped" },
    ]);
  });

  it("keeps live, questioned, fresh and terminal Tickets", () => {
    const live = ticket({ id: "live", latestReport: "ready", openedAt: old });
    const asked = ticket({
      id: "asked",
      latestReport: "ready",
      openedAt: old,
      attention: { question: "merge?", askedAt: old },
    });
    const fresh = ticket({ id: "fresh", latestReport: "ready", openedAt: AT });
    const closed = ticket({
      id: "closed",
      latestReport: "ready",
      openedAt: old,
      endedAt: old,
      outcome: "completed",
    });
    expect(
      staleCandidates([live, asked, fresh, closed], [assignment({ state: "running" })], now),
    ).toEqual([]);
  });

  it("honours an explicit stale window", () => {
    const ready = ticket({ latestReport: "ready", openedAt: AT });
    expect(
      staleCandidates([ready], [assignment({ state: "ended", endedAt: AT })], now, {
        readyAfterMs: 0,
        failedAfterMs: 0,
      }),
    ).toHaveLength(1);
  });
});

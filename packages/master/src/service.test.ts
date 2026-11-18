import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { LaunchFailure } from "./executor";
import type { AssignmentReceipt, Report, Trigger } from "./model";
import { LAUNCH_TIMEOUT_CODE, type LaunchPort, type LaunchRequest } from "./ports";
import { MasterService } from "./service";

const policy = {
  version: 1,
  acceptMode: "auto" as const,
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

function clock(start = Date.parse("2026-11-16T00:00:00.000Z")) {
  let now = start;
  return {
    now: () => {
      now += 1_000;
      return new Date(now);
    },
  };
}

function fakeLaunch(report: Report, fail = false): LaunchPort {
  return {
    async launch(request: LaunchRequest) {
      if (fail) throw new Error("worker exploded");
      const receipt: AssignmentReceipt = {
        assignment: request.assignment.id,
        ticket: request.ticket.id,
        worker: request.assignment.worker,
        workspace: request.assignment.workspace,
        packDigest: request.packDigest,
        at: "2026-11-16T00:10:00.000Z",
        outcome: "completed",
        summary: report.summary,
        events: 3,
      };
      return { report, receipt };
    },
  };
}

describe("MasterService", () => {
  it("runs a trigger through eval, launch and receipt, then persists across restart", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-master-service-"));
    const dbPath = join(dir, "master.sqlite");
    const first = new MasterService({
      dbPath,
      policy,
      launch: fakeLaunch({ kind: "ready", summary: "done", at: "2026-11-16T00:10:00.000Z" }),
      clock: clock(),
    });
    const result = await first.submit(trigger);
    expect(result.stored).toBe("stored");
    expect(result.decision?.outcome).toBe("accept");
    expect(first.store.tickets.list()).toHaveLength(1);
    expect(first.store.assignments.list()[0]?.state).toBe("ended");
    expect(first.store.listReceipts()).toHaveLength(1);
    first.stop();

    const second = new MasterService({
      dbPath,
      launch: fakeLaunch({ kind: "ready", summary: "again", at: "2026-11-16T00:10:00.000Z" }),
      clock: clock(),
    });
    expect(second.store.tickets.list()).toHaveLength(1);
    expect(second.store.listReceipts()).toHaveLength(1);
    second.stop();
  });

  it("reports a failed launch as a failed ticket", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-master-fail-"));
    const service = new MasterService({
      dbPath: join(dir, "master.sqlite"),
      policy,
      launch: fakeLaunch(
        { kind: "ready", summary: "unused", at: "2026-11-16T00:10:00.000Z" },
        true,
      ),
      clock: clock(),
      outboxIntervalMs: 60_000,
    });
    const result = await service.submit(trigger);
    expect(result.decision?.outcome).toBe("accept");
    expect(service.store.tickets.list()[0]?.latestReport).toBe("failed");
    service.stop();
  });

  it("refuses a second process on the same database", () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-master-lock-"));
    const dbPath = join(dir, "master.sqlite");
    const first = new MasterService({
      dbPath,
      policy,
      launch: fakeLaunch({ kind: "ready", summary: "x", at: "2026-11-16T00:10:00.000Z" }),
    });
    expect(
      () =>
        new MasterService({
          dbPath,
          policy,
          launch: fakeLaunch({ kind: "ready", summary: "x", at: "2026-11-16T00:10:00.000Z" }),
        }),
    ).toThrow(/FILE_LOCKED/);
    first.stop();
  });

  it("waits for the receipt instead of snapshotting a failed first attempt", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-master-wait-"));
    let attempts = 0;
    const receiptFor = (request: LaunchRequest): AssignmentReceipt => ({
      assignment: request.assignment.id,
      ticket: request.ticket.id,
      worker: request.assignment.worker,
      workspace: request.assignment.workspace,
      packDigest: request.packDigest,
      at: "2026-11-16T00:20:00.000Z",
      outcome: "completed",
      summary: "retry succeeded",
      events: 1,
    });
    const service = new MasterService({
      dbPath: join(dir, "master.sqlite"),
      policy,
      outboxIntervalMs: 20,
      launch: {
        async launch(request: LaunchRequest) {
          attempts += 1;
          if (attempts === 1) throw new Error("transient worker failure");
          return {
            report: { kind: "ready", summary: "retry succeeded", at: "2026-11-16T00:20:00.000Z" },
            receipt: receiptFor(request),
          };
        },
      },
    });
    service.start();
    try {
      const before = new Set(service.store.tickets.list().map((ticket) => ticket.id));
      await service.submit({ ...trigger, id: "trigger-wait-1" });
      const ticketId = service.store.tickets.list().find((ticket) => !before.has(ticket.id))?.id;
      expect(ticketId).toBeDefined();
      const settlement = await service.waitForTicket(ticketId as string, {
        timeoutMs: 5_000,
        pollMs: 50,
      });
      expect(settlement.settled).toBe(true);
      expect(settlement.receipt).toBe(true);
      expect(settlement.ticket?.latestReport).toBe("ready");
      expect(attempts).toBeGreaterThanOrEqual(2);
    } finally {
      service.stop();
    }
  });

  it("reports a silence timeout to the host instead of failing silently", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-master-timeout-"));
    const failures: LaunchFailure[] = [];
    const service = new MasterService({
      dbPath: join(dir, "master.sqlite"),
      policy,
      clock: clock(),
      launch: {
        async launch() {
          throw Object.assign(new Error("assignment produced no activity"), {
            code: LAUNCH_TIMEOUT_CODE,
          });
        },
      },
      onLaunchFailure: (failure) => failures.push(failure),
    });
    try {
      await service.submit(trigger);
      expect(failures).toHaveLength(1);
      expect(failures[0]).toMatchObject({
        report: { kind: "failed" },
        timedOut: true,
        request: { assignment: { id: expect.any(String) } },
      });
      expect(service.store.listReceipts()[0]?.outcome).toBe("failed");
    } finally {
      service.stop();
    }
  });

  it("returns a settled snapshot for a synchronous launch", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-master-wait-"));
    const service = new MasterService({
      dbPath: join(dir, "master.sqlite"),
      policy,
      launch: fakeLaunch({
        kind: "ready",
        summary: "done",
        at: "2026-11-16T00:20:00.000Z",
      }),
    });
    service.start();
    try {
      const before = new Set(service.store.tickets.list().map((ticket) => ticket.id));
      await service.submit({ ...trigger, id: "trigger-wait-2" });
      const ticketId = service.store.tickets.list().find((ticket) => !before.has(ticket.id))?.id;
      expect(ticketId).toBeDefined();
      const settlement = await service.waitForTicket(ticketId as string, {
        timeoutMs: 500,
        pollMs: 10,
      });
      expect(settlement).toMatchObject({ settled: true, receipt: true, liveAssignments: 0 });
    } finally {
      service.stop();
    }
  });
});

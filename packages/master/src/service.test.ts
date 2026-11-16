import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { AssignmentReceipt, Report, Trigger } from "./model";
import type { LaunchPort, LaunchRequest } from "./ports";
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
});

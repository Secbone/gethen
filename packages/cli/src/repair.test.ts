import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteStore, type Trigger } from "@gethen/master";
import { createMasterRuntime, newEnvelope, SqliteMailboxStore } from "@gethen/master-runtime";
import { afterEach, describe, expect, it } from "vitest";
import { repairLateResult } from "./commands/repair";

const dirs: string[] = [];
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "gethen-repair-"));
  dirs.push(dir);
  return dir;
}

describe("gethen repair late-result", () => {
  it("dry-runs, backs up, updates the ticket and appends a late receipt", async () => {
    const dir = tempDir();
    const dbPath = join(dir, "master.sqlite");
    const mailboxDbPath = join(dir, "mailbox.sqlite");
    const runtime = createMasterRuntime({ dbPath });
    const trigger: Trigger = {
      id: "trigger-repair",
      source: "local",
      thread: "local",
      actor: "local-user",
      bot: false,
      addressed: "mention",
      text: "repair me",
      at: "2026-11-18T00:00:00.000Z",
      meta: { scope: "local", workspace: "/ws" },
    };
    await runtime.service.submit(trigger);
    const ticket = runtime.service.store.tickets.list().at(-1);
    const assignment = runtime.service.store.assignments.list().at(-1);
    if (ticket === undefined || assignment === undefined) throw new Error("missing ticket");
    await runtime.close();

    const seed = new SqliteStore(dbPath);
    seed.tickets.save({ ...ticket, latestReport: "failed", endedAt: undefined });
    seed.close();

    const mailbox = new SqliteMailboxStore(mailboxDbPath);
    mailbox.enqueue(
      "mailbox:master",
      newEnvelope({
        id: "status-result-1",
        source: `agent:${assignment.worker}`,
        type: "gethen.task.status.result",
        time: "2026-11-18T00:00:00.000Z",
        data: {
          assignmentId: assignment.id,
          state: {
            assignmentId: assignment.id,
            phase: "done",
            step: 1,
            seq: 1,
            lastEventAt: "2026-11-18T00:00:00.000Z",
          },
          report: { assignmentId: assignment.id, kind: "ready", summary: "late repaired" },
        },
      }),
    );
    mailbox.close();

    const dry = repairLateResult({
      assignmentId: assignment.id,
      dbPath,
      mailboxDbPath,
      dryRun: true,
    });
    expect(dry).toMatchObject({ dryRun: true, kind: "ready", summary: "late repaired" });
    const before = new SqliteStore(dbPath);
    expect(before.tickets.get(ticket.id)?.latestReport).toBe("failed");
    before.close();

    const repaired = repairLateResult({ assignmentId: assignment.id, dbPath, mailboxDbPath });
    expect(repaired.dryRun).toBe(false);
    expect(repaired.backup).toBeDefined();
    expect(existsSync(repaired.backup ?? "")).toBe(true);

    const after = new SqliteStore(dbPath);
    try {
      expect(after.tickets.get(ticket.id)?.latestReport).toBe("ready");
      const receipts = after.listReceipts().filter((row) => row.assignment === assignment.id);
      expect(receipts.at(-1)).toMatchObject({
        outcome: "completed",
        summary: "late repaired",
      });
    } finally {
      after.close();
    }
  });
});

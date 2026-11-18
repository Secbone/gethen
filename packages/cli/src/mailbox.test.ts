import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteMailboxStore } from "@gethen/master-runtime";
import { describe, expect, it } from "vitest";
import { mailboxCommand } from "./commands/mailbox";

const AT = "2026-11-18T00:00:00.000Z";
const MAILBOX = "mailbox:master";

function deadLetter(store: SqliteMailboxStore, id: string): void {
  store.enqueue(MAILBOX, {
    specversion: "1.0",
    id,
    source: "agent:worker-1",
    type: "gethen.task.result",
    time: AT,
    data: { assignmentId: "assignment-1" },
  } as never);
  const [message] = store.receive(MAILBOX, { limit: 1, visibilityTimeoutMs: 1000 });
  if (message === undefined) throw new Error("expected a leased message");
  store.nack(MAILBOX, [message.envelope.id], { action: "dead" });
}

describe("gethen mailbox", () => {
  it("lists, replays and clears dead letters", () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-mailbox-cli-"));
    const db = join(dir, "mailbox.sqlite");
    const store = new SqliteMailboxStore(db);
    const lines: string[] = [];
    const errors: string[] = [];
    const options = {
      write: (line: string) => lines.push(line),
      error: (line: string) => errors.push(line),
    };
    try {
      deadLetter(store, "dead-1");
      expect(mailboxCommand(["list", "--db", db, "--mailbox", MAILBOX], options)).toBe(0);
      expect(lines.join("\n")).toContain("dead-1");
      expect(lines.join("\n")).toContain("1 dead letter");

      expect(mailboxCommand(["replay", "--db", db, "--mailbox", MAILBOX], options)).toBe(0);
      expect(lines.at(-1)).toContain("replayed 1 dead letter");
      expect(store.deadLetters(MAILBOX)).toHaveLength(0);
      const [replayed] = store.receive(MAILBOX, { limit: 1, visibilityTimeoutMs: 1000 });
      expect(replayed?.envelope.id).toBe("dead-1");
      if (replayed !== undefined) store.ack(MAILBOX, [replayed.envelope.id]);

      deadLetter(store, "dead-2");
      expect(mailboxCommand(["clear", "--db", db, "--mailbox", MAILBOX], options)).toBe(0);
      expect(lines.at(-1)).toContain("cleared 1 dead letter");
      expect(store.deadLetters(MAILBOX)).toHaveLength(0);

      expect(mailboxCommand(["list", "--db", db], options)).toBe(1);
      expect(errors.at(-1)).toContain("--mailbox is required");
    } finally {
      store.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

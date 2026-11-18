#!/usr/bin/env node
// Repair one assignment whose launch was settled as a timeout/failure but whose
// Worker later retained a complete report (a `gethen.task.status.result` in the
// mailbox). Applies the report to the Ticket and appends a late receipt.
//
// Usage: node scripts/repair-late-result.mjs <assignmentId> <master.sqlite> <mailbox.sqlite>
import { DatabaseSync } from "node:sqlite";
import { MailboxClient, newEnvelope, SqliteMailboxStore } from "@gethen/mailbox";
import { Engine, SqliteStore } from "@gethen/master";

const [assignmentId, masterDb, mailboxDb] = process.argv.slice(2);
if (assignmentId === undefined || masterDb === undefined || mailboxDb === undefined) {
  process.stderr.write(
    "usage: repair-late-result.mjs <assignmentId> <master.sqlite> <mailbox.sqlite>\n",
  );
  process.exit(1);
}

const mailbox = new DatabaseSync(mailboxDb, { readOnly: true });
let report;
try {
  const rows = mailbox
    .prepare(
      `SELECT created_at, envelope_json FROM messages
       WHERE state = 'done' AND envelope_json LIKE ?
       ORDER BY created_at DESC LIMIT 50`,
    )
    .all(`%${assignmentId}%`);
  for (const row of rows) {
    let envelope;
    try {
      envelope = JSON.parse(row.envelope_json);
    } catch {
      continue;
    }
    if (envelope.type !== "gethen.task.status.result") continue;
    const data = envelope.data ?? {};
    if (data.assignmentId !== assignmentId) continue;
    const candidate = data.report;
    if (
      candidate !== undefined &&
      candidate !== null &&
      typeof candidate.kind === "string" &&
      typeof candidate.summary === "string" &&
      candidate.summary.trim().length > 0
    ) {
      report = candidate;
      break;
    }
  }
} finally {
  mailbox.close();
}
const store = new SqliteStore(masterDb);
try {
  const assignment = store.assignments.get(assignmentId);
  if (assignment === undefined) throw new Error(`unknown assignment: ${assignmentId}`);
  if (report === undefined && assignment.worker.length > 0) {
    report = await probeWorkerReport(mailboxDb, assignment.worker, assignmentId);
  }
  if (report === undefined) {
    throw new Error(`no retained status report found for ${assignmentId}`);
  }
  const ticket = store.tickets.get(assignment.ticket);
  if (ticket === undefined) throw new Error(`unknown ticket: ${assignment.ticket}`);
  const now = new Date().toISOString();
  const normalized = {
    kind: report.kind,
    summary: report.summary,
    at: typeof report.at === "string" ? report.at : now,
    ...(typeof report.question === "string" && report.question.length > 0
      ? { question: report.question }
      : {}),
  };
  const engine = new Engine({
    repos: store,
    clock: { now: () => new Date() },
    ids: { next: (prefix) => `${prefix}-repair` },
  });
  engine.report(ticket.id, normalized);
  store.append({
    assignment: assignment.id,
    ticket: assignment.ticket,
    worker: assignment.worker,
    workspace: assignment.workspace,
    packDigest: assignment.packDigest,
    at: now,
    outcome: "completed",
    summary: normalized.summary,
    events: 0,
  });
  store.flush?.();
  process.stdout.write(
    `repaired ${assignmentId}: ticket=${ticket.id} kind=${normalized.kind} ` +
      `summary=${JSON.stringify(normalized.summary)}\n`,
  );
} finally {
  store.close();
}

/**
 * Ask a live Worker for its retained report through a private reply mailbox,
 * so the live Master bridge cannot consume the reply first.
 */
async function probeWorkerReport(mailboxDb, workerId, assignmentId) {
  const store = new SqliteMailboxStore(mailboxDb);
  const client = new MailboxClient({ store });
  const replyTo = "mailbox:repair";
  try {
    client.send(
      newEnvelope({
        source: "master:repair",
        type: "gethen.task.status",
        to: `mailbox:${workerId}`,
        replyTo,
        subject: assignmentId,
        correlationId: assignmentId,
        time: new Date().toISOString(),
        data: { assignmentId },
      }),
    );
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      const [message] = client.receive(replyTo, { limit: 1, visibilityTimeoutMs: 2_000 });
      if (message === undefined) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        continue;
      }
      const envelope = message.envelope;
      const data = envelope.data ?? {};
      const candidate = data.report;
      if (
        envelope.type === "gethen.task.status.result" &&
        data.assignmentId === assignmentId &&
        candidate !== undefined &&
        candidate !== null &&
        typeof candidate.summary === "string" &&
        candidate.summary.trim().length > 0
      ) {
        return candidate;
      }
    }
    return undefined;
  } finally {
    store.close();
  }
}

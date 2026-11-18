/**
 * `gethen repair late-result`: re-apply a Worker report that arrived after its
 * launch was already settled (timeout/failure). Reads the retained report from
 * the mailbox (or probes the live Worker), optionally backs up the master DB,
 * then updates the Ticket and appends a late receipt.
 */
import { existsSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { Engine, type Report, SqliteStore } from "@gethen/master";

export interface RepairOptions {
  readonly assignmentId: string;
  readonly dbPath: string;
  readonly mailboxDbPath: string;
  readonly dryRun?: boolean;
  readonly backup?: boolean;
  readonly write?: (line: string) => void;
  readonly error?: (line: string) => void;
}

export interface RepairResult {
  assignment: string;
  ticket: string;
  kind: string;
  summary: string;
  dryRun: boolean;
  backup?: string;
}

export function repairCommand(argv: string[], options: Partial<RepairOptions> = {}): number {
  const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
  const error = options.error ?? ((line: string) => process.stderr.write(`${line}\n`));
  if (argv[0] !== "late-result" || argv.includes("-h") || argv.includes("--help")) {
    write(REPAIR_USAGE);
    return argv[0] === "late-result" ? 0 : 1;
  }
  const assignmentId = flagValue(argv, "--assignment") ?? "";
  const dbPath = flagValue(argv, "--db") ?? "";
  const mailboxDbPath = flagValue(argv, "--mailbox-db") ?? "";
  if (assignmentId.length === 0 || dbPath.length === 0 || mailboxDbPath.length === 0) {
    error("gethen repair late-result: --assignment, --db and --mailbox-db are required");
    return 1;
  }
  try {
    const result = repairLateResult({
      assignmentId,
      dbPath,
      mailboxDbPath,
      dryRun: argv.includes("--dry-run"),
      backup: !argv.includes("--no-backup"),
      write,
      error,
    });
    write(
      result.dryRun
        ? `dry-run: would repair ${result.assignment} ticket=${result.ticket} kind=${result.kind}`
        : `repaired ${result.assignment} ticket=${result.ticket} kind=${result.kind}`,
    );
    if (result.backup !== undefined) write(`backup: ${result.backup}`);
    write(`summary: ${result.summary}`);
    return 0;
  } catch (failure) {
    error(`gethen repair late-result: ${messageOf(failure)}`);
    return 1;
  }
}

export function repairLateResult(options: RepairOptions): RepairResult {
  if (!existsSync(options.dbPath)) throw new Error(`master database not found: ${options.dbPath}`);
  if (!existsSync(options.mailboxDbPath)) {
    throw new Error(`mailbox database not found: ${options.mailboxDbPath}`);
  }
  const report = findMailboxReport(options.mailboxDbPath, options.assignmentId);
  if (report === undefined) {
    throw new Error(`no retained status report found for ${options.assignmentId}`);
  }

  const store = new SqliteStore(options.dbPath);
  try {
    const assignment = store.assignments.get(options.assignmentId);
    if (assignment === undefined) throw new Error(`unknown assignment: ${options.assignmentId}`);
    const ticket = store.tickets.get(assignment.ticket);
    if (ticket === undefined) throw new Error(`unknown ticket: ${assignment.ticket}`);
    if (options.dryRun === true) {
      return {
        assignment: assignment.id,
        ticket: ticket.id,
        kind: report.kind,
        summary: report.summary,
        dryRun: true,
      };
    }

    const backup = options.backup === false ? undefined : backupDatabase(options.dbPath);
    const now = new Date().toISOString();
    const normalized: Report = {
      kind: report.kind,
      summary: report.summary,
      at: report.at ?? now,
      ...(report.question === undefined ? {} : { question: report.question }),
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
    return {
      assignment: assignment.id,
      ticket: ticket.id,
      kind: normalized.kind,
      summary: normalized.summary,
      dryRun: false,
      ...(backup === undefined ? {} : { backup }),
    };
  } finally {
    store.close();
  }
}

/** Pick the newest complete `gethen.task.status.result` report for an assignment. */
export function findMailboxReport(
  mailboxDbPath: string,
  assignmentId: string,
): { kind: Report["kind"]; summary: string; at?: string; question?: string } | undefined {
  const db = new DatabaseSync(mailboxDbPath, { readOnly: true });
  try {
    const rows = db
      .prepare(
        `SELECT envelope_json FROM messages
         WHERE envelope_json LIKE ?
         ORDER BY created_at DESC LIMIT 50`,
      )
      .all(`%${assignmentId}%`);
    for (const row of rows) {
      let envelope: Record<string, unknown>;
      try {
        envelope = JSON.parse(String(row.envelope_json)) as Record<string, unknown>;
      } catch {
        continue;
      }
      if (envelope.type !== "gethen.task.status.result") continue;
      const data = envelope.data as Record<string, unknown> | undefined;
      if (data?.assignmentId !== assignmentId) continue;
      const report = data.report as Record<string, unknown> | undefined;
      if (report === undefined || typeof report.kind !== "string") continue;
      if (typeof report.summary !== "string" || report.summary.trim().length === 0) continue;
      return {
        kind: report.kind as Report["kind"],
        summary: report.summary,
        ...(typeof report.at === "string" ? { at: report.at } : {}),
        ...(typeof report.question === "string" ? { question: report.question } : {}),
      };
    }
    return undefined;
  } finally {
    db.close();
  }
}

function backupDatabase(dbPath: string): string {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const target = join(dirname(dbPath), `${basename(dbPath)}.bak-${stamp}`);
  const db = new DatabaseSync(dbPath);
  try {
    db.exec(`VACUUM INTO '${target.replace(/'/g, "''")}'`);
  } finally {
    db.close();
  }
  return target;
}

const REPAIR_USAGE = `Usage:
  gethen repair late-result --assignment <id> --db <master.sqlite> \\
    --mailbox-db <mailbox.sqlite> [--dry-run] [--no-backup]

Default behaviour backs up the master DB (VACUUM INTO) before writing.
--dry-run prints the Ticket/report that would be repaired without writing.`;

function flagValue(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name);
  return index < 0 ? undefined : argv[index + 1];
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

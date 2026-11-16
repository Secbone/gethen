import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { SqliteStore, type TicketStage, ticketStage } from "@gethen/master";
import { defaultMasterDbPath } from "../paths";

/** How many recent tickets `gethen status` reports. */
const RECENT_LIMIT = 10;

export interface StatusOptions {
  /** Override the home directory used for the default db path (tests). */
  readonly home?: string;
  /** Environment used for `GETHEN_STATE_DIR` (tests). Defaults to `process.env`. */
  readonly env?: NodeJS.ProcessEnv;
  /** Sink for stdout lines (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Sink for error lines (tests). Defaults to stderr. */
  readonly error?: (line: string) => void;
}

/** One row of the `recent` section, in newest-first order. */
export interface RecentTicket {
  id: string;
  /** Derived exactly like the master's panel stage (`ticketStage`). */
  stage: TicketStage;
  latestReport: string;
  assignments: string[];
  openedAt: string;
  endedAt?: string;
}

/** The persisted master state that `gethen status` reports. */
export interface StatusSummary {
  db: string;
  tickets: number;
  open: number;
  receipts: number;
  escalations: number;
  /** The newest {@link RECENT_LIMIT} tickets, newest first. */
  recent: RecentTicket[];
}

/**
 * Read a master sqlite file without starting the outbox, acquiring the master
 * lock or issuing any business write. The store opens the schema idempotently
 * and every query is a read; this is safe against a live master (WAL mode).
 */
export function readStatus(dbPath: string): StatusSummary {
  const store = new SqliteStore(dbPath);
  try {
    const tickets = store.tickets.list();
    const liveTickets = new Set(
      store.assignments
        .list()
        .filter((assignment) => assignment.state === "pending" || assignment.state === "running")
        .map((assignment) => assignment.ticket),
    );
    const recent = tickets
      .slice(-RECENT_LIMIT)
      .reverse()
      .map((ticket): RecentTicket => {
        const stage = ticketStage(ticket, {
          liveAssignment: liveTickets.has(ticket.id),
          // Ticket rows do not carry the report timestamp, so the persisted
          // view cannot tell whether a person spoke after the last report.
          personSinceReport: false,
          deliveryFailed: ticket.delivery !== undefined,
        });
        return {
          id: ticket.id,
          stage,
          latestReport: ticket.latestReport ?? "",
          assignments: [...ticket.assignments],
          openedAt: ticket.openedAt,
          ...(ticket.endedAt === undefined ? {} : { endedAt: ticket.endedAt }),
        };
      });
    return {
      db: dbPath,
      tickets: tickets.length,
      open: tickets.filter((ticket) => ticket.endedAt === undefined).length,
      receipts: store.listReceipts().length,
      escalations: store.listEscalations().length,
      recent,
    };
  } finally {
    store.close();
  }
}

/** `gethen status [--db <file>] [--json]` */
export function statusCommand(argv: string[], options: StatusOptions = {}): number {
  const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
  const writeError = options.error ?? ((line: string) => process.stderr.write(`${line}\n`));

  if (argv.includes("-h") || argv.includes("--help")) {
    write(STATUS_USAGE);
    return 0;
  }

  const env = options.env ?? process.env;
  const dbPath = flagValue(argv, "--db") ?? defaultMasterDbPath(env, options.home ?? homedir());
  if (!existsSync(dbPath)) {
    writeError(`gethen status: no master database at ${dbPath}`);
    writeError("start `gethen serve` first, or pass --db <file>");
    return 1;
  }

  let summary: StatusSummary;
  try {
    summary = readStatus(dbPath);
  } catch (error) {
    writeError(`gethen status: cannot read ${dbPath}: ${messageOf(error)}`);
    return 1;
  }

  if (argv.includes("--json")) {
    write(JSON.stringify(summary, null, 2));
  } else {
    renderText(summary, write);
  }
  return 0;
}

function renderText(summary: StatusSummary, write: (line: string) => void): void {
  write(`db: ${summary.db}`);
  write(
    `tickets: ${summary.tickets}  open: ${summary.open}  ` +
      `receipts: ${summary.receipts}  escalations: ${summary.escalations}`,
  );
  write("");
  if (summary.recent.length === 0) {
    write("(no tickets recorded)");
    return;
  }

  const headers = ["ID", "STAGE", "REPORT", "ASSIGN", "OPENED"];
  const rows = summary.recent.map((ticket) => [
    ticket.id,
    ticket.stage,
    ticket.latestReport === "" ? "-" : ticket.latestReport,
    String(ticket.assignments.length),
    ticket.openedAt,
  ]);
  const widths = headers.map((header, column) =>
    Math.max(header.length, ...rows.map((row) => (row[column] ?? "").length)),
  );
  const format = (cells: readonly string[]): string =>
    cells
      .map((cell, column) => cell.padEnd(widths[column] ?? 0))
      .join("  ")
      .trimEnd();
  write(format(headers));
  for (const row of rows) write(format(row));
  if (summary.tickets > summary.recent.length) {
    write(`(${summary.recent.length} of ${summary.tickets} tickets shown)`);
  }
  write("");
  write("assignment ids are included in --json output");
}

const STATUS_USAGE = `Usage: gethen status [--db <file>] [--json]

Reads the master sqlite file (no outbox or launch port is started) and reports
ticket/receipt/escalation counts plus the last ${RECENT_LIMIT} tickets.
The default database is $GETHEN_STATE_DIR/master.sqlite or ~/.gethen/master.sqlite.
--json prints the same data as one JSON document (including assignment ids).
`;

function flagValue(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

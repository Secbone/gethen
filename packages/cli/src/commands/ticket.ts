import type { TicketCloseOutcome, TicketSummary } from "@gethen/master";
import { masterEndpoint } from "./run";

export type TicketAction = "ack" | "close" | "archive";

export interface TicketArgs {
  action: TicketAction;
  ticketId: string;
  /** Master HTTP origin (or origin + base path). */
  master: string;
  /** Print the raw JSON response. */
  json: boolean;
  /** `close`: terminal outcome; defaults to the master's default. */
  outcome?: TicketCloseOutcome;
  /** `close`: forwarded to a best-effort worker cancel. */
  reason?: string;
}

export interface TicketOptions {
  /** Environment for the `GETHEN_MASTER_URL` fallback (tests). */
  readonly env?: NodeJS.ProcessEnv;
  /** Sink for stdout (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Sink for errors (tests). Defaults to stderr. */
  readonly error?: (line: string) => void;
}

const FLAGS_WITH_VALUE = new Set(["--master", "--outcome", "--reason"]);
const OUTCOMES: readonly TicketCloseOutcome[] = ["completed", "closed", "merged", "dropped"];

/** Parse `gethen ticket ack|close|archive <ticketId> … --master <url>`. */
export function parseTicketArgs(argv: string[], env: NodeJS.ProcessEnv = process.env): TicketArgs {
  const action = argv[0];
  if (action !== "ack" && action !== "close" && action !== "archive") {
    throw new Error('expected a subcommand: "ack", "close" or "archive"');
  }
  const master = flagValue(argv, "--master") ?? env.GETHEN_MASTER_URL;
  if (master === undefined || master.trim().length === 0) {
    throw new Error("--master <url> is required (or set GETHEN_MASTER_URL)");
  }
  const ticketId = positionalArgs(argv.slice(1))[0]?.trim();
  if (ticketId === undefined || ticketId.length === 0) {
    throw new Error("a ticket id is required");
  }
  const json = argv.includes("--json");
  if (action !== "close") {
    return { action, ticketId, master: master.trim(), json };
  }
  const outcome = optionalOutcome(flagValue(argv, "--outcome"));
  const reason = flagValue(argv, "--reason")?.trim();
  return {
    action,
    ticketId,
    master: master.trim(),
    json,
    ...(outcome === undefined ? {} : { outcome }),
    ...(reason === undefined || reason.length === 0 ? {} : { reason }),
  };
}

/** The route `gethen ticket` calls on the master for one parsed command. */
export function ticketPath(args: TicketArgs): string {
  return `/gethen/tickets/${encodeURIComponent(args.ticketId)}/${args.action}`;
}

/** `gethen ticket ack|close|archive … --master <url>` */
export async function ticketCommand(argv: string[], options: TicketOptions = {}): Promise<number> {
  const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
  const writeError = options.error ?? ((line: string) => process.stderr.write(`${line}\n`));

  if (argv.includes("-h") || argv.includes("--help")) {
    write(TICKET_USAGE);
    return 0;
  }
  if (argv.length === 0) {
    writeError(TICKET_USAGE);
    return 1;
  }

  let args: TicketArgs;
  let endpoint: string;
  try {
    args = parseTicketArgs(argv, options.env ?? process.env);
    endpoint = masterEndpoint(args.master, ticketPath(args));
  } catch (error) {
    writeError(`gethen ticket: ${messageOf(error)}`);
    return 1;
  }

  let response: Response;
  try {
    response = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(closeBody(args)),
    });
  } catch (error) {
    writeError(`gethen ticket: cannot reach master at ${endpoint}: ${messageOf(error)}`);
    return 1;
  }
  const raw = await response.text();
  if (!response.ok) {
    writeError(`gethen ticket: ${httpError(response.status, raw)}`);
    return 1;
  }
  if (args.json) {
    write(raw.trim());
    return 0;
  }

  let body: { ticket?: TicketSummary };
  try {
    body = JSON.parse(raw) as { ticket?: TicketSummary };
  } catch {
    writeError(`gethen ticket: master returned invalid JSON: ${truncate(raw)}`);
    return 1;
  }
  renderTicket(args.action, body.ticket, write);
  return 0;
}

const TICKET_USAGE = `Usage:
  gethen ticket ack <ticketId> --master <url> [--json]
  gethen ticket close <ticketId> --master <url> [--outcome completed|closed|merged|dropped]
                          [--reason "<text>"] [--json]
  gethen ticket archive <ticketId> --master <url> [--json]

Talks to a running \`gethen serve\`: ack clears the current "needs action" flag
until the Ticket reports again; close writes the terminal outcome (idempotent
for the same outcome); archive hides a closed Ticket from the default overview.
--master may be an origin or an origin with a base path; GETHEN_MASTER_URL is
the fallback.
`;

function closeBody(args: TicketArgs): Record<string, string> {
  if (args.action !== "close") return {};
  return {
    ...(args.outcome === undefined ? {} : { outcome: args.outcome }),
    ...(args.reason === undefined ? {} : { reason: args.reason }),
  };
}

function renderTicket(
  action: TicketAction,
  ticket: TicketSummary | undefined,
  write: (line: string) => void,
): void {
  if (ticket === undefined) {
    write(`${action}: no ticket returned`);
    return;
  }
  write(`${action}: ${ticket.id}`);
  write(
    `lifecycle: ${ticket.lifecycle}  stage: ${ticket.stage}  ` +
      `needs action: ${ticket.needsAction ? "yes" : "no"}`,
  );
  if (ticket.outcome !== undefined) write(`outcome: ${ticket.outcome}`);
  if (ticket.endedAt !== undefined) write(`ended: ${ticket.endedAt}`);
  if (ticket.archivedAt !== undefined) write(`archived: ${ticket.archivedAt}`);
}

function optionalOutcome(value: string | undefined): TicketCloseOutcome | undefined {
  if (value === undefined) return undefined;
  if (!(OUTCOMES as readonly string[]).includes(value)) {
    throw new Error(`--outcome must be one of: ${OUTCOMES.join(", ")}`);
  }
  return value as TicketCloseOutcome;
}

function httpError(status: number, raw: string): string {
  const parsed = safeJson(raw);
  if (typeof parsed === "object" && parsed !== null) {
    const error = (parsed as { error?: unknown }).error;
    if (typeof error === "string" && error.length > 0) return `HTTP ${status}: ${error}`;
  }
  const text = raw.trim();
  return text.length > 0 ? `HTTP ${status}: ${truncate(text)}` : `HTTP ${status}`;
}

function safeJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    return undefined;
  }
}

function flagValue(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
}

/** Positional words, skipping flags and the values they consume. */
function positionalArgs(argv: string[]): string[] {
  const words: string[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === undefined) continue;
    if (FLAGS_WITH_VALUE.has(arg)) {
      index += 1;
      continue;
    }
    if (arg.startsWith("-")) continue;
    words.push(arg);
  }
  return words;
}

function truncate(value: string, limit = 500): string {
  return value.length <= limit ? value : `${value.slice(0, limit)}…`;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

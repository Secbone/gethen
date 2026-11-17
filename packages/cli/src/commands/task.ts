import type { TaskState } from "@gethen/master-runtime";
import { masterEndpoint } from "./run";

export type TaskAction = "status" | "steer" | "cancel";

export interface TaskArgs {
  action: TaskAction;
  assignmentId: string;
  /** Master HTTP origin (or origin + base path). */
  master: string;
  /** `status`: print the raw JSON response. */
  json: boolean;
  /** `steer`: the instruction text. */
  text?: string;
  /** `cancel`: why the assignment is cancelled. */
  reason?: string;
  /** `status`: how long to wait for the worker's state. */
  timeoutMs?: number;
}

export interface TaskOptions {
  /** Environment for the `GETHEN_MASTER_URL` fallback (tests). */
  readonly env?: NodeJS.ProcessEnv;
  /** Sink for stdout (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Sink for errors (tests). Defaults to stderr. */
  readonly error?: (line: string) => void;
}

const FLAGS_WITH_VALUE = new Set(["--master", "--text", "--reason", "--timeout-ms"]);

/**
 * Parse `gethen task status|steer|cancel <assignmentId> … --master <url>`.
 * Positional words after the assignment id are the steer text / cancel reason
 * when the matching flag is absent.
 */
export function parseTaskArgs(argv: string[], env: NodeJS.ProcessEnv = process.env): TaskArgs {
  const action = argv[0];
  if (action !== "status" && action !== "steer" && action !== "cancel") {
    throw new Error('expected a subcommand: "status", "steer" or "cancel"');
  }
  const master = flagValue(argv, "--master") ?? env.GETHEN_MASTER_URL;
  if (master === undefined || master.trim().length === 0) {
    throw new Error("--master <url> is required (or set GETHEN_MASTER_URL)");
  }
  const positionals = positionalArgs(argv.slice(1));
  const assignmentId = positionals[0]?.trim();
  if (assignmentId === undefined || assignmentId.length === 0) {
    throw new Error("an assignment id is required");
  }
  const rest = positionals.slice(1).join(" ").trim();

  if (action === "status") {
    const timeoutMs = positiveInteger(flagValue(argv, "--timeout-ms"), "--timeout-ms");
    return {
      action,
      assignmentId,
      master: master.trim(),
      json: argv.includes("--json"),
      ...(timeoutMs === undefined ? {} : { timeoutMs }),
    };
  }
  if (action === "steer") {
    const text = (flagValue(argv, "--text") ?? rest).trim();
    if (text.length === 0) {
      throw new Error('steer needs instruction text: "<text>" or --text <text>');
    }
    return { action, assignmentId, master: master.trim(), json: false, text };
  }
  const reason = (flagValue(argv, "--reason") ?? rest).trim();
  return {
    action,
    assignmentId,
    master: master.trim(),
    json: false,
    ...(reason.length === 0 ? {} : { reason }),
  };
}

/** The route `gethen task` calls on the master for one parsed command. */
export function taskPath(args: TaskArgs): string {
  const prefix = `/gethen/tasks/${encodeURIComponent(args.assignmentId)}`;
  if (args.action === "status") {
    return args.timeoutMs === undefined ? prefix : `${prefix}?timeoutMs=${args.timeoutMs}`;
  }
  return `${prefix}/${args.action}`;
}

/** `gethen task status|steer|cancel … --master <url>` */
export async function taskCommand(argv: string[], options: TaskOptions = {}): Promise<number> {
  const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
  const writeError = options.error ?? ((line: string) => process.stderr.write(`${line}\n`));

  if (argv.includes("-h") || argv.includes("--help")) {
    write(TASK_USAGE);
    return 0;
  }
  if (argv.length === 0) {
    writeError(TASK_USAGE);
    return 1;
  }

  let args: TaskArgs;
  let endpoint: string;
  try {
    args = parseTaskArgs(argv, options.env ?? process.env);
    endpoint = masterEndpoint(args.master, taskPath(args));
  } catch (error) {
    writeError(`gethen task: ${messageOf(error)}`);
    return 1;
  }

  const init: RequestInit =
    args.action === "status"
      ? { method: "GET" }
      : {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(
            args.action === "steer" ? { text: args.text ?? "" } : reasonBody(args.reason),
          ),
        };

  let response: Response;
  try {
    response = await fetch(endpoint, init);
  } catch (error) {
    writeError(`gethen task: cannot reach master at ${endpoint}: ${messageOf(error)}`);
    return 1;
  }
  const raw = await response.text();
  if (!response.ok) {
    writeError(`gethen task: ${httpError(response.status, raw)}`);
    return 1;
  }

  if (args.action !== "status") {
    write(`${args.action} sent for ${args.assignmentId}`);
    return 0;
  }

  if (args.json) {
    write(raw.trim());
    return 0;
  }
  let body: { assignmentId?: string; state?: TaskState };
  try {
    body = JSON.parse(raw) as { assignmentId?: string; state?: TaskState };
  } catch {
    writeError(`gethen task: master returned invalid JSON: ${truncate(raw)}`);
    return 1;
  }
  renderStatus(body, write);
  return 0;
}

const TASK_USAGE = `Usage:
  gethen task status <assignmentId> --master <url> [--json] [--timeout-ms <n>]
  gethen task steer <assignmentId> "<text>" --master <url>
  gethen task cancel <assignmentId> [--reason "<text>"] --master <url>

Talks to a running \`gethen serve\`: status asks the Worker for live task state
(404 for an unknown assignment, 504 when the Worker does not answer in time);
steer/cancel send control messages to the running assignment. --master may be an
origin (http://127.0.0.1:8080) or an origin with a base path; GETHEN_MASTER_URL
is the fallback. Exit code is non-zero for transport/HTTP/validation errors.
`;

function renderStatus(
  body: { assignmentId?: string; state?: TaskState },
  write: (line: string) => void,
): void {
  const state = body.state;
  write(`assignment: ${body.assignmentId ?? ""}`);
  if (state === undefined) {
    write("state: unknown");
    return;
  }
  write(`phase: ${state.phase}`);
  write(`step: ${state.step}`);
  write(`seq: ${state.seq}`);
  write(`last event: ${state.lastEventAt}`);
  if (state.lastMessage !== undefined) write(`last message: ${state.lastMessage}`);
  if (state.lastTool !== undefined) {
    write(`last tool: ${state.lastTool.name} (${state.lastTool.startedAt})`);
  }
  if (state.updatedAt !== undefined) write(`updated: ${state.updatedAt}`);
}

function reasonBody(reason: string | undefined): Record<string, string> {
  return reason === undefined ? {} : { reason };
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

function positiveInteger(value: string | undefined, label: string): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
  return parsed;
}

function truncate(value: string, limit = 500): string {
  return value.length <= limit ? value : `${value.slice(0, limit)}…`;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

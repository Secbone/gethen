import type { SessionSpec } from "@gethen/master";

/** What `POST /gethen/run` returns from a master (`gethen serve`). */
export interface RunResult {
  stored?: string;
  outcome?: string;
  ticket?: string;
  report?: string;
  receipt?: boolean;
  error?: string;
}

export interface RunArgs {
  /** Master HTTP origin (`--master`, or `GETHEN_MASTER_URL`). */
  master: string;
  text: string;
  /** Print the master's raw JSON response instead of the summary. */
  json: boolean;
  /** Session binding sent with the run (`new` unless --session-id/--reuse-key). */
  session: SessionSpec;
}

export interface RunOptions {
  /** Environment for the `GETHEN_MASTER_URL` fallback (tests). */
  readonly env?: NodeJS.ProcessEnv;
  /** Sink for stdout (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Sink for errors (tests). Defaults to stderr. */
  readonly error?: (line: string) => void;
}

/**
 * Validate a `--master` URL and return its origin + base path without a
 * trailing slash (`http://127.0.0.1:8080`, `https://host/api`). Query strings
 * and fragments are dropped: they belong to the route, not the master.
 */
export function masterBaseUrl(master: string): string {
  let url: URL;
  try {
    url = new URL(master);
  } catch {
    throw new Error(`--master "${master}" is not a valid URL`);
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error(`--master must be an http(s) URL, got "${url.protocol}"`);
  }
  url.search = "";
  url.hash = "";
  url.pathname = url.pathname.replace(/\/+$/, "");
  const base = url.toString();
  return base.endsWith("/") ? base.slice(0, -1) : base;
}

/** Join a checked master URL with a route path (e.g. `/gethen/tasks`). */
export function masterEndpoint(master: string, path: string): string {
  return `${masterBaseUrl(master)}${path.startsWith("/") ? path : `/${path}`}`;
}

/**
 * Resolve a master URL to its run endpoint. Accepts an origin
 * (`http://127.0.0.1:8080`), an origin with a base path, or the full
 * `/gethen/run` URL.
 */
export function runEndpoint(master: string): string {
  const base = masterBaseUrl(master);
  return base.endsWith("/gethen/run") ? base : `${base}/gethen/run`;
}

/** Parse `gethen run --master <url> "<text>"|--text <text> [--json]`. */
export function parseRunArgs(argv: string[], env: NodeJS.ProcessEnv = process.env): RunArgs {
  const master = flagValue(argv, "--master") ?? env.GETHEN_MASTER_URL;
  if (master === undefined || master.trim().length === 0) {
    throw new Error("--master <url> is required (or set GETHEN_MASTER_URL)");
  }
  const textFlag = flagValue(argv, "--text");
  const text = textFlag ?? positionalArgs(argv).join(" ");
  if (text.trim().length === 0) {
    throw new Error('run text is required: pass "<text>" or --text <text>');
  }
  return {
    master: master.trim(),
    text: text.trim(),
    json: argv.includes("--json"),
    session: parseSessionFlags(argv),
  };
}

/** `--session-id <id>` or `--reuse-key <key>` select reuse; default is `new`. */
function parseSessionFlags(argv: string[]): SessionSpec {
  const sessionId = flagValue(argv, "--session-id")?.trim();
  const key = flagValue(argv, "--reuse-key")?.trim();
  if (sessionId !== undefined && sessionId.length === 0) {
    throw new Error("--session-id must be a non-empty string");
  }
  if (key !== undefined && key.length === 0) {
    throw new Error("--reuse-key must be a non-empty string");
  }
  if (sessionId !== undefined && key !== undefined) {
    throw new Error("--session-id and --reuse-key are mutually exclusive");
  }
  if (sessionId !== undefined) return { mode: "reuse", sessionId };
  if (key !== undefined) return { mode: "reuse", key };
  return { mode: "new" };
}

/** `gethen run --master <url> "<text>" [--json]` */
export async function runCommand(argv: string[], options: RunOptions = {}): Promise<number> {
  const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
  const writeError = options.error ?? ((line: string) => process.stderr.write(`${line}\n`));

  if (argv.includes("-h") || argv.includes("--help")) {
    write(RUN_USAGE);
    return 0;
  }

  let args: RunArgs;
  let endpoint: string;
  try {
    args = parseRunArgs(argv, options.env ?? process.env);
    endpoint = runEndpoint(args.master);
  } catch (error) {
    writeError(`gethen run: ${messageOf(error)}`);
    return 1;
  }

  let response: Response;
  try {
    response = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: args.text, session: args.session }),
    });
  } catch (error) {
    writeError(`gethen run: cannot reach master at ${endpoint}: ${messageOf(error)}`);
    return 1;
  }

  const raw = await response.text();
  if (!response.ok) {
    writeError(`gethen run: master returned HTTP ${response.status}: ${truncate(raw)}`);
    return 1;
  }

  let result: RunResult;
  try {
    result = JSON.parse(raw) as RunResult;
  } catch {
    writeError(`gethen run: master returned invalid JSON: ${truncate(raw)}`);
    return 1;
  }

  if (args.json) {
    write(raw.trim());
  } else {
    write(
      JSON.stringify(
        {
          ticket: result.ticket ?? "",
          report: result.report ?? "",
          receipt: result.receipt === true,
        },
        null,
        2,
      ),
    );
  }

  if (result.error !== undefined) {
    writeError(`gethen run: ${result.error}`);
    return 1;
  }
  // A stored ticket whose worker reported `failed` did not do the work; make
  // that visible to scripts instead of exiting 0.
  return result.report === "failed" ? 1 : 0;
}

const RUN_USAGE = `Usage: gethen run --master <url> "<text>" [--json]
   or: gethen run --master <url> --text <text> [--json]
                    [--session-id <id> | --reuse-key <key>]

POSTs {"text": ..., "session": ...} to <master>/gethen/run and prints
{ticket, report, receipt}. --json prints the master's raw JSON response.
Sessions: the default is a fresh session; --session-id reuses an exact harness
session and --reuse-key reuses the Worker's registry entry for a key (mutually
exclusive). The default master URL fallback is $GETHEN_MASTER_URL. Exit code is
non-zero when the master is unreachable, the request fails, or the worker report
is "failed".
`;

function flagValue(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
}

/** Positional words, skipping known flag names and their values. */
function positionalArgs(argv: string[]): string[] {
  const words: string[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === undefined) continue;
    if (arg === "--master" || arg === "--text" || arg === "--session-id" || arg === "--reuse-key") {
      index += 1;
      continue;
    }
    if (arg === "--json" || arg === "-h" || arg === "--help") continue;
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

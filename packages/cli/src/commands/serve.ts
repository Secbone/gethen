import { mkdirSync, readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { dirname } from "node:path";
import type {
  EscalationRecord,
  EscalationResolutionInput,
  MasterService,
  Trigger,
} from "@gethen/master";
import { createMasterRuntime, type MasterRuntime } from "@gethen/master-runtime";
import type { TokenRecord } from "@gethen/transport";
import { defaultMasterDbPath } from "../paths";

/** The only upgrade path this host accepts for the worker control plane. */
export const WORKER_PATH = "/gethen/worker";

export interface ServeConfig {
  /** SQLite file; defaults to `$GETHEN_STATE_DIR/master.sqlite` or `~/.gethen/master.sqlite`. */
  dbPath?: string;
  /** Bind address; defaults to loopback. */
  host?: string;
  /** Port; defaults to an ephemeral port (0). */
  port?: number;
  /**
   * Worker credentials served by the transport server. Absent or empty keeps
   * the fake launch (dev/tests) and does not expose the worker endpoint.
   */
  tokens?: readonly TokenRecord[];
  /**
   * Answers a worker escalation. Without it, an escalation is parked and the
   * launch reports `waiting` (a person has to decide), which is the safe default.
   */
  resolveEscalation?: (record: EscalationRecord) => Promise<EscalationResolutionInput | undefined>;
  /**
   * Worker-side workspace put into the dispatch when the engine leaves the
   * assignment workspace empty (defaults to `process.cwd()`). The P1 engine does
   * not model a workspace yet; until it does, the host supplies the default.
   */
  workspace?: string;
  /** Pack digest recorded for dispatches without one. */
  packDigest?: string;
  /** The single worker served; defaults to the only configured token's id. */
  workerId?: string;
  /** Mailbox sqlite file; defaults to `<db dir>/mailbox.sqlite`. */
  mailboxDbPath?: string;
  /** Full master mailbox name; defaults to `mailbox:master`. */
  masterMailbox?: string;
  /** Bridge/link poll interval (tests and verify scripts). */
  mailboxPollIntervalMs?: number;
  /** Lease/visibility timeout for mailbox deliveries (tests and verify scripts). */
  mailboxVisibilityTimeoutMs?: number;
}

export interface ServeHandle {
  server: Server;
  service: MasterService;
  port: number;
  /** `"ws"` when the mailbox transport stack is attached, else the fake launch. */
  launch: "ws" | "fake";
  /** The assembled runtime (task state/control, bridge, service). */
  runtime: MasterRuntime;
  close(): Promise<void>;
}

export interface ServeCommandOptions {
  /** Environment for `--tokens` fallback (tests). Defaults to `process.env`. */
  readonly env?: NodeJS.ProcessEnv;
  /** Sink for stdout lines (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Sink for error lines (tests). Defaults to stderr. */
  readonly error?: (line: string) => void;
}

const MAX_BODY_BYTES = 64 * 1024;

function text(value: unknown, workspace: string): Trigger {
  const message = typeof value === "string" ? value.trim() : "";
  if (message.length === 0) throw new Error("text is required");
  return {
    id: `trigger-${crypto.randomUUID()}`,
    source: "local",
    thread: "local",
    actor: "local-user",
    bot: false,
    addressed: "mention",
    text: message,
    at: new Date().toISOString(),
    meta: { scope: "local", workspace },
  };
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.byteLength;
    if (size > MAX_BODY_BYTES) throw new Error("body too large");
    chunks.push(buffer);
  }
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
}

function send(response: ServerResponse, status: number, value: unknown): void {
  const body = JSON.stringify(value);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
  });
  response.end(body);
}

/** Resolution text for auto-answered escalations; logs show it was not a person. */
export const AUTO_ESCALATION_TEXT = "auto-answered by gethen serve --escalation auto";

/**
 * The `--escalation auto` policy: answer with the escalation's declared default
 * option, else its first option, and mark the resolution text as automatic.
 * `park` (the default) passes no resolver, so the launch reports `waiting`.
 */
export function autoEscalationResolver(): (
  record: EscalationRecord,
) => Promise<EscalationResolutionInput> {
  return async (record) => {
    const option = record.defaultOption ?? record.options[0]?.id;
    return {
      kind: "answer",
      text: AUTO_ESCALATION_TEXT,
      ...(option === undefined ? {} : { option }),
    };
  };
}

/**
 * Start the master control plane through `@gethen/master-runtime`: sqlite store
 * + outbox + L1 ws transport + L2 mailbox stack (or the fake launch when no
 * worker tokens are configured), plus a small HTTP surface for health and local
 * dispatch. The worker upgrade path is `/gethen/worker`.
 */
export async function startServe(config: ServeConfig = {}): Promise<ServeHandle> {
  const dbPath = config.dbPath ?? defaultMasterDbPath();
  mkdirSync(dirname(dbPath), { recursive: true });

  const runtime = createMasterRuntime({
    dbPath,
    workspace: config.workspace ?? process.cwd(),
    ...(config.tokens === undefined ? {} : { tokens: config.tokens }),
    ...(config.packDigest === undefined ? {} : { packDigest: config.packDigest }),
    ...(config.resolveEscalation === undefined
      ? {}
      : { resolveEscalation: config.resolveEscalation }),
    ...(config.workerId === undefined ? {} : { workerId: config.workerId }),
    ...(config.mailboxDbPath === undefined ? {} : { mailboxDbPath: config.mailboxDbPath }),
    ...(config.masterMailbox === undefined ? {} : { masterMailbox: config.masterMailbox }),
    ...(config.mailboxPollIntervalMs === undefined
      ? {}
      : { pollIntervalMs: config.mailboxPollIntervalMs }),
    ...(config.mailboxVisibilityTimeoutMs === undefined
      ? {}
      : { visibilityTimeoutMs: config.mailboxVisibilityTimeoutMs }),
  });
  const service = runtime.service;
  const startedAt = Date.now();

  const server = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url ?? "/", "http://localhost");
      const { pathname } = url;
      try {
        if (request.method === "GET" && pathname === "/health") {
          send(response, 200, {
            ok: true,
            uptimeMs: Date.now() - startedAt,
            ...runtime.health(),
          });
          return;
        }
        if (request.method === "POST" && pathname === "/gethen/run") {
          const body = await readJson(request);
          const trigger = text(body.text as string | undefined, config.workspace ?? process.cwd());
          const result = await service.submit(trigger);
          const ticket = service.store.tickets.list().at(-1);
          send(response, 200, {
            stored: result.stored,
            outcome: result.decision?.outcome ?? "",
            ticket: ticket?.id ?? "",
            report: ticket?.latestReport ?? "",
            receipt: ticket
              ? service.store.listReceipts().some((row) => row.ticket === ticket.id)
              : false,
          });
          return;
        }
        if (pathname === "/gethen/tasks" || pathname.startsWith("/gethen/tasks/")) {
          await handleTaskRoutes(runtime, request, response, pathname, url.searchParams);
          return;
        }
        send(response, 404, { error: "not found" });
      } catch (error) {
        send(response, 400, { error: error instanceof Error ? error.message : String(error) });
      }
    })();
  });

  // Only the worker control-plane path upgrades; everything else is a plain
  // 404 and a destroyed socket (never a half-open connection).
  server.on("upgrade", (request, socket, head) => {
    const { pathname } = new URL(request.url ?? "/", "http://localhost");
    if (runtime.handleUpgrade === undefined || pathname !== WORKER_PATH) {
      socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
      socket.destroy();
      return;
    }
    runtime.handleUpgrade(request, socket, head);
  });

  const port = await new Promise<number>((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port ?? 0, config.host ?? "127.0.0.1", () => {
      const address = server.address();
      resolve(typeof address === "object" && address !== null ? address.port : 0);
    });
  });

  let closed = false;
  return {
    server,
    service,
    port,
    launch: runtime.launch,
    runtime,
    async close() {
      if (closed) return;
      closed = true;
      // Stop accepting upgrades first, then let the runtime close the bridge,
      // session links, transport server, mailbox store and service.
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await runtime.close();
    },
  };
}

const TASK_ROUTE = /^\/gethen\/tasks(?:\/([^/]+))?(?:\/(steer|cancel))?$/;

/**
 * `GET /gethen/tasks`, `GET /gethen/tasks/:id`,
 * `POST /gethen/tasks/:id/steer`, `POST /gethen/tasks/:id/cancel`.
 * Errors are JSON with the runtime/bridge error code; status queries map an
 * unanswered worker to 504 and an unknown assignment to 404.
 */
async function handleTaskRoutes(
  runtime: MasterRuntime,
  request: IncomingMessage,
  response: ServerResponse,
  pathname: string,
  search: URLSearchParams,
): Promise<void> {
  const match = TASK_ROUTE.exec(pathname);
  if (match === null) {
    send(response, 404, { error: "not found" });
    return;
  }

  let timeoutMs: number | undefined;
  try {
    timeoutMs = optionalTimeout(search);
  } catch (error) {
    send(response, 400, { error: messageOf(error) });
    return;
  }

  const encodedId = match[1];
  const action = match[2];
  try {
    const assignmentId = encodedId === undefined ? undefined : decodeURIComponent(encodedId);
    if (assignmentId === undefined) {
      if (request.method !== "GET") {
        send(response, 405, { error: "method not allowed" });
        return;
      }
      send(response, 200, { tasks: runtime.tasks.states() });
      return;
    }
    if (action === undefined) {
      if (request.method !== "GET") {
        send(response, 405, { error: "method not allowed" });
        return;
      }
      const known = runtime.tasks.state(assignmentId);
      if (known !== undefined) {
        send(response, 200, { assignmentId, state: known });
        return;
      }
      const state = await runtime.tasks.status(
        assignmentId,
        timeoutMs === undefined ? {} : { timeoutMs },
      );
      if (state === null) {
        send(response, 404, { error: `unknown task "${assignmentId}"` });
        return;
      }
      send(response, 200, { assignmentId, state });
      return;
    }
    if (request.method !== "POST") {
      send(response, 405, { error: "method not allowed" });
      return;
    }
    if (action === "steer") {
      const body = await readJson(request);
      const message = typeof body.text === "string" ? body.text.trim() : "";
      if (message.length === 0) {
        send(response, 400, { error: "steer needs a non-empty text" });
        return;
      }
      await runtime.tasks.steer(assignmentId, message);
      send(response, 202, { ok: true, assignmentId, action: "steer" });
      return;
    }
    const body = await readJson(request);
    const reason =
      typeof body.reason === "string" && body.reason.length > 0 ? body.reason : undefined;
    await runtime.tasks.cancel(assignmentId, reason);
    send(response, 202, { ok: true, assignmentId, action: "cancel" });
  } catch (error) {
    const code = taskErrorCode(error);
    send(response, taskErrorStatus(code), {
      error: messageOf(error),
      ...(code === undefined ? {} : { code }),
    });
  }
}

function taskErrorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code : undefined;
}

function taskErrorStatus(code: string | undefined): number {
  switch (code) {
    case "RUNTIME_COMMS_UNAVAILABLE":
      return 409;
    case "RUNTIME_CLOSED":
    case "MAILBOX_CLOSED":
      return 503;
    case "MAILBOX_STATUS_TIMEOUT":
      return 504;
    default:
      return 500;
  }
}

function optionalTimeout(search: URLSearchParams): number | undefined {
  const raw = search.get("timeoutMs");
  if (raw === null) return undefined;
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error("timeoutMs must be a positive integer");
  }
  return parsed;
}

/**
 * Parse the worker credential JSON: `[{"workerId":"...","token":"..."}]`.
 * Structural validation only; duplicate/invalid entries are rejected by the
 * token store when the transport server starts.
 */
export function parseWorkerTokens(raw: string): TokenRecord[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`invalid worker tokens JSON: ${messageOf(error)}`);
  }
  if (!Array.isArray(parsed)) {
    throw new Error("worker tokens must be a JSON array of { workerId, token }");
  }
  return parsed.map((entry: unknown, index: number): TokenRecord => {
    if (!isRecord(entry)) throw new Error(`worker token entry ${index} must be an object`);
    const workerId = entry.workerId;
    const token = entry.token;
    if (typeof workerId !== "string" || workerId.length === 0) {
      throw new Error(`worker token entry ${index} needs a non-empty string workerId`);
    }
    if (typeof token !== "string" || token.length === 0) {
      throw new Error(`worker token entry ${index} needs a non-empty string token`);
    }
    return { workerId, token };
  });
}

/** `gethen serve [--db <file>] [--host <addr>] [--port <n>] …` */
export async function serveCommand(
  argv: string[],
  options: ServeCommandOptions = {},
): Promise<number> {
  const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
  const writeError = options.error ?? ((line: string) => process.stderr.write(`${line}\n`));
  if (argv.includes("-h") || argv.includes("--help")) {
    write(
      "Usage: gethen serve [--db <file>] [--host <addr>] [--port <n>]\n" +
        "                    [--tokens <file>] [--workspace <dir>]\n" +
        "                    [--worker-id <id>] [--mailbox-db <file>]\n" +
        "                    [--master-mailbox <name>] [--escalation auto|park]\n" +
        "Starts the master control plane (L1 ws transport + L2 mailbox).\n" +
        "Worker tokens come from --tokens <file> or GETHEN_WORKER_TOKENS_JSON\n" +
        '([{"workerId":"...","token":"..."}]); with them the /gethen/worker endpoint is\n' +
        "served and assignments are dispatched over the mailbox stack. Without them the\n" +
        "fake launch is used (dev convenience).\n" +
        "--worker-id is the single worker served (defaults to the only token);\n" +
        "--mailbox-db defaults to <db dir>/mailbox.sqlite and --master-mailbox to\n" +
        "mailbox:master.\n" +
        "--workspace is the worker-side path dispatched assignments run in\n" +
        "(the engine has no per-assignment workspace yet).\n" +
        "--escalation auto|park (default park): park waits for a person (the run\n" +
        'reports "waiting"); auto answers the declared default/first option and\n' +
        "marks the resolution text as auto-answered.\n",
    );
    return 0;
  }
  const env = options.env ?? process.env;
  const option = (name: string): string | undefined => {
    const index = argv.indexOf(name);
    return index >= 0 ? argv[index + 1] : undefined;
  };

  const escalation = option("--escalation") ?? "park";
  if (escalation !== "auto" && escalation !== "park") {
    writeError(`gethen serve: --escalation must be "auto" or "park", got "${escalation}"`);
    return 1;
  }

  let tokens: TokenRecord[] | undefined;
  const tokensPath = option("--tokens");
  try {
    const raw =
      tokensPath === undefined ? env.GETHEN_WORKER_TOKENS_JSON : readFileSync(tokensPath, "utf8");
    if (raw !== undefined && raw.trim().length > 0) tokens = parseWorkerTokens(raw);
  } catch (error) {
    writeError(`gethen serve: ${messageOf(error)}`);
    return 1;
  }

  const host = option("--host") ?? "127.0.0.1";
  const dbPath = option("--db");
  const portValue = option("--port");
  const workspace = option("--workspace");
  const workerId = option("--worker-id");
  const mailboxDbPath = option("--mailbox-db");
  const masterMailbox = option("--master-mailbox");
  let handle: ServeHandle;
  try {
    handle = await startServe({
      host,
      ...(dbPath === undefined ? {} : { dbPath }),
      ...(portValue === undefined ? {} : { port: Number(portValue) }),
      ...(workspace === undefined ? {} : { workspace }),
      ...(tokens === undefined ? {} : { tokens }),
      ...(escalation === "auto" ? { resolveEscalation: autoEscalationResolver() } : {}),
      ...(workerId === undefined ? {} : { workerId }),
      ...(mailboxDbPath === undefined ? {} : { mailboxDbPath }),
      ...(masterMailbox === undefined ? {} : { masterMailbox }),
    });
  } catch (error) {
    writeError(`gethen serve: ${messageOf(error)}`);
    return 1;
  }

  write(`gethen master listening on http://${host}:${handle.port}`);
  if (escalation === "auto") {
    write("escalations: auto (answering the default/first option)");
  }
  if (handle.launch === "ws") {
    const served =
      workerId ?? (tokens !== undefined && tokens.length === 1 ? tokens[0]?.workerId : undefined);
    write(
      `worker endpoint: ws://${host}:${handle.port}${WORKER_PATH} (launch: ws${
        served === undefined ? "" : `, worker: ${served}`
      })`,
    );
  } else {
    write("launch: fake (no worker tokens configured)");
  }
  await new Promise<void>((resolve) => {
    const shutdown = (): void => {
      process.off("SIGINT", shutdown);
      process.off("SIGTERM", shutdown);
      resolve();
    };
    process.once("SIGINT", shutdown);
    process.once("SIGTERM", shutdown);
  });
  await handle.close();
  return 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

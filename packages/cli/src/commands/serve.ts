import { mkdirSync, readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { dirname } from "node:path";
import {
  type EscalationRecord,
  type EscalationResolutionInput,
  type MasterService,
  type OverviewFilter,
  parseSessionSpec,
  type TicketCloseOutcome,
  type Trigger,
} from "@gethen/master";
import {
  type ControlPlane,
  createEscalationHooks,
  createMasterRuntime,
  DEFAULT_WORKSTREAM,
  type MasterRuntime,
  type RoutingOptions,
  type TaskEventQuery,
  type WarningThresholds,
} from "@gethen/master-runtime";
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
   * Logical workstream id put into the dispatch when the engine leaves the
   * assignment workspace empty (defaults to `DEFAULT_WORKSTREAM`). It is not a
   * Master filesystem path: the Worker owns the workspace directory.
   */
  workspace?: string;
  /** Pack digest recorded for dispatches without one. */
  packDigest?: string;
  /** Archive every terminal Ticket after this window (all outcomes). */
  retentionMs?: number;
  /** Close stale ready/failed Tickets after this window (all outcomes). */
  staleCloseMs?: number;
  /** Master build commit Workers should match; defaults to GETHEN_BUILD_COMMIT. */
  buildCommit?: string;
  /** Thresholds for health warnings; each defaults to 1. */
  warnings?: WarningThresholds;
  /** Restrict dispatch to this Worker; defaults to every configured token. */
  workerId?: string;
  /** Workers accepted from a dynamic token verifier (tokens are preferred). */
  workers?: readonly string[];
  /** Router pin, extra hard requirements and queue bounds. */
  routing?: RoutingOptions;
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

function optionalWaitMs(value: unknown): number | undefined {
  if (value === undefined || value === null) return undefined;
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("waitMs must be a positive integer");
  }
  return parsed;
}

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

  let runtime: MasterRuntime | undefined;
  runtime = createMasterRuntime({
    dbPath,
    workspace: config.workspace ?? DEFAULT_WORKSTREAM,
    ...(config.tokens === undefined ? {} : { tokens: config.tokens }),
    ...(config.packDigest === undefined ? {} : { packDigest: config.packDigest }),
    ...(config.retentionMs === undefined ? {} : { retentionMs: config.retentionMs }),
    ...(config.staleCloseMs === undefined ? {} : { staleCloseMs: config.staleCloseMs }),
    ...(config.buildCommit === undefined ? {} : { buildCommit: config.buildCommit }),
    ...(config.warnings === undefined ? {} : { warnings: config.warnings }),
    ...(config.resolveEscalation === undefined
      ? {}
      : { resolveEscalation: config.resolveEscalation }),
    ...(config.workerId === undefined ? {} : { workerId: config.workerId }),
    ...(config.workers === undefined ? {} : { workers: config.workers }),
    ...(config.routing === undefined ? {} : { routing: config.routing }),
    ...(config.mailboxDbPath === undefined ? {} : { mailboxDbPath: config.mailboxDbPath }),
    ...(config.masterMailbox === undefined ? {} : { masterMailbox: config.masterMailbox }),
    ...(config.mailboxPollIntervalMs === undefined
      ? {}
      : { pollIntervalMs: config.mailboxPollIntervalMs }),
    ...(config.mailboxVisibilityTimeoutMs === undefined
      ? {}
      : { visibilityTimeoutMs: config.mailboxVisibilityTimeoutMs }),
    // Same persistence/engine rules as the DSH host: a parked escalation keeps
    // its Assignment waiting and a resolution resumes it.
    ...createEscalationHooks(() => runtime?.service),
  });
  const service = runtime.service;
  const control = runtime.control;
  const startedAt = Date.now();

  const server = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url ?? "/", "http://localhost");
      const { pathname } = url;
      try {
        if (request.method === "GET" && pathname === "/health") {
          const counts = control.status(0).counts;
          send(response, 200, {
            ok: true,
            uptimeMs: Date.now() - startedAt,
            ...runtime.health(),
            workerDetails: runtime.workers(),
            needsAction: counts.needsAction,
            archived: counts.archived,
          });
          return;
        }
        if (request.method === "GET" && pathname === "/gethen/workers") {
          send(response, 200, {
            workers: runtime.workers(),
            launch: runtime.launch,
            scheduling: runtime.scheduling(),
            settlements: runtime.settlements(),
          });
          return;
        }
        if (request.method === "GET" && pathname === "/gethen/overview") {
          const filter = parseOverviewFilter(url.searchParams);
          send(response, 200, control.overview({ filter }));
          return;
        }
        if (pathname === "/gethen/tickets" || pathname.startsWith("/gethen/tickets/")) {
          await handleTicketRoutes(control, request, response, pathname, url.searchParams);
          return;
        }
        if (request.method === "POST" && pathname === "/gethen/run") {
          const body = await readJson(request);
          const session = runSessionMeta(body.session);
          const trigger = text(
            body.text as string | undefined,
            config.workspace ?? DEFAULT_WORKSTREAM,
          );
          // Dispatch-and-poll by default: long tasks must not keep an HTTP
          // request (or a Master tool call) blocked for minutes.
          const waitMs = optionalWaitMs(body.waitMs) ?? 5_000;
          const result = await service.submit(
            session === undefined ? trigger : { ...trigger, meta: { ...trigger.meta, session } },
            { awaitEffects: false },
          );
          const created = service.store.tickets.list().at(-1);
          // Wait for the launch effect + receipt: an immediate snapshot can race
          // with a failed first attempt that the outbox retries successfully.
          const settlement =
            created === undefined
              ? undefined
              : await service.waitForTicket(created.id, { timeoutMs: waitMs });
          const ticket = settlement?.ticket ?? created;
          send(response, 200, {
            stored: result.stored,
            outcome: result.decision?.outcome ?? "",
            ticket: ticket?.id ?? "",
            report: ticket?.latestReport ?? "",
            receipt: settlement?.receipt ?? false,
            settled: settlement?.settled ?? false,
            assignmentId: ticket?.assignments.at(-1) ?? "",
          });
          return;
        }
        if (pathname === "/gethen/tasks" || pathname.startsWith("/gethen/tasks/")) {
          await handleTaskRoutes(control, request, response, pathname, url.searchParams);
          return;
        }
        if (pathname === "/gethen/sessions" || pathname.startsWith("/gethen/sessions/")) {
          await handleSessionRoutes(control, request, response, pathname, url.searchParams);
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

const TASK_ROUTE = /^\/gethen\/tasks(?:\/([^/]+))?(?:\/(steer|cancel|events|answer))?$/;

/** Session directory + history routes: `/gethen/sessions[/:id/history]`. */
const SESSION_ROUTE = /^\/gethen\/sessions(?:\/([^/]+)\/history)?$/;

/**
 * `GET /gethen/tasks`, `GET /gethen/tasks/:id`,
 * `POST /gethen/tasks/:id/steer`, `POST /gethen/tasks/:id/cancel`.
 * Errors are JSON with the runtime/bridge error code; status queries map an
 * unanswered worker to 504 and an unknown assignment to 404.
 */
async function handleTaskRoutes(
  control: ControlPlane,
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
      send(response, 200, { tasks: control.states() });
      return;
    }
    if (action === undefined) {
      if (request.method !== "GET") {
        send(response, 405, { error: "method not allowed" });
        return;
      }
      const result = await control.task({
        action: "status",
        assignmentId,
        ...(timeoutMs === undefined ? {} : { timeoutMs }),
      });
      if (result.state === null || result.state === undefined) {
        send(response, 404, { error: `unknown task "${assignmentId}"` });
        return;
      }
      send(response, 200, { assignmentId, state: result.state });
      return;
    }
    if (action === "events") {
      if (request.method !== "GET") {
        send(response, 405, { error: "method not allowed" });
        return;
      }
      let query: TaskEventQuery;
      try {
        query = eventsQuery(search);
      } catch (error) {
        send(response, 400, { error: messageOf(error) });
        return;
      }
      const result = await control.task({ action: "logs", assignmentId, ...query });
      send(response, 200, {
        assignmentId,
        events: result.events ?? [],
        count: result.count ?? 0,
        dropped: result.dropped ?? 0,
      });
      return;
    }
    if (request.method !== "POST") {
      send(response, 405, { error: "method not allowed" });
      return;
    }
    if (action === "answer") {
      const body = await readJson(request);
      let resolution: EscalationResolutionInput;
      try {
        resolution = escalationResolution(body);
      } catch (error) {
        send(response, 400, { error: messageOf(error) });
        return;
      }
      const escalationId =
        typeof body.escalationId === "string" && body.escalationId.length > 0
          ? body.escalationId
          : undefined;
      const result = await control.task({
        action: "answer",
        assignmentId,
        resolution,
        ...(escalationId === undefined ? {} : { escalationId }),
      });
      send(response, 200, {
        assignmentId,
        action: "answer",
        escalation: result.escalation,
      });
      return;
    }
    if (action === "steer") {
      const body = await readJson(request);
      const message = typeof body.text === "string" ? body.text.trim() : "";
      if (message.length === 0) {
        send(response, 400, { error: "steer needs a non-empty text" });
        return;
      }
      await control.task({ action: "steer", assignmentId, text: message });
      send(response, 202, { ok: true, assignmentId, action: "steer" });
      return;
    }
    const body = await readJson(request);
    const reason =
      typeof body.reason === "string" && body.reason.length > 0 ? body.reason : undefined;
    await control.task({
      action: "cancel",
      assignmentId,
      ...(reason === undefined ? {} : { reason }),
    });
    send(response, 202, { ok: true, assignmentId, action: "cancel" });
  } catch (error) {
    const code = taskErrorCode(error);
    send(response, taskErrorStatus(code), {
      error: messageOf(error),
      ...(code === undefined ? {} : { code }),
    });
  }
}

function escalationResolution(body: Record<string, unknown>): EscalationResolutionInput {
  const rawKind = body.kind === undefined ? "answer" : body.kind;
  if (rawKind !== "answer" && rawKind !== "escalate" && rawKind !== "drop") {
    throw new Error('kind must be "answer", "escalate" or "drop"');
  }
  const text = typeof body.text === "string" ? body.text.trim() : "";
  if (text.length === 0) throw new Error("answer needs a non-empty text");
  const option =
    typeof body.option === "string" && body.option.length > 0 ? body.option : undefined;
  return {
    kind: rawKind,
    text,
    ...(option === undefined ? {} : { option }),
  };
}

const TICKET_ROUTE = /^\/gethen\/tickets(?:\/([^/]+)\/(ack|close|archive|answer))?$/;

function parseOverviewFilter(search: URLSearchParams): OverviewFilter {
  const raw = search.get("filter") ?? "needs-action";
  if (raw !== "needs-action" && raw !== "open" && raw !== "all") {
    throw new Error('filter must be "needs-action", "open" or "all"');
  }
  return raw;
}

const CLOSE_OUTCOMES: readonly TicketCloseOutcome[] = ["completed", "closed", "merged", "dropped"];

/**
 * `GET /gethen/tickets?filter=needs-action|open|all` and
 * `POST /gethen/tickets/:id/ack|close|archive`. These routes and the DSH tool
 * call the same `ControlPlane` methods; only the wire format differs.
 */
async function handleTicketRoutes(
  control: ControlPlane,
  request: IncomingMessage,
  response: ServerResponse,
  pathname: string,
  search: URLSearchParams,
): Promise<void> {
  const match = TICKET_ROUTE.exec(pathname);
  if (match === null) {
    send(response, 404, { error: "not found" });
    return;
  }
  const encodedId = match[1];
  const action = match[2];

  try {
    if (encodedId === undefined) {
      if (request.method !== "GET") {
        send(response, 405, { error: "method not allowed" });
        return;
      }
      send(response, 200, control.overview({ filter: parseOverviewFilter(search) }));
      return;
    }
    if (request.method !== "POST") {
      send(response, 405, { error: "method not allowed" });
      return;
    }
    const ticketId = decodeURIComponent(encodedId);
    if (action === "ack") {
      send(response, 200, { ok: true, action, ticket: control.acknowledge(ticketId) });
      return;
    }
    if (action === "archive") {
      send(response, 200, { ok: true, action, ticket: control.archive(ticketId) });
      return;
    }
    if (action === "answer") {
      const body = await readJson(request);
      const text = typeof body.text === "string" ? body.text.trim() : "";
      if (text.length === 0) {
        send(response, 400, { error: "answer needs a non-empty text" });
        return;
      }
      const option =
        typeof body.option === "string" && body.option.length > 0 ? body.option : undefined;
      const escalationId =
        typeof body.escalationId === "string" && body.escalationId.length > 0
          ? body.escalationId
          : undefined;
      const result = await control.answer({
        ticketId,
        text,
        ...(option === undefined ? {} : { option }),
        ...(escalationId === undefined ? {} : { escalationId }),
      });
      send(response, 200, { ok: true, action, ...result });
      return;
    }
    const body = await readJson(request);
    const outcome = optionalOutcome(body.outcome);
    const reason =
      typeof body.reason === "string" && body.reason.trim().length > 0
        ? body.reason.trim()
        : undefined;
    const ticket = await control.close({
      ticketId,
      ...(outcome === undefined ? {} : { outcome }),
      ...(reason === undefined ? {} : { reason }),
    });
    send(response, 200, { ok: true, action, ticket });
  } catch (error) {
    const message = messageOf(error);
    send(response, ticketErrorStatus(message), { error: message });
  }
}

function optionalOutcome(value: unknown): TicketCloseOutcome | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !(CLOSE_OUTCOMES as readonly string[]).includes(value)) {
    throw new Error(`outcome must be one of: ${CLOSE_OUTCOMES.join(", ")}`);
  }
  return value as TicketCloseOutcome;
}

function ticketErrorStatus(message: string): number {
  if (message.startsWith("unknown ticket")) return 404;
  if (message.startsWith("unknown escalation")) return 404;
  if (
    message.includes("has ended") ||
    message.includes("already ended") ||
    message.includes("is open") ||
    message.includes("already resolved") ||
    message.includes("no escalation waiting")
  ) {
    return 409;
  }
  return 400;
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

/** Parse the optional `session` run-body field into `trigger.meta.session` JSON. */
function runSessionMeta(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  const parsed = parseSessionSpec(value);
  if (!parsed.ok) throw new Error(`invalid session: ${parsed.reason}`);
  return JSON.stringify(parsed.value);
}

function eventsQuery(search: URLSearchParams): TaskEventQuery {
  const limit = optionalPositiveInt(search, "limit");
  const afterSeq = optionalNonNegativeInt(search, "afterSeq");
  const type = optionalNonEmpty(search, "type");
  return {
    ...(limit === undefined ? {} : { limit }),
    ...(afterSeq === undefined ? {} : { afterSeq }),
    ...(type === undefined ? {} : { type }),
  };
}

/**
 * `GET /gethen/sessions[?timeoutMs=]` and
 * `GET /gethen/sessions/:id/history[?limit=&afterSeq=&timeoutMs=]`.
 */
async function handleSessionRoutes(
  control: ControlPlane,
  request: IncomingMessage,
  response: ServerResponse,
  pathname: string,
  search: URLSearchParams,
): Promise<void> {
  const match = SESSION_ROUTE.exec(pathname);
  if (match === null) {
    send(response, 404, { error: "not found" });
    return;
  }
  if (request.method !== "GET") {
    send(response, 405, { error: "method not allowed" });
    return;
  }

  let timeoutMs: number | undefined;
  let limit: number | undefined;
  let afterSeq: number | undefined;
  try {
    timeoutMs = optionalTimeout(search);
    limit = optionalPositiveInt(search, "limit");
    afterSeq = optionalNonNegativeInt(search, "afterSeq");
  } catch (error) {
    send(response, 400, { error: messageOf(error) });
    return;
  }

  const encoded = match[1];
  try {
    if (encoded === undefined) {
      const sessions = await control.sessions(timeoutMs === undefined ? {} : { timeoutMs });
      send(response, 200, { sessions });
      return;
    }
    const sessionId = decodeURIComponent(encoded);
    const history = await control.sessionHistory(sessionId, {
      ...(limit === undefined ? {} : { limit }),
      ...(afterSeq === undefined ? {} : { afterSeq }),
      ...(timeoutMs === undefined ? {} : { timeoutMs }),
    });
    send(response, 200, history);
  } catch (error) {
    const code = taskErrorCode(error);
    send(response, taskErrorStatus(code), {
      error: messageOf(error),
      ...(code === undefined ? {} : { code }),
    });
  }
}

function optionalPositiveInt(search: URLSearchParams, name: string): number | undefined {
  const raw = search.get(name);
  if (raw === null) return undefined;
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function optionalNonNegativeInt(search: URLSearchParams, name: string): number | undefined {
  const raw = search.get(name);
  if (raw === null) return undefined;
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

function optionalNonEmpty(search: URLSearchParams, name: string): string | undefined {
  const raw = search.get(name);
  if (raw === null) return undefined;
  const value = raw.trim();
  if (value.length === 0) throw new Error(`${name} must be a non-empty string`);
  return value;
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
        "                    [--worker-id <id>] [--mailbox-db <file>] [--retention-ms <n>]\n" +
        "                    [--master-mailbox <name>] [--escalation auto|park]\n" +
        "Starts the master control plane (L1 ws transport + L2 mailbox).\n" +
        "Worker tokens come from --tokens <file> or GETHEN_WORKER_TOKENS_JSON\n" +
        '([{"workerId":"...","token":"..."}]); with them the /gethen/worker endpoint is\n' +
        "served and assignments are dispatched over the mailbox stack. Without them the\n" +
        "fake launch is used (dev convenience).\n" +
        "--worker-id is the single worker served (defaults to the only token);\n" +
        "--mailbox-db defaults to <db dir>/mailbox.sqlite and --master-mailbox to\n" +
        "mailbox:master.\n" +
        "--retention-ms (or GETHEN_RETENTION_MS) archives terminal tickets after n ms\n" +
        "--stale-close-ms (or GETHEN_STALE_CLOSE_MS) closes stale ready/failed tickets\n" +
        "--build-commit (or GETHEN_BUILD_COMMIT) is the expected Worker build\n" +
        "--workspace is the logical workstream id dispatched with assignments\n" +
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
  const retentionFlag = option("--retention-ms") ?? env.GETHEN_RETENTION_MS;
  const staleFlag = option("--stale-close-ms") ?? env.GETHEN_STALE_CLOSE_MS;
  const buildCommit = option("--build-commit") ?? env.GETHEN_BUILD_COMMIT;
  let staleCloseMs: number | undefined;
  if (staleFlag !== undefined) {
    staleCloseMs = Number(staleFlag);
    if (!Number.isSafeInteger(staleCloseMs) || staleCloseMs < 0) {
      writeError("gethen serve: --stale-close-ms must be a non-negative integer");
      return 1;
    }
  }
  let retentionMs: number | undefined;
  if (retentionFlag !== undefined) {
    retentionMs = Number(retentionFlag);
    if (!Number.isSafeInteger(retentionMs) || retentionMs < 0) {
      writeError("gethen serve: --retention-ms must be a non-negative integer");
      return 1;
    }
  }
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
      ...(retentionMs === undefined ? {} : { retentionMs }),
      ...(staleCloseMs === undefined ? {} : { staleCloseMs }),
      ...(buildCommit === undefined ? {} : { buildCommit }),
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

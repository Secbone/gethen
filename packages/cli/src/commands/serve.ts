import { mkdirSync, readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { dirname } from "node:path";
import { createWsLaunchPort, type WsLaunchBridge } from "@gethen/bridge-ws";
import {
  createFakeLaunch,
  type EscalationRecord,
  type EscalationResolutionInput,
  type LaunchPort,
  type LaunchRequest,
  MasterService,
  type Policy,
  type Trigger,
} from "@gethen/master";
import {
  createMemoryTokenStore,
  createWorkerGateway,
  type WorkerGateway,
  type WorkerTokenRecord,
} from "@gethen/transport-ws";
import { PROTOCOL_NAME } from "@gethen/worker-protocol";
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
   * Worker credentials served by the WS gateway. Absent or empty keeps the
   * fake launch and does not expose the worker endpoint.
   */
  tokens?: readonly WorkerTokenRecord[];
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
  /** Pack digest recorded for dispatches without one; defaults to `DEFAULT_PACK_DIGEST`. */
  packDigest?: string;
}

export interface ServeHandle {
  server: Server;
  service: MasterService;
  port: number;
  /** `"ws"` when the worker gateway is attached, else the fake launch. */
  launch: "ws" | "fake";
  close(): Promise<void>;
}

const POLICY: Policy = {
  version: 1,
  acceptMode: "auto",
  rules: [],
  sources: { local: { allowed: ["local-user"], bots: [] } },
};

const MAX_BODY_BYTES = 64 * 1024;

/** Pack digest recorded when the engine has none (no real pack pinning yet). */
const DEFAULT_PACK_DIGEST = "sha256:local-unpinned";

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

/**
 * The P1 engine persists assignments without a workspace or pack digest, which
 * the WS dispatch frame rejects. Fill host-level defaults in at the launch
 * boundary only; the engine's own rows stay untouched.
 */
function withLaunchDefaults(
  port: LaunchPort,
  defaults: { workspace: string; packDigest: string },
): LaunchPort {
  return {
    launch(request: LaunchRequest) {
      const assignment = {
        ...request.assignment,
        workspace:
          request.assignment.workspace.length > 0
            ? request.assignment.workspace
            : defaults.workspace,
        packDigest:
          request.assignment.packDigest.length > 0
            ? request.assignment.packDigest
            : defaults.packDigest,
      };
      return port.launch({
        ...request,
        assignment,
        packDigest: request.packDigest.length > 0 ? request.packDigest : defaults.packDigest,
      });
    },
  };
}

/**
 * Start the master control plane: sqlite + outbox, a small HTTP surface for
 * health and local dispatch, and (when worker tokens are configured) the
 * `gethen.worker/v1` WS endpoint on the same server, with the Master launching
 * assignments through the WS `LaunchPort` instead of the fake one.
 */
export async function startServe(config: ServeConfig = {}): Promise<ServeHandle> {
  const dbPath = config.dbPath ?? defaultMasterDbPath();
  mkdirSync(dirname(dbPath), { recursive: true });

  const tokens = config.tokens ?? [];
  let gateway: WorkerGateway | undefined;
  let bridge: WsLaunchBridge | undefined;
  if (tokens.length > 0) {
    const tokenStore = createMemoryTokenStore(tokens);
    gateway = createWorkerGateway({
      verifyToken: (token) => tokenStore.lookup(token),
      onHello: () => undefined,
      // `bridge` is assigned below; frames can only arrive after the server
      // starts listening, so the closure always observes the finished bridge.
      onFrame: (connection, frame) => bridge?.handleFrame(connection, frame),
      onClose: () => undefined,
      onError: () => undefined,
    });
    bridge = createWsLaunchPort({
      gateway,
      ...(config.resolveEscalation === undefined
        ? {}
        : { resolveEscalation: config.resolveEscalation }),
    });
  }
  const launch: "ws" | "fake" = bridge === undefined ? "fake" : "ws";
  const workspace = config.workspace ?? process.cwd();
  const launchPort = withLaunchDefaults(bridge?.launchPort ?? createFakeLaunch(), {
    workspace,
    packDigest: config.packDigest ?? DEFAULT_PACK_DIGEST,
  });

  const service = new MasterService({
    dbPath,
    policy: POLICY,
    launch: launchPort,
  });
  service.start();
  const startedAt = Date.now();

  const server = createServer((request, response) => {
    void (async () => {
      const { pathname } = new URL(request.url ?? "/", "http://localhost");
      try {
        if (request.method === "GET" && pathname === "/health") {
          const tickets = service.store.tickets.list();
          const workers =
            gateway === undefined
              ? 0
              : gateway.connections().filter((connection) => connection.workerId !== undefined)
                  .length;
          send(response, 200, {
            ok: true,
            uptimeMs: Date.now() - startedAt,
            tickets: tickets.length,
            open: tickets.filter((ticket) => ticket.endedAt === undefined).length,
            receipts: service.store.listReceipts().length,
            escalations: service.store.listEscalations().length,
            workers,
            launch,
            protocol: PROTOCOL_NAME,
          });
          return;
        }
        if (request.method === "POST" && pathname === "/gethen/run") {
          const body = await readJson(request);
          const trigger = text(body.text as string | undefined, workspace);
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
    if (gateway === undefined || pathname !== WORKER_PATH) {
      socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
      socket.destroy();
      return;
    }
    gateway.handleUpgrade(request, socket, head);
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
    launch,
    async close() {
      if (closed) return;
      closed = true;
      // Gateway first: it closes worker sockets, so `server.close` can finish.
      await gateway?.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      service.stop();
    },
  };
}

/**
 * Parse the worker credential JSON: `[{"workerId":"...","token":"..."}]`.
 * Structural validation only; duplicate/invalid entries are rejected by
 * `createMemoryTokenStore` when the server starts.
 */
export function parseWorkerTokens(raw: string): WorkerTokenRecord[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`invalid worker tokens JSON: ${messageOf(error)}`);
  }
  if (!Array.isArray(parsed)) {
    throw new Error("worker tokens must be a JSON array of { workerId, token }");
  }
  return parsed.map((entry: unknown, index: number): WorkerTokenRecord => {
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

/** `gethen serve [--db <file>] [--host <addr>] [--port <n>] [--tokens <file>]` */
export async function serveCommand(argv: string[]): Promise<number> {
  if (argv.includes("-h") || argv.includes("--help")) {
    process.stdout.write(
      "Usage: gethen serve [--db <file>] [--host <addr>] [--port <n>]\n" +
        "                    [--tokens <file>] [--workspace <dir>]\n" +
        "Starts the master control plane.\n" +
        "Worker tokens come from --tokens <file> or GETHEN_WORKER_TOKENS_JSON\n" +
        `([{"workerId":"...","token":"..."}]); with them the ${PROTOCOL_NAME} endpoint is\n` +
        `served at ${WORKER_PATH}. Without them the fake launch is used.\n` +
        "--workspace is the worker-side path dispatched assignments run in\n" +
        "(the engine has no per-assignment workspace yet). Worker escalations are\n" +
        "parked for a person; this CLI has no interactive resolver yet.\n",
    );
    return 0;
  }
  const option = (name: string): string | undefined => {
    const index = argv.indexOf(name);
    return index >= 0 ? argv[index + 1] : undefined;
  };

  let tokens: WorkerTokenRecord[] | undefined;
  const tokensPath = option("--tokens");
  try {
    const raw =
      tokensPath === undefined
        ? process.env.GETHEN_WORKER_TOKENS_JSON
        : readFileSync(tokensPath, "utf8");
    if (raw !== undefined) tokens = parseWorkerTokens(raw);
  } catch (error) {
    process.stderr.write(`gethen serve: ${messageOf(error)}\n`);
    return 1;
  }

  const host = option("--host") ?? "127.0.0.1";
  const dbPath = option("--db");
  const portValue = option("--port");
  const workspace = option("--workspace");
  const handle = await startServe({
    host,
    ...(dbPath === undefined ? {} : { dbPath }),
    ...(portValue === undefined ? {} : { port: Number(portValue) }),
    ...(workspace === undefined ? {} : { workspace }),
    ...(tokens === undefined ? {} : { tokens }),
  });

  process.stdout.write(`gethen master listening on http://${host}:${handle.port}\n`);
  if (handle.launch === "ws") {
    process.stdout.write(
      `worker endpoint: ws://${host}:${handle.port}${WORKER_PATH} (launch: ws)\n`,
    );
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

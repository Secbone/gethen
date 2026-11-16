import { mkdirSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { createFakeLaunch, MasterService, type Policy, type Trigger } from "@gethen/master";

export interface ServeConfig {
  /** SQLite file; defaults to `$GETHEN_STATE_DIR/master.sqlite` or `~/.gethen/master.sqlite`. */
  dbPath?: string;
  /** Bind address; defaults to loopback. */
  host?: string;
  /** Port; defaults to an ephemeral port (0). */
  port?: number;
}

export interface ServeHandle {
  server: Server;
  service: MasterService;
  port: number;
  close(): Promise<void>;
}

const POLICY: Policy = {
  version: 1,
  acceptMode: "auto",
  rules: [],
  sources: { local: { allowed: ["local-user"], bots: [] } },
};

const MAX_BODY_BYTES = 64 * 1024;

function text(value: unknown): Trigger {
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
    meta: { scope: "local" },
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
 * Start the standalone master control plane (R1): sqlite + outbox + fake launch,
 * a small HTTP surface for health and local dispatch. The worker WS endpoint is
 * added in R2 on the same server.
 */
export async function startServe(config: ServeConfig = {}): Promise<ServeHandle> {
  const stateDir = process.env.GETHEN_STATE_DIR ?? join(homedir(), ".gethen");
  const dbPath = config.dbPath ?? join(stateDir, "master.sqlite");
  mkdirSync(dirname(dbPath), { recursive: true });
  const service = new MasterService({ dbPath, policy: POLICY, launch: createFakeLaunch() });
  service.start();
  const startedAt = Date.now();

  const server = createServer((request, response) => {
    void (async () => {
      const { pathname } = new URL(request.url ?? "/", "http://localhost");
      try {
        if (request.method === "GET" && pathname === "/health") {
          const tickets = service.store.tickets.list();
          send(response, 200, {
            ok: true,
            uptimeMs: Date.now() - startedAt,
            tickets: tickets.length,
            open: tickets.filter((ticket) => ticket.endedAt === undefined).length,
            receipts: service.store.listReceipts().length,
            escalations: service.store.listEscalations().length,
            launch: "fake",
          });
          return;
        }
        if (request.method === "POST" && pathname === "/gethen/run") {
          const body = await readJson(request);
          const trigger = text(body.text as string | undefined);
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

  const port = await new Promise<number>((resolve, reject) => {
    server.once("error", reject);
    server.listen(config.port ?? 0, config.host ?? "127.0.0.1", () => {
      const address = server.address();
      resolve(typeof address === "object" && address !== null ? address.port : 0);
    });
  });

  return {
    server,
    service,
    port,
    async close() {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      service.stop();
    },
  };
}

/** `gethen serve [--db <file>] [--host <addr>] [--port <n>]` */
export async function serveCommand(argv: string[]): Promise<number> {
  if (argv.includes("-h") || argv.includes("--help")) {
    process.stdout.write(
      "Usage: gethen serve [--db <file>] [--host <addr>] [--port <n>]\n" +
        "Starts the standalone master control plane (no DSH required).\n",
    );
    return 0;
  }
  const option = (name: string): string | undefined => {
    const index = argv.indexOf(name);
    return index >= 0 ? argv[index + 1] : undefined;
  };
  const portValue = option("--port");
  const handle = await startServe({
    ...(option("--db") === undefined ? {} : { dbPath: option("--db") as string }),
    ...(option("--host") === undefined ? {} : { host: option("--host") as string }),
    ...(portValue === undefined ? {} : { port: Number(portValue) }),
  });
  process.stdout.write(`gethen master listening on http://127.0.0.1:${handle.port}\n`);
  await new Promise<void>((resolve) => {
    const shutdown = () => resolve();
    process.once("SIGINT", shutdown);
    process.once("SIGTERM", shutdown);
  });
  await handle.close();
  return 0;
}

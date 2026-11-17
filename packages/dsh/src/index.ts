/**
 * DSH mount for the self-built Gethen master.
 *
 * The plugin is a thin host around `@gethen/master-runtime`, the same assembly
 * the standalone `gethen serve` uses. One `createMasterRuntime` call wires the
 * control plane on the L1/L2 communication stack:
 *
 * - no `ws` config / no worker tokens: a deterministic fake LaunchPort (dev and
 *   tests), no HTTP route;
 * - with worker tokens: the durable L2 mailbox + ws transport server, exposed
 *   at `/gethen/worker` through the DSH `webServer` upgrade route when that
 *   service is mounted. Worker agents dial outward and exchange mailbox
 *   envelopes; the master side is `@gethen/bridge-mailbox`.
 *
 * DSH itself is not imported: the narrow `DshContext` below is the whole
 * integration contract.
 */
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import type { IncomingMessage } from "node:http";
import { join } from "node:path";
import type { Duplex } from "node:stream";
import {
  type EscalationRecord,
  type EscalationResolutionInput,
  type MasterService,
  type Policy,
  type Trigger,
  ticketStage,
} from "@gethen/master";
import {
  createMasterRuntime,
  DEFAULT_MASTER_POLICY,
  type MailboxHealth,
  type MasterRuntime,
} from "@gethen/master-runtime";

/** The narrow DSH surface this plugin uses; no DSH package is imported. */
export interface DshToolDefinition {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
  output: {
    schema: Record<string, unknown>;
    render(args: unknown, value: unknown): Array<{ type: "text"; text: string }>;
  };
  execute(args: unknown, exec: unknown): Promise<unknown>;
}

/** One exact-path HTTP upgrade route (subset of DSH's `webServer` contract). */
export interface DshUpgradeRoute {
  path: string;
  handler: (request: IncomingMessage, socket: Duplex, head: Buffer) => void;
}

/** The DSH web-server service surface used to host `/gethen/worker`. */
export interface DshWebServer {
  registerUpgrade(route: DshUpgradeRoute): () => void;
}

export interface DshContext {
  provide(name: string, value: unknown): () => void;
  tools: { register(definition: DshToolDefinition): () => void };
  effect?(setup: () => (() => void) | void): () => void;
  /** Present when the DSH webServer service is available in this profile. */
  get?(name: string): unknown;
  /** Register a sub-plugin that runs once its injected services are available. */
  inject?(deps: string[], callback: (scope: DshContext) => (() => void) | void): unknown;
  logger?: { error?(message: string): void };
}

/** Worker credentials in the mailbox transport token format. */
export interface WorkerToken {
  workerId: string;
  token: string;
}

/**
 * Mailbox control plane: with this config the plugin hosts the worker
 * transport and dispatches through the durable mailbox instead of the fake
 * launch. Without tokens the runtime keeps the fake launch and exposes no
 * route, which is the dev/test shape.
 */
export interface GethenWsConfig {
  /** JSON file with `[{"workerId":"...","token":"..."}]`. Wins over tokensJson. */
  tokensFile?: string;
  /** Inline token JSON (same shape); used when tokensFile is absent. */
  tokensJson?: string;
  /** Worker-side workspace for dispatches; defaults to `process.cwd()`. */
  workspace?: string;
  /** Pack digest recorded for dispatches; defaults to the runtime's constant. */
  packDigest?: string;
  /** Worker served by this runtime; defaults to the only configured token. */
  workerId?: string;
  /** mailbox sqlite file; defaults to `<dirname(dbPath)>/mailbox.sqlite`. */
  mailboxDbPath?: string;
  /** Master mailbox name, `<prefix>master`; defaults to `mailbox:master`. */
  masterMailbox?: string;
}

export interface GethenConfig {
  /** State directory; defaults to `$GETHEN_STATE_DIR` or `<cwd>/.gethen/master`. */
  dir?: string;
  /** Mailbox control-plane config; absent means the fake launch (no route). */
  ws?: GethenWsConfig;
  /**
   * Temporary deterministic resolver for verification: answers every Worker
   * escalation with this text. Without it an escalation is parked for a person.
   */
  escalation?: { answer?: string };
}

/** The worker transport endpoint registered on the DSH web server. */
export const WORKER_PATH = "/gethen/worker";

export const name = "gethen";
export const inject = ["tools"];

/**
 * Single-operator local policy: the local source admits `local-user` and the
 * master accepts automatically (shared with the CLI runtime).
 */
export const DEFAULT_POLICY: Policy = DEFAULT_MASTER_POLICY;

const DEFAULT_TOKENS_SOURCE = "worker tokens";

function text(value: unknown): Array<{ type: "text"; text: string }> {
  return [{ type: "text", text: JSON.stringify(value) }];
}

function objectSchema(
  properties: Record<string, unknown>,
  required: string[],
): Record<string, unknown> {
  return { type: "object", properties, required, additionalProperties: false };
}

/**
 * Parse the worker credential JSON: `[{"workerId":"...","token":"..."}]`.
 * Structural validation only; duplicate or empty entries are rejected later by
 * the transport token store.
 */
export function parseWorkerTokens(
  raw: string,
  source: string = DEFAULT_TOKENS_SOURCE,
): WorkerToken[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`invalid ${source} JSON: ${messageOf(error)}`);
  }
  if (!Array.isArray(parsed)) {
    throw new Error(`${source} must be a JSON array of { workerId, token }`);
  }
  return parsed.map((entry: unknown, index: number): WorkerToken => {
    if (!isRecord(entry)) throw new Error(`${source} entry ${index} must be an object`);
    const workerId = entry.workerId;
    const token = entry.token;
    if (typeof workerId !== "string" || workerId.length === 0) {
      throw new Error(`${source} entry ${index} needs a non-empty string workerId`);
    }
    if (typeof token !== "string" || token.length === 0) {
      throw new Error(`${source} entry ${index} needs a non-empty string token`);
    }
    return { workerId, token };
  });
}

/** Read the configured tokens; an empty list keeps the fake launch. */
export function readWorkerTokens(config: GethenWsConfig): WorkerToken[] {
  if (config.tokensFile !== undefined) {
    return parseWorkerTokens(
      readFileSync(config.tokensFile, "utf8"),
      `worker tokens file ${config.tokensFile}`,
    );
  }
  if (config.tokensJson !== undefined) {
    return parseWorkerTokens(config.tokensJson, "ws.tokensJson");
  }
  return [];
}

/**
 * Persist escalation lifecycle into the master store: the mailbox bridge owns
 * the protocol, the plugin owns the audit trail.
 */
function escalationHooks(serviceOf: () => MasterService | undefined): {
  onEscalation(record: EscalationRecord): void;
  onResolution(record: EscalationRecord): void;
} {
  return {
    onEscalation(record) {
      const service = serviceOf();
      if (service === undefined) return;
      const saved = service.engine.escalate(record.ticket, {
        id: record.id,
        assignment: record.assignment,
        session: record.session,
        kind: record.kind,
        question: record.question,
        options: record.options,
        ...(record.defaultOption === undefined ? {} : { defaultOption: record.defaultOption }),
      });
      if (saved !== undefined) service.store.saveEscalation(saved);
    },
    onResolution(record) {
      const service = serviceOf();
      if (service === undefined) return;
      if (record.resolution === undefined) {
        service.store.saveEscalation(record);
        return;
      }
      const saved = service.engine.resolve(record.ticket, record.resolution);
      service.store.saveEscalation(saved ?? record);
    },
  };
}

interface ToolDeps {
  service: MasterService;
  /** What the launch port currently is: mailbox transport or the fake. */
  launch(): "ws" | "fake";
  /** Connected workers reported by the runtime (0 in fake mode). */
  workers(): number;
  /** Mailbox queue counters; `undefined` in fake mode. */
  mailbox?(): MailboxHealth | undefined;
}

/** Register the read/write tool surface against one master service. */
function registerTools(ctx: DshContext, deps: ToolDeps): void {
  const { service } = deps;

  ctx.tools.register({
    name: "gethen_status",
    description:
      "Return master counts: tickets, open tickets, receipts, running Assignments, " +
      "workers and mailbox counters.",
    parameters: {},
    output: {
      schema: objectSchema(
        {
          tickets: { type: "number" },
          open: { type: "number" },
          receipts: { type: "number" },
          running: { type: "number" },
          escalations: { type: "number" },
          launch: { type: "string", enum: ["fake", "ws"] },
          workers: { type: "number" },
          mailbox: {
            type: "object",
            properties: {
              pending: { type: "number" },
              leased: { type: "number" },
              dead: { type: "number" },
            },
            required: ["pending", "leased", "dead"],
          },
        },
        ["tickets", "open", "receipts", "running", "escalations", "launch", "workers"],
      ),
      render: (_args, value) => text(value),
    },
    async execute() {
      const tickets = service.store.tickets.list();
      const mailbox = deps.mailbox?.();
      return {
        tickets: tickets.length,
        open: tickets.filter((ticket) => ticket.endedAt === undefined).length,
        receipts: service.store.listReceipts().length,
        running: service.store.assignments
          .list()
          .filter((assignment) => assignment.state === "running").length,
        escalations: service.store.listEscalations().length,
        launch: deps.launch(),
        workers: deps.workers(),
        ...(mailbox === undefined ? {} : { mailbox }),
      };
    },
  });

  ctx.tools.register({
    name: "gethen_run",
    description: "Accept and run one local request end-to-end through the master.",
    parameters: {
      text: { type: "string", required: true, description: "Request text" },
    },
    output: {
      schema: objectSchema(
        {
          stored: { type: "string" },
          outcome: { type: "string" },
          ticket: { type: "string" },
          report: { type: "string" },
          receipt: { type: "number" },
        },
        ["stored", "outcome", "ticket", "report", "receipt"],
      ),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      const args = raw as { text?: unknown };
      if (typeof args.text !== "string" || args.text.length === 0) {
        throw new Error("gethen_run requires a non-empty text");
      }
      const before = new Set(service.store.tickets.list().map((ticket) => ticket.id));
      const trigger: Trigger = {
        id: `trigger-${randomUUID()}`,
        source: "local",
        thread: "local",
        actor: "local-user",
        bot: false,
        addressed: "mention",
        text: args.text,
        at: new Date().toISOString(),
        meta: { scope: "local" },
      };
      const result = await service.submit(trigger);
      const ticket = service.store.tickets.list().find((item) => !before.has(item.id));
      const received =
        ticket !== undefined &&
        service.store.listReceipts().some((receipt) => receipt.ticket === ticket.id);
      return {
        stored: result.stored,
        outcome: result.decision?.outcome ?? "",
        ticket: ticket?.id ?? "",
        report: ticket?.latestReport ?? "",
        receipt: received ? 1 : 0,
      };
    },
  });

  ctx.tools.register({
    name: "gethen_overview",
    description: "Return every ticket with its computed stage, report and Assignment count.",
    parameters: {},
    output: {
      schema: objectSchema({ tickets: { type: "array", items: { type: "object" } } }, ["tickets"]),
      render: (_args, value) => text(value),
    },
    async execute() {
      const assignments = service.store.assignments.list();
      const counts = new Map<string, number>();
      const live = new Set<string>();
      for (const assignment of assignments) {
        counts.set(assignment.ticket, (counts.get(assignment.ticket) ?? 0) + 1);
        if (assignment.state === "pending" || assignment.state === "running") {
          live.add(assignment.ticket);
        }
      }
      return {
        tickets: service.store.tickets.list().map((ticket) => ({
          id: ticket.id,
          stage: ticketStage(ticket, {
            liveAssignment: live.has(ticket.id),
            personSinceReport: false,
            deliveryFailed: ticket.delivery !== undefined,
          }),
          outcome: ticket.outcome ?? "",
          latestReport: ticket.latestReport ?? "",
          assignments: counts.get(ticket.id) ?? 0,
        })),
      };
    },
  });

  ctx.tools.register({
    name: "gethen_escalations",
    description: "List Worker escalations with their question, status and resolution.",
    parameters: {},
    output: {
      schema: objectSchema({ escalations: { type: "array", items: { type: "object" } } }, [
        "escalations",
      ]),
      render: (_args, value) => text(value),
    },
    async execute() {
      return { escalations: service.store.listEscalations() };
    },
  });
}

/**
 * Mount the master service and its tools into DSH.
 *
 * Always assembled through `createMasterRuntime`: no `ws` config (or no worker
 * tokens) keeps the fake launch and registers no route; otherwise the mailbox
 * stack is attached and `/gethen/worker` is exposed when the DSH webServer
 * service is available.
 */
export function apply(ctx: DshContext, config: GethenConfig = {}): void {
  const dir =
    config.dir ?? process.env.GETHEN_STATE_DIR ?? join(process.cwd(), ".gethen", "master");
  const ws = config.ws;
  const tokens = ws === undefined ? [] : readWorkerTokens(ws);
  const answer = config.escalation?.answer;
  const resolveEscalation =
    answer === undefined
      ? undefined
      : async (_record: EscalationRecord): Promise<EscalationResolutionInput> => ({
          kind: "answer",
          text: answer,
        });

  let runtime: MasterRuntime | undefined;
  runtime = createMasterRuntime({
    dbPath: join(dir, "master.sqlite"),
    policy: DEFAULT_POLICY,
    workspace: ws?.workspace ?? process.cwd(),
    ...(ws?.packDigest === undefined ? {} : { packDigest: ws.packDigest }),
    ...(ws?.workerId === undefined ? {} : { workerId: ws.workerId }),
    ...(ws?.mailboxDbPath === undefined ? {} : { mailboxDbPath: ws.mailboxDbPath }),
    ...(ws?.masterMailbox === undefined ? {} : { masterMailbox: ws.masterMailbox }),
    ...(tokens.length === 0 ? {} : { tokens }),
    ...(resolveEscalation === undefined ? {} : { resolveEscalation }),
    ...escalationHooks(() => runtime?.service),
  });
  const mounted = runtime;

  ctx.provide("gethen", mounted.service);

  const handleUpgrade = mounted.handleUpgrade;
  const mountRoute = (scope: DshContext): (() => void) | void => {
    const webServer = scope.get?.("webServer") as DshWebServer | undefined;
    if (webServer === undefined || handleUpgrade === undefined) return;
    const dispose = webServer.registerUpgrade({
      path: WORKER_PATH,
      handler: (request, socket, head) => handleUpgrade(request, socket, head),
    });
    return () => dispose();
  };
  // The webServer service may be created after this plugin (entry order), so
  // register through an injected sub-plugin instead of a one-shot ctx.get.
  if (ctx.inject !== undefined) {
    ctx.inject(["webServer"], mountRoute);
  } else if (ctx.get?.("webServer") !== undefined) {
    mountRoute(ctx);
  } else if (handleUpgrade !== undefined) {
    ctx.logger?.error?.(
      "gethen: mailbox config is set but the DSH webServer service is not mounted; " +
        `${WORKER_PATH} is not exposed and no worker can connect`,
    );
  }

  ctx.effect?.(() => () => {
    void mounted.close().catch((error: unknown) => {
      ctx.logger?.error?.(`gethen: closing the master runtime failed: ${messageOf(error)}`);
    });
  });
  registerTools(ctx, {
    service: mounted.service,
    launch: () => mounted.launch,
    workers: () => mounted.health().workers,
    mailbox: () => (mounted.launch === "ws" ? mounted.health().mailbox : undefined),
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export { applyWorker } from "./worker-entry";

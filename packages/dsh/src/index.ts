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
  parseSessionSpec,
  type SessionSpec,
  type Trigger,
} from "@gethen/master";
import {
  type BridgeSettlementMetrics,
  type ControlPlane,
  createEscalationHooks,
  createMasterRuntime,
  DEFAULT_MASTER_POLICY,
  DEFAULT_WORKSTREAM,
  type MailboxHealth,
  type MasterRuntime,
  type MasterRuntimeTasks,
  type RoutingOptions,
  type RuntimeWarning,
  type SchedulingMetrics,
  taskSummary,
  type WarningThresholds,
  type WorkerSnapshot,
} from "@gethen/master-runtime";
import { type AgentLike, type AgentsServiceLike, GethenNotifier } from "./notifier";
import { registerTaskTool } from "./task-tool";
import { registerTicketTool } from "./ticket-tool";

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
  /** Live DSH agents (present when the `agents` service is mounted). */
  agents?: AgentsServiceLike;
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
  /**
   * Logical workstream id dispatched with assignments; defaults to
   * `DEFAULT_WORKSTREAM`. Never a Master filesystem path: the Worker owns the
   * workspace and delivers `feat/<id>`.
   */
  workspace?: string;
  /** Pack digest recorded for dispatches; defaults to the runtime's constant. */
  packDigest?: string;
  /** Restrict dispatch to this Worker; defaults to every configured token. */
  workerId?: string;
  /** Workers accepted from a dynamic token verifier (tokens are preferred). */
  workers?: readonly string[];
  /** Router pin, extra hard requirements and queue bounds. */
  routing?: RoutingOptions;
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
  /**
   * How long `gethen_run` waits for settlement before returning `settled:false`.
   * Defaults to 5s (dispatch-and-poll); set 600000 for blocking CLI-style runs.
   */
  runWaitMs?: number;
  /** Wake the Master DSH agent when Gethen has an event; default `steer`. */
  wake?: "steer" | "followup" | "off";
  /** Master session to wake; defaults to the session that calls a gethen tool. */
  notifySession?: string;
  /** Coalesce notification bursts into one wake message (default 50ms). */
  wakeBatchMs?: number;
  /** Archive terminal Tickets after this window (all outcomes). */
  retentionMs?: number;
  /** Close stale ready/failed Tickets after this window (all outcomes). */
  staleCloseMs?: number;
  /** Thresholds for health warnings; each defaults to 1. */
  warnings?: WarningThresholds;
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

interface ToolDeps {
  service: MasterService;
  /** Shared control surface: status/overview/ticket actions. */
  control: ControlPlane;
  /** What the launch port currently is: mailbox transport or the fake. */
  launch(): "ws" | "fake";
  /** Connected workers reported by the runtime (0 in fake mode). */
  workers(): number;
  /** Per-Worker registration, health and load snapshots. */
  workerDetails(): WorkerSnapshot[];
  /** Mailbox queue counters; `undefined` in fake mode. */
  mailbox?(): MailboxHealth | undefined;
  /** Runtime task state/control; fake mode reports `RUNTIME_COMMS_UNAVAILABLE`. */
  tasks?: MasterRuntimeTasks;
  /** Upper bound for `gethen_run` waiting on settlement (default 5s). */
  runWaitMs?: number;
  /** Durable wake notifications, shared with the notifier / gethen_wait. */
  notifications?: MasterRuntime["notifications"];
  /** Live scheduling counters (queued/running/reused/queue-timeout). */
  scheduling?(): SchedulingMetrics;
  /** Direct result envelopes vs status-recovery settlements. */
  settlements?(): BridgeSettlementMetrics;
  /** Actionable health warnings from the runtime. */
  warnings?(): RuntimeWarning[];
  /**
   * Logical workstream dispatched with `gethen_run` so the router can match it.
   * Never a Master filesystem path; the Worker owns the workspace.
   */
  workspace?: string;
}

/**
 * Tool hosts may JSON-encode numeric arguments as strings; accept both forms
 * rather than failing a valid 0/1000 wait. Everything else is rejected.
 */
export function parseWaitTimeoutMs(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  const parsed = typeof value === "string" && value.trim().length > 0 ? Number(value) : value;
  if (typeof parsed !== "number" || !Number.isFinite(parsed) || parsed < 0 || parsed > 3_600_000) {
    throw new Error('gethen_wait "timeoutMs" must be a number between 0 and 3600000');
  }
  return parsed;
}

/** Register the read/write tool surface against one master service. */
function registerTools(ctx: DshContext, deps: ToolDeps): void {
  const { service } = deps;

  ctx.tools.register({
    name: "gethen_wait",
    description:
      "Wait for the next Gethen event (worker finished or escalation parked) instead of " +
      "polling. Returns immediately when one is queued; returns timedOut=true after the " +
      "timeout. Auto-wake usually delivers these events without calling this tool; it is " +
      "the fallback for hosts without agent wake-up.",
    parameters: {
      timeoutMs: {
        type: "number",
        required: false,
        description: "Wait bound in milliseconds, 0-3600000; default 30000.",
      },
    },
    output: {
      schema: objectSchema(
        {
          event: { type: "object" },
          timedOut: { type: "boolean" },
        },
        ["event", "timedOut"],
      ),
      render: (_args, value) => text(value),
    },
    async execute(raw, exec) {
      const timeoutMs = parseWaitTimeoutMs((raw as { timeoutMs?: unknown }).timeoutMs);
      const hub = deps.notifications;
      if (hub === undefined) throw new Error("gethen_wait requires the master notifications hub");
      const signal = (exec as { signal?: AbortSignal } | undefined)?.signal;
      const row = await hub.waitForNext({
        timeoutMs: timeoutMs ?? 30_000,
        ...(signal === undefined ? {} : { signal }),
      });
      if (row === undefined) {
        return { event: { kind: "none" }, timedOut: true };
      }
      hub.markDelivered(row.id);
      return { event: row, timedOut: false };
    },
  });

  ctx.tools.register({
    name: "gethen_status",
    description:
      "Return master counts: tickets, open and needs-action tickets, receipts, running " +
      "Assignments, workers and mailbox counters.",
    parameters: {},
    output: {
      schema: objectSchema(
        {
          tickets: { type: "number" },
          open: { type: "number" },
          needsAction: { type: "number" },
          archived: { type: "number" },
          receipts: { type: "number" },
          running: { type: "number" },
          escalations: { type: "number" },
          launch: { type: "string", enum: ["fake", "ws"] },
          workers: { type: "number" },
          workerDetails: { type: "array", items: { type: "object" } },
          warnings: {
            type: "array",
            items: {
              type: "object",
              properties: {
                code: { type: "string" },
                message: { type: "string" },
                count: { type: "number" },
              },
              required: ["code", "message", "count"],
            },
          },
          settlements: {
            type: "object",
            properties: {
              directCompleted: { type: "number" },
              recoveredCompleted: { type: "number" },
              recoveredWithoutReport: { type: "number" },
              lateCompleted: { type: "number" },
            },
            required: [
              "directCompleted",
              "recoveredCompleted",
              "recoveredWithoutReport",
              "lateCompleted",
            ],
          },
          scheduling: {
            type: "object",
            properties: {
              queued: { type: "number" },
              running: { type: "number" },
              dispatched: { type: "number" },
              reused: { type: "number" },
              queueEntries: { type: "number" },
              queueTimeouts: { type: "number" },
              maxQueueDepth: { type: "number" },
              parked: { type: "number" },
            },
            required: [
              "queued",
              "running",
              "dispatched",
              "reused",
              "queueEntries",
              "queueTimeouts",
              "maxQueueDepth",
              "parked",
            ],
          },
          mailbox: {
            type: "object",
            properties: {
              pending: { type: "number" },
              leased: { type: "number" },
              dead: { type: "number" },
            },
            required: ["pending", "leased", "dead"],
          },
          tasks: {
            type: "object",
            properties: {
              count: { type: "number" },
              recent: { type: "array", items: { type: "object" } },
            },
            required: ["count", "recent"],
          },
        },
        [
          "tickets",
          "open",
          "needsAction",
          "archived",
          "receipts",
          "running",
          "escalations",
          "launch",
          "workers",
        ],
      ),
      render: (_args, value) => text(value),
    },
    async execute() {
      const status = deps.control.status();
      const mailbox = deps.mailbox?.();
      const tasks = taskSummary(deps.tasks);
      return {
        tickets: status.counts.tickets,
        open: status.counts.open,
        needsAction: status.counts.needsAction,
        archived: status.counts.archived,
        receipts: status.counts.receipts,
        running: service.store.assignments
          .list()
          .filter((assignment) => assignment.state === "running").length,
        escalations: status.counts.escalations,
        launch: deps.launch(),
        workers: deps.workers(),
        warnings: deps.warnings?.() ?? [],
        settlements: deps.settlements?.() ?? {
          directCompleted: 0,
          recoveredCompleted: 0,
          recoveredWithoutReport: 0,
          lateCompleted: 0,
        },
        scheduling: deps.scheduling?.() ?? {
          queued: 0,
          running: 0,
          dispatched: 0,
          reused: 0,
          queueEntries: 0,
          queueTimeouts: 0,
          maxQueueDepth: 0,
          parked: 0,
        },
        ...(deps.workerDetails().length === 0 ? {} : { workerDetails: deps.workerDetails() }),
        ...(mailbox === undefined ? {} : { mailbox }),
        ...(tasks === undefined ? {} : { tasks }),
      };
    },
  });

  ctx.tools.register({
    name: "gethen_run",
    description: "Accept and run one local request end-to-end through the master.",
    parameters: {
      text: { type: "string", required: true, description: "Request text" },
      session: {
        type: "object",
        required: false,
        description:
          'Session binding: {"mode":"new"} (default) or {"mode":"reuse","sessionId"?|"key"?} ' +
          "with exactly one of sessionId/key.",
        properties: {
          mode: { type: "string", enum: ["new", "reuse"] },
          sessionId: { type: "string" },
          key: { type: "string" },
        },
      },
    },
    output: {
      schema: objectSchema(
        {
          stored: { type: "string" },
          outcome: { type: "string" },
          ticket: { type: "string" },
          report: { type: "string" },
          receipt: { type: "number" },
          settled: { type: "boolean" },
          assignmentId: { type: "string" },
          sessionId: { type: "string" },
        },
        ["stored", "outcome", "ticket", "report", "receipt", "settled"],
      ),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      const args = raw as { text?: unknown; session?: unknown };
      if (typeof args.text !== "string" || args.text.length === 0) {
        throw new Error("gethen_run requires a non-empty text");
      }
      const parsedSession = parseSessionSpec(args.session);
      if (!parsedSession.ok) {
        throw new Error(`gethen_run "session" is invalid: ${parsedSession.reason}`);
      }
      const session: SessionSpec = parsedSession.value;
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
        meta: {
          scope: "local",
          session: JSON.stringify(session),
          ...(deps.workspace === undefined ? {} : { workspace: deps.workspace }),
        },
      };
      const result = await service.submit(trigger, { awaitEffects: false });
      const created = service.store.tickets.list().find((item) => !before.has(item.id));
      // A failed first launch attempt is retried by the outbox; snapshotting
      // tickets/receipts immediately can observe `failed`/0 while the retry
      // later completes. Wait for the launch effect and receipt instead.
      const settlement =
        created === undefined
          ? undefined
          : await service.waitForTicket(created.id, {
              // Return quickly for long tasks; the caller polls gethen_status /
              // gethen_task instead of blocking the agent turn for minutes.
              timeoutMs: deps.runWaitMs ?? 5_000,
            });
      const ticket = settlement?.ticket ?? created;
      const assignmentId = ticket?.assignments.at(-1);
      const assignment =
        assignmentId === undefined ? undefined : service.store.assignments.get(assignmentId);
      const sessionId = assignment?.session?.sessionId;
      return {
        stored: result.stored,
        outcome: result.decision?.outcome ?? "",
        ticket: ticket?.id ?? "",
        report: ticket?.latestReport ?? "",
        receipt: settlement?.receipt === true ? 1 : 0,
        settled: settlement?.settled ?? false,
        ...(assignmentId === undefined ? {} : { assignmentId }),
        ...(sessionId === undefined ? {} : { sessionId }),
      };
    },
  });

  ctx.tools.register({
    name: "gethen_overview",
    description:
      "Return tickets that need a person by default; pass filter=open or filter=all " +
      "(archived included) for an explicit operator view. Each ticket carries its stage, " +
      "lifecycle, outcome and Assignment count.",
    parameters: {
      filter: {
        type: "string",
        required: false,
        enum: ["needs-action", "open", "all"],
        description: "needs-action (default) | open | all",
      },
    },
    output: {
      schema: objectSchema(
        {
          filter: { type: "string", enum: ["needs-action", "open", "all"] },
          counts: {
            type: "object",
            properties: {
              tickets: { type: "number" },
              open: { type: "number" },
              needsAction: { type: "number" },
              archived: { type: "number" },
              receipts: { type: "number" },
              escalations: { type: "number" },
            },
            required: ["tickets", "open", "needsAction", "archived"],
          },
          tickets: { type: "array", items: { type: "object" } },
        },
        ["filter", "counts", "tickets"],
      ),
      render: (_args, value) => text(value),
    },
    async execute(raw) {
      const args = raw as { filter?: unknown };
      const filter = args.filter ?? "needs-action";
      if (filter !== "needs-action" && filter !== "open" && filter !== "all") {
        throw new Error('gethen_overview "filter" must be needs-action, open or all');
      }
      const overview = deps.control.overview({ filter });
      return {
        filter: overview.filter,
        counts: overview.counts,
        tickets: overview.tickets,
      };
    },
  });

  ctx.tools.register({
    name: "gethen_escalations",
    description:
      "List Worker escalations with their question, status and resolution. Answer a waiting " +
      "one with gethen_ticket action=answer (ticketId, text, optional option/escalationId).",
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

  registerTaskTool(ctx, deps.control);
  registerTicketTool(ctx, deps.control);
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
    workspace: ws?.workspace ?? DEFAULT_WORKSTREAM,
    ...(ws?.packDigest === undefined ? {} : { packDigest: ws.packDigest }),
    ...(config.retentionMs === undefined ? {} : { retentionMs: config.retentionMs }),
    ...(config.staleCloseMs === undefined ? {} : { staleCloseMs: config.staleCloseMs }),
    ...(config.warnings === undefined ? {} : { warnings: config.warnings }),
    ...(ws?.workerId === undefined ? {} : { workerId: ws.workerId }),
    ...(ws?.workers === undefined ? {} : { workers: ws.workers }),
    ...(ws?.routing === undefined ? {} : { routing: ws.routing }),
    ...(ws?.mailboxDbPath === undefined ? {} : { mailboxDbPath: ws.mailboxDbPath }),
    ...(ws?.masterMailbox === undefined ? {} : { masterMailbox: ws.masterMailbox }),
    ...(tokens.length === 0 ? {} : { tokens }),
    ...(resolveEscalation === undefined ? {} : { resolveEscalation }),
    ...createEscalationHooks(() => runtime?.service),
  });
  const mounted = runtime;
  const notifier = new GethenNotifier({
    notifications: mounted.notifications,
    mode: config.wake ?? "steer",
    ...(config.notifySession === undefined ? {} : { sessionId: config.notifySession }),
    ...(config.wakeBatchMs === undefined ? {} : { batchWindowMs: config.wakeBatchMs }),
    onError: (error: unknown) => {
      ctx.logger?.error?.(`gethen: wake delivery failed: ${messageOf(error)}`);
    },
  });
  const stopNotifier = notifier.start();
  if (ctx.inject !== undefined) {
    ctx.inject(["agents"], (scope) => {
      const agents = scope.get?.("agents") as AgentsServiceLike | undefined;
      if (agents !== undefined) notifier.setAgents(agents);
    });
  } else {
    const agents = ctx.get?.("agents") as AgentsServiceLike | undefined;
    if (agents !== undefined) notifier.setAgents(agents);
  }

  ctx.provide("gethen", mounted.service);
  // Hosts/tests can inspect notifications or publish synthetic events.
  ctx.provide("gethen.runtime", mounted);

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
  // Capture the live Master agent from any gethen tool call so later worker
  // events can steer it. This is the same `exec.agent` surface Agent Teams uses.
  const toolContext: DshContext = {
    ...ctx,
    tools: {
      register(definition) {
        return ctx.tools.register({
          ...definition,
          async execute(args, exec) {
            const agent = (exec as { agent?: AgentLike } | undefined)?.agent;
            if (agent !== undefined) notifier.bind(agent);
            return definition.execute(args, exec);
          },
        });
      },
    },
  };
  registerTools(toolContext, {
    service: mounted.service,
    control: mounted.control,
    launch: () => mounted.launch,
    workers: () => mounted.health().workers,
    workerDetails: () => mounted.workers(),
    mailbox: () => (mounted.launch === "ws" ? mounted.health().mailbox : undefined),
    tasks: mounted.tasks,
    notifications: mounted.notifications,
    workspace: config.ws?.workspace ?? DEFAULT_WORKSTREAM,
    scheduling: () => mounted.scheduling(),
    settlements: () => mounted.settlements(),
    warnings: () => mounted.health().warnings,
    ...(config.runWaitMs === undefined ? {} : { runWaitMs: config.runWaitMs }),
  });
  ctx.effect?.(() => () => {
    stopNotifier();
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export { applyWorker } from "./worker-entry";

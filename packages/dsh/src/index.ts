/**
 * DSH mount for the self-built Gethen master.
 *
 * Two assembly shapes share one tool surface:
 *
 * - default (no `ws` config): the plugin builds a `MasterService` itself and
 *   launches Assignments through the DSH Remote client (`config.remote`) or the
 *   deterministic fake worker — the P1/P3 behaviour, unchanged;
 * - `config.ws`: the plugin uses `@gethen/master-runtime`, the same assembly the
 *   standalone `gethen serve` uses: `MasterService` + `gethen.worker.v1`
 *   `WorkerGateway` + `WsLaunchPort`. When the DSH `webServer` service is
 *   present the plugin exposes the endpoint at `/gethen/worker` through
 *   `webServer.registerUpgrade`; the gateway authenticates bearer tokens and
 *   the bridge dispatches to connected outward-only Worker agents.
 *
 * DSH itself is not imported: the narrow `DshContext` below is the whole
 * integration contract.
 */
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import type { IncomingMessage } from "node:http";
import { join } from "node:path";
import type { Duplex } from "node:stream";
import type { RemoteClientLike } from "@gethen/bridge-remote";
import {
  createFakeLaunch,
  type EscalationRecord,
  type EscalationResolutionInput,
  type LaunchPort,
  MasterService,
  type Policy,
  type Trigger,
  ticketStage,
} from "@gethen/master";
import {
  createMasterRuntime,
  DEFAULT_MASTER_POLICY,
  type MasterRuntime,
} from "@gethen/master-runtime";
import { createScriptedRemoteClient } from "./dev/fake-remote";
import { createRemoteLaunchPort, type RemoteLaunchConfig } from "./remote-launch";

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
  /** Present when the DSH Remote client plugin is mounted in this profile. */
  get?(name: string): unknown;
  /** Register a sub-plugin that runs once its injected services are available. */
  inject?(deps: string[], callback: (scope: DshContext) => (() => void) | void): unknown;
  logger?: { error?(message: string): void };
}

/** Worker credentials in the `gethen.worker.v1` gateway format. */
export interface WorkerToken {
  workerId: string;
  token: string;
}

/**
 * Experimental WS control plane: with this config the plugin hosts the worker
 * endpoint instead of launching through the DSH Remote client.
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
}

export interface GethenConfig {
  /** State directory; defaults to `$GETHEN_STATE_DIR` or `<cwd>/.gethen/master`. */
  dir?: string;
  /**
   * Dispatch through the DSH Remote client to a Worker host. Requires the
   * `dshRemoteClient` service; without it the fake launch is used.
   */
  remote?: RemoteLaunchConfig;
  /**
   * Temporary deterministic resolver for verification: answers every Worker
   * escalation with this text. Without it an escalation is parked for a person.
   */
  escalation?: { answer?: string };
  /** Dev only: use the bundled scripted Remote client instead of a real one. */
  devRemote?: boolean;
  /** WS control plane: host `/gethen/worker` and dispatch to `worker-agent`s. */
  ws?: GethenWsConfig;
}

/** The WS worker endpoint registered on the DSH web server. */
export const WORKER_PATH = "/gethen/worker";

export const name = "gethen";
export const inject = ["tools"];

/**
 * Single-operator P1 policy: the local source admits `local-user` and the
 * master accepts automatically. The fake LaunchPort keeps P1 self-contained.
 */
export const DEFAULT_POLICY: Policy = DEFAULT_MASTER_POLICY;

const DEFAULT_TOKENS_SOURCE = "worker tokens";

function createMasterService(dir: string, launch: LaunchPort): MasterService {
  return new MasterService({
    dbPath: join(dir, "master.sqlite"),
    policy: DEFAULT_POLICY,
    launch,
  });
}

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
 * the gateway's token store.
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
 * Persist escalation lifecycle into the master store. The same hooks serve the
 * Remote launch (P3) and the WS launch (R5): the adapters own the protocol, the
 * plugin owns the audit trail.
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
  /** What the launch port currently is: remote (P3), WS (R5) or the fake. */
  launch(): "fake" | "remote" | "ws";
  /** Connected workers (from the runtime for the WS path, 0 otherwise). */
  workers(): number;
}

/** Register the read/write tool surface against one master service. */
function registerTools(ctx: DshContext, deps: ToolDeps): void {
  const { service } = deps;

  ctx.tools.register({
    name: "gethen_status",
    description:
      "Return master counts: tickets, open tickets, receipts, running Assignments and workers.",
    parameters: {},
    output: {
      schema: objectSchema(
        {
          tickets: { type: "number" },
          open: { type: "number" },
          receipts: { type: "number" },
          running: { type: "number" },
          escalations: { type: "number" },
          launch: { type: "string", enum: ["fake", "remote", "ws"] },
          workers: { type: "number" },
        },
        ["tickets", "open", "receipts", "running", "escalations", "launch", "workers"],
      ),
      render: (_args, value) => text(value),
    },
    async execute() {
      const tickets = service.store.tickets.list();
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

/** Mount the WS control plane: runtime service plus the `/gethen/worker` route. */
function applyWs(
  ctx: DshContext,
  config: GethenConfig & { ws: GethenWsConfig },
  dir: string,
  resolution:
    | ((record: EscalationRecord) => Promise<EscalationResolutionInput | undefined>)
    | undefined,
): void {
  const ws = config.ws;
  const tokens = readWorkerTokens(ws);
  let runtime: MasterRuntime | undefined;
  runtime = createMasterRuntime({
    dbPath: join(dir, "master.sqlite"),
    policy: DEFAULT_POLICY,
    workspace: ws.workspace ?? process.cwd(),
    ...(ws.packDigest === undefined ? {} : { packDigest: ws.packDigest }),
    ...(tokens.length === 0 ? {} : { tokens }),
    ...(resolution === undefined ? {} : { resolveEscalation: resolution }),
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
  } else if (ctx.get?.("webServer") === undefined) {
    ctx.logger?.error?.(
      "gethen: ws config is set but the DSH webServer service is not mounted; " +
        `${WORKER_PATH} is not exposed and no worker can connect`,
    );
  } else {
    mountRoute(ctx);
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
  });
}

/**
 * Mount the master service and its tools into DSH.
 *
 * `config.ws` selects the WS control plane; otherwise the legacy
 * Remote/fake launch path is used exactly as before.
 */
export function apply(ctx: DshContext, config: GethenConfig = {}): void {
  const dir =
    config.dir ?? process.env.GETHEN_STATE_DIR ?? join(process.cwd(), ".gethen", "master");
  const answer = config.escalation?.answer;
  const resolveEscalation =
    answer === undefined
      ? undefined
      : async (_record: EscalationRecord): Promise<EscalationResolutionInput> => ({
          kind: "answer",
          text: answer,
        });

  if (config.ws !== undefined) {
    applyWs(ctx, { ...config, ws: config.ws }, dir, resolveEscalation);
    return;
  }

  const clientService = config.remote?.clientService ?? "dshRemoteClient";
  const devClient =
    config.devRemote === true || process.env.GETHEN_DEV_REMOTE === "1"
      ? createScriptedRemoteClient()
      : undefined;
  const remoteConfig =
    config.remote ??
    (devClient === undefined ? undefined : { hostId: "dev-local", workspaceId: "ws-dev" });
  const useRemote = remoteConfig !== undefined;
  const launchMode: "fake" | "remote" = useRemote ? "remote" : "fake";
  let service: MasterService | undefined;
  const hooks = escalationHooks(() => service);
  const launch: LaunchPort =
    useRemote && remoteConfig !== undefined
      ? createRemoteLaunchPort({
          ...remoteConfig,
          clientProvider: () =>
            devClient ?? (ctx.get?.(clientService) as RemoteClientLike | undefined),
          ...(resolveEscalation === undefined ? {} : { resolveEscalation }),
          ...hooks,
        })
      : createFakeLaunch();
  service = createMasterService(dir, launch);
  service.start();
  ctx.provide("gethen", service);
  ctx.effect?.(() => () => service.stop());
  registerTools(ctx, {
    service,
    launch: () => launchMode,
    workers: () => 0,
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export { applyWorker } from "./worker-entry";

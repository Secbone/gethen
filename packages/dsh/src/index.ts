/**
 * Minimal DSH mount for the self-built Gethen master (P1).
 *
 * The plugin owns no control-plane logic: `@gethen/master` provides the engine,
 * sqlite store, policy and outbox. This module only assembles one
 * `MasterService` per plugin instance, exposes it as the `gethen` service and
 * registers three tools. DSH itself is not imported; the narrow surface below
 * is the whole integration contract.
 */
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import type { RemoteClientLike } from "@gethen/bridge-remote";
import {
  createFakeLaunch,
  type LaunchPort,
  MasterService,
  type Policy,
  type Trigger,
  ticketStage,
} from "@gethen/master";
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

export interface DshContext {
  provide(name: string, value: unknown): () => void;
  tools: { register(definition: DshToolDefinition): () => void };
  effect?(setup: () => (() => void) | void): () => void;
  /** Present when the DSH Remote client plugin is mounted in this profile. */
  get?(name: string): unknown;
  logger?: { error?(message: string): void };
}

export interface GethenConfig {
  /** State directory; defaults to `$GETHEN_STATE_DIR` or `<cwd>/.gethen/master`. */
  dir?: string;
  /**
   * Dispatch through the DSH Remote client to a Worker host. Requires the
   * `dshRemoteClient` service; without it the fake launch is used.
   */
  remote?: RemoteLaunchConfig;
}

export const name = "gethen";
export const inject = ["tools"];

/**
 * Single-operator P1 policy: the local source admits `local-user` and the
 * master accepts automatically. The fake LaunchPort keeps P1 self-contained.
 */
export const DEFAULT_POLICY: Policy = {
  version: 1,
  acceptMode: "auto",
  rules: [],
  sources: { local: { allowed: ["local-user"], bots: [] } },
};

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

/** Mount the master service and its tools into DSH. */
export function apply(ctx: DshContext, config: GethenConfig = {}): void {
  const dir =
    config.dir ?? process.env.GETHEN_STATE_DIR ?? join(process.cwd(), ".gethen", "master");
  const remoteClient = ctx.get?.("dshRemoteClient") as RemoteClientLike | undefined;
  const useRemote = config.remote !== undefined && remoteClient !== undefined;
  const launchMode = useRemote ? "remote" : "fake";
  const launch: LaunchPort =
    useRemote && config.remote !== undefined
      ? createRemoteLaunchPort(config.remote, remoteClient as RemoteClientLike)
      : createFakeLaunch();
  const service = createMasterService(dir, launch);
  service.start();
  ctx.provide("gethen", service);
  ctx.effect?.(() => () => service.stop());

  ctx.tools.register({
    name: "gethen_status",
    description: "Return master counts: tickets, open tickets, receipts and running Assignments.",
    parameters: {},
    output: {
      schema: objectSchema(
        {
          tickets: { type: "number" },
          open: { type: "number" },
          receipts: { type: "number" },
          running: { type: "number" },
          launch: { type: "string", enum: ["fake", "remote"] },
        },
        ["tickets", "open", "receipts", "running", "launch"],
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
        launch: launchMode,
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
}

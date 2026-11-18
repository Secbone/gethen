/**
 * Control-plane assembly shared by the standalone CLI (`gethen serve`) and the
 * DSH plugin.
 *
 * One `createMasterRuntime` call wires the whole Master side on the L1/L2
 * communication stack:
 *
 * - `MasterService` (sqlite store + engine + outbox) with the local-default
 *   policy, taking a `LaunchPort` from `@gethen/bridge-mailbox`;
 * - `@gethen/mailbox` over sqlite as the durable L2 layer, with one
 *   `@gethen/mailbox-link` per accepted worker session;
 * - `@gethen/transport`'s ws transport server as the L1 layer; the host forwards
 *   its `upgrade` events to `runtime.handleUpgrade` (path routing stays host's);
 * - with no `tokens`/`verifyToken` there is no transport server and the fake
 *   `LaunchPort` is used, which is only useful for dev/tests.
 *
 * There is intentionally no legacy frame-gateway path and no compatibility shim:
 * the package owns assembly only and imports no DSH code.
 */
import type { IncomingMessage } from "node:http";
import { basename, dirname, join } from "node:path";
import type { Duplex } from "node:stream";
import {
  type BridgeSettlementMetrics,
  createMailboxLaunchPort,
  DEFAULT_MAILBOX_PREFIX,
  type HarnessSessionHistory,
  type HarnessSessionSummary,
  MASTER_MAILBOX,
  type MailboxLaunchBridge,
  type TaskEvent,
  type TaskEventQuery,
  type TaskState,
} from "@gethen/bridge-mailbox";
import { MailboxClient, SqliteMailboxStore } from "@gethen/mailbox";
import { createMailboxServerLink, type MailboxServerLink } from "@gethen/mailbox-link";
import {
  type AssignmentReceipt,
  createFakeLaunch,
  type EscalationRecord,
  type EscalationResolutionInput,
  type LaunchFailure,
  type LaunchPort,
  type LaunchRequest,
  MasterService,
  type Policy,
  type Report,
} from "@gethen/master";
import {
  createMemoryTokenStore,
  createWsTransportServer,
  type TokenRecord,
  type WsTransportServer,
} from "@gethen/transport";
import { type ControlPlane, createControlPlane } from "./control";
import {
  createRoutedLaunchPort,
  emptySchedulingMetrics,
  evidenceFromReceipts,
  type RoutingOptions,
  type SchedulingMetrics,
  WorkerDirectory,
  type WorkerSnapshot,
} from "./fleet";
import { NotificationHub } from "./notifications";

/** Task state/event and session types (re-exported from bridge-mailbox). */
export type {
  HarnessSessionEvent,
  HarnessSessionHistory,
  HarnessSessionSummary,
  TaskEvent,
  TaskEventQuery,
  TaskState,
} from "@gethen/bridge-mailbox";

/** Pack digest recorded when the engine has none (no real pack pinning yet). */
export const DEFAULT_PACK_DIGEST = "sha256:local-unpinned";

/**
 * Logical workspace dispatched when the host configures none. It is a
 * workstream id, not a Master filesystem path: the Worker owns the directory
 * and creates `feat/<id>` for delivery.
 */
export const DEFAULT_WORKSTREAM = "default";

/** How often the runtime closes stale Tickets / archives expired ones. */
export const TICKET_SWEEP_INTERVAL_MS = 5 * 60 * 1000;

/**
 * Single-operator local policy shared by the CLI and the DSH plugin: the local
 * source admits `local-user` and the Master accepts automatically.
 */
export const DEFAULT_MASTER_POLICY: Policy = {
  version: 1,
  acceptMode: "auto",
  rules: [],
  sources: { local: { allowed: ["local-user"], bots: [] } },
};

/** `"ws"` when the mailbox transport stack is attached, else the fake launch. */
export type LaunchMode = "ws" | "fake";

/** Mailbox queue counters rendered by `health()`. */
export interface MailboxHealth {
  /** Ready + leased messages on the worker and master mailboxes. */
  pending: number;
  leased: number;
  dead: number;
}

/** What a host HTTP server hands the transport upgrade handler. */
export type UpgradeHandler = (request: IncomingMessage, socket: Duplex, head: Buffer) => void;

/** A token lookup: what `@gethen/transport`'s token store or a durable host implements. */
export type TokenVerifier = (token: string) => Promise<{ workerId: string } | undefined>;

/** Stable failure codes for the runtime task-control surface. */
export type MasterRuntimeErrorCode = "RUNTIME_CLOSED" | "RUNTIME_COMMS_UNAVAILABLE";

/** A task-control failure with a stable code. */
export class MasterRuntimeError extends Error {
  readonly code: MasterRuntimeErrorCode;

  constructor(code: MasterRuntimeErrorCode, message: string) {
    super(`${code}: ${message}`);
    this.name = "MasterRuntimeError";
    this.code = code;
  }
}

/**
 * Live task state/control over the mailbox bridge (single-worker C3).
 * Only available when the runtime assembled the mailbox transport; the
 * fake-launch mode has no worker to ask.
 */
export interface MasterRuntimeTasks {
  /** Last known state of every assignment the bridge has seen. */
  states(): Record<string, TaskState>;
  /** Last known state of one assignment, without asking the Worker. */
  state(assignmentId: string): TaskState | undefined;
  /** Ask the Worker for its live state; `null` when it has none. */
  status(assignmentId: string, options?: { timeoutMs?: number }): Promise<TaskState | null>;
  /** Send `gethen.task.steer` to the running assignment. */
  steer(assignmentId: string, text: string): Promise<void>;
  /** Send `gethen.task.cancel` to the running assignment. */
  cancel(assignmentId: string, reason?: string): Promise<void>;
  /** Cached task events (oldest first; tail page unless `afterSeq` is set). */
  events(assignmentId: string, query?: TaskEventQuery): TaskEvent[];
  /** Number of cached events for one assignment. */
  eventCount(assignmentId: string): number;
  /** Events evicted by the per-assignment ring buffer. */
  eventDropped(assignmentId: string): number;
  /** Ask the Worker for its harness session directory. */
  sessions(options?: { timeoutMs?: number }): Promise<HarnessSessionSummary[]>;
  /** Ask the Worker for one session's event history. */
  sessionHistory(
    sessionId: string,
    options?: { limit?: number; afterSeq?: number; timeoutMs?: number },
  ): Promise<HarnessSessionHistory>;
  /** Answer a parked Worker escalation; late results update the ticket/receipt. */
  answer(
    assignmentId: string,
    resolution: EscalationResolutionInput,
    options?: { escalationId?: string },
  ): EscalationRecord;
}

/** A result produced after the original launch had already returned `waiting`. */
export interface LateLaunchResult {
  request: LaunchRequest;
  report: Report;
  receipt: AssignmentReceipt;
  escalations?: EscalationRecord[];
}

export interface MasterRuntimeOptions {
  /** SQLite file for tickets, effects, receipts, escalations and policy. */
  dbPath: string;
  /** Worker credentials served by the transport server (in-memory token store). */
  tokens?: readonly TokenRecord[];
  /** Durable token lookup (e.g. sqlite-backed); takes precedence over `tokens`. */
  verifyToken?: TokenVerifier;
  /** Master policy; defaults to {@link DEFAULT_MASTER_POLICY}. */
  policy?: Policy;
  /**
   * Logical workstream id dispatched with assignments; defaults to
   * {@link DEFAULT_WORKSTREAM}. This is never a Master filesystem path: the
   * Worker owns and materializes the workspace.
   */
  workspace?: string;
  /** Pack digest for dispatches; defaults to {@link DEFAULT_PACK_DIGEST}. */
  packDigest?: string;
  /** Answers a Worker escalation; absent parks it for a person. */
  resolveEscalation?: (record: EscalationRecord) => Promise<EscalationResolutionInput | undefined>;
  /** Observer for escalation records (the DSH plugin persists them). */
  onEscalation?: (record: EscalationRecord) => void;
  /** Observer for resolved/parked escalation records. */
  onResolution?: (record: EscalationRecord) => void;
  /** Observer for a result that arrives after a parked escalation was answered. */
  onLateResult?: (input: LateLaunchResult) => void;
  /** Transport ping interval in milliseconds. */
  heartbeatMs?: number;
  now?: () => Date;
  /**
   * Restrict dispatch to this Worker (also the default for a single-Worker
   * deployment). Without it every configured token is routable.
   */
  workerId?: string;
  /**
   * Workers accepted from a dynamic `verifyToken` lookup. Not needed when
   * `tokens` is given; one of the two is required for a multi-Worker runtime.
   */
  workers?: readonly string[];
  /** Routing behavior: operator pin, extra hard requirements, queue bounds. */
  routing?: RoutingOptions;
  /** Mailbox sqlite file; defaults to `<dirname(dbPath)>/mailbox.sqlite`. */
  mailboxDbPath?: string;
  /** Pending-launch persistence file; defaults to `<dbPath>.pending.json`. */
  pendingStateFile?: string;
  /**
   * Silence window before a dispatched launch is settled as timed out. A late
   * Worker result still lands on the Ticket; defaults to the bridge's 10 min.
   */
  launchTimeoutMs?: number;
  /**
   * Full Master mailbox name, which must be `<prefix>master` (the bridge-mailbox
   * derives both mailbox names from one prefix); defaults to `mailbox:master`.
   */
  masterMailbox?: string;
  /** Lease/visibility timeout for mailbox deliveries. */
  visibilityTimeoutMs?: number;
  /** Periodic status reconciliation interval; defaults to the bridge's 15s. */
  reconcileIntervalMs?: number;
  /** Grace before a status probe may settle a launch; defaults to the bridge's 5s. */
  recoveryGraceMs?: number;
  /** Poll interval for the master mailbox and link delivery loops. */
  pollIntervalMs?: number;
}

export type {
  NotificationInput,
  NotificationKind,
  NotificationQuery,
  NotificationRow,
} from "@gethen/master";
export type { NotificationHub } from "./notifications";

/** Counts a host renders on `/health` or a status widget. */
export interface MasterRuntimeHealth {
  launch: LaunchMode;
  workers: number;
  tickets: number;
  open: number;
  receipts: number;
  escalations: number;
  mailbox: MailboxHealth;
  /** Live queued/running/reused/queue-timeout counters. */
  scheduling: SchedulingMetrics;
  /** Direct result envelopes vs status-recovery settlements. */
  settlements: BridgeSettlementMetrics;
}

export interface MasterRuntime {
  readonly service: MasterService;
  /** Mailbox launch bridge; `undefined` when no credentials are configured. */
  readonly bridge: MailboxLaunchBridge | undefined;
  readonly launch: LaunchMode;
  /** Live task state/control; fake mode reports `RUNTIME_COMMS_UNAVAILABLE`. */
  readonly tasks: MasterRuntimeTasks;
  /** Durable wake-up notifications (worker result / parked escalation). */
  readonly notifications: NotificationHub;
  /** Shared status/overview/ticket/task control for every host adapter. */
  readonly control: ControlPlane;
  /** Forward the host server's worker upgrade here; `undefined` in fake mode. */
  readonly handleUpgrade: UpgradeHandler | undefined;
  /** Per-Worker registration/health/load projection (empty in fake mode). */
  workers(): WorkerSnapshot[];
  /** Live scheduling counters (zeros in fake mode). */
  scheduling(): SchedulingMetrics;
  /** Direct vs recovered settlement counters (zeros in fake mode). */
  settlements(): BridgeSettlementMetrics;
  health(): MasterRuntimeHealth;
  /**
   * Close the communication stack first (bridge -> session links -> transport
   * server -> mailbox store), then stop the Master service.
   */
  close(): Promise<void>;
}

/**
 * Build the Master control plane. Synchronous: the sqlite stores and the
 * transport server object are created here, while listening stays with the host.
 */
export function createMasterRuntime(options: MasterRuntimeOptions): MasterRuntime {
  const workspace = options.workspace ?? DEFAULT_WORKSTREAM;
  const packDigest = options.packDigest ?? DEFAULT_PACK_DIGEST;
  const nowIso = (): string => (options.now?.() ?? new Date()).toISOString();

  const tokenStore =
    options.tokens !== undefined && options.tokens.length > 0
      ? createMemoryTokenStore(options.tokens)
      : undefined;
  const verifyToken = options.verifyToken ?? tokenStore?.lookup;
  const workerIds = resolveWorkerIds(
    options.workerId,
    options.tokens,
    options.workers,
    verifyToken,
  );
  const defaultWorkerId = options.workerId ?? (workerIds.length === 1 ? workerIds[0] : undefined);
  const routing: RoutingOptions = options.routing ?? {};
  const directory = new WorkerDirectory(workerIds, {
    ...(routing.preferWorker === undefined ? {} : { preferWorker: routing.preferWorker }),
  });

  let mailboxStore: SqliteMailboxStore | undefined;
  let mailboxServer: WsTransportServer | undefined;
  let mailboxBridge: MailboxLaunchBridge | undefined;
  const serverLinks = new Set<MailboxServerLink>();
  let closed = false;
  let ticketSweep: ReturnType<typeof setInterval> | undefined;

  function handleBridgeResult(input: LateLaunchResult & { late: boolean }): void {
    notificationHub?.publish({
      kind: "assignment.result",
      ticket: input.request.ticket.id,
      assignment: input.request.assignment.id,
      data: {
        report: input.report.kind,
        summary: input.report.summary,
        late: input.late,
        at: input.report.at,
      },
    });
  }

  function handleLaunchFailure(failure: LaunchFailure): void {
    notificationHub?.publish({
      kind: "assignment.result",
      ticket: failure.request.ticket.id,
      assignment: failure.request.assignment.id,
      data: {
        report: failure.report.kind,
        summary: failure.report.summary,
        late: false,
        timedOut: failure.timedOut,
        at: failure.report.at,
      },
    });
  }

  function handleResolution(record: EscalationRecord): void {
    options.onResolution?.(record);
    if (record.status === "parked") {
      notificationHub?.publish({
        kind: "escalation.parked",
        ticket: record.ticket,
        assignment: record.assignment,
        data: {
          escalationId: record.id,
          kind: record.kind,
          question: record.question,
          options: record.options,
          at: record.at,
        },
      });
    }
  }

  function handleLateResult(input: LateLaunchResult): void {
    const service = serviceRef;
    if (service === undefined) return;
    service.engine.report(input.request.ticket.id, input.report);
    service.store.append(input.receipt);
    options.onLateResult?.(input);
  }

  if (verifyToken !== undefined) {
    const masterMailboxName = options.masterMailbox ?? `${DEFAULT_MAILBOX_PREFIX}${MASTER_MAILBOX}`;
    const mailboxPrefix = mailboxPrefixFor(masterMailboxName);
    const store = new SqliteMailboxStore(
      options.mailboxDbPath ?? join(dirname(options.dbPath), "mailbox.sqlite"),
      options.now === undefined ? {} : { now: options.now },
    );
    mailboxStore = store;
    const client = new MailboxClient({
      store,
      ...(options.now === undefined ? {} : { now: options.now }),
      ...(options.visibilityTimeoutMs === undefined
        ? {}
        : { visibilityTimeoutMs: options.visibilityTimeoutMs }),
      ...(options.pollIntervalMs === undefined ? {} : { pollIntervalMs: options.pollIntervalMs }),
    });
    mailboxBridge = createMailboxLaunchPort({
      client,
      workers: workerIds,
      ...(defaultWorkerId === undefined ? {} : { workerId: defaultWorkerId }),
      mailboxPrefix,
      ...(options.visibilityTimeoutMs === undefined
        ? {}
        : { leaseMs: options.visibilityTimeoutMs }),
      ...(options.reconcileIntervalMs === undefined
        ? {}
        : { reconcileIntervalMs: options.reconcileIntervalMs }),
      ...(options.recoveryGraceMs === undefined
        ? {}
        : { recoveryGraceMs: options.recoveryGraceMs }),
      ...(options.pollIntervalMs === undefined ? {} : { pollIntervalMs: options.pollIntervalMs }),
      ...(options.resolveEscalation === undefined
        ? {}
        : { resolveEscalation: options.resolveEscalation }),
      ...(options.onEscalation === undefined ? {} : { onEscalation: options.onEscalation }),
      onResolution: handleResolution,
      onResult: handleBridgeResult,
      onLateResult: handleLateResult,
      pendingStateFile:
        options.pendingStateFile ??
        join(dirname(options.dbPath), `${basename(options.dbPath)}.pending.json`),
      ...(options.launchTimeoutMs === undefined ? {} : { timeoutMs: options.launchTimeoutMs }),
      ...(options.now === undefined ? {} : { now: options.now }),
    });
    const bridge = mailboxBridge;
    mailboxServer = createWsTransportServer({
      verifyAuth: async (auth) => {
        if (auth.kind !== "bearer" || auth.token.length === 0) return undefined;
        const identity = await verifyToken(auth.token);
        if (identity === undefined) return undefined;
        // An explicit workerId keeps the historical single-Worker restriction;
        // otherwise every configured Worker may attach.
        if (options.workerId !== undefined && identity.workerId !== options.workerId) {
          return undefined;
        }
        return workerIds.includes(identity.workerId) ? identity : undefined;
      },
      onSession: (session, context) => {
        const workerId = context.identity.workerId;
        directory.connect(workerId);
        const link = createMailboxServerLink({
          session,
          store,
          workers: [bridge.mailboxFor(workerId)],
          ...(options.visibilityTimeoutMs === undefined
            ? {}
            : { visibilityTimeoutMs: options.visibilityTimeoutMs }),
          ...(options.pollIntervalMs === undefined
            ? {}
            : { pollIntervalMs: options.pollIntervalMs }),
          ...(options.now === undefined ? {} : { now: options.now }),
          onHello: (hello) => {
            // The transport already authenticated the worker id; a manifest
            // claiming another id must never enter the routing pool.
            if (hello.worker === undefined) return;
            directory.register(hello.worker, nowIso(), workerId);
          },
          onError: () => undefined,
        });
        serverLinks.add(link);
        session.onState((state) => {
          if (state !== "closed") return;
          link.close();
          serverLinks.delete(link);
          directory.disconnect(workerId, nowIso());
        });
      },
      onError: () => undefined,
      ...(options.heartbeatMs === undefined ? {} : { heartbeatMs: options.heartbeatMs }),
    });
  }

  let serviceRef: MasterService | undefined;
  let notificationHub: NotificationHub | undefined;
  const routedPort =
    mailboxBridge === undefined
      ? undefined
      : createRoutedLaunchPort({
          directory,
          ports: new Map(workerIds.map((workerId) => [workerId, mailboxBridge.launchPort])),
          routing,
          evidence: () => evidenceFromReceipts(serviceRef?.store.listReceipts() ?? []),
          onRoute: (request) => {
            // Persist the routed Worker before the launch can block, so task
            // observability does not show an empty worker while it runs.
            serviceRef?.engine.assignmentRouted(
              request.ticket.id,
              request.assignment.id,
              request.assignment.worker,
            );
          },
          ...(options.now === undefined ? {} : { now: options.now }),
        });
  const launchPort: LaunchPort =
    routedPort === undefined
      ? withLaunchDefaults(createFakeLaunch(), { workspace, packDigest })
      : withLaunchDefaults(routedPort, { workspace, packDigest });
  const service = new MasterService({
    dbPath: options.dbPath,
    policy: options.policy ?? DEFAULT_MASTER_POLICY,
    launch: launchPort,
    onLaunchFailure: handleLaunchFailure,
    ...(options.now === undefined ? {} : { clock: { now: options.now } }),
  });
  serviceRef = service;
  service.start();
  notificationHub = new NotificationHub({
    store: service.store,
    ...(options.now === undefined ? {} : { now: options.now }),
  });

  const finalServer = mailboxServer;
  const handleUpgrade: UpgradeHandler | undefined =
    finalServer === undefined
      ? undefined
      : (request, socket, head) => {
          finalServer.handleUpgrade(request, socket, head);
        };

  function mailboxHealth(): MailboxHealth {
    const counters: MailboxHealth = { pending: 0, leased: 0, dead: 0 };
    const store = mailboxStore;
    if (store === undefined || closed) return counters;
    const mailboxes = new Set<string>();
    if (mailboxBridge !== undefined) {
      for (const workerMailbox of mailboxBridge.workerMailboxes) mailboxes.add(workerMailbox);
      mailboxes.add(mailboxBridge.masterMailbox);
    }
    try {
      for (const mailbox of mailboxes) {
        const stats = store.stats(mailbox);
        counters.pending += stats.pending;
        counters.leased += stats.leased;
        counters.dead += stats.dead;
      }
    } catch {
      // A closing/closed store reports zeros rather than breaking /health.
    }
    return counters;
  }

  function health(): MasterRuntimeHealth {
    const launch: LaunchMode = mailboxBridge === undefined ? "fake" : "ws";
    if (closed) {
      // The service/store are stopped; report a clean zero snapshot.
      return {
        launch,
        workers: 0,
        tickets: 0,
        open: 0,
        receipts: 0,
        escalations: 0,
        mailbox: { pending: 0, leased: 0, dead: 0 },
        scheduling: emptySchedulingMetrics(),
        settlements: { directCompleted: 0, recoveredCompleted: 0, recoveredWithoutReport: 0 },
      };
    }
    const tickets = service.store.tickets.list();
    return {
      launch,
      workers: mailboxServer === undefined ? 0 : mailboxServer.sessions().length,
      tickets: tickets.length,
      open: tickets.filter((ticket) => ticket.endedAt === undefined).length,
      receipts: service.store.listReceipts().length,
      escalations: service.store.listEscalations().length,
      mailbox: mailboxHealth(),
      scheduling: (routedPort ?? { metrics: emptySchedulingMetrics }).metrics(),
      settlements: mailboxBridge?.settlementMetrics() ?? {
        directCompleted: 0,
        recoveredCompleted: 0,
        recoveredWithoutReport: 0,
      },
    };
  }

  /** The bridge task control needs; throws a coded error when unavailable. */
  function taskBridge(): MailboxLaunchBridge {
    if (closed) {
      throw new MasterRuntimeError("RUNTIME_CLOSED", "master runtime is closed");
    }
    if (mailboxBridge === undefined) {
      throw new MasterRuntimeError(
        "RUNTIME_COMMS_UNAVAILABLE",
        "task state/control requires the mailbox transport (configure worker tokens)",
      );
    }
    return mailboxBridge;
  }

  const tasks: MasterRuntimeTasks = {
    states: () => taskBridge().taskStates(),
    state: (assignmentId) => taskBridge().taskState(assignmentId),
    // Async wrappers: an unavailable/closed runtime must reject the promise
    // rather than throw synchronously from a `Promise`-returning method.
    status: async (assignmentId, options) => taskBridge().requestStatus(assignmentId, options),
    steer: async (assignmentId, text) => {
      await taskBridge().steer(assignmentId, text);
    },
    cancel: async (assignmentId, reason) => {
      await taskBridge().cancel(assignmentId, reason);
    },
    events: (assignmentId, query) => taskBridge().taskEvents(assignmentId, query),
    eventCount: (assignmentId) => taskBridge().taskEventCount(assignmentId),
    eventDropped: (assignmentId) => taskBridge().taskEventDropped(assignmentId),
    sessions: async (options) => taskBridge().listSessions(options),
    sessionHistory: async (sessionId, options) => taskBridge().sessionHistory(sessionId, options),
    answer: (assignmentId, resolution, options) =>
      taskBridge().answer(assignmentId, resolution, options),
  };

  const control = createControlPlane({
    service,
    tasks,
    workers: () => directory.snapshots(),
    scheduling: () => (routedPort ?? { metrics: emptySchedulingMetrics }).metrics(),
    settlements: () =>
      mailboxBridge?.settlementMetrics() ?? {
        directCompleted: 0,
        recoveredCompleted: 0,
        recoveredWithoutReport: 0,
      },
    ...(options.now === undefined ? {} : { now: options.now }),
  });
  // Archive/close stale Tickets even when no operator or agent is polling.
  ticketSweep = setInterval(() => {
    try {
      control.autoArchive();
    } catch {
      // A sweep failure must not take down the control plane; the next tick retries.
    }
  }, TICKET_SWEEP_INTERVAL_MS);
  ticketSweep.unref?.();

  async function close(): Promise<void> {
    if (closed) return;
    closed = true;
    if (ticketSweep !== undefined) {
      clearInterval(ticketSweep);
      ticketSweep = undefined;
    }
    try {
      // Bridge first (stops launches), then the session links, the transport
      // server and the mailbox store; the service stops in `finally`.
      mailboxBridge?.close();
      for (const link of [...serverLinks]) link.close();
      serverLinks.clear();
      await mailboxServer?.close();
      mailboxStore?.close();
    } finally {
      // The service must stop even if the transport teardown fails.
      service.stop();
    }
  }

  return {
    service,
    bridge: mailboxBridge,
    launch: mailboxBridge === undefined ? "fake" : "ws",
    tasks,
    notifications: notificationHub,
    control,
    handleUpgrade,
    workers: () => directory.snapshots(),
    scheduling: () => (routedPort ?? { metrics: emptySchedulingMetrics }).metrics(),
    settlements: () =>
      mailboxBridge?.settlementMetrics() ?? {
        directCompleted: 0,
        recoveredCompleted: 0,
        recoveredWithoutReport: 0,
      },
    health,
    close,
  };
}

/**
 * Which Workers this runtime may route to: an explicit `workerId` restriction,
 * else every configured token, else an explicit `workers` list. A dynamic
 * verifier with no list cannot be routed and fails fast instead of guessing.
 */
function resolveWorkerIds(
  explicit: string | undefined,
  tokens: readonly TokenRecord[] | undefined,
  workers: readonly string[] | undefined,
  hasVerifier: TokenVerifier | undefined,
): string[] {
  const unique = (ids: readonly string[]): string[] => [
    ...new Set(ids.filter((id) => typeof id === "string" && id.length > 0)),
  ];
  if (explicit !== undefined && explicit.length > 0) return [explicit];
  if (tokens !== undefined && tokens.length > 0)
    return unique(tokens.map((token) => token.workerId));
  if (workers !== undefined && workers.length > 0) return unique(workers);
  if (hasVerifier !== undefined) {
    throw new Error(
      "master-runtime: configure tokens/workers (or an explicit workerId) so Workers can be routed",
    );
  }
  return [];
}

/**
 * `@gethen/bridge-mailbox` derives both mailbox names from one prefix
 * (`<prefix><workerId>` / `<prefix>master`), so the configured Master mailbox
 * must be `<prefix>master`.
 */
function mailboxPrefixFor(masterMailbox: string): string {
  const suffix = MASTER_MAILBOX;
  if (!masterMailbox.endsWith(suffix)) {
    throw new Error(
      `master-runtime: masterMailbox must be "<prefix>${suffix}" (got "${masterMailbox}")`,
    );
  }
  const prefix = masterMailbox.slice(0, -suffix.length);
  if (prefix.length === 0) {
    throw new Error(`master-runtime: masterMailbox "${masterMailbox}" has an empty prefix`);
  }
  return prefix;
}

/**
 * The engine persists assignments without a workspace or pack digest, which the
 * mailbox assign payload rejects. Fill host-level defaults in at the launch
 * boundary only; the engine's own rows stay untouched.
 *
 * The normalization is applied *in place* on the request's Assignment: a routed
 * launch port records the selected Worker by mutating `request.assignment`, and
 * the launch executor reads that object back to attribute a settled/timeout
 * receipt. Copying it here would hide the routed Worker from the receipt.
 */
function withLaunchDefaults(
  port: LaunchPort,
  defaults: { workspace: string; packDigest: string },
): LaunchPort {
  return {
    launch(request: LaunchRequest) {
      if (request.assignment.workspace.length === 0) {
        request.assignment.workspace = defaults.workspace;
      }
      if (request.assignment.packDigest.length === 0) {
        request.assignment.packDigest = defaults.packDigest;
      }
      if (request.packDigest.length === 0) {
        request.packDigest = defaults.packDigest;
      }
      return port.launch(request);
    },
  };
}

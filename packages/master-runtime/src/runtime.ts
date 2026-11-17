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
import { dirname, join } from "node:path";
import type { Duplex } from "node:stream";
import {
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
  /** Worker-side workspace for dispatches; defaults to `process.cwd()`. */
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
  /** Worker served by this runtime (single-worker C3). Defaults to the only token. */
  workerId?: string;
  /** Mailbox sqlite file; defaults to `<dirname(dbPath)>/mailbox.sqlite`. */
  mailboxDbPath?: string;
  /**
   * Full Master mailbox name, which must be `<prefix>master` (the bridge-mailbox
   * derives both mailbox names from one prefix); defaults to `mailbox:master`.
   */
  masterMailbox?: string;
  /** Lease/visibility timeout for mailbox deliveries. */
  visibilityTimeoutMs?: number;
  /** Poll interval for the master mailbox and link delivery loops. */
  pollIntervalMs?: number;
}

/** Counts a host renders on `/health` or a status widget. */
export interface MasterRuntimeHealth {
  launch: LaunchMode;
  workers: number;
  tickets: number;
  open: number;
  receipts: number;
  escalations: number;
  mailbox: MailboxHealth;
}

export interface MasterRuntime {
  readonly service: MasterService;
  /** Mailbox launch bridge; `undefined` when no credentials are configured. */
  readonly bridge: MailboxLaunchBridge | undefined;
  readonly launch: LaunchMode;
  /** Live task state/control; fake mode reports `RUNTIME_COMMS_UNAVAILABLE`. */
  readonly tasks: MasterRuntimeTasks;
  /** Shared status/overview/ticket/task control for every host adapter. */
  readonly control: ControlPlane;
  /** Forward the host server's worker upgrade here; `undefined` in fake mode. */
  readonly handleUpgrade: UpgradeHandler | undefined;
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
  const workspace = options.workspace ?? process.cwd();
  const packDigest = options.packDigest ?? DEFAULT_PACK_DIGEST;

  const tokenStore =
    options.tokens !== undefined && options.tokens.length > 0
      ? createMemoryTokenStore(options.tokens)
      : undefined;
  const verifyToken = options.verifyToken ?? tokenStore?.lookup;

  let mailboxStore: SqliteMailboxStore | undefined;
  let mailboxServer: WsTransportServer | undefined;
  let mailboxBridge: MailboxLaunchBridge | undefined;
  const serverLinks = new Set<MailboxServerLink>();
  let closed = false;

  function handleLateResult(input: LateLaunchResult): void {
    service.engine.report(input.request.ticket.id, input.report);
    service.store.append(input.receipt);
    options.onLateResult?.(input);
  }

  if (verifyToken !== undefined) {
    const workerId = resolveWorkerId(options.workerId, options.tokens);
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
      workerId,
      mailboxPrefix,
      ...(options.visibilityTimeoutMs === undefined
        ? {}
        : { leaseMs: options.visibilityTimeoutMs }),
      ...(options.pollIntervalMs === undefined ? {} : { pollIntervalMs: options.pollIntervalMs }),
      ...(options.resolveEscalation === undefined
        ? {}
        : { resolveEscalation: options.resolveEscalation }),
      ...(options.onEscalation === undefined ? {} : { onEscalation: options.onEscalation }),
      ...(options.onResolution === undefined ? {} : { onResolution: options.onResolution }),
      onLateResult: handleLateResult,
      ...(options.now === undefined ? {} : { now: options.now }),
    });
    const workerMailbox = mailboxBridge.workerMailbox;
    mailboxServer = createWsTransportServer({
      verifyAuth: async (auth) => {
        if (auth.kind !== "bearer" || auth.token.length === 0) return undefined;
        const identity = await verifyToken(auth.token);
        // C3 is single-worker: only the configured worker may attach.
        if (identity === undefined || identity.workerId !== workerId) return undefined;
        return identity;
      },
      onSession: (session) => {
        const link = createMailboxServerLink({
          session,
          store,
          workers: [workerMailbox],
          ...(options.visibilityTimeoutMs === undefined
            ? {}
            : { visibilityTimeoutMs: options.visibilityTimeoutMs }),
          ...(options.pollIntervalMs === undefined
            ? {}
            : { pollIntervalMs: options.pollIntervalMs }),
          ...(options.now === undefined ? {} : { now: options.now }),
          onError: () => undefined,
        });
        serverLinks.add(link);
        session.onState((state) => {
          if (state !== "closed") return;
          link.close();
          serverLinks.delete(link);
        });
      },
      onError: () => undefined,
      ...(options.heartbeatMs === undefined ? {} : { heartbeatMs: options.heartbeatMs }),
    });
  }

  const launchPort = withLaunchDefaults(mailboxBridge?.launchPort ?? createFakeLaunch(), {
    workspace,
    packDigest,
  });
  const service = new MasterService({
    dbPath: options.dbPath,
    policy: options.policy ?? DEFAULT_MASTER_POLICY,
    launch: launchPort,
    ...(options.now === undefined ? {} : { clock: { now: options.now } }),
  });
  service.start();

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
      mailboxes.add(mailboxBridge.workerMailbox);
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
    ...(options.now === undefined ? {} : { now: options.now }),
  });

  async function close(): Promise<void> {
    if (closed) return;
    closed = true;
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
    control,
    handleUpgrade,
    health,
    close,
  };
}

/** Single-worker resolution: explicit > the only configured token > error. */
function resolveWorkerId(
  explicit: string | undefined,
  tokens: readonly TokenRecord[] | undefined,
): string {
  if (explicit !== undefined && explicit.length > 0) return explicit;
  const only = tokens?.length === 1 ? tokens[0] : undefined;
  if (only !== undefined) return only.workerId;
  throw new Error(
    "master-runtime: workerId is required (multiple worker tokens need an explicit workerId)",
  );
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

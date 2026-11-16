/**
 * Control-plane assembly shared by the standalone CLI (`gethen serve`) and the
 * DSH plugin.
 *
 * One `createMasterRuntime` call wires the whole Master side:
 *
 * - `MasterService` (sqlite store + engine + outbox) with the local-default
 *   policy, taking a `LaunchPort` that is either the fake one or the WS bridge;
 * - when workers are configured (`tokens` and/or `verifyToken`, or an injected
 *   `gateway`), a `WorkerGateway` plus the `@gethen/bridge-ws` `WsLaunchBridge`,
 *   with the bridge's `selectWorker` hook backed by the runtime's live
 *   `WorkerSnapshot[]`;
 * - with `comms.mode = "mailbox"`, the L1 ws transport + L2 sqlite mailbox stack
 *   (`@gethen/transport` server, one `@gethen/mailbox-link` link per session,
 *   `@gethen/bridge-mailbox` as the `LaunchPort`) instead of the legacy
 *   gateway/bridge; the `handleUpgrade` entry point stays the same;
 * - the host-facing helpers: `handleUpgrade` (route matched by the host), the
 *   frame sink for injected gateways, `workers()` snapshots and `health()`.
 *
 * The package owns assembly only; it never re-implements dispatch, escalation
 * or report handling (that stays in `@gethen/bridge-ws`) and imports no DSH
 * code, so the CLI and the plugin share exactly the same control plane.
 */
import type { IncomingMessage } from "node:http";
import { dirname, join } from "node:path";
import type { Duplex } from "node:stream";
import {
  createMailboxLaunchPort,
  DEFAULT_MAILBOX_PREFIX,
  MASTER_MAILBOX,
  type MailboxLaunchBridge,
} from "@gethen/bridge-mailbox";
import { createWsLaunchPort, type WorkerSelection, type WsLaunchBridge } from "@gethen/bridge-ws";
import { MailboxClient, SqliteMailboxStore } from "@gethen/mailbox";
import { createMailboxServerLink, type MailboxServerLink } from "@gethen/mailbox-link";
import {
  createFakeLaunch,
  type EscalationRecord,
  type EscalationResolutionInput,
  type LaunchPort,
  type LaunchRequest,
  MasterService,
  type Policy,
} from "@gethen/master";
import { createWsTransportServer, type WsTransportServer } from "@gethen/transport";
import {
  createMemoryTokenStore,
  createWorkerGateway,
  type WorkerTokenIdentity,
  type WorkerTokenRecord,
} from "@gethen/transport-ws";
import type { DownFrame, HelloFrame, LoadSnapshot, UpFrame } from "@gethen/worker-protocol";

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

/** `"ws"` when a remote launch bridge is attached, else the fake launch. */
export type LaunchMode = "ws" | "fake";

/** Which communication stack the runtime assembles. */
export type CommsMode = "legacy" | "mailbox";

/**
 * `comms` selects the communication stack:
 *
 * - `"legacy"` (default): `WorkerGateway` (`gethen.worker.v1` frames) plus
 *   `@gethen/bridge-ws`, exactly as before;
 * - `"mailbox"`: L1 `@gethen/transport` ws server + L2 `@gethen/mailbox`
 *   sqlite store + one `@gethen/mailbox-link` per session, launching through
 *   `@gethen/bridge-mailbox`.
 */
export interface CommsOptions {
  mode: CommsMode;
  /**
   * Single worker served in mailbox mode. Defaults to the only configured
   * token's worker id; otherwise required.
   */
  workerId?: string;
  /** Mailbox sqlite file; defaults to `<dirname(dbPath)>/mailbox.sqlite`. */
  mailboxDbPath?: string;
  /**
   * Full Master mailbox name, which must be `<prefix>master` (C3 derives both
   * mailbox names from one prefix); defaults to `mailbox:master`.
   */
  masterMailbox?: string;
  /** Lease/visibility timeout for deliveries; default from `@gethen/mailbox`. */
  visibilityTimeoutMs?: number;
  /** Poll interval for the master mailbox and link delivery loops. */
  pollIntervalMs?: number;
}

/** Mailbox queue counters rendered by `health()` in mailbox mode. */
export interface MailboxHealth {
  /** Ready + leased messages on the worker and master mailboxes. */
  pending: number;
  leased: number;
  dead: number;
}

/** What a host HTTP server hands the gateway's upgrade handler. */
export type UpgradeHandler = (request: IncomingMessage, socket: Duplex, head: Buffer) => void;

/** One connected Worker, derived from its hello/heartbeat frames. */
export interface WorkerSnapshot {
  workerId: string;
  machine: string;
  /** Connection is still open (gateway removes closed connections). */
  online: boolean;
  workspaces: string[];
  runtimes: string[];
  models: string[];
  running: number;
  quota: number;
  /** RFC 3339; last hello or heartbeat the Master saw. */
  lastSeenAt: string;
}

/** The connection fields the runtime needs; a real `WorkerConnection` has more. */
export interface WorkerConnectionLike {
  readonly id: string;
  readonly workerId?: string;
  readonly isOpen?: boolean;
}

/**
 * The gateway surface used by the assembly. The production `WorkerGateway`
 * satisfies it; tests and hosts can pass a small fake instead.
 */
export interface WorkerGatewayLike {
  connections(): readonly WorkerConnectionLike[];
  get(workerId: string): WorkerConnectionLike | undefined;
  dispatch(workerId: string, frame: DownFrame): Promise<void>;
  close(): Promise<void>;
  handleUpgrade?(request: IncomingMessage, socket: Duplex, head: Buffer): void;
}

export interface MasterRuntimeOptions {
  /** SQLite file for tickets, effects, receipts, escalations and policy. */
  dbPath: string;
  /**
   * Worker credentials served by the WS gateway. Empty/absent (with no
   * `verifyToken` and no `gateway`) keeps the fake launch.
   */
  tokens?: readonly WorkerTokenRecord[];
  /** Durable token lookup (e.g. sqlite-backed) instead of the in-memory store. */
  verifyToken?: (token: string) => Promise<WorkerTokenIdentity | undefined>;
  /**
   * Test/host injection: use an existing gateway instead of creating one from
   * `tokens`/`verifyToken`. The host then must forward frames to
   * `runtime.handleFrame`. Production wiring passes credentials, not a gateway.
   */
  gateway?: WorkerGatewayLike;
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
  /** Gateway ping interval in milliseconds (default from transport-ws). */
  heartbeatMs?: number;
  /**
   * Fleet-aware worker choice: receives the launch and the live snapshots.
   * `undefined` means no worker fits (the launch rejects `WS_NO_WORKER`).
   * When absent, {@link defaultSelectWorker} is used.
   */
  selectWorker?: (request: LaunchRequest, workers: WorkerSnapshot[]) => WorkerSnapshot | undefined;
  /** Communication stack; defaults to `"legacy"`. */
  comms?: CommsOptions;
  now?: () => Date;
}

/** Counts a host renders on `/health` or a status widget. */
export interface MasterRuntimeHealth {
  launch: LaunchMode;
  /** The assembled communication stack. */
  comms: CommsMode;
  workers: number;
  tickets: number;
  open: number;
  receipts: number;
  escalations: number;
  /** Mailbox queue counters; present only in `comms.mode = "mailbox"`. */
  mailbox?: MailboxHealth;
}

export interface MasterRuntime {
  readonly service: MasterService;
  readonly gateway: WorkerGatewayLike | undefined;
  /** Active launch bridge: the legacy WS bridge or the mailbox bridge. */
  readonly bridge: WsLaunchBridge | MailboxLaunchBridge | undefined;
  /** Mailbox-mode bridge (same object as `bridge` in that mode). */
  readonly mailboxBridge: MailboxLaunchBridge | undefined;
  readonly launch: LaunchMode;
  readonly comms: CommsMode;
  /** Forward the host server's worker upgrade here (path routing is the host's). */
  readonly handleUpgrade: UpgradeHandler | undefined;
  /** Feed gateway frames here when the host injected its own gateway. */
  handleFrame(conn: WorkerConnectionLike, frame: UpFrame): void;
  health(): MasterRuntimeHealth;
  /** Live worker snapshots, sorted by worker id. */
  workers(): WorkerSnapshot[];
  /**
   * Close the transport stack first (gateway, or mailbox bridge -> session
   * links -> server -> store), then stop the Master service.
   */
  close(): Promise<void>;
}

interface CachedWorker {
  readonly hello: HelloFrame;
  load: LoadSnapshot;
  lastSeenAt: string;
}

/**
 * Build the Master control plane. Synchronous: the sqlite store and the
 * gateway object are created here, while sockets/listening stay with the host.
 */
export function createMasterRuntime(options: MasterRuntimeOptions): MasterRuntime {
  const now = options.now ?? (() => new Date());
  const nowIso = (): string => now().toISOString();
  const workspace = options.workspace ?? process.cwd();
  const packDigest = options.packDigest ?? DEFAULT_PACK_DIGEST;
  const helloCache = new Map<string, CachedWorker>();
  const commsMode: CommsMode = options.comms?.mode ?? "legacy";
  let bridge: WsLaunchBridge | undefined;
  let gateway: WorkerGatewayLike | undefined = options.gateway;
  let mailboxBridge: MailboxLaunchBridge | undefined;
  let mailboxStore: SqliteMailboxStore | undefined;
  let mailboxServer: WsTransportServer | undefined;
  const serverLinks = new Set<MailboxServerLink>();
  let closed = false;

  function cacheHello(frame: HelloFrame): void {
    helloCache.set(frame.workerId, { hello: frame, load: frame.load, lastSeenAt: nowIso() });
  }

  function handleFrame(conn: WorkerConnectionLike, frame: UpFrame): void {
    if (frame.type === "hello") cacheHello(frame);
    if (frame.type === "heartbeat" && conn.workerId !== undefined) {
      const cached = helloCache.get(conn.workerId);
      if (cached !== undefined) {
        cached.load = frame.load;
        cached.lastSeenAt = frame.at.length > 0 ? frame.at : nowIso();
      }
    }
    bridge?.handleFrame(conn, frame);
  }

  /** Turn a fleet choice into the id-only selection bridge-ws expects. */
  const select = (request: LaunchRequest): WorkerSelection | undefined => {
    const snapshots = workers();
    if (options.selectWorker !== undefined) {
      const chosen = options.selectWorker(request, snapshots);
      return chosen === undefined ? undefined : { workerId: chosen.workerId };
    }
    const chosen = defaultSelectWorker(request, snapshots, workspace);
    return chosen === undefined ? undefined : { workerId: chosen.workerId };
  };

  const tokenStore =
    options.tokens !== undefined && options.tokens.length > 0
      ? createMemoryTokenStore(options.tokens)
      : undefined;
  const verifyToken = options.verifyToken ?? tokenStore?.lookup;

  if (commsMode === "mailbox" && options.gateway !== undefined) {
    throw new Error(
      'master-runtime: options.gateway is legacy-only; use comms.mode = "mailbox" instead',
    );
  }

  if (commsMode === "legacy" && gateway === undefined && verifyToken !== undefined) {
    gateway = createWorkerGateway({
      verifyToken,
      onHello: (_conn, frame) => cacheHello(frame),
      onFrame: (conn, frame) => handleFrame(conn, frame),
      onClose: (conn) => {
        if (conn.workerId !== undefined) helloCache.delete(conn.workerId);
      },
      onError: () => undefined,
      ...(options.heartbeatMs === undefined ? {} : { heartbeatMs: options.heartbeatMs }),
    });
  }

  if (commsMode === "legacy" && gateway !== undefined) {
    bridge = createWsLaunchPort({
      gateway,
      selectWorker: select,
      ...(options.resolveEscalation === undefined
        ? {}
        : { resolveEscalation: options.resolveEscalation }),
      ...(options.onEscalation === undefined ? {} : { onEscalation: options.onEscalation }),
      ...(options.onResolution === undefined ? {} : { onResolution: options.onResolution }),
      now,
    });
  }

  if (commsMode === "mailbox") {
    const comms = options.comms;
    if (verifyToken === undefined) {
      throw new Error(
        'master-runtime: comms.mode = "mailbox" needs tokens or verifyToken for ws transport auth',
      );
    }
    const workerId = resolveMailboxWorkerId(comms, options.tokens);
    const masterMailboxName = comms?.masterMailbox ?? `${DEFAULT_MAILBOX_PREFIX}${MASTER_MAILBOX}`;
    const mailboxPrefix = mailboxPrefixFor(masterMailboxName);
    const store = new SqliteMailboxStore(
      comms?.mailboxDbPath ?? join(dirname(options.dbPath), "mailbox.sqlite"),
      options.now === undefined ? {} : { now: options.now },
    );
    mailboxStore = store;
    const client = new MailboxClient({
      store,
      ...(options.now === undefined ? {} : { now: options.now }),
      ...(comms?.visibilityTimeoutMs === undefined
        ? {}
        : { visibilityTimeoutMs: comms.visibilityTimeoutMs }),
      ...(comms?.pollIntervalMs === undefined ? {} : { pollIntervalMs: comms.pollIntervalMs }),
    });
    mailboxBridge = createMailboxLaunchPort({
      client,
      workerId,
      mailboxPrefix,
      ...(comms?.visibilityTimeoutMs === undefined ? {} : { leaseMs: comms.visibilityTimeoutMs }),
      ...(comms?.pollIntervalMs === undefined ? {} : { pollIntervalMs: comms.pollIntervalMs }),
      ...(options.resolveEscalation === undefined
        ? {}
        : { resolveEscalation: options.resolveEscalation }),
      ...(options.onEscalation === undefined ? {} : { onEscalation: options.onEscalation }),
      ...(options.onResolution === undefined ? {} : { onResolution: options.onResolution }),
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
          ...(comms?.visibilityTimeoutMs === undefined
            ? {}
            : { visibilityTimeoutMs: comms.visibilityTimeoutMs }),
          ...(comms?.pollIntervalMs === undefined ? {} : { pollIntervalMs: comms.pollIntervalMs }),
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

  const launchPort = withLaunchDefaults(
    bridge?.launchPort ?? mailboxBridge?.launchPort ?? createFakeLaunch(),
    {
      workspace,
      packDigest,
    },
  );
  const service = new MasterService({
    dbPath: options.dbPath,
    policy: options.policy ?? DEFAULT_MASTER_POLICY,
    launch: launchPort,
    ...(options.now === undefined ? {} : { clock: { now: options.now } }),
  });
  service.start();

  const upgradeServer = mailboxServer;
  const upgradeGateway = gateway;
  const gatewayUpgrade = upgradeGateway?.handleUpgrade;
  const handleUpgrade: UpgradeHandler | undefined =
    upgradeServer !== undefined
      ? (request, socket, head) => {
          upgradeServer.handleUpgrade(request, socket, head);
        }
      : gatewayUpgrade === undefined || upgradeGateway === undefined
        ? undefined
        : (request, socket, head) => {
            gatewayUpgrade.call(upgradeGateway, request, socket, head);
          };

  function workers(): WorkerSnapshot[] {
    const current = gateway;
    if (current === undefined) return [];
    const snapshots: WorkerSnapshot[] = [];
    for (const conn of current.connections()) {
      const workerId = conn.workerId;
      if (workerId === undefined) continue;
      const cached = helloCache.get(workerId);
      const hello = cached?.hello;
      const load = cached?.load ?? hello?.load ?? { running: 0, quota: 0 };
      snapshots.push({
        workerId,
        machine: hello?.machine ?? "",
        online: conn.isOpen ?? true,
        workspaces: hello === undefined ? [] : [...hello.workspaces],
        runtimes: hello === undefined ? [] : [...hello.runtimes],
        models: hello === undefined ? [] : [...hello.models],
        running: load.running,
        quota: load.quota,
        lastSeenAt: cached?.lastSeenAt ?? nowIso(),
      });
    }
    snapshots.sort((left, right) => left.workerId.localeCompare(right.workerId));
    return snapshots;
  }

  function health(): MasterRuntimeHealth {
    const launch: LaunchMode = bridge === undefined && mailboxBridge === undefined ? "fake" : "ws";
    if (closed) {
      // The service/store are stopped; report a clean zero snapshot instead of
      // throwing from /health after shutdown.
      return {
        launch,
        comms: commsMode,
        workers: 0,
        tickets: 0,
        open: 0,
        receipts: 0,
        escalations: 0,
        ...(mailboxStore === undefined ? {} : { mailbox: { pending: 0, leased: 0, dead: 0 } }),
      };
    }
    const tickets = service.store.tickets.list();
    const counters: MasterRuntimeHealth = {
      launch,
      comms: commsMode,
      workers: mailboxServer === undefined ? workers().length : mailboxServer.sessions().length,
      tickets: tickets.length,
      open: tickets.filter((ticket) => ticket.endedAt === undefined).length,
      receipts: service.store.listReceipts().length,
      escalations: service.store.listEscalations().length,
    };
    if (mailboxStore !== undefined) counters.mailbox = mailboxHealth();
    return counters;
  }

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

  async function close(): Promise<void> {
    if (closed) return;
    closed = true;
    try {
      if (commsMode === "mailbox") {
        // Bridge first (stops launches), then the session links, the ws
        // server and the mailbox store; the service stops in `finally`.
        mailboxBridge?.close();
        for (const link of [...serverLinks]) link.close();
        serverLinks.clear();
        await mailboxServer?.close();
        mailboxStore?.close();
      } else {
        await gateway?.close();
      }
    } finally {
      // The service must stop even if the transport teardown fails.
      service.stop();
    }
  }

  return {
    service,
    gateway,
    bridge: bridge ?? mailboxBridge,
    mailboxBridge,
    launch: bridge === undefined && mailboxBridge === undefined ? "fake" : "ws",
    comms: commsMode,
    handleUpgrade,
    handleFrame,
    health,
    workers,
    close,
  };
}

/**
 * Minimal fleet-aware selector used when the host does not supply one:
 * online, workspace-capable (`workspaces` contains the launch workspace or
 * `"*"`) and below quota; lowest `running / quota`, then worker id.
 */
export function defaultSelectWorker(
  request: LaunchRequest,
  workers: readonly WorkerSnapshot[],
  workspace?: string,
): WorkerSnapshot | undefined {
  const wanted = workspaceFor(request, workspace);
  const candidates = workers.filter(
    (worker) =>
      worker.online &&
      worker.quota > 0 &&
      worker.running < worker.quota &&
      matchesWorkspace(worker.workspaces, wanted),
  );
  candidates.sort(
    (left, right) =>
      left.running / left.quota - right.running / right.quota ||
      left.workerId.localeCompare(right.workerId),
  );
  return candidates[0];
}

/** The workspace a dispatch targets: the Assignment's, else the host default. */
function workspaceFor(request: LaunchRequest, fallback: string | undefined): string | undefined {
  if (request.assignment.workspace.length > 0) return request.assignment.workspace;
  return fallback !== undefined && fallback.length > 0 ? fallback : undefined;
}

function matchesWorkspace(workspaces: readonly string[], wanted: string | undefined): boolean {
  if (wanted === undefined) return true;
  return workspaces.includes(wanted) || workspaces.includes("*");
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
 * Mailbox mode serves exactly one worker: `comms.workerId`, or the only
 * configured token's worker id. More workers need the C5 multi-worker routing.
 */
function resolveMailboxWorkerId(
  comms: CommsOptions | undefined,
  tokens: readonly WorkerTokenRecord[] | undefined,
): string {
  const configured = comms?.workerId;
  if (configured !== undefined && configured.length > 0) return configured;
  const only = tokens?.length === 1 ? tokens[0] : undefined;
  if (only !== undefined) return only.workerId;
  throw new Error(
    'master-runtime: comms.mode = "mailbox" needs comms.workerId (or exactly one configured worker token)',
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
      `master-runtime: comms.masterMailbox must be "<prefix>${suffix}" (got "${masterMailbox}")`,
    );
  }
  const prefix = masterMailbox.slice(0, -suffix.length);
  if (prefix.length === 0) {
    throw new Error(`master-runtime: comms.masterMailbox "${masterMailbox}" has an empty prefix`);
  }
  return prefix;
}

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
 * - the host-facing helpers: `handleUpgrade` (route matched by the host), the
 *   frame sink for injected gateways, `workers()` snapshots and `health()`.
 *
 * The package owns assembly only; it never re-implements dispatch, escalation
 * or report handling (that stays in `@gethen/bridge-ws`) and imports no DSH
 * code, so the CLI and the plugin share exactly the same control plane.
 */
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { createWsLaunchPort, type WorkerSelection, type WsLaunchBridge } from "@gethen/bridge-ws";
import {
  createFakeLaunch,
  type EscalationRecord,
  type EscalationResolutionInput,
  type LaunchPort,
  type LaunchRequest,
  MasterService,
  type Policy,
} from "@gethen/master";
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

/** `"ws"` when the worker gateway/bridge are attached, else the fake launch. */
export type LaunchMode = "ws" | "fake";

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
  now?: () => Date;
}

/** Counts a host renders on `/health` or a status widget. */
export interface MasterRuntimeHealth {
  launch: LaunchMode;
  workers: number;
  tickets: number;
  open: number;
  receipts: number;
  escalations: number;
}

export interface MasterRuntime {
  readonly service: MasterService;
  readonly gateway: WorkerGatewayLike | undefined;
  readonly bridge: WsLaunchBridge | undefined;
  readonly launch: LaunchMode;
  /** Forward the host server's worker upgrade here (path routing is the host's). */
  readonly handleUpgrade: UpgradeHandler | undefined;
  /** Feed gateway frames here when the host injected its own gateway. */
  handleFrame(conn: WorkerConnectionLike, frame: UpFrame): void;
  health(): MasterRuntimeHealth;
  /** Live worker snapshots, sorted by worker id. */
  workers(): WorkerSnapshot[];
  /** Close the gateway first (worker sockets), then stop the Master service. */
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
  let bridge: WsLaunchBridge | undefined;
  let gateway: WorkerGatewayLike | undefined = options.gateway;

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

  if (gateway === undefined && verifyToken !== undefined) {
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

  if (gateway !== undefined) {
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

  const launchPort = withLaunchDefaults(bridge?.launchPort ?? createFakeLaunch(), {
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

  const upgradeGateway = gateway;
  const upgrade = upgradeGateway?.handleUpgrade;
  const handleUpgrade: UpgradeHandler | undefined =
    upgrade === undefined || upgradeGateway === undefined
      ? undefined
      : (request, socket, head) => {
          upgrade.call(upgradeGateway, request, socket, head);
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
    const tickets = service.store.tickets.list();
    return {
      launch: bridge === undefined ? "fake" : "ws",
      workers: workers().length,
      tickets: tickets.length,
      open: tickets.filter((ticket) => ticket.endedAt === undefined).length,
      receipts: service.store.listReceipts().length,
      escalations: service.store.listEscalations().length,
    };
  }

  let closed = false;
  async function close(): Promise<void> {
    if (closed) return;
    closed = true;
    try {
      await gateway?.close();
    } finally {
      // The service must stop even if the socket teardown fails.
      service.stop();
    }
  }

  return {
    service,
    gateway,
    bridge,
    launch: bridge === undefined ? "fake" : "ws",
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

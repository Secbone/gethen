/**
 * Multi-Worker dispatch: the live Worker directory plus the `LaunchPort` that
 * turns a routing decision into a mailbox dispatch.
 *
 * The directory is fed by mailbox `hello` registrations (workspace/model/
 * capability/quota/health) and by the Master's own in-flight launch accounting.
 * It never enforces a Worker's sandbox: the declared capabilities are only hard
 * filters for *selecting* a Worker, while the Worker keeps its local DSH
 * approval policy.
 */
import type {
  AssignmentReceipt,
  LaunchPort,
  LaunchRequest,
  LaunchResult,
  Report,
  SessionSpec,
} from "@gethen/master";
import {
  type EvidenceMap,
  planTaskRoute,
  type RoutePlan,
  type TaskRequirement,
  type WorkerCapabilities,
  type WorkerManifest,
} from "@gethen/router";
import type { WorkerBuild, WorkerHealth, WorkerSessionStats } from "@gethen/worker-bridge";

/** A zeroed scheduling snapshot (fake launch, closed runtime, no routing). */
export function emptySchedulingMetrics(): SchedulingMetrics {
  return {
    queued: 0,
    running: 0,
    dispatched: 0,
    reused: 0,
    queueEntries: 0,
    queueTimeouts: 0,
    maxQueueDepth: 0,
    parked: 0,
  };
}

/** Operator-level needs added to every dispatch (workspace comes from the Assignment). */
export interface RoutingRequirements {
  model?: string;
  capabilities?: Partial<WorkerCapabilities>;
}

export interface RoutingOptions {
  /** Operator pin; honored only for Workers that pass the hard filters. */
  preferWorker?: string;
  /** Extra hard requirements for every dispatch. */
  require?: RoutingRequirements;
  /** How long a QUEUE decision waits for a slot; defaults to 30s. */
  queueTimeoutMs?: number;
  /** Poll interval while queued; defaults to 100ms. */
  queuePollMs?: number;
  /** Reject Workers whose build commit differs from the Master's expected commit. */
  rejectStaleWorkers?: boolean;
  /** Treat stale heartbeat Workers as unavailable; default false. */
  rejectStaleHealth?: boolean;
  /**
   * When no Worker advertises the requested workspace, route to the best
   * otherwise-eligible Worker instead of parking. Default false (strict
   * workspace isolation); enabled decisions carry a `workspace-fallback`
   * reason.
   */
  allowWorkspaceFallback?: boolean;
}

/**
 * Live scheduling counters for capacity decisions. They are process-local and
 * reset when the Master restarts; Tickets/receipts remain the durable record.
 */
export interface SchedulingMetrics {
  /** Dispatches currently waiting in the bounded queue. */
  queued: number;
  /** Dispatches currently executing on a Worker. */
  running: number;
  /** Every launch that reached a Worker, including reuses. */
  dispatched: number;
  /** Dispatches that reused a session-affine Worker. */
  reused: number;
  /** Total attempts that entered the queue (not the current depth). */
  queueEntries: number;
  /** Queue waits that ended in `ROUTER_QUEUE_TIMEOUT`. */
  queueTimeouts: number;
  /** Highest observed queue depth. */
  maxQueueDepth: number;
  /** Dispatches parked because no Worker could accept them. */
  parked: number;
}

/** A routed launch port plus the live scheduling counters it maintains. */
export interface RoutedLaunchPort extends LaunchPort {
  metrics(): SchedulingMetrics;
}

/** The Master's view of one configured Worker, registered or not. */
export interface WorkerSnapshot {
  id: string;
  /** True once a `hello` registration supplied the manifest. */
  registered: boolean;
  /** True while the authenticated transport session is attached. */
  connected: boolean;
  health: WorkerHealth;
  machine?: string;
  runtime?: WorkerManifest["runtime"];
  workspaces: WorkerManifest["workspaces"];
  models: string[];
  capabilities?: WorkerCapabilities;
  costClass?: WorkerManifest["costClass"];
  build?: WorkerBuild;
  workspaceMap?: Record<string, string>;
  defaultWorkspace?: string;
  /** Last local DSH session observation reported in `hello`. */
  sessions?: WorkerSessionStats;
  load: { running: number; quota: number };
  lastHeartbeatAt?: string;
  /** Assignment ids currently dispatched to this Worker. */
  active: number;
}

interface WorkerEntry {
  manifest?: WorkerManifest;
  connected: boolean;
  registeredAt?: string;
  lastHeartbeatAt?: string;
  active: Set<string>;
}

/** The registration/health/load state the router and status projection read. */
export class WorkerDirectory {
  private readonly entries = new Map<string, WorkerEntry>();
  private readonly sessions = new Map<string, string>();
  private readonly preferWorker: string | undefined;
  private readonly staleAfterMs: number;
  private readonly now: () => Date;

  constructor(
    workerIds: readonly string[] = [],
    options: { preferWorker?: string; staleAfterMs?: number; now?: () => Date } = {},
  ) {
    for (const workerId of workerIds) {
      if (workerId.length > 0) this.entries.set(workerId, emptyEntry());
    }
    this.preferWorker = options.preferWorker;
    this.staleAfterMs = options.staleAfterMs ?? 90_000;
    this.now = options.now ?? (() => new Date());
  }

  /** Derive liveness from the last heartbeat; explicit offline wins. */
  private healthOf(manifest: WorkerManifest): WorkerHealth {
    if (manifest.health.state === "offline") return "offline";
    if (this.staleAfterMs <= 0) return manifest.health.state;
    const at = Date.parse(manifest.health.lastHeartbeatAt);
    if (!Number.isFinite(at)) return "stale";
    return this.now().getTime() - at > this.staleAfterMs ? "stale" : manifest.health.state;
  }

  /** Configured Workers, in id order. */
  knownIds(): string[] {
    return [...this.entries.keys()].sort((left, right) => left.localeCompare(right));
  }

  /** A Worker's manifest once its `hello` has been accepted. */
  manifest(workerId: string): WorkerManifest | undefined {
    return this.entries.get(workerId)?.manifest;
  }

  /**
   * Accept a `hello` registration. The caller has already authenticated the
   * worker id; a manifest with a different id is a protocol error.
   */
  register(manifest: WorkerManifest, at: string, expectedWorkerId?: string): WorkerManifest {
    if (expectedWorkerId !== undefined && manifest.id !== expectedWorkerId) {
      throw new Error(
        `worker hello id "${manifest.id}" does not match authenticated worker "${expectedWorkerId}"`,
      );
    }
    const entry = this.entries.get(manifest.id) ?? emptyEntry();
    const running = entry.active.size;
    const normalized: WorkerManifest = {
      ...manifest,
      load: { running, quota: manifest.load.quota },
      health: { ...manifest.health, lastHeartbeatAt: at },
    };
    entry.manifest = normalized;
    entry.connected = true;
    entry.registeredAt = at;
    entry.lastHeartbeatAt = at;
    this.entries.set(manifest.id, entry);
    return normalized;
  }

  /** Mark an authenticated session as attached before its `hello` arrives. */
  connect(workerId: string): void {
    const entry = this.entries.get(workerId) ?? emptyEntry();
    entry.connected = true;
    this.entries.set(workerId, entry);
  }

  /** Mark one Worker offline without forgetting its last manifest. */
  disconnect(workerId: string, at: string): void {
    const entry = this.entries.get(workerId);
    if (entry === undefined) return;
    entry.connected = false;
    if (entry.manifest !== undefined) {
      entry.manifest = {
        ...entry.manifest,
        health: { ...entry.manifest.health, state: "offline", lastHeartbeatAt: at },
      };
    }
    entry.lastHeartbeatAt = at;
  }

  /** True while at least one authenticated session is attached. */
  isConnected(workerId: string): boolean {
    return this.entries.get(workerId)?.connected === true;
  }

  /** Manifests for the router: registered Workers, including offline ones. */
  manifests(): WorkerManifest[] {
    return this.knownIds()
      .map((workerId) => this.entries.get(workerId)?.manifest)
      .filter((manifest): manifest is WorkerManifest => manifest !== undefined)
      .map((manifest) => ({
        ...manifest,
        health: { ...manifest.health, state: this.healthOf(manifest) },
      }));
  }

  /** Account one dispatch against a Worker's quota. */
  acquire(workerId: string, assignmentId: string): void {
    const entry = this.entries.get(workerId);
    if (entry === undefined) return;
    entry.active.add(assignmentId);
    this.syncLoad(entry);
  }

  /** Release a dispatch slot; safe to call more than once. */
  release(workerId: string, assignmentId: string): void {
    const entry = this.entries.get(workerId);
    if (entry === undefined) return;
    entry.active.delete(assignmentId);
    this.syncLoad(entry);
  }

  /** Remember which Worker owns a session key/id so a later reuse can stay there. */
  recordSession(workerId: string, session: SessionSpec | undefined): void {
    if (session === undefined) return;
    if (session.sessionId !== undefined && session.sessionId.length > 0) {
      this.sessions.set(`id:${session.sessionId}`, workerId);
    }
    if (session.key !== undefined && session.key.length > 0) {
      this.sessions.set(`key:${session.key}`, workerId);
    }
  }

  /** The Worker that served a session binding, when this process remembers it. */
  sessionWorker(session: SessionSpec | undefined): string | undefined {
    if (session === undefined) return undefined;
    if (session.sessionId !== undefined && session.sessionId.length > 0) {
      const found = this.sessions.get(`id:${session.sessionId}`);
      if (found !== undefined) return found;
    }
    if (session.key !== undefined && session.key.length > 0) {
      return this.sessions.get(`key:${session.key}`);
    }
    return undefined;
  }

  /** Plan one dispatch with the current manifests and operator options. */
  plan(
    requirement: TaskRequirement,
    evidence: EvidenceMap = {},
    reuseWorker?: string,
    build?: {
      rejectStaleWorkers?: boolean;
      /** Treat stale heartbeat Workers as unavailable; default false. */
      rejectStaleHealth?: boolean;
      expectedBuildCommit?: string;
      preferredWorkspace?: string;
    },
  ): RoutePlan {
    return planTaskRoute(this.manifests(), requirement, evidence, {
      ...(this.preferWorker === undefined ? {} : { prefer: this.preferWorker }),
      ...(reuseWorker === undefined ? {} : { reuseWorker }),
      ...(build?.rejectStaleWorkers === undefined
        ? {}
        : { rejectStaleWorkers: build.rejectStaleWorkers }),
      ...(build?.rejectStaleHealth === undefined
        ? {}
        : { rejectStaleHealth: build.rejectStaleHealth }),
      ...(build?.expectedBuildCommit === undefined
        ? {}
        : { expectedBuildCommit: build.expectedBuildCommit }),
      ...(build?.preferredWorkspace === undefined
        ? {}
        : { preferredWorkspace: build.preferredWorkspace }),
    });
  }

  /** Status/fleet projection for `/health`, the control plane and the DSH status tool. */
  snapshots(): WorkerSnapshot[] {
    return this.knownIds().map((workerId) => {
      const entry = this.entries.get(workerId) ?? emptyEntry();
      const manifest = entry.manifest;
      return {
        id: workerId,
        registered: manifest !== undefined,
        connected: entry.connected,
        health: manifest === undefined ? "offline" : this.healthOf(manifest),
        ...(manifest === undefined ? {} : { machine: manifest.machine }),
        ...(manifest === undefined ? {} : { runtime: manifest.runtime }),
        workspaces: manifest?.workspaces ?? [],
        models: manifest?.models ?? [],
        ...(manifest === undefined ? {} : { capabilities: manifest.capabilities }),
        ...(manifest === undefined ? {} : { costClass: manifest.costClass }),
        ...(manifest?.build === undefined ? {} : { build: manifest.build }),
        ...(manifest?.workspaceMap === undefined ? {} : { workspaceMap: manifest.workspaceMap }),
        ...(manifest?.defaultWorkspace === undefined
          ? {}
          : { defaultWorkspace: manifest.defaultWorkspace }),
        ...(manifest?.sessions === undefined ? {} : { sessions: manifest.sessions }),
        load: manifest?.load ?? { running: entry.active.size, quota: 0 },
        ...(entry.lastHeartbeatAt === undefined ? {} : { lastHeartbeatAt: entry.lastHeartbeatAt }),
        active: entry.active.size,
      };
    });
  }

  /** Connected Workers that have registered (a subset of transport sessions). */
  onlineCount(): number {
    return this.snapshots().filter((snapshot) => snapshot.registered && snapshot.connected).length;
  }

  private syncLoad(entry: WorkerEntry): void {
    if (entry.manifest === undefined) return;
    entry.manifest = {
      ...entry.manifest,
      load: { running: entry.active.size, quota: entry.manifest.load.quota },
    };
  }
}

function emptyEntry(): WorkerEntry {
  return { connected: false, active: new Set() };
}

/** Pooled receipt evidence per Worker (overall; by-kind stays optional). */
export function evidenceFromReceipts(receipts: readonly AssignmentReceipt[]): EvidenceMap {
  const evidence: EvidenceMap = {};
  for (const receipt of receipts) {
    if (receipt.worker.length === 0) continue;
    const entry = evidence[receipt.worker] ?? { attempts: 0, successes: 0 };
    entry.attempts += 1;
    if (receipt.outcome === "completed") entry.successes += 1;
    evidence[receipt.worker] = entry;
  }
  return evidence;
}

export interface RoutedLaunchPortOptions {
  directory: WorkerDirectory;
  /** Master build commit used by `routing.rejectStaleWorkers`. */
  expectedBuildCommit?: string;
  /** One launch port per Worker, usually `bridge.launchPort` for the shared bridge. */
  ports: ReadonlyMap<string, LaunchPort>;
  /** Called once a Worker is selected (before dispatch) so the host persists it. */
  onRoute?: (request: LaunchRequest, plan: RoutePlan) => void;
  routing?: RoutingOptions;
  /** Receipt evidence provider; defaults to no evidence. */
  evidence?: () => EvidenceMap;
  now?: () => Date;
}

/**
 * The `LaunchPort` the Master service actually calls:
 *
 * - a single configured Worker keeps the historical direct dispatch;
 * - multiple Workers route by workspace/model/capability hard filters and
 *   affinity/evidence/load scoring;
 * - QUEUE waits (bounded) for a slot, PARK returns a terminal failed receipt
 *   instead of retrying an impossible dispatch forever.
 */
export function createRoutedLaunchPort(options: RoutedLaunchPortOptions): RoutedLaunchPort {
  const { directory } = options;
  const now = options.now ?? (() => new Date());
  const routing = options.routing ?? {};
  const queueTimeoutMs = positive(routing.queueTimeoutMs, 300_000);
  const queuePollMs = positive(routing.queuePollMs, 100);
  const metrics: SchedulingMetrics = {
    queued: 0,
    running: 0,
    dispatched: 0,
    reused: 0,
    queueEntries: 0,
    queueTimeouts: 0,
    maxQueueDepth: 0,
    parked: 0,
  };

  function requirementFor(request: LaunchRequest): TaskRequirement {
    const require = routing.require ?? {};
    return {
      kind: request.ticket.source.length > 0 ? request.ticket.source : "assignment",
      ...(request.assignment.workspace.length === 0
        ? {}
        : { workspace: request.assignment.workspace }),
      ...(require.model === undefined ? {} : { model: require.model }),
      ...(require.capabilities === undefined ? {} : { capabilities: require.capabilities }),
    };
  }

  /**
   * Single-Worker plan: dispatch when the Worker is unknown (legacy setups) or
   * has capacity, otherwise queue. Capability/workspace filters stay bypassed
   * exactly as before.
   */
  function singleWorkerPlan(worker: string): RoutePlan {
    const route: RoutePlan["route"] = { candidates: [], rejected: [] };
    const snapshot = directory.snapshots().find((entry) => entry.id === worker);
    const quota = snapshot?.load.quota ?? 0;
    if (snapshot === undefined || !snapshot.registered || quota <= 0) {
      return { action: "start", worker, reason: "single-worker", route };
    }
    if (routing.rejectStaleHealth === true && snapshot.health === "stale") {
      return { action: "park", worker, reason: "stale-health", route };
    }
    if (
      routing.rejectStaleWorkers === true &&
      options.expectedBuildCommit !== undefined &&
      options.expectedBuildCommit !== "unknown"
    ) {
      const build = snapshot.build?.commit ?? "unknown";
      if (build !== options.expectedBuildCommit) {
        return { action: "park", worker, reason: `stale-build:${build}`, route };
      }
    }
    if (snapshot.load.running < quota) {
      return { action: "start", worker, reason: "single-worker", route };
    }
    return { action: "queue", worker, reason: "quota-exhausted", route };
  }

  function planFor(request: LaunchRequest): RoutePlan {
    const requirement = requirementFor(request);
    const evidence = options.evidence?.() ?? {};
    const reuseWorker = directory.sessionWorker(request.assignment.session);
    const build = {
      ...(routing.rejectStaleWorkers === undefined
        ? {}
        : { rejectStaleWorkers: routing.rejectStaleWorkers }),
      ...(routing.rejectStaleHealth === undefined
        ? {}
        : { rejectStaleHealth: routing.rejectStaleHealth }),
      ...(options.expectedBuildCommit === undefined
        ? {}
        : { expectedBuildCommit: options.expectedBuildCommit }),
      ...(requirement.workspace === undefined ? {} : { preferredWorkspace: requirement.workspace }),
    };
    // 1. Hard filters + stale filter for the requested workspace.
    const plan = directory.plan(requirement, evidence, reuseWorker, build);
    if (plan.action !== "park") return plan;

    const staleRejected = plan.route.rejected.filter((rejected) =>
      rejected.reason.startsWith("stale-build:"),
    );
    // 2. If no exact workspace candidate can serve, optionally fall back to
    //    the best remaining Worker (hard filters and stale filter still apply).
    if (
      routing.allowWorkspaceFallback === true &&
      requirement.workspace !== undefined &&
      plan.route.rejected.some((rejected) => rejected.reason.startsWith("workspace-missing:"))
    ) {
      const fallback = directory.plan(
        { ...requirement, workspace: undefined },
        evidence,
        reuseWorker,
        build,
      );
      if (fallback.action === "start" || fallback.action === "reuse") {
        const fallbackReason =
          staleRejected.length > 0
            ? `stale-build-fallback:${staleRejected[0]?.reason.slice("stale-build:".length)}`
            : `workspace-fallback:${plan.reason}`;
        return {
          ...fallback,
          reason: fallbackReason,
          route: {
            ...fallback.route,
            candidates: fallback.route.candidates.map((candidate) => ({
              ...candidate,
              reasons: [
                ...candidate.reasons,
                {
                  code: staleRejected.length > 0 ? "stale-build-fallback" : "workspace-fallback",
                },
              ],
            })),
          },
        };
      }
    }
    // 3. Distinguish "every candidate was stale" from a mixed/other park.
    if (
      staleRejected.length > 0 &&
      plan.route.rejected.every((rejected) => rejected.reason.startsWith("stale-build:"))
    ) {
      return { ...plan, reason: "stale-build:all" };
    }
    return plan;
  }

  function failed(request: LaunchRequest, plan: RoutePlan, summary: string): LaunchResult {
    const at = now().toISOString();
    const report: Report = { kind: "failed", summary, at };
    const receipt: AssignmentReceipt = {
      assignment: request.assignment.id,
      ticket: request.ticket.id,
      worker: plan.worker ?? request.assignment.worker,
      workspace: request.assignment.workspace,
      packDigest: request.packDigest,
      at,
      outcome: "failed",
      summary,
      events: 0,
    };
    return { report, receipt };
  }

  async function dispatch(
    request: LaunchRequest,
    plan: RoutePlan,
    worker: string,
  ): Promise<LaunchResult> {
    const port = options.ports.get(worker);
    if (port === undefined) {
      return failed(request, plan, `ROUTER_NO_PORT: worker ${worker} has no dispatch port`);
    }
    request.assignment.worker = worker;
    options.onRoute?.(request, plan);
    directory.recordSession(worker, request.assignment.session);
    directory.acquire(worker, request.assignment.id);
    metrics.dispatched += 1;
    metrics.running += 1;
    if (plan.action === "reuse") metrics.reused += 1;
    try {
      return await port.launch(request);
    } finally {
      metrics.running -= 1;
      directory.release(worker, request.assignment.id);
    }
  }

  return {
    metrics: () => ({ ...metrics }),
    async launch(request: LaunchRequest): Promise<LaunchResult> {
      // Single-Worker compatibility: keep the direct dispatch (no capability or
      // workspace filters) but honor the Worker's advertised quota. Otherwise a
      // second dispatch is pushed at a busy single Worker and dropped when its
      // agent runs out of delivery attempts instead of waiting for a slot.
      const known = directory.knownIds();
      const single = known.length === 1 ? known[0] : undefined;
      if (single !== undefined) {
        const plan = singleWorkerPlan(single);
        if (plan.action === "start") return dispatch(request, plan, single);
      }

      const deadline = Date.now() + queueTimeoutMs;
      let queued = false;
      const leaveQueue = (): void => {
        if (!queued) return;
        queued = false;
        metrics.queued -= 1;
      };
      for (;;) {
        const plan = single === undefined ? planFor(request) : singleWorkerPlan(single);
        if ((plan.action === "start" || plan.action === "reuse") && plan.worker !== undefined) {
          leaveQueue();
          return dispatch(request, plan, plan.worker);
        }
        if (plan.action === "park") {
          leaveQueue();
          metrics.parked += 1;
          return failed(request, plan, `ROUTER_PARKED:${plan.reason}`);
        }
        // QUEUE: wait for a slot; a slot frees when an in-flight launch settles.
        if (Date.now() >= deadline) {
          leaveQueue();
          metrics.queueTimeouts += 1;
          return failed(
            request,
            plan,
            `ROUTER_QUEUE_TIMEOUT: no Worker capacity within ${queueTimeoutMs}ms`,
          );
        }
        if (!queued) {
          queued = true;
          metrics.queued += 1;
          metrics.queueEntries += 1;
          metrics.maxQueueDepth = Math.max(metrics.maxQueueDepth, metrics.queued);
        }
        await sleep(queuePollMs);
      }
    },
  };
}

function positive(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : fallback;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

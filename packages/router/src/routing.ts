import type { WorkerCapabilities, WorkerManifest } from "@gethen/worker-bridge";

/** What a task needs from a Worker before routing can consider it. */
export interface TaskRequirement {
  /** Task kind used to pick per-kind evidence, e.g. `code-change`. */
  kind: string;
  workspace?: string;
  repo?: string;
  model?: string;
  capabilities?: Partial<WorkerCapabilities>;
}

export interface EvidenceCount {
  attempts: number;
  successes: number;
}

/** Receipt-derived success history for one Worker. */
export interface WorkerEvidence extends EvidenceCount {
  byKind?: Record<string, EvidenceCount>;
}

export type EvidenceMap = Record<string, WorkerEvidence>;

export interface RoutingWeights {
  affinity: number;
  evidence: number;
  load: number;
  preference: number;
}

export const DEFAULT_WEIGHTS: RoutingWeights = {
  affinity: 0.35,
  evidence: 0.4,
  load: 0.15,
  preference: 0.1,
};

export interface RouteReason {
  code: string;
  values?: Record<string, string | number>;
}

export interface RejectedWorker {
  worker: string;
  reason: string;
}

export interface WorkerScore {
  worker: string;
  score: number;
  reasons: RouteReason[];
}

export interface RouteDecision {
  worker: string;
  score: number;
  reasons: RouteReason[];
}

export interface RouteOptions {
  weights?: RoutingWeights;
  /** Operator pin; honored only when the Worker passes the hard filters. */
  prefer?: string;
  /** Reject Workers whose build commit differs from the expected commit. */
  rejectStaleWorkers?: boolean;
  /** Master's expected build commit; `unknown` is treated as unset. */
  expectedBuildCommit?: string;
}

export interface RouteResult {
  decision?: RouteDecision;
  /** Every eligible Worker, best first. */
  candidates: WorkerScore[];
  rejected: RejectedWorker[];
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(1, Math.max(0, value));
}

function round(value: number): number {
  return Math.round(value * 1e6) / 1e6;
}

/** Hard filters: a Worker that fails one is never scored. */
export function rejectReason(
  manifest: WorkerManifest,
  requirement: TaskRequirement,
  options: RouteOptions = {},
): string | undefined {
  if (manifest.health.state === "offline") return "offline";
  if (
    options.rejectStaleWorkers === true &&
    options.expectedBuildCommit !== undefined &&
    options.expectedBuildCommit !== "unknown"
  ) {
    const commit = manifest.build?.commit ?? "unknown";
    if (commit !== options.expectedBuildCommit) return `stale-build:${commit}`;
  }
  if (manifest.load.quota <= 0 || manifest.load.running >= manifest.load.quota)
    return "at-capacity";
  if (requirement.workspace !== undefined) {
    const found = manifest.workspaces.some((workspace) => workspace.id === requirement.workspace);
    if (!found) return `workspace-missing:${requirement.workspace}`;
  }
  if (requirement.repo !== undefined) {
    const found = manifest.workspaces.some((workspace) => workspace.repo === requirement.repo);
    if (!found) return `repo-missing:${requirement.repo}`;
  }
  if (requirement.model !== undefined && !manifest.models.includes(requirement.model))
    return `model-missing:${requirement.model}`;
  for (const [key, expected] of Object.entries(requirement.capabilities ?? {}) as [
    keyof WorkerCapabilities,
    string | boolean,
  ][]) {
    if (manifest.capabilities[key] !== expected) return `capability-mismatch:${key}`;
  }
  return undefined;
}

function affinityOf(
  manifest: WorkerManifest,
  requirement: TaskRequirement,
): { score: number; reason?: RouteReason } {
  if (requirement.workspace !== undefined) {
    const match = manifest.workspaces.some((workspace) => workspace.id === requirement.workspace);
    if (match) return { score: 1, reason: { code: "workspace-affinity" } };
  }
  if (requirement.repo !== undefined) {
    const match = manifest.workspaces.some((workspace) => workspace.repo === requirement.repo);
    if (match) return { score: 0.6, reason: { code: "repo-affinity" } };
  }
  return { score: 0 };
}

function evidenceOf(
  manifest: WorkerManifest,
  requirement: TaskRequirement,
  evidence: EvidenceMap,
): { score: number; reason?: RouteReason } {
  const entry = evidence[manifest.id];
  const counts = entry?.byKind?.[requirement.kind] ?? entry;
  if (!counts || counts.attempts <= 0) return { score: 0 };
  const rate = clamp01(counts.successes / counts.attempts);
  return {
    score: rate,
    reason: { code: "evidence", values: { rate: round(rate), attempts: counts.attempts } },
  };
}

function loadOf(manifest: WorkerManifest): { score: number; reason: RouteReason } {
  const headroom = clamp01((manifest.load.quota - manifest.load.running) / manifest.load.quota);
  return {
    score: headroom,
    reason: {
      code: "load-headroom",
      values: { running: manifest.load.running, quota: manifest.load.quota },
    },
  };
}

/**
 * Route one task: hard filters first, then a weighted score over affinity,
 * receipt evidence, load headroom and an operator pin. Ties break by Worker id
 * so the same inputs always produce the same decision.
 */
export function routeWorkers(
  manifests: readonly WorkerManifest[],
  requirement: TaskRequirement,
  evidence: EvidenceMap = {},
  options: RouteOptions = {},
): RouteResult {
  const weights = options.weights ?? DEFAULT_WEIGHTS;
  const rejected: RejectedWorker[] = [];
  const candidates: WorkerScore[] = [];
  for (const manifest of manifests) {
    const reason = rejectReason(manifest, requirement, options);
    if (reason !== undefined) {
      rejected.push({ worker: manifest.id, reason });
      continue;
    }
    const affinity = affinityOf(manifest, requirement);
    const evidenceScore = evidenceOf(manifest, requirement, evidence);
    const load = loadOf(manifest);
    const preferred = options.prefer === manifest.id;
    const reasons: RouteReason[] = [];
    if (affinity.reason) reasons.push(affinity.reason);
    if (evidenceScore.reason) reasons.push(evidenceScore.reason);
    reasons.push(load.reason);
    if (preferred) reasons.push({ code: "preferred" });
    let score =
      weights.affinity * affinity.score +
      weights.evidence * evidenceScore.score +
      weights.load * load.score +
      weights.preference * (preferred ? 1 : 0);
    if (manifest.health.state === "degraded") {
      score -= 0.15;
      reasons.push({ code: "degraded", values: { penalty: 0.15 } });
    }
    candidates.push({ worker: manifest.id, score: round(score), reasons });
  }
  candidates.sort(
    (left, right) => right.score - left.score || left.worker.localeCompare(right.worker),
  );
  const best = candidates[0];
  return {
    ...(best
      ? { decision: { worker: best.worker, score: best.score, reasons: best.reasons } }
      : {}),
    candidates,
    rejected,
  };
}

/** What the Master should do with one task after routing. */
export type RouteAction = "reuse" | "start" | "queue" | "park";

/** A concrete dispatch plan derived from {@link routeWorkers}. */
export interface RoutePlan {
  action: RouteAction;
  /** Selected Worker for `reuse`/`start`. */
  worker?: string;
  /** Stable reason code, also useful in logs and tests. */
  reason: string;
  /** Full scoring result, including rejected Workers and their reasons. */
  route: RouteResult;
}

export interface RoutePlanOptions extends RouteOptions {
  /**
   * Worker that already owns the requested session key. When it is still
   * eligible the plan reuses it even if another Worker scores slightly higher.
   */
  reuseWorker?: string;
}

/**
 * Turn the single-Worker route into the fleet action the dispatch path uses:
 *
 * - REUSE: the session-affine Worker is still eligible;
 * - START: an eligible Worker has free capacity;
 * - QUEUE: eligible Workers exist but every one is at capacity;
 * - PARK: policy/availability excludes every Worker (offline, workspace/model/
 *   capability mismatch, or no registration at all).
 *
 * The Worker declarations are filters only; the Master never turns them into
 * remote permissions.
 */
export function planTaskRoute(
  manifests: readonly WorkerManifest[],
  requirement: TaskRequirement,
  evidence: EvidenceMap = {},
  options: RoutePlanOptions = {},
): RoutePlan {
  const { reuseWorker, ...routeOptions } = options;
  const route = routeWorkers(manifests, requirement, evidence, routeOptions);
  const eligibility = new Set(route.candidates.map((candidate) => candidate.worker));
  if (reuseWorker !== undefined && eligibility.has(reuseWorker)) {
    return { action: "reuse", worker: reuseWorker, reason: "reuse-session-worker", route };
  }
  if (route.decision !== undefined) {
    return {
      action: "start",
      worker: route.decision.worker,
      reason: "start-free-capacity",
      route,
    };
  }
  if (route.rejected.some((rejected) => rejected.reason === "at-capacity")) {
    return { action: "queue", reason: "at-capacity", route };
  }
  const reason = route.rejected[0]?.reason ?? "no-workers";
  return { action: "park", reason, route };
}

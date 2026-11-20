/**
 * Transport-level contract between a Gethen Master and its Workers.
 *
 * This package must stay free of DSH, Cordis and network imports: adapters
 * (Remote, stdio, ...) implement it, the Master consumes it. See ADR-0008.
 */

export type WorkerId = string;
export type WorkspaceId = string;
export type SessionId = string;
export type TicketId = string;
export type AssignmentId = string;

export type WorkerHealth = "online" | "degraded" | "stale" | "offline";
export type RuntimeKind = "dsh" | "opencode" | "pi";
export type CostClass = "low" | "mid" | "high";

/** What a Worker can accept from the Master. Mirrors the execution contract. */
export interface WorkerCapabilities {
  promptInjection: "system" | "prefix" | "none";
  skills: "filesystem" | "mcp" | "none";
  mcp: boolean;
  permissions: "sandbox" | "approval" | "none";
  streaming: boolean;
  steer: boolean;
  fork: boolean;
}

/** Build identity a Worker reports in hello (commit/builtAt/bundle may be unknown). */
export interface WorkerBuild {
  commit: string;
  builtAt?: string;
  bundle?: string;
}

export interface WorkerWorkspace {
  id: WorkspaceId;
  repo?: string;
  branch?: string;
}

/**
 * Local DSH session-store observation a Worker reports in its manifest.
 * The Master only aggregates these numbers for operators; it never inspects a
 * Worker filesystem itself. `oldestDays` is absent when no session exists.
 */
export interface WorkerSessionStats {
  /** Session directories under the Worker's local DSH home. */
  sessions: number;
  /** Total size of those directories, in bytes. */
  totalBytes: number;
  /** Age in days of the least recently modified session. */
  oldestDays?: number;
  /** Entries in the durable key -> sessionId registries on the Worker. */
  keyRegistryEntries: number;
  /** When the Worker sampled these values (RFC 3339). */
  observedAt: string;
}

/** The Master's view of one selectable Worker instance. */
export interface WorkerManifest {
  id: WorkerId;
  machine: string;
  runtime: RuntimeKind;
  workspaces: WorkerWorkspace[];
  capabilities: WorkerCapabilities;
  models: string[];
  costClass: CostClass;
  load: { running: number; quota: number };
  health: { state: WorkerHealth; lastHeartbeatAt: string; detail?: string };
  /** Build identity; absent on older Workers. */
  build?: WorkerBuild;
  /** Master path -> Worker-local path mapping, for soft workspace fallback. */
  workspaceMap?: Record<string, string>;
  /** Default directory a Worker uses when the requested workspace is absent. */
  defaultWorkspace?: string;
  /** Latest local DSH session-store observation; absent on older Workers. */
  sessions?: WorkerSessionStats;
}

/** One Assignment sent to a Worker. */
export interface DispatchRequest {
  ticket: TicketId;
  assignment: AssignmentId;
  worker: WorkerId;
  workspace: WorkspaceId;
  prompt: string;
  /** Opaque to the bridge: the capability pack as sent to the Worker. */
  pack: unknown;
  model?: string;
  branch?: string;
}

export interface WorkerSessionRef {
  session: SessionId;
  worker: WorkerId;
  workspace: WorkspaceId;
}

export type WorkerState = "running" | "waiting" | "idle" | "ended" | "unknown";

export interface TranscriptEvent {
  seq: number;
  at: string;
  type: string;
  text?: string;
}

export interface TranscriptPage {
  session: SessionId;
  events: TranscriptEvent[];
  /** Older-page cursor; absent when the start of the transcript was reached. */
  next?: { beforeSeq: number; throughSeq: number };
  truncated: boolean;
}

/** What one Assignment reported back to the Master. */
export interface AssignmentReceipt {
  assignment: AssignmentId;
  ticket: TicketId;
  worker: WorkerId;
  workspace: WorkspaceId;
  session: SessionId;
  packDigest: string;
  at: string;
  outcome: "completed" | "failed" | "aborted";
  summary: string;
  events: number;
  usage?: { inputTokens: number; outputTokens: number; costUsd?: number };
}

export type EscalationKind = "decision" | "blocked" | "permission" | "scope" | "conflict";

export interface EscalationOption {
  id: string;
  label: string;
  tradeoff?: string;
}

/**
 * A Worker question. A blocking escalation ends the Worker's turn and moves the
 * Assignment to `waiting`; the Master answers or escalates to a person.
 */
export interface Escalation {
  id: string;
  ticket: TicketId;
  assignment: AssignmentId;
  session: SessionId;
  kind: EscalationKind;
  question: string;
  options: EscalationOption[];
  context: { summary: string; tried?: string[]; evidence?: string[] };
  /** Safe option for reversible questions; the Master may pick it explicitly. */
  defaultOption?: string;
  blocking: boolean;
  at: string;
}

export type ResolutionBasis = "policy" | "memory" | "inference";

export type Resolution =
  | {
      id: string;
      escalation: string;
      kind: "answer";
      option?: string;
      text: string;
      confidence: number;
      basis: ResolutionBasis[];
      at: string;
    }
  | {
      id: string;
      escalation: string;
      kind: "escalate";
      to: "human";
      question: string;
      options?: EscalationOption[];
      at: string;
    }
  | {
      id: string;
      escalation: string;
      kind: "reroute";
      worker: WorkerId;
      reason: string;
      at: string;
    }
  | { id: string; escalation: string; kind: "drop"; reason: string; at: string };

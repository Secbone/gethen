/**
 * Pure control-plane domain: work authorization and ticket lifecycle.
 * No IO, no clock, no DSH imports. See docs/design/p1-master-research.md.
 */

export type SourceId = string;
export type ThreadId = string;
export type TriggerId = string;
export type EvalId = string;
export type DecisionId = string;
export type TicketId = string;
export type MandateId = string;
export type AssignmentId = string;
export type InputId = string;
export type EffectId = string;

export type Strength = "explicit" | "inferred" | "not_admitted";
export type AcceptMode = "off" | "observe" | "auto";
export type DecisionOutcome = "accept" | "stop" | "ignore" | "unavailable";
export type AssignmentState = "pending" | "waiting" | "running" | "ended";
export type ReportKind = "progress" | "waiting" | "ready" | "blocked" | "failed" | "dropped";
export type TicketOutcome = "merged" | "closed" | "completed" | "dropped" | "stopped";
export type Addressed = "mention" | "direct" | "none";

export const DAY_MS = 86_400_000;
export const DEFAULT_EVAL_DEADLINE_MS = 5 * 60_000;
export const MAX_EVAL_EXPIRIES = 3;

/** One immutable event from a Source. */
export interface Trigger {
  id: TriggerId;
  source: SourceId;
  thread: ThreadId;
  actor: string;
  bot: boolean;
  addressed: Addressed;
  text: string;
  /** RFC 3339 UTC. */
  at: string;
  meta?: Readonly<Record<string, string>>;
}

/** Triggers in time order; the unit within which one Eval runs at a time. */
export interface Thread {
  id: ThreadId;
  source: SourceId;
  triggers: TriggerId[];
  /** Triggers waiting for the next Eval on this Thread. */
  pending: TriggerId[];
  subscriptions: TicketId[];
  activeEval?: EvalId;
  /** Consecutive Evals that expired without a decision. */
  expired?: number;
}

export interface Reason {
  code: string;
  values?: Readonly<Record<string, string | number>>;
  triggerId?: TriggerId;
  text?: string;
}

export interface Eval {
  id: EvalId;
  thread: ThreadId;
  triggers: TriggerId[];
  policyVersion: number;
  startedAt: string;
  deadlineAt: string;
  deliveredAt?: string;
}

export interface Decision {
  id: DecisionId;
  eval: EvalId;
  outcome: DecisionOutcome;
  strength: Strength;
  reasons: Reason[];
  at: string;
  dismissedAt?: string;
}

/** Facts a Source supplies to the gate; the core never parses message text. */
export interface TriggerFacts {
  source: SourceId;
  sender: string;
  addressed: Addressed;
  bot: boolean;
  /** A person's message on a Thread that already has Tickets. */
  followup: boolean;
  /** Receiving scope (chat, repository area, direct message, ...). */
  scope: string;
}

export interface PolicyRule {
  id: string;
  name: string;
  instruction: string;
  enabled: boolean;
}

export interface SourcePolicy {
  /** Identities allowed to originate explicit work on this Source. */
  allowed: string[];
  /** Bots whose messages may become inferred candidates. */
  bots: string[];
}

/**
 * P1 keeps a single-operator policy: no cross-source admission matrix. The
 * accept mode and the per-source allowlists are the whole authorization surface.
 */
export interface Policy {
  version: number;
  acceptMode: AcceptMode;
  rules: PolicyRule[];
  sources: Record<SourceId, SourcePolicy>;
}

export interface MandateBasis {
  via: "addressed" | "followup" | "admission" | "bot";
  source: SourceId;
  sender: string;
  scope?: string;
}

/** Why work may be done, how far, and under which Policy version. */
export interface Mandate {
  id: MandateId;
  ticket: TicketId;
  policyVersion: number;
  strength: Strength;
  scope: string;
  basis: MandateBasis;
  matchedRules: string[];
  createdAt: string;
  withdrawnAt?: string;
}

export interface TicketAttention {
  question: string;
  options?: { id: string; label: string }[];
  askedAt: string;
}

/** A unit of authorized work. Its state never follows a Worker session. */
export interface Ticket {
  id: TicketId;
  mandate: MandateId;
  source: SourceId;
  threads: ThreadId[];
  subscriptions: ThreadId[];
  channels: string[];
  inputs: InputId[];
  assignments: AssignmentId[];
  openedAt: string;
  endedAt?: string;
  outcome?: TicketOutcome;
  latestReport?: ReportKind;
  attention?: TicketAttention;
  /** Last input delivery failed; retried with backoff. */
  delivery?: { error: string; at: string; attempt: number };
}

/** The binding of a Ticket to one Worker instance. */
export interface Assignment {
  id: AssignmentId;
  ticket: TicketId;
  worker: string;
  workspace: string;
  /** Digest of the Capability Pack the Worker received. */
  packDigest: string;
  state: AssignmentState;
  /** Idempotency key for the launch effect. */
  operationId: string;
  startedAt?: string;
  endedAt?: string;
}

/** What an Assignment reports to its Ticket. */
export interface Report {
  kind: ReportKind;
  summary: string;
  /** One concrete question for a person, when the Report needs one. */
  question?: string;
  at: string;
}

export interface Input {
  id: InputId;
  ticket: TicketId;
  kind: "trigger" | "policy" | "resume" | "text";
  text?: string;
  trigger?: TriggerId;
  deliveredAt?: string;
  readAt?: string;
  fromPerson: boolean;
}

/** A durable effect; executed at least once, keyed for idempotence. */
export interface Effect {
  id: EffectId;
  kind: string;
  key: string;
  payload: unknown;
  status: "waiting" | "running" | "done" | "failed";
  attempt: number;
  dueAt: string;
  createdAt: string;
  doneAt?: string;
}

/** Evidence written when an Assignment ends. */
export interface AssignmentReceipt {
  assignment: AssignmentId;
  ticket: TicketId;
  worker: string;
  workspace: string;
  packDigest: string;
  at: string;
  outcome: "completed" | "failed" | "aborted";
  summary: string;
  events: number;
  usage?: { inputTokens: number; outputTokens: number; costUsd?: number };
}

export type TicketStage =
  | "person/delivery"
  | "person/answer"
  | "person/review"
  | "person/blocked"
  | "person/failed"
  | "working/queued"
  | "idle"
  | "working";

export interface StageFacts {
  /** A pending or running Assignment exists. */
  liveAssignment: boolean;
  /** A person's message arrived since the latest report. */
  personSinceReport: boolean;
  /** Delivery of the latest input failed. */
  deliveryFailed: boolean;
}

/**
 * First matching row wins. Mirrors the reference implementation's stage table so
 * the panel and the engine agree about "who is this waiting for".
 */
export function ticketStage(ticket: Ticket, facts: StageFacts): Exclude<TicketStage, never> {
  if (ticket.endedAt !== undefined) return "idle";
  if (facts.deliveryFailed) return "person/delivery";
  if (ticket.attention !== undefined) return "person/answer";
  const report = ticket.latestReport;
  if (report === "ready") return "person/review";
  if ((report === "blocked" || report === "failed") && !facts.personSinceReport)
    return report === "blocked" ? "person/blocked" : "person/failed";
  if (!facts.liveAssignment) return "working/queued";
  if (report === "waiting") return "idle";
  return "working";
}

/** A ticket is open while it has no end; `stop` ends it and withdraws the mandate. */
export function isOpen(ticket: Ticket): boolean {
  return ticket.endedAt === undefined;
}

/** A Worker's structured request for a decision (P3b). */
export type EscalationKindLike = "decision" | "blocked" | "permission" | "scope" | "conflict";
export type EscalationStatus = "waiting" | "resolved" | "parked";

export interface EscalationOptionLike {
  id: string;
  label: string;
}

export interface EscalationRecord {
  id: string;
  ticket: TicketId;
  assignment: AssignmentId;
  session: string;
  kind: EscalationKindLike;
  question: string;
  options: EscalationOptionLike[];
  defaultOption?: string;
  status: EscalationStatus;
  at: string;
  resolution?: { kind: "answer" | "escalate" | "drop"; text: string; option?: string; at: string };
}

export interface EscalationInput {
  id: string;
  assignment: AssignmentId;
  session: string;
  kind: EscalationKindLike;
  question: string;
  options: EscalationOptionLike[];
  defaultOption?: string;
}

export interface EscalationResolutionInput {
  kind: "answer" | "escalate" | "drop";
  text: string;
  option?: string;
}

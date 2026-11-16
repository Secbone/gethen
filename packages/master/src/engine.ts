/**
 * The pure control plane: Triggers in; Decisions, Tickets, Mandates,
 * Assignments and outbox effects out. No clock, no randomness, no IO beyond the
 * injected repositories; every external side effect leaves through `drain()`.
 *
 * Design notes:
 * - At most one Eval runs per Thread. Triggers that arrive meanwhile wait in
 *   `thread.pending` and form the next batch.
 * - An Eval that outlives `evalDeadlineMs` is retired and its batch is retried,
 *   up to `maxEvalExpiries` consecutive times; the expiry after that records an
 *   `unavailable` Decision and lets the Thread settle.
 * - Accepting creates the whole work unit (Mandate + Ticket + Subscription +
 *   pending Assignment) and queues exactly one `launch` effect. Replaying an
 *   accept returns the recorded Decision and creates nothing.
 */
import {
  type Assignment,
  DEFAULT_EVAL_DEADLINE_MS,
  type Decision,
  type DecisionOutcome,
  type EscalationInput,
  type EscalationRecord,
  type EscalationResolutionInput,
  type Eval,
  type Input,
  MAX_EVAL_EXPIRIES,
  type Mandate,
  type MandateBasis,
  type Policy,
  type Reason,
  type Report,
  type Strength,
  type Thread,
  type Ticket,
  type TicketAttention,
  type Trigger,
} from "./model";
import type { Clock, Ids, PendingEffect, Repositories } from "./ports";

/** Everything the engine may read, with time and ids injected by the caller. */
export interface EngineOptions {
  repos: Repositories;
  clock: Clock;
  ids: Ids;
  /** How long a started Eval may stay undecided before it expires. */
  evalDeadlineMs?: number;
  /** Consecutive expiries after which a Thread records `unavailable`. */
  maxEvalExpiries?: number;
}

/** The active Eval a Trigger belongs to. */
export interface Candidate {
  eval: Eval;
  thread: Thread;
  trigger: Trigger;
}

/** The outbox payload of a `launch` effect. */
export interface LaunchEffectPayload {
  ticket: Ticket;
  assignment: Assignment;
  packDigest: string;
}

export class EngineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EngineError";
  }
}

/**
 * The single writer of the control-plane state. Commands are synchronous and
 * deterministic: time comes from `clock`, ids from `ids`, and external work is
 * returned by `drain()` instead of being performed here.
 */
export class Engine {
  private readonly repos: Repositories;
  private readonly clock: Clock;
  private readonly ids: Ids;
  private readonly evalDeadlineMs: number;
  private readonly maxEvalExpiries: number;
  private readonly effects: PendingEffect[] = [];
  /**
   * Escalations this process saw, keyed by Ticket. `resolve` returns the same
   * record it was given by `escalate`; the caller persists records through the
   * store, so the engine keeps only the in-flight ones.
   */
  private readonly waitingEscalations = new Map<string, EscalationRecord>();

  constructor(options: EngineOptions) {
    this.repos = options.repos;
    this.clock = options.clock;
    this.ids = options.ids;
    this.evalDeadlineMs = options.evalDeadlineMs ?? DEFAULT_EVAL_DEADLINE_MS;
    this.maxEvalExpiries = options.maxEvalExpiries ?? MAX_EVAL_EXPIRIES;
  }

  /**
   * Store one Source event once, open its Thread if needed, and let it wait for
   * the next pump. Receiving never starts an Eval by itself.
   */
  receive(trigger: Trigger): "stored" | "duplicate" {
    const existing = this.repos.triggers
      .list()
      .find((item) => item.source === trigger.source && item.id === trigger.id);
    if (existing) return "duplicate";

    this.repos.triggers.save(trigger);
    const thread = this.threadFor(trigger);
    thread.triggers.push(trigger.id);
    thread.pending.push(trigger.id);
    this.repos.threads.save(thread);
    return "stored";
  }

  /**
   * Expire Evals past their deadline and start one Eval per Thread that has
   * Triggers waiting. Returns how many Evals started, so a repeated call with
   * no new work returns 0. An expired Eval's batch is requeued for the next
   * pump rather than restarted within this one.
   */
  evaluateDue(now?: Date): number {
    const at = now ?? this.clock.now();
    const policy = this.repos.policy.current();
    let started = 0;
    for (const thread of this.repos.threads.list()) {
      if (thread.activeEval !== undefined) {
        this.expireEval(thread, at);
        continue;
      }
      if (thread.pending.length === 0) continue;
      this.startEval(thread, at, policy);
      started += 1;
    }
    return started;
  }

  /** The Eval that currently holds this Trigger, if any. */
  candidate(triggerId: string): Candidate | undefined {
    const trigger = this.repos.triggers.get(triggerId);
    if (!trigger) return undefined;
    const thread = this.repos.threads.get(trigger.thread);
    if (!thread || thread.activeEval === undefined) return undefined;
    const evaluation = this.repos.evals.get(thread.activeEval);
    if (!evaluation?.triggers.includes(triggerId)) return undefined;
    return { eval: evaluation, thread, trigger };
  }

  /**
   * Record the outcome of the Eval holding `triggerId`. Only an active Eval can
   * decide; replaying a Trigger that already has a Decision returns it. When
   * the Policy version moved during the Eval the outcome is recorded as
   * `unavailable` and the batch is queued again under the current Policy.
   */
  decide(triggerId: string, outcome: DecisionOutcome, reasons: Reason[] = []): Decision {
    const at = this.clock.now();
    const view = this.candidate(triggerId);
    if (!view) return this.replayDecision(triggerId);

    const existing = this.repos.decisions.list().find((item) => item.eval === view.eval.id);
    if (existing) {
      this.release(view.thread);
      return existing;
    }

    const policy = this.repos.policy.current();
    const stale = view.eval.policyVersion !== policy.version;
    const finalReasons = [...reasons];
    if (stale) {
      finalReasons.push({
        code: "policy_version_changed",
        values: { eval: view.eval.policyVersion, current: policy.version },
      });
    }
    const finalOutcome: DecisionOutcome = stale ? "unavailable" : outcome;

    if (finalOutcome === "accept") {
      const prior = this.acceptedDecision(triggerId);
      if (prior) {
        // The Trigger already produced its Ticket; retire this Eval without new work.
        this.recordDecision(
          view.eval,
          view.thread,
          "ignore",
          [{ code: "already_accepted", triggerId }],
          at,
          0,
        );
        return prior;
      }
    }

    const decision = this.recordDecision(view.eval, view.thread, finalOutcome, finalReasons, at, 0);
    if (stale) {
      this.requeue(view.thread, view.eval.triggers);
      this.repos.threads.save(view.thread);
    }
    if (finalOutcome === "accept") this.accept(view, policy, at);
    return decision;
  }

  /** The Worker picked a pending Assignment up. */
  assignmentStarted(ticketId: string, assignmentId: string): void {
    const ticket = this.requireTicket(ticketId);
    const assignment = this.repos.assignments.get(assignmentId);
    if (!assignment || assignment.ticket !== ticket.id) {
      throw new EngineError(`assignment ${assignmentId} does not belong to ticket ${ticketId}`);
    }
    assignment.state = "running";
    assignment.startedAt = this.timestamp();
    this.repos.assignments.save(assignment);
  }

  /**
   * A Worker's Report: the latest kind drives the Ticket stage, a question
   * becomes Attention, and the running Assignment ends with the report.
   */
  report(ticketId: string, report: Report): void {
    const ticket = this.requireTicket(ticketId);
    ticket.latestReport = report.kind;
    if (report.question !== undefined && report.question.length > 0) {
      ticket.attention = { question: report.question, askedAt: report.at };
    } else {
      delete ticket.attention;
    }
    this.repos.tickets.save(ticket);
    this.endRunningAssignments(ticket, report.at);
  }

  /**
   * A Worker asks for a decision: the Ticket shows Attention (stage
   * `person/answer`) and its Assignment waits for an answer or for a person.
   * Returns the record the caller persists; `resolve` updates the same one.
   */
  escalate(ticketId: string, input: EscalationInput): EscalationRecord {
    const ticket = this.requireTicket(ticketId);
    if (ticket.endedAt !== undefined) {
      throw new EngineError(`ticket ${ticketId} has ended and cannot escalate`);
    }
    const at = this.timestamp();
    const options = input.options.map((option) => ({ ...option }));
    const assignment = this.assignmentFor(ticket, input.assignment);
    if (assignment !== undefined) {
      assignment.state = "waiting";
      this.repos.assignments.save(assignment);
    }

    ticket.attention = {
      question: input.question,
      options: options.map((option) => ({ ...option })),
      askedAt: at,
    };
    this.repos.tickets.save(ticket);

    const record: EscalationRecord = {
      id: input.id,
      ticket: ticket.id,
      assignment: assignment?.id ?? input.assignment,
      session: input.session,
      kind: input.kind,
      question: input.question,
      options,
      status: "waiting",
      at,
    };
    if (input.defaultOption !== undefined) record.defaultOption = input.defaultOption;
    this.waitingEscalations.set(ticketId, record);
    return record;
  }

  /**
   * Answer a waiting escalation (or any Attention written by a report): clear
   * the Attention, resume the Assignment that waited, append the answer as a
   * text Input, and return the escalation record with its resolution.
   */
  resolve(ticketId: string, resolution: EscalationResolutionInput): EscalationRecord {
    const ticket = this.requireTicket(ticketId);
    const at = this.timestamp();
    const pending = this.waitingEscalations.get(ticketId);
    const attention = ticket.attention;

    let record: EscalationRecord;
    if (pending !== undefined) {
      record = { ...pending };
    } else if (attention !== undefined) {
      // Attention written by a report, or an escalation from before a restart:
      // rebuild what is still stored on the Ticket instead of losing it.
      record = this.fallbackEscalation(ticket, attention);
    } else {
      throw new EngineError(`ticket ${ticketId} has no escalation waiting`);
    }
    this.waitingEscalations.delete(ticketId);

    const resolutionRecord: NonNullable<EscalationRecord["resolution"]> = {
      kind: resolution.kind,
      text: resolution.text,
      at,
    };
    if (resolution.option !== undefined) resolutionRecord.option = resolution.option;
    record.status = resolution.kind === "answer" ? "resolved" : "parked";
    record.resolution = resolutionRecord;

    const input: Input = {
      id: this.ids.next("input"),
      ticket: ticketId,
      kind: "text",
      text: resolution.text,
      fromPerson: false,
    };
    this.repos.inputs.save(input);
    ticket.inputs.push(input.id);
    delete ticket.attention;
    this.repos.tickets.save(ticket);

    for (const assignmentId of ticket.assignments) {
      const assignment = this.repos.assignments.get(assignmentId);
      if (assignment?.state !== "waiting") continue;
      assignment.state = "running";
      this.repos.assignments.save(assignment);
    }
    return record;
  }

  /**
   * A person's stop: end the Ticket, withdraw its Mandate (so it can never be
   * resumed) and end its running work. There is no resume command in P1.
   */
  stopTicket(ticketId: string, reason: string): void {
    const ticket = this.requireTicket(ticketId);
    const at = this.timestamp();
    ticket.endedAt = at;
    ticket.outcome = "stopped";
    this.repos.tickets.save(ticket);

    const mandate = this.repos.mandates.get(ticket.mandate);
    if (mandate) {
      mandate.withdrawnAt = at;
      this.repos.mandates.save(mandate);
    }
    this.endRunningAssignments(ticket, at);
    // The reason belongs to the caller's audit trail; P1 writes no Decision for
    // a manual stop.
    void reason;
  }

  /** Start another Assignment for the same open Ticket and queue its launch. */
  retryTicket(ticketId: string): Assignment {
    const ticket = this.requireTicket(ticketId);
    if (ticket.endedAt !== undefined) {
      throw new EngineError(`ticket ${ticketId} has ended and cannot be retried`);
    }
    const at = this.clock.now();
    const assignment = this.newAssignment(ticketId);
    this.repos.assignments.save(assignment);
    ticket.assignments.push(assignment.id);
    this.repos.tickets.save(ticket);
    this.enqueueLaunch(ticket, assignment, at);
    return assignment;
  }

  /** Add a free-text Input for the Ticket. */
  note(ticketId: string, text: string): Input {
    const ticket = this.requireTicket(ticketId);
    const input: Input = {
      id: this.ids.next("input"),
      ticket: ticketId,
      kind: "text",
      text,
      fromPerson: false,
    };
    this.repos.inputs.save(input);
    ticket.inputs.push(input.id);
    this.repos.tickets.save(ticket);
    return input;
  }

  /**
   * Take a Trigger away from Eval and hand it to an existing Ticket: it becomes
   * an Input and its Thread joins the Ticket.
   */
  attach(ticketId: string, triggerId: string): void {
    const ticket = this.requireTicket(ticketId);
    const trigger = this.repos.triggers.get(triggerId);
    if (!trigger) throw new EngineError(`trigger ${triggerId} not found`);

    const input: Input = {
      id: this.ids.next("input"),
      ticket: ticketId,
      kind: "trigger",
      trigger: triggerId,
      fromPerson: !trigger.bot,
    };
    this.repos.inputs.save(input);
    ticket.inputs.push(input.id);
    if (!ticket.threads.includes(trigger.thread)) ticket.threads.push(trigger.thread);
    if (!ticket.subscriptions.includes(trigger.thread)) ticket.subscriptions.push(trigger.thread);
    this.repos.tickets.save(ticket);

    const thread = this.repos.threads.get(trigger.thread);
    if (!thread) return;
    if (!thread.subscriptions.includes(ticketId)) thread.subscriptions.push(ticketId);
    thread.pending = thread.pending.filter((id) => id !== triggerId);
    this.repos.threads.save(thread);
  }

  /**
   * The task text a Ticket carries: its Trigger inputs (in order) plus any
   * free-text notes, joined for the assignment envelope.
   */
  ticketText(ticketId: string): string | undefined {
    const ticket = this.repos.tickets.get(ticketId);
    if (!ticket) return undefined;
    const parts: string[] = [];
    for (const inputId of ticket.inputs) {
      const input = this.repos.inputs.get(inputId);
      if (!input) continue;
      const text =
        input.text ??
        (input.trigger === undefined ? undefined : this.repos.triggers.get(input.trigger)?.text);
      if (text !== undefined && text.trim().length > 0) parts.push(text.trim());
    }
    return parts.length === 0 ? undefined : parts.join("\n\n---\n\n");
  }

  /** Return and clear the effects the engine has queued since the last drain. */
  drain(): PendingEffect[] {
    return this.effects.splice(0, this.effects.length);
  }

  private threadFor(trigger: Trigger): Thread {
    const existing = this.repos.threads.get(trigger.thread);
    if (existing) return existing;
    const thread: Thread = {
      id: trigger.thread,
      source: trigger.source,
      triggers: [],
      pending: [],
      subscriptions: [],
    };
    this.repos.threads.save(thread);
    return thread;
  }

  private expireEval(thread: Thread, now: Date): void {
    const evalId = thread.activeEval;
    if (evalId === undefined) return;
    const evaluation = this.repos.evals.get(evalId);
    if (!evaluation) {
      thread.activeEval = undefined;
      this.repos.threads.save(thread);
      return;
    }
    if (Date.parse(evaluation.deadlineAt) > now.getTime()) return;

    const expiries = (thread.expired ?? 0) + 1;
    if (expiries < this.maxEvalExpiries) {
      thread.expired = expiries;
      thread.activeEval = undefined;
      this.requeue(thread, evaluation.triggers);
      this.repos.threads.save(thread);
      return;
    }
    this.recordDecision(
      evaluation,
      thread,
      "unavailable",
      [{ code: "eval_expired", values: { expiries } }],
      now,
      expiries,
    );
  }

  private startEval(thread: Thread, now: Date, policy: Policy): void {
    // A Thread that settled after its expiry budget starts a new batch afresh.
    if ((thread.expired ?? 0) >= this.maxEvalExpiries) thread.expired = 0;
    const evaluation: Eval = {
      id: this.ids.next("eval"),
      thread: thread.id,
      triggers: [...thread.pending],
      policyVersion: policy.version,
      startedAt: now.toISOString(),
      deadlineAt: new Date(now.getTime() + this.evalDeadlineMs).toISOString(),
    };
    thread.pending = [];
    thread.activeEval = evaluation.id;
    this.repos.evals.save(evaluation);
    this.repos.threads.save(thread);
  }

  /** Put a batch back in front of whatever arrived while its Eval ran. */
  private requeue(thread: Thread, triggerIds: readonly string[]): void {
    const waiting = thread.pending.filter((id) => !triggerIds.includes(id));
    thread.pending = [...triggerIds, ...waiting];
  }

  private recordDecision(
    evaluation: Eval,
    thread: Thread,
    outcome: DecisionOutcome,
    reasons: Reason[],
    at: Date,
    expired: number,
  ): Decision {
    const decision: Decision = {
      id: this.ids.next("decision"),
      eval: evaluation.id,
      outcome,
      strength: this.strengthOf(evaluation),
      reasons,
      at: at.toISOString(),
    };
    this.repos.decisions.save(decision);
    thread.activeEval = undefined;
    thread.expired = expired;
    this.repos.threads.save(thread);
    return decision;
  }

  private accept(view: Candidate, policy: Policy, now: Date): void {
    const at = now.toISOString();
    const ticketId = this.ids.next("ticket");
    const mandate: Mandate = {
      id: this.ids.next("mandate"),
      ticket: ticketId,
      policyVersion: policy.version,
      strength: this.strengthOf(view.eval),
      scope: this.scopeOf(view.trigger),
      basis: this.basisFor(view.trigger),
      matchedRules: policy.rules.filter((rule) => rule.enabled).map((rule) => rule.id),
      createdAt: at,
    };
    const assignment = this.newAssignment(ticketId);
    const workspace = view.trigger.meta?.workspace ?? this.scopeOf(view.trigger);
    if (workspace !== "") assignment.workspace = workspace;
    const input: Input = {
      id: this.ids.next("input"),
      ticket: ticketId,
      kind: "trigger",
      trigger: view.trigger.id,
      text: view.trigger.text,
      fromPerson: !view.trigger.bot,
    };
    const ticket: Ticket = {
      id: ticketId,
      mandate: mandate.id,
      source: view.trigger.source,
      threads: [view.thread.id],
      subscriptions: [view.thread.id],
      channels: [],
      inputs: [input.id],
      assignments: [assignment.id],
      openedAt: at,
    };

    this.repos.mandates.save(mandate);
    this.repos.inputs.save(input);
    this.repos.assignments.save(assignment);
    this.repos.tickets.save(ticket);
    if (!view.thread.subscriptions.includes(ticket.id)) {
      view.thread.subscriptions.push(ticket.id);
      this.repos.threads.save(view.thread);
    }
    this.enqueueLaunch(ticket, assignment, now);
  }

  private newAssignment(ticketId: string): Assignment {
    const id = this.ids.next("assignment");
    return {
      id,
      ticket: ticketId,
      worker: "",
      workspace: "",
      packDigest: "",
      state: "pending",
      operationId: `launch:${id}`,
    };
  }

  private enqueueLaunch(ticket: Ticket, assignment: Assignment, now: Date): void {
    // Snapshot the entities: later mutations (a start, a report) must not change
    // what the relay will execute.
    const payload: LaunchEffectPayload = {
      ticket: {
        ...ticket,
        threads: [...ticket.threads],
        subscriptions: [...ticket.subscriptions],
        channels: [...ticket.channels],
        inputs: [...ticket.inputs],
        assignments: [...ticket.assignments],
      },
      assignment: { ...assignment },
      packDigest: assignment.packDigest,
    };
    this.effects.push({
      kind: "launch",
      key: assignment.id,
      payload,
      dueAt: now.toISOString(),
    });
  }

  private endRunningAssignments(ticket: Ticket, at: string): void {
    for (const assignmentId of ticket.assignments) {
      const assignment = this.repos.assignments.get(assignmentId);
      if (assignment?.state !== "running") continue;
      assignment.state = "ended";
      assignment.endedAt = at;
      this.repos.assignments.save(assignment);
    }
  }

  /** The Assignment an escalation waits: the named one, else the latest. */
  private assignmentFor(ticket: Ticket, preferred: string): Assignment | undefined {
    if (preferred.length > 0) {
      const named = this.repos.assignments.get(preferred);
      if (named !== undefined && named.ticket === ticket.id) return named;
    }
    const latestId = ticket.assignments.at(-1);
    return latestId === undefined ? undefined : this.repos.assignments.get(latestId);
  }

  /** Rebuild an escalation record from Attention the engine did not create. */
  private fallbackEscalation(ticket: Ticket, attention: TicketAttention): EscalationRecord {
    const latestId = ticket.assignments.at(-1);
    return {
      id: this.ids.next("escalation"),
      ticket: ticket.id,
      assignment: latestId ?? "",
      session: "",
      kind: "decision",
      question: attention.question,
      options: (attention.options ?? []).map((option) => ({ ...option })),
      status: "waiting",
      at: attention.askedAt,
    };
  }

  private release(thread: Thread): void {
    if (thread.activeEval === undefined) return;
    thread.activeEval = undefined;
    thread.expired = 0;
    this.repos.threads.save(thread);
  }

  private replayDecision(triggerId: string): Decision {
    const decision = this.decisionsCovering(triggerId).at(-1);
    if (!decision) throw new EngineError(`trigger ${triggerId} has no active eval`);
    return decision;
  }

  private decisionsCovering(triggerId: string): Decision[] {
    return this.repos.decisions.list().filter((decision) => {
      const evaluation = this.repos.evals.get(decision.eval);
      return evaluation?.triggers.includes(triggerId) ?? false;
    });
  }

  private acceptedDecision(triggerId: string): Decision | undefined {
    return this.decisionsCovering(triggerId)
      .filter((decision) => decision.outcome === "accept")
      .at(-1);
  }

  /** The strongest strength among a batch's Triggers. */
  private strengthOf(evaluation: Eval): Strength {
    let strength: Strength = "not_admitted";
    for (const triggerId of evaluation.triggers) {
      const trigger = this.repos.triggers.get(triggerId);
      if (!trigger) continue;
      const candidate: Strength = trigger.bot
        ? "inferred"
        : trigger.addressed === "none"
          ? "not_admitted"
          : "explicit";
      if (candidate === "explicit") return "explicit";
      if (candidate === "inferred") strength = "inferred";
    }
    return strength;
  }

  private scopeOf(trigger: Trigger): string {
    return trigger.meta?.scope ?? "";
  }

  private basisFor(trigger: Trigger): MandateBasis {
    return {
      via: trigger.bot ? "bot" : trigger.addressed === "none" ? "admission" : "addressed",
      source: trigger.source,
      sender: trigger.actor,
      scope: this.scopeOf(trigger),
    };
  }

  private requireTicket(ticketId: string): Ticket {
    const ticket = this.repos.tickets.get(ticketId);
    if (!ticket) throw new EngineError(`ticket ${ticketId} not found`);
    return ticket;
  }

  private timestamp(when?: Date): string {
    return (when ?? this.clock.now()).toISOString();
  }
}

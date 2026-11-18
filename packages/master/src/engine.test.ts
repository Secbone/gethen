import { describe, expect, it } from "vitest";
import { Engine, EngineError, type LaunchEffectPayload } from "./engine";
import { needsAction } from "./lifecycle";
import { MemoryRepositories } from "./memory";
import {
  type EscalationInput,
  type Policy,
  type Report,
  type StageFacts,
  type Ticket,
  type TicketStage,
  type Trigger,
  ticketStage,
} from "./model";

const START = "2026-11-16T09:00:00.000Z";

function policy(version = 1): Policy {
  return {
    version,
    acceptMode: "auto",
    rules: [
      {
        id: "fix-bug",
        name: "Fix a bug",
        instruction: "Reproduce the reported bug and fix it",
        enabled: true,
      },
    ],
    sources: { github: { allowed: ["alice"], bots: ["dependabot"] } },
  };
}

function trigger(id: string, overrides: Partial<Trigger> = {}): Trigger {
  return {
    id,
    source: "github",
    thread: "thread-1",
    actor: "alice",
    bot: false,
    addressed: "mention",
    text: `request ${id}`,
    at: START,
    ...overrides,
  };
}

function report(kind: Report["kind"], overrides: Partial<Report> = {}): Report {
  return { kind, summary: `${kind} report`, at: "2026-11-16T09:05:00.000Z", ...overrides };
}

function facts(overrides: Partial<StageFacts> = {}): StageFacts {
  return { liveAssignment: false, personSinceReport: false, deliveryFailed: false, ...overrides };
}

function setup(
  options: { policy?: Policy; evalDeadlineMs?: number; maxEvalExpiries?: number } = {},
) {
  const repos = new MemoryRepositories(options.policy ?? policy());
  let now = new Date(START);
  let counter = 0;
  const engine = new Engine({
    repos,
    clock: { now: () => new Date(now) },
    ids: { next: (prefix) => `${prefix}-${++counter}` },
    evalDeadlineMs: options.evalDeadlineMs ?? 60_000,
    maxEvalExpiries: options.maxEvalExpiries ?? 3,
  });
  return {
    repos,
    engine,
    at: () => now.toISOString(),
    advance(ms: number) {
      now = new Date(now.getTime() + ms);
    },
    setPolicy(next: Policy) {
      repos.setPolicy(next);
    },
    receive(id: string, overrides: Partial<Trigger> = {}) {
      return engine.receive(trigger(id, overrides));
    },
  };
}

type Context = ReturnType<typeof setup>;

function onlyTicket(ctx: Context): Ticket {
  const ticket = ctx.repos.tickets.list()[0];
  if (!ticket) throw new Error("expected one ticket");
  return ticket;
}

function lastAssignment(ctx: Context, ticket: Ticket) {
  const id = ticket.assignments.at(-1);
  const assignment = id === undefined ? undefined : ctx.repos.assignments.get(id);
  if (!assignment) throw new Error("expected one assignment");
  return assignment;
}

/** Receive one Trigger, pump it and accept it: the happy path into a Ticket. */
function accepted(ctx: Context, triggerId = "t1") {
  ctx.receive(triggerId);
  ctx.engine.evaluateDue();
  const decision = ctx.engine.decide(triggerId, "accept");
  return { decision, ticket: onlyTicket(ctx) };
}

describe("MemoryRepositories", () => {
  it("keeps each collection in its own map and swaps policy snapshots", () => {
    const repos = new MemoryRepositories(policy(4));
    expect(repos.policy.current().version).toBe(4);
    repos.setPolicy(policy(5));
    expect(repos.policy.current().version).toBe(5);

    repos.triggers.save(trigger("t1"));
    expect(repos.triggers.get("t1")?.id).toBe("t1");
    expect(repos.triggers.list()).toHaveLength(1);
    repos.triggers.save(trigger("t1", { text: "replaced" }));
    expect(repos.triggers.list()).toHaveLength(1);
    expect(repos.triggers.get("t1")?.text).toBe("replaced");
    expect(repos.threads.list()).toEqual([]);
  });
});

describe("intake", () => {
  it("stores a Trigger once and opens its Thread", () => {
    const ctx = setup();
    expect(ctx.receive("t1")).toBe("stored");
    expect(ctx.receive("t1")).toBe("duplicate");

    expect(ctx.repos.triggers.list()).toHaveLength(1);
    const thread = ctx.repos.threads.get("thread-1");
    expect(thread?.source).toBe("github");
    expect(thread?.triggers).toEqual(["t1"]);
    expect(thread?.pending).toEqual(["t1"]);
  });

  it("keeps the same Trigger id on another Source as a separate event", () => {
    const ctx = setup();
    expect(ctx.receive("t1")).toBe("stored");
    expect(ctx.receive("t1", { source: "feishu", thread: "thread-2" })).toBe("stored");
    expect(ctx.receive("t1", { source: "feishu", thread: "thread-2" })).toBe("duplicate");

    expect(ctx.repos.triggers.list()).toHaveLength(2);
    expect(ctx.repos.threads.list()).toHaveLength(2);
  });
});

describe("eval scheduling", () => {
  it("starts one Eval per waiting Thread and stays idempotent", () => {
    const ctx = setup();
    ctx.receive("t1");
    ctx.receive("t2", { thread: "thread-2" });

    expect(ctx.engine.evaluateDue()).toBe(2);
    expect(ctx.engine.evaluateDue()).toBe(0);
    expect(ctx.repos.evals.list()).toHaveLength(2);
    expect(ctx.repos.threads.list().every((thread) => thread.activeEval !== undefined)).toBe(true);
  });

  it("serializes Evals per Thread and merges Triggers that arrive meanwhile", () => {
    const ctx = setup();
    ctx.receive("t1");
    expect(ctx.engine.evaluateDue()).toBe(1);
    expect(ctx.repos.evals.list()[0]?.triggers).toEqual(["t1"]);

    ctx.receive("t2");
    ctx.receive("t3");
    expect(ctx.engine.evaluateDue()).toBe(0);
    expect(ctx.repos.threads.get("thread-1")?.pending).toEqual(["t2", "t3"]);

    ctx.engine.decide("t1", "ignore");
    expect(ctx.engine.evaluateDue()).toBe(1);
    const second = ctx.repos.evals.list()[1];
    expect(second?.triggers).toEqual(["t2", "t3"]);
    expect(second?.policyVersion).toBe(1);
    expect(second?.deadlineAt).toBe("2026-11-16T09:01:00.000Z");
  });
});

describe("eval deadlines", () => {
  it("counts an expiry, requeues its batch and retries on the next pump", () => {
    const ctx = setup({ evalDeadlineMs: 1_000 });
    ctx.receive("t1");
    expect(ctx.engine.evaluateDue()).toBe(1);

    ctx.advance(1_000);
    expect(ctx.engine.evaluateDue()).toBe(0);
    const thread = ctx.repos.threads.get("thread-1");
    expect(thread?.expired).toBe(1);
    expect(thread?.activeEval).toBeUndefined();
    expect(thread?.pending).toEqual(["t1"]);
    expect(ctx.repos.decisions.list()).toEqual([]);

    expect(ctx.engine.evaluateDue()).toBe(1);
    expect(ctx.repos.evals.list()).toHaveLength(2);
  });

  it("records unavailable once the expiry budget is spent", () => {
    const ctx = setup({ evalDeadlineMs: 1_000, maxEvalExpiries: 2 });
    ctx.receive("t1");
    expect(ctx.engine.evaluateDue()).toBe(1);
    ctx.advance(1_000);
    expect(ctx.engine.evaluateDue()).toBe(0); // expiry 1 requeues
    expect(ctx.engine.evaluateDue()).toBe(1); // a fresh Eval
    ctx.advance(1_000);
    expect(ctx.engine.evaluateDue()).toBe(0); // expiry 2 gives up

    const [decision] = ctx.repos.decisions.list();
    expect(decision?.outcome).toBe("unavailable");
    expect(decision?.reasons[0]?.code).toBe("eval_expired");
    const thread = ctx.repos.threads.get("thread-1");
    expect(thread?.expired).toBe(2);
    expect(thread?.activeEval).toBeUndefined();
    expect(thread?.pending).toEqual([]);
    expect(ctx.repos.tickets.list()).toEqual([]);
  });
});

describe("decisions", () => {
  it("accepts into a Mandate, Ticket, Subscription and pending Assignment with one launch", () => {
    const ctx = setup();
    ctx.receive("t1");
    ctx.engine.evaluateDue();
    const decision = ctx.engine.decide("t1", "accept", [
      { code: "matched_rule", values: { rule: "fix-bug" } },
    ]);

    expect(decision.outcome).toBe("accept");
    expect(decision.strength).toBe("explicit");
    expect(decision.at).toBe(START);
    expect(decision.reasons).toHaveLength(1);

    const ticket = onlyTicket(ctx);
    const mandate = ctx.repos.mandates.get(ticket.mandate);
    expect(mandate?.policyVersion).toBe(1);
    expect(mandate?.strength).toBe("explicit");
    expect(mandate?.basis).toEqual({
      via: "addressed",
      source: "github",
      sender: "alice",
      scope: "",
    });
    expect(mandate?.matchedRules).toEqual(["fix-bug"]);
    expect(mandate?.withdrawnAt).toBeUndefined();
    expect(ticket.threads).toEqual(["thread-1"]);
    expect(ticket.subscriptions).toEqual(["thread-1"]);
    expect(ticket.openedAt).toBe(START);
    expect(ticket.endedAt).toBeUndefined();

    const assignment = lastAssignment(ctx, ticket);
    expect(assignment.state).toBe("pending");
    expect(assignment.operationId).toBe(`launch:${assignment.id}`);
    expect(ctx.repos.threads.get("thread-1")?.subscriptions).toEqual([ticket.id]);

    const effects = ctx.engine.drain();
    expect(effects).toHaveLength(1);
    const effect = effects[0];
    expect(effect?.kind).toBe("launch");
    expect(effect?.key).toBe(assignment.id);
    expect(effect?.dueAt).toBe(START);
    const payload = effect?.payload as LaunchEffectPayload;
    expect(payload.ticket.id).toBe(ticket.id);
    expect(payload.assignment.id).toBe(assignment.id);
    expect(payload.packDigest).toBe("");
    expect(ctx.engine.drain()).toEqual([]);
  });

  it("returns the recorded Decision when the same Trigger is accepted twice", () => {
    const ctx = setup();
    const { decision: first } = accepted(ctx);
    const second = ctx.engine.decide("t1", "accept");

    expect(second.id).toBe(first.id);
    expect(ctx.repos.decisions.list()).toHaveLength(1);
    expect(ctx.repos.tickets.list()).toHaveLength(1);
    expect(ctx.repos.mandates.list()).toHaveLength(1);
    expect(ctx.repos.assignments.list()).toHaveLength(1);
    expect(ctx.engine.drain()).toHaveLength(1);
  });

  it("does not replace the Decision an Eval already has", () => {
    const ctx = setup();
    ctx.receive("t1");
    ctx.engine.evaluateDue();
    const ignored = ctx.engine.decide("t1", "ignore");
    const again = ctx.engine.decide("t1", "accept");

    expect(again.id).toBe(ignored.id);
    expect(again.outcome).toBe("ignore");
    expect(ctx.repos.tickets.list()).toEqual([]);
  });

  it("records unavailable when the Policy changed during the Eval and retries under the new version", () => {
    const ctx = setup();
    ctx.receive("t1");
    ctx.engine.evaluateDue();
    ctx.setPolicy(policy(2));

    const decision = ctx.engine.decide("t1", "accept");
    expect(decision.outcome).toBe("unavailable");
    expect(decision.reasons.some((reason) => reason.code === "policy_version_changed")).toBe(true);
    expect(ctx.repos.tickets.list()).toEqual([]);
    expect(ctx.repos.threads.get("thread-1")?.pending).toEqual(["t1"]);

    expect(ctx.engine.evaluateDue()).toBe(1);
    expect(ctx.repos.evals.list().at(-1)?.policyVersion).toBe(2);
    expect(ctx.engine.decide("t1", "accept").outcome).toBe("accept");
    expect(ctx.repos.mandates.list()[0]?.policyVersion).toBe(2);
  });

  it("exposes a Trigger only while its Eval is active", () => {
    const ctx = setup();
    ctx.receive("t1");
    expect(ctx.engine.candidate("t1")).toBeUndefined();
    expect(ctx.engine.evaluateDue()).toBe(1);

    const view = ctx.engine.candidate("t1");
    expect(view?.thread.id).toBe("thread-1");
    expect(view?.trigger.id).toBe("t1");
    expect(view?.eval.triggers).toEqual(["t1"]);

    ctx.engine.decide("t1", "ignore");
    expect(ctx.engine.candidate("t1")).toBeUndefined();
    expect(() => ctx.engine.decide("missing", "ignore")).toThrow(EngineError);
  });
});

describe("assignments and reports", () => {
  it("marks an Assignment running on start and ended by its report", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = lastAssignment(ctx, ticket);

    ctx.engine.assignmentStarted(ticket.id, assignment.id);
    expect(assignment.state).toBe("running");
    expect(assignment.startedAt).toBe(START);

    const reported = report("progress", { at: "2026-11-16T09:10:00.000Z" });
    ctx.engine.report(ticket.id, reported);
    expect(ticket.latestReport).toBe("progress");
    expect(assignment.state).toBe("ended");
    expect(assignment.endedAt).toBe(reported.at);
    expect(ticketStage(ticket, facts())).toBe("working/queued");
  });

  it("writes and clears Attention from Report questions", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);

    ctx.engine.report(ticket.id, report("progress", { question: "Merge or rebase?" }));
    expect(ticket.attention).toEqual({
      question: "Merge or rebase?",
      askedAt: "2026-11-16T09:05:00.000Z",
    });
    expect(ticketStage(ticket, facts({ liveAssignment: true }))).toBe("person/answer");

    ctx.engine.report(ticket.id, report("progress"));
    expect(ticket.attention).toBeUndefined();
    expect(ticketStage(ticket, facts({ liveAssignment: true }))).toBe("working");
  });

  it("drives ticketStage from the latest Report kind", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const cases: [Report["kind"], StageFacts, TicketStage][] = [
      ["ready", facts(), "person/review"],
      ["blocked", facts(), "person/blocked"],
      ["failed", facts(), "person/failed"],
      ["waiting", facts({ liveAssignment: true }), "idle"],
      ["waiting", facts(), "working/queued"],
    ];
    for (const [kind, stageFacts, expected] of cases) {
      ctx.engine.report(ticket.id, report(kind));
      expect(ticketStage(ticket, stageFacts)).toBe(expected);
    }
  });

  it("refuses commands that name unknown work", () => {
    const ctx = setup();
    expect(() => ctx.engine.decide("missing", "ignore")).toThrow(EngineError);
    expect(() => ctx.engine.stopTicket("missing", "stop")).toThrow(EngineError);
    expect(() => ctx.engine.retryTicket("missing")).toThrow(EngineError);
    expect(() => ctx.engine.note("missing", "hello")).toThrow(EngineError);
    expect(() => ctx.engine.assignmentStarted("missing", "missing")).toThrow(EngineError);
    expect(() => ctx.engine.report("missing", report("progress"))).toThrow(EngineError);
  });
});

describe("stop and retry", () => {
  it("stops a Ticket, withdraws its Mandate and ends running work for good", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = lastAssignment(ctx, ticket);
    ctx.engine.assignmentStarted(ticket.id, assignment.id);
    ctx.advance(5_000);
    ctx.engine.stopTicket(ticket.id, "not needed any more");

    expect(ctx.repos.mandates.get(ticket.mandate)?.withdrawnAt).toBe(ctx.at());
    expect(ticket.endedAt).toBe(ctx.at());
    expect(ticket.outcome).toBe("stopped");
    expect(assignment.state).toBe("ended");
    expect(assignment.endedAt).toBe(ctx.at());
    expect(ticketStage(ticket, facts())).toBe("idle");

    // P1 has no resume: pumping and retrying cannot bring the Ticket back.
    ctx.engine.evaluateDue();
    expect(ticket.endedAt).toBe(ctx.at());
    expect(() => ctx.engine.retryTicket(ticket.id)).toThrow(EngineError);
  });

  it("retries an open Ticket with a new pending Assignment and its own launch", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    ctx.engine.drain();
    const first = lastAssignment(ctx, ticket);
    ctx.engine.assignmentStarted(ticket.id, first.id);
    ctx.engine.report(ticket.id, report("failed"));

    const replacement = ctx.engine.retryTicket(ticket.id);
    expect(replacement.id).not.toBe(first.id);
    expect(replacement.state).toBe("pending");
    expect(ticket.assignments).toEqual([first.id, replacement.id]);

    const effects = ctx.engine.drain();
    expect(effects.map((effect) => effect.key)).toEqual([replacement.id]);
    const effect = effects[0];
    if (!effect) throw new Error("expected one launch effect");
    expect((effect.payload as LaunchEffectPayload).assignment.id).toBe(replacement.id);
  });
});

describe("inputs", () => {
  it("adds notes and attached Triggers as Ticket Inputs", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    ctx.engine.drain();

    const note = ctx.engine.note(ticket.id, "check the logs first");
    expect(note).toMatchObject({
      ticket: ticket.id,
      kind: "text",
      text: "check the logs first",
      fromPerson: false,
    });
    expect(ticket.inputs).toContain(note.id);

    ctx.receive("t2");
    ctx.engine.attach(ticket.id, "t2");
    const attached = ctx.repos.inputs.list().at(-1);
    expect(attached).toMatchObject({
      ticket: ticket.id,
      kind: "trigger",
      trigger: "t2",
      fromPerson: true,
    });
    expect(ticket.inputs).toContain(attached?.id);
    expect(ctx.repos.threads.get("thread-1")?.pending).toEqual([]);
    expect(ctx.engine.evaluateDue()).toBe(0);

    expect(ctx.engine.drain()).toEqual([]);
    expect(() => ctx.engine.attach(ticket.id, "missing")).toThrow(EngineError);
  });
});

describe("escalations", () => {
  function escalationInput(overrides: Partial<EscalationInput> = {}): EscalationInput {
    return {
      id: "esc-1",
      assignment: "",
      session: "session-1",
      kind: "decision",
      question: "Merge or rebase?",
      options: [
        { id: "merge", label: "Merge" },
        { id: "rebase", label: "Rebase" },
      ],
      ...overrides,
    };
  }

  it("writes Attention, waits the Assignment and returns a waiting record", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = lastAssignment(ctx, ticket);
    ctx.engine.assignmentStarted(ticket.id, assignment.id);

    const record = ctx.engine.escalate(
      ticket.id,
      escalationInput({ assignment: assignment.id, defaultOption: "rebase" }),
    );

    expect(record).toEqual({
      id: "esc-1",
      ticket: ticket.id,
      assignment: assignment.id,
      session: "session-1",
      kind: "decision",
      question: "Merge or rebase?",
      options: [
        { id: "merge", label: "Merge" },
        { id: "rebase", label: "Rebase" },
      ],
      defaultOption: "rebase",
      status: "waiting",
      at: START,
    });
    expect(assignment.state).toBe("waiting");
    expect(ctx.repos.assignments.get(assignment.id)?.state).toBe("waiting");
    expect(ticket.attention).toEqual({
      question: "Merge or rebase?",
      options: [
        { id: "merge", label: "Merge" },
        { id: "rebase", label: "Rebase" },
      ],
      askedAt: START,
    });
    expect(ctx.repos.tickets.get(ticket.id)?.attention).toEqual(ticket.attention);
    expect(ticketStage(ticket, facts())).toBe("person/answer");
  });

  it("ends a waiting Assignment when its late report arrives", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = lastAssignment(ctx, ticket);
    ctx.engine.assignmentStarted(ticket.id, assignment.id);
    ctx.engine.escalate(ticket.id, escalationInput({ assignment: assignment.id }));
    expect(assignment.state).toBe("waiting");

    ctx.engine.report(ticket.id, report("ready"));

    const saved = ctx.repos.assignments.get(assignment.id);
    expect(saved?.state).toBe("ended");
    expect(saved?.endedAt).toBe("2026-11-16T09:05:00.000Z");
    expect(ctx.repos.tickets.get(ticket.id)?.latestReport).toBe("ready");
  });

  it("prefers the named Assignment and otherwise waits the latest one", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const first = lastAssignment(ctx, ticket);
    const replacement = ctx.engine.retryTicket(ticket.id);

    ctx.engine.escalate(ticket.id, escalationInput({ assignment: first.id }));
    expect(first.state).toBe("waiting");
    expect(replacement.state).toBe("pending");

    ctx.engine.resolve(ticket.id, { kind: "answer", text: "Use the first." });
    expect(first.state).toBe("running");

    const second = ctx.engine.escalate(
      ticket.id,
      escalationInput({ id: "esc-2", assignment: "unknown-assignment" }),
    );
    expect(second.assignment).toBe(replacement.id);
    expect(replacement.state).toBe("waiting");
  });

  it("refuses to escalate an ended Ticket", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = lastAssignment(ctx, ticket);
    ctx.engine.assignmentStarted(ticket.id, assignment.id);
    ctx.engine.stopTicket(ticket.id, "done");

    expect(() => ctx.engine.escalate(ticket.id, escalationInput())).toThrow(EngineError);
  });

  it("resolves an escalation: clears Attention, resumes the Assignment, writes an Input", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = lastAssignment(ctx, ticket);
    ctx.engine.assignmentStarted(ticket.id, assignment.id);
    const asked = ctx.engine.escalate(ticket.id, escalationInput({ assignment: assignment.id }));

    ctx.advance(1_000);
    const resolved = ctx.engine.resolve(ticket.id, {
      kind: "answer",
      text: "Rebase on main.",
      option: "rebase",
    });

    expect(resolved.id).toBe(asked.id);
    expect(resolved.status).toBe("resolved");
    expect(resolved.resolution).toEqual({
      kind: "answer",
      text: "Rebase on main.",
      option: "rebase",
      at: ctx.at(),
    });
    expect(ticket.attention).toBeUndefined();
    expect(ctx.repos.tickets.get(ticket.id)?.attention).toBeUndefined();
    expect(assignment.state).toBe("running");
    expect(ticketStage(ticket, facts({ liveAssignment: true }))).toBe("working");

    const input = ctx.repos.inputs.list().at(-1);
    expect(input).toMatchObject({
      ticket: ticket.id,
      kind: "text",
      text: "Rebase on main.",
      fromPerson: false,
    });
    expect(ticket.inputs).toContain(input?.id);
  });

  it("parks the escalation when the Master cannot answer", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = lastAssignment(ctx, ticket);
    ctx.engine.assignmentStarted(ticket.id, assignment.id);
    ctx.engine.escalate(ticket.id, escalationInput({ assignment: assignment.id }));

    const parked = ctx.engine.resolve(ticket.id, {
      kind: "escalate",
      text: "A person must decide this.",
    });

    expect(parked.status).toBe("parked");
    expect(parked.resolution).toEqual({
      kind: "escalate",
      text: "A person must decide this.",
      at: ctx.at(),
    });
    expect(parked.resolution).not.toHaveProperty("option");
    expect(ticket.attention).toBeUndefined();
  });

  it("resolves an Attention written by a report", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    ctx.engine.report(ticket.id, report("blocked", { question: "Which environment?" }));
    expect(ticket.attention?.question).toBe("Which environment?");

    const resolved = ctx.engine.resolve(ticket.id, { kind: "answer", text: "Staging." });

    expect(resolved.ticket).toBe(ticket.id);
    expect(resolved.id).toMatch(/^escalation-/);
    expect(resolved.question).toBe("Which environment?");
    expect(resolved.status).toBe("resolved");
    expect(ticket.attention).toBeUndefined();
    expect(ctx.repos.inputs.list().at(-1)?.text).toBe("Staging.");
  });

  it("refuses to resolve when nothing waits", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    expect(() => ctx.engine.resolve(ticket.id, { kind: "answer", text: "x" })).toThrow(EngineError);
  });
});

describe("ticket lifecycle commands", () => {
  it("acknowledges the current needs-action state and asks again on a new report", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    ctx.engine.report(ticket.id, report("ready"));
    expect(needsAction(ticket, ctx.repos.assignments.list())).toBe(true);

    const acknowledged = ctx.engine.acknowledge(ticket.id);
    expect(acknowledged.ack).toMatchObject({ at: START, stage: "person/review" });
    expect(needsAction(acknowledged, ctx.repos.assignments.list())).toBe(false);

    ctx.engine.report(ticket.id, report("blocked"));
    expect(needsAction(ticket, ctx.repos.assignments.list())).toBe(true);
  });

  it("closes with an explicit outcome, ends live work and withdraws the mandate", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = lastAssignment(ctx, ticket);
    const replacement = ctx.engine.retryTicket(ticket.id);
    ctx.engine.assignmentStarted(ticket.id, assignment.id);
    expect(replacement.state).toBe("pending");

    ctx.engine.report(ticket.id, report("progress", { question: "Merge or rebase?" }));
    const closed = ctx.engine.closeTicket(ticket.id, "merged");

    expect(closed.endedAt).toBe(START);
    expect(closed.outcome).toBe("merged");
    expect(closed.attention).toBeUndefined();
    expect(ctx.repos.mandates.get(ticket.mandate)?.withdrawnAt).toBe(START);
    expect(ctx.repos.assignments.get(assignment.id)?.state).toBe("ended");
    expect(ctx.repos.assignments.get(replacement.id)?.state).toBe("ended");
    expect(needsAction(ticket, ctx.repos.assignments.list())).toBe(false);
  });

  it("treats the same close as idempotent and refuses to rewrite a different outcome", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    ctx.engine.closeTicket(ticket.id, "completed");
    expect(ctx.engine.closeTicket(ticket.id, "completed").outcome).toBe("completed");
    expect(() => ctx.engine.closeTicket(ticket.id, "dropped")).toThrow(/already ended/);
  });

  it("refuses to acknowledge an ended Ticket", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    ctx.engine.closeTicket(ticket.id, "completed");
    expect(() => ctx.engine.acknowledge(ticket.id)).toThrow(/has ended/);
  });

  it("archives only closed Tickets and applies the retention sweep", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    expect(() => ctx.engine.archive(ticket.id)).toThrow(/close it before archiving/);

    ctx.engine.closeTicket(ticket.id, "completed");
    const archived = ctx.engine.archive(ticket.id);
    expect(archived.archivedAt).toBe(START);
    expect(ctx.engine.archive(ticket.id).archivedAt).toBe(START);

    ctx.advance(30 * 86_400_000);
    expect(ctx.engine.archiveDue()).toEqual([]);
  });

  it("sweeps ended Tickets whose outcome retention has elapsed", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    ctx.engine.closeTicket(ticket.id, "dropped");
    ctx.advance(2 * 86_400_000);

    expect(ctx.engine.archiveDue()).toEqual([ticket.id]);
    expect(ctx.repos.tickets.get(ticket.id)?.archivedAt).toBe(ctx.at());
    expect(ctx.engine.archiveDue()).toEqual([]);
  });

  it("closes stale open reports and archives them after retention", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignmentId = ticket.assignments[0];
    if (assignmentId === undefined) throw new Error("expected an assignment");
    ctx.engine.assignmentStarted(ticket.id, assignmentId);
    ctx.engine.report(ticket.id, { kind: "ready", summary: "done", at: ctx.at() });

    ctx.advance(8 * 86_400_000);
    expect(ctx.engine.convergeDue()).toEqual([]);
    expect(ctx.repos.tickets.get(ticket.id)).toMatchObject({
      endedAt: ctx.at(),
      outcome: "completed",
    });

    ctx.advance(8 * 86_400_000);
    expect(ctx.engine.convergeDue()).toEqual([ticket.id]);
    expect(ctx.repos.tickets.get(ticket.id)?.archivedAt).toBe(ctx.at());
  });

  it("persists the routed worker before the launch settles", () => {
    const ctx = setup();
    const { ticket } = accepted(ctx);
    const assignment = ticket.assignments[0];
    if (assignment === undefined) throw new Error("expected an assignment");
    expect(ctx.repos.assignments.get(assignment)?.worker).toBe("");

    ctx.engine.assignmentRouted(ticket.id, assignment, "worker-b", "/ws-b");
    expect(ctx.repos.assignments.get(assignment)).toMatchObject({
      worker: "worker-b",
      workspace: "/ws-b",
      state: "pending",
    });
    expect(() => ctx.engine.assignmentRouted("missing", assignment, "worker-a")).toThrow(
      EngineError,
    );
  });
});

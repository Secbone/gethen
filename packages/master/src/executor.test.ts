import { describe, expect, it, vi } from "vitest";
import { Engine } from "./engine";
import { createLaunchHandler, createOutboxHandlers, type LaunchHandlerDeps } from "./executor";
import { MemoryRepositories } from "./memory";
import type { Assignment, AssignmentReceipt, Effect, Report, Ticket } from "./model";
import { type EffectQueue, OutboxRunner } from "./outbox";
import type { LaunchPort, LaunchRequest, ReceiptSink } from "./ports";
import { ASSIGNMENT_OPEN } from "./prompts";

const NOW = "2026-11-16T12:00:00.000Z";
const READY_AT = "2026-11-16T12:01:00.000Z";

function createTicket(): Ticket {
  return {
    id: "ticket-1",
    mandate: "mandate-1",
    source: "local",
    threads: ["thread-1"],
    subscriptions: ["thread-1"],
    channels: ["#dev"],
    inputs: [],
    assignments: ["assignment-1"],
    openedAt: "2026-11-16T11:59:00.000Z",
  };
}

function createAssignment(): Assignment {
  return {
    id: "assignment-1",
    ticket: "ticket-1",
    worker: "worker-a",
    workspace: "/tmp/gethen/ws",
    packDigest: "sha256:abc123",
    state: "pending",
    operationId: "launch:assignment-1",
  };
}

function createReceipt(): AssignmentReceipt {
  return {
    assignment: "assignment-1",
    ticket: "ticket-1",
    worker: "worker-a",
    workspace: "/tmp/gethen/ws",
    packDigest: "sha256:abc123",
    at: READY_AT,
    outcome: "completed",
    summary: "all tests pass",
    events: 12,
  };
}

class MemoryReceipts implements ReceiptSink {
  readonly rows: AssignmentReceipt[] = [];

  append(receipt: AssignmentReceipt): void {
    this.rows.push(receipt);
  }
}

interface Harness {
  repos: MemoryRepositories;
  engine: Engine;
  ticket: Ticket;
  assignment: Assignment;
  payload: unknown;
}

function harness(): Harness {
  const repos = new MemoryRepositories();
  const ticket = createTicket();
  const assignment = createAssignment();
  repos.tickets.save(ticket);
  repos.assignments.save(assignment);
  const engine = new Engine({
    repos,
    clock: { now: () => new Date(NOW) },
    ids: { next: (prefix: string) => `${prefix}-1` },
  });
  return {
    repos,
    engine,
    ticket,
    assignment,
    payload: { ticket, assignment, packDigest: assignment.packDigest },
  };
}

function readyLaunch(
  report: Report,
  receipt: AssignmentReceipt,
): {
  launch: LaunchPort;
  requests: LaunchRequest[];
} {
  const requests: LaunchRequest[] = [];
  const launch: LaunchPort = {
    launch: async (request: LaunchRequest) => {
      requests.push(request);
      return { report, receipt };
    },
  };
  return { launch, requests };
}

function failingLaunch(error: Error): LaunchPort {
  return {
    launch: async () => {
      throw error;
    },
  };
}

describe("createLaunchHandler", () => {
  it("starts the assignment, launches it, reports the result and files the receipt", async () => {
    const { repos, engine, ticket, assignment, payload } = harness();
    const report: Report = { kind: "ready", summary: "all tests pass", at: READY_AT };
    const receipt = createReceipt();
    const { launch, requests } = readyLaunch(report, receipt);
    const receipts = new MemoryReceipts();

    await expect(
      createLaunchHandler({ engine, launch, receipts })(payload),
    ).resolves.toBeUndefined();

    const savedAssignment = repos.assignments.get(assignment.id);
    expect(savedAssignment?.state).toBe("ended");
    expect(savedAssignment?.startedAt).toBe(NOW);
    expect(savedAssignment?.endedAt).toBe(READY_AT);

    const savedTicket = repos.tickets.get(ticket.id);
    expect(savedTicket?.latestReport).toBe("ready");
    expect(savedTicket?.attention).toBeUndefined();

    expect(requests).toHaveLength(1);
    const request = requests.at(0);
    expect(request?.ticket.id).toBe("ticket-1");
    expect(request?.assignment.id).toBe("assignment-1");
    expect(request?.packDigest).toBe("sha256:abc123");
    expect(request?.prompt.startsWith(ASSIGNMENT_OPEN)).toBe(true);
    expect(request?.prompt).toContain("Complete the work authorized by ticket ticket-1");

    expect(receipts.rows).toHaveLength(1);
    expect(receipts.rows[0]).toBe(receipt);
  });

  it("reports a failed launch and rethrows so the outbox retries", async () => {
    const { repos, engine, ticket, assignment, payload } = harness();
    const error = new Error("bridge down");
    const receipts = new MemoryReceipts();
    const report = vi.spyOn(engine, "report");

    await expect(
      createLaunchHandler({
        engine,
        launch: failingLaunch(error),
        receipts,
        clock: { now: () => new Date(NOW) },
      })(payload),
    ).rejects.toThrow("bridge down");

    const savedAssignment = repos.assignments.get(assignment.id);
    expect(savedAssignment?.state).toBe("ended");
    expect(savedAssignment?.startedAt).toBe(NOW);
    expect(savedAssignment?.endedAt).toBe(NOW);

    expect(repos.tickets.get(ticket.id)?.latestReport).toBe("failed");
    expect(receipts.rows).toHaveLength(0);
    expect(report).toHaveBeenCalledTimes(1);
    expect(report).toHaveBeenCalledWith("ticket-1", {
      kind: "failed",
      summary: "Error: bridge down",
      at: NOW,
    });
  });

  it("uses a caller-supplied objective in the envelope", async () => {
    const { engine, assignment, payload } = harness();
    const { launch, requests } = readyLaunch(
      { kind: "ready", summary: "done", at: READY_AT },
      createReceipt(),
    );
    const seen: LaunchRequest[] = [];
    const deps: LaunchHandlerDeps = {
      engine,
      launch,
      receipts: new MemoryReceipts(),
      objective: (request) => {
        seen.push(request);
        return "Ship the report parser";
      },
    };

    await createLaunchHandler(deps)(payload);

    expect(seen).toHaveLength(1);
    expect(seen[0]?.ticket.id).toBe("ticket-1");
    expect(seen[0]?.assignment.id).toBe(assignment.id);
    expect(seen[0]?.packDigest).toBe("sha256:abc123");
    expect(seen[0]?.prompt.startsWith(ASSIGNMENT_OPEN)).toBe(true);
    expect(requests[0]?.prompt).toContain("Ship the report parser");
  });

  it("rejects a malformed payload before touching the assignment", async () => {
    const { repos, engine, assignment } = harness();
    const { launch, requests } = readyLaunch(
      { kind: "ready", summary: "done", at: READY_AT },
      createReceipt(),
    );
    const handler = createLaunchHandler({ engine, launch, receipts: new MemoryReceipts() });

    await expect(handler(undefined)).rejects.toThrow(/payload must be an object/);
    await expect(handler({})).rejects.toThrow(/ticket/);
    await expect(
      handler({ ticket: { id: "ticket-1" }, assignment: { id: "assignment-1" } }),
    ).rejects.toThrow(/packDigest/);

    expect(requests).toHaveLength(0);
    expect(repos.assignments.get(assignment.id)?.state).toBe("pending");
  });
});

describe("createOutboxHandlers", () => {
  it("registers a launch handler the relay can run to completion", async () => {
    const { repos, engine, ticket, assignment, payload } = harness();
    const report: Report = { kind: "ready", summary: "done", at: READY_AT };
    const receipt = createReceipt();
    const { launch } = readyLaunch(report, receipt);
    const receipts = new MemoryReceipts();
    const handlers = createOutboxHandlers({ engine, launch, receipts });
    expect(Object.keys(handlers)).toEqual(["launch"]);

    const effects: Effect[] = [
      {
        id: "effect-1",
        kind: "launch",
        key: assignment.id,
        payload,
        status: "waiting",
        attempt: 0,
        dueAt: NOW,
        createdAt: NOW,
      },
    ];
    const completed: string[] = [];
    const failed: string[] = [];
    const queue: EffectQueue = {
      claimDue: () => effects.splice(0, 1),
      complete: (id: string) => {
        completed.push(id);
      },
      fail: (id: string) => {
        failed.push(id);
      },
    };
    const runner = new OutboxRunner({ store: queue, handlers, now: () => new Date(NOW) });

    expect(await runner.runDue()).toBe(1);
    expect(completed).toEqual(["effect-1"]);
    expect(failed).toEqual([]);
    expect(repos.tickets.get(ticket.id)?.latestReport).toBe("ready");
    expect(receipts.rows).toHaveLength(1);
  });
});

import type { Assignment, LaunchRequest, Ticket } from "@gethen/master";
import type { WorkerConnection, WorkerGateway } from "@gethen/transport-ws";
import {
  type DispatchFrame,
  type DownFrame,
  type EscalationFrame,
  type EventFrame,
  PROTOCOL_VERSION,
  type ReportFrame,
  type ResolutionFrame,
} from "@gethen/worker-protocol";
import { describe, expect, it, vi } from "vitest";
import {
  createWsLaunchPort,
  DEFAULT_LAUNCH_TIMEOUT_MS,
  type WorkerConnectionLike,
  type WorkerGatewayLike,
  WsLaunchError,
} from "./launch";

const NOW = new Date("2026-11-16T03:00:00.000Z");
const AT = NOW.toISOString();

const ticket: Ticket = {
  id: "t1",
  mandate: "m1",
  source: "local",
  threads: ["th1"],
  subscriptions: ["th1"],
  channels: [],
  inputs: [],
  assignments: ["a1"],
  openedAt: "2026-11-16T00:00:00.000Z",
};

const assignment: Assignment = {
  id: "a1",
  ticket: "t1",
  worker: "worker-a",
  workspace: "ws-1",
  packDigest: "sha256:pack",
  state: "pending",
  operationId: "launch-0123456789abcdef",
};

const request: LaunchRequest = {
  assignment,
  ticket,
  packDigest: "sha256:pack",
  prompt: "[GETHEN-ASSIGNMENT]{...}[/GETHEN-ASSIGNMENT]",
};

/** A second, independent Assignment for concurrency tests. */
function requestFor(assignmentId: string, ticketId: string): LaunchRequest {
  return {
    assignment: { ...assignment, id: assignmentId, ticket: ticketId },
    ticket: { ...ticket, id: ticketId, assignments: [assignmentId] },
    packDigest: assignment.packDigest,
    prompt: `prompt for ${assignmentId}`,
  };
}

const conn: WorkerConnectionLike = { id: "conn-1", workerId: "worker-a" };

interface DispatchRecord {
  workerId: string;
  frame: DownFrame;
}

interface FakeGateway extends WorkerGatewayLike {
  readonly dispatched: DispatchRecord[];
}

/** Small gateway double: records sends, never touches a socket. */
function fakeGateway(workerIds: readonly string[] = ["worker-a"], fail?: Error): FakeGateway {
  const connections: WorkerConnectionLike[] = workerIds.map((workerId, index) => ({
    id: `conn-${index + 1}`,
    workerId,
  }));
  const dispatched: DispatchRecord[] = [];
  return {
    dispatched,
    connections: () => connections,
    get: (workerId) => connections.find((connection) => connection.workerId === workerId),
    dispatch: async (workerId, frame) => {
      if (fail !== undefined) throw fail;
      dispatched.push({ workerId, frame });
    },
  };
}

function reportFrame(assignmentId: string, overrides: Partial<ReportFrame> = {}): ReportFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "report",
    assignmentId,
    kind: "ready",
    summary: "done",
    receipt: { packDigest: "sha256:pack", events: 3 },
    ...overrides,
  };
}

function escalationFrame(
  assignmentId: string,
  overrides: Partial<EscalationFrame> = {},
): EscalationFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "escalation",
    assignmentId,
    escalationId: "esc-1",
    kind: "decision",
    question: "Which branch?",
    options: [
      { id: "merge", label: "Merge" },
      { id: "rebase", label: "Rebase" },
    ],
    defaultOption: "merge",
    blocking: true,
    ...overrides,
  };
}

function eventFrame(assignmentId: string, seq: number): EventFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "event",
    assignmentId,
    seq,
    at: AT,
    event: { type: "message", text: `event ${seq}` },
  };
}

function flush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function dispatchFrames(gateway: FakeGateway): DispatchFrame[] {
  return gateway.dispatched
    .map((record) => record.frame)
    .filter((frame): frame is DispatchFrame => frame.type === "dispatch");
}

function resolutionFrames(gateway: FakeGateway): ResolutionFrame[] {
  return gateway.dispatched
    .map((record) => record.frame)
    .filter((frame): frame is ResolutionFrame => frame.type === "resolution");
}

describe("createWsLaunchPort", () => {
  it("dispatches the assignment and resolves on the matching report", async () => {
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({ gateway, now: () => NOW });

    const resultPromise = bridge.launchPort.launch(request);
    expect(gateway.dispatched).toHaveLength(1);
    const sent = gateway.dispatched.at(0);
    expect(sent?.workerId).toBe("worker-a");
    expect(sent?.frame).toEqual({
      v: PROTOCOL_VERSION,
      type: "dispatch",
      assignmentId: "a1",
      ticketId: "t1",
      workspace: "ws-1",
      packDigest: "sha256:pack",
      envelope: { prompt: request.prompt },
    });

    let settled = false;
    void resultPromise.then(() => {
      settled = true;
    });
    await flush();
    expect(settled).toBe(false);

    bridge.handleFrame(conn, reportFrame("a1"));
    const result = await resultPromise;
    expect(result.report).toEqual({ kind: "ready", summary: "done", at: AT });
    expect(result.receipt).toEqual({
      assignment: "a1",
      ticket: "t1",
      worker: "worker-a",
      workspace: "ws-1",
      packDigest: "sha256:pack",
      at: AT,
      outcome: "completed",
      summary: "done",
      events: 3,
    });
    expect(result.escalations).toBeUndefined();
  });

  it("prefers the configured worker over the first connection", async () => {
    const gateway = fakeGateway(["worker-a", "worker-b"]);
    const bridge = createWsLaunchPort({ gateway, workerId: "worker-b", now: () => NOW });

    const resultPromise = bridge.launchPort.launch(request);
    expect(dispatchFrames(gateway).at(0)?.assignmentId).toBe("a1");
    expect(gateway.dispatched.at(0)?.workerId).toBe("worker-b");
    bridge.handleFrame(conn, reportFrame("a1"));
    await expect(resultPromise).resolves.toMatchObject({ report: { kind: "ready" } });
  });

  it("rejects when no worker is connected", async () => {
    const bridge = createWsLaunchPort({ gateway: fakeGateway([]) });
    await expect(bridge.launchPort.launch(request)).rejects.toThrow(/WS_NO_WORKER/);
  });

  it("rejects when the configured worker is not connected", async () => {
    const bridge = createWsLaunchPort({ gateway: fakeGateway(["worker-a"]), workerId: "worker-b" });
    await expect(bridge.launchPort.launch(request)).rejects.toThrow(/WS_WORKER_UNAVAILABLE/);
  });

  it("lets selectWorker choose the worker and passes it the LaunchRequest", async () => {
    const gateway = fakeGateway(["worker-a", "worker-b"]);
    const selectWorker = vi.fn((received: LaunchRequest) => {
      expect(received).toBe(request);
      return { workerId: "worker-b" };
    });
    const bridge = createWsLaunchPort({
      gateway,
      workerId: "worker-a",
      now: () => NOW,
      selectWorker,
    });

    const resultPromise = bridge.launchPort.launch(request);
    await flush();

    expect(selectWorker).toHaveBeenCalledTimes(1);
    expect(selectWorker).toHaveBeenCalledWith(request);
    expect(gateway.dispatched.at(0)?.workerId).toBe("worker-b");

    bridge.handleFrame(conn, reportFrame("a1"));
    const result = await resultPromise;
    expect(result.report).toMatchObject({ kind: "ready" });
    expect(result.receipt.worker).toBe("worker-b");
  });

  it("awaits an asynchronous selectWorker before dispatching", async () => {
    const gateway = fakeGateway(["worker-a", "worker-b"]);
    const seen: string[] = [];
    const bridge = createWsLaunchPort({
      gateway,
      now: () => NOW,
      selectWorker: async (received) => {
        seen.push(received.assignment.id);
        return { workerId: "worker-b" };
      },
    });

    const resultPromise = bridge.launchPort.launch(request);
    // The dispatch waits for the asynchronous choice.
    expect(gateway.dispatched).toHaveLength(0);
    await flush();

    expect(seen).toEqual(["a1"]);
    expect(gateway.dispatched.at(0)?.workerId).toBe("worker-b");

    bridge.handleFrame(conn, reportFrame("a1"));
    await expect(resultPromise).resolves.toMatchObject({ receipt: { worker: "worker-b" } });
  });

  it("rejects with WS_NO_WORKER when selectWorker finds no worker", async () => {
    const gateway = fakeGateway(["worker-a"]);
    const bridge = createWsLaunchPort({ gateway, selectWorker: () => undefined });

    const failure = await bridge.launchPort.launch(request).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(WsLaunchError);
    expect((failure as WsLaunchError).code).toBe("WS_NO_WORKER");
    expect(gateway.dispatched).toHaveLength(0);
  });

  it("rejects with WS_WORKER_UNAVAILABLE when selectWorker picks a disconnected worker", async () => {
    const gateway = fakeGateway(["worker-a"]);
    const bridge = createWsLaunchPort({
      gateway,
      selectWorker: () => ({ workerId: "worker-gone" }),
    });

    const failure = await bridge.launchPort.launch(request).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(WsLaunchError);
    expect((failure as WsLaunchError).code).toBe("WS_WORKER_UNAVAILABLE");
    expect((failure as WsLaunchError).message).toContain("worker-gone");
    expect(gateway.dispatched).toHaveLength(0);
  });

  it("wraps a throwing or rejecting selectWorker as WS_WORKER_UNAVAILABLE", async () => {
    const gateway = fakeGateway();
    const throwing = createWsLaunchPort({
      gateway,
      selectWorker: () => {
        throw new Error("fleet down");
      },
    });
    const rejecting = createWsLaunchPort({
      gateway,
      selectWorker: async () => {
        throw new Error("fleet timeout");
      },
    });

    for (const [bridge, cause] of [
      [throwing, "fleet down"],
      [rejecting, "fleet timeout"],
    ] as const) {
      const failure = await bridge.launchPort.launch(request).then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(failure).toBeInstanceOf(WsLaunchError);
      expect((failure as WsLaunchError).code).toBe("WS_WORKER_UNAVAILABLE");
      expect((failure as WsLaunchError).message).toContain(cause);
      expect((failure as WsLaunchError).message).toContain("a1");
    }
    expect(gateway.dispatched).toHaveLength(0);
  });

  it("closes the escalation loop: answer, resolution frame, then report", async () => {
    const statuses: string[] = [];
    const resolveEscalation = vi.fn(async () => ({
      kind: "answer" as const,
      text: "Merge it.",
      option: "merge",
    }));
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({
      gateway,
      now: () => NOW,
      resolveEscalation,
      onEscalation: (record) => statuses.push(`escalation:${record.status}`),
      onResolution: (record) => statuses.push(`resolution:${record.status}`),
    });

    const resultPromise = bridge.launchPort.launch(request);
    bridge.handleFrame(conn, escalationFrame("a1"));
    await flush();

    expect(resolveEscalation).toHaveBeenCalledTimes(1);
    expect(statuses).toEqual(["escalation:waiting", "resolution:resolved"]);

    const resolution = resolutionFrames(gateway).at(0);
    expect(resolution).toEqual({
      v: PROTOCOL_VERSION,
      type: "resolution",
      assignmentId: "a1",
      escalationId: "esc-1",
      resolution: { kind: "answer", text: "Merge it.", option: "merge" },
    });
    expect(gateway.dispatched.at(1)?.workerId).toBe("worker-a");

    bridge.handleFrame(conn, reportFrame("a1", { summary: "merged" }));
    const result = await resultPromise;
    expect(result.report).toMatchObject({ kind: "ready", summary: "merged" });

    const record = result.escalations?.at(0);
    expect(record).toMatchObject({
      id: "esc-1",
      ticket: "t1",
      assignment: "a1",
      session: "worker-a",
      kind: "decision",
      question: "Which branch?",
      defaultOption: "merge",
      status: "resolved",
    });
    expect(record?.options).toEqual([
      { id: "merge", label: "Merge" },
      { id: "rebase", label: "Rebase" },
    ]);
    expect(record?.resolution).toEqual({
      kind: "answer",
      text: "Merge it.",
      option: "merge",
      at: AT,
    });
  });

  it("parks for a person when no resolver is configured", async () => {
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({ gateway, now: () => NOW });

    const resultPromise = bridge.launchPort.launch(request);
    bridge.handleFrame(conn, eventFrame("a1", 0));
    bridge.handleFrame(conn, eventFrame("a1", 1));
    bridge.handleFrame(conn, escalationFrame("a1"));
    const result = await resultPromise;

    expect(result.report).toEqual({
      kind: "waiting",
      summary: "waiting for a person: Which branch?",
      question: "Which branch?",
      at: AT,
    });
    expect(result.receipt).toMatchObject({
      assignment: "a1",
      ticket: "t1",
      worker: "worker-a",
      workspace: "ws-1",
      outcome: "completed",
      summary: "waiting for a person: Which branch?",
      events: 2,
    });
    expect(result.escalations?.at(0)).toMatchObject({ status: "parked" });
    expect(resolutionFrames(gateway)).toHaveLength(0);
  });

  it("parks when the resolver returns undefined", async () => {
    const resolveEscalation = vi.fn(async () => undefined);
    const statuses: string[] = [];
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({
      gateway,
      now: () => NOW,
      resolveEscalation,
      onResolution: (record) => statuses.push(record.status),
    });

    const resultPromise = bridge.launchPort.launch(request);
    bridge.handleFrame(conn, escalationFrame("a1"));
    const result = await resultPromise;

    expect(resolveEscalation).toHaveBeenCalledTimes(1);
    expect(statuses).toEqual(["parked"]);
    expect(result.report).toMatchObject({ kind: "waiting", question: "Which branch?" });
    expect(result.escalations?.at(0)?.resolution).toBeUndefined();
    expect(result.escalations?.at(0)?.status).toBe("parked");
    expect(resolutionFrames(gateway)).toHaveLength(0);
  });

  it("parks (and still resumes) on an escalate resolution", async () => {
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({
      gateway,
      now: () => NOW,
      resolveEscalation: async () => ({ kind: "escalate", text: "A person must decide." }),
    });

    const resultPromise = bridge.launchPort.launch(request);
    bridge.handleFrame(conn, escalationFrame("a1"));
    await flush();

    expect(resolutionFrames(gateway).at(0)).toMatchObject({
      resolution: { kind: "escalate", text: "A person must decide." },
    });

    bridge.handleFrame(conn, reportFrame("a1", { summary: "merged" }));
    const result = await resultPromise;
    expect(result.report.kind).toBe("ready");
    expect(result.escalations?.at(0)).toMatchObject({
      status: "parked",
      resolution: { kind: "escalate", text: "A person must decide." },
    });
  });

  it("fails with WS_LAUNCH_TIMEOUT when no report arrives", async () => {
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({ gateway, timeoutMs: 10 });

    const failure = await bridge.launchPort.launch(request).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(WsLaunchError);
    expect((failure as WsLaunchError).code).toBe("WS_LAUNCH_TIMEOUT");
    // A late frame for the timed-out assignment is ignored rather than throwing.
    expect(() => bridge.handleFrame(conn, reportFrame("a1"))).not.toThrow();
  });

  it("fails with WS_DISPATCH_FAILED when gateway.dispatch rejects", async () => {
    const gateway = fakeGateway(["worker-a"], new Error("socket gone"));
    const bridge = createWsLaunchPort({ gateway });
    await expect(bridge.launchPort.launch(request)).rejects.toThrow(/WS_DISPATCH_FAILED/);
  });

  it("maps a failed report to a failed receipt", async () => {
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({ gateway, now: () => NOW });

    const resultPromise = bridge.launchPort.launch(request);
    bridge.handleFrame(conn, reportFrame("a1", { kind: "failed", summary: "boom" }));
    const result = await resultPromise;
    expect(result.report).toEqual({ kind: "failed", summary: "boom", at: AT });
    expect(result.receipt.outcome).toBe("failed");
  });

  it("keeps two concurrent assignments isolated", async () => {
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({ gateway, now: () => NOW });

    const first = bridge.launchPort.launch(request);
    const second = bridge.launchPort.launch(requestFor("a2", "t2"));
    expect(dispatchFrames(gateway).map((frame) => frame.assignmentId)).toEqual(["a1", "a2"]);

    // An unrelated frame for a third assignment must not disturb either launch.
    bridge.handleFrame(conn, reportFrame("a3", { summary: "stranger" }));
    bridge.handleFrame(conn, reportFrame("a2", { summary: "second" }));
    bridge.handleFrame(conn, reportFrame("a1", { summary: "first" }));

    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(firstResult.report.summary).toBe("first");
    expect(firstResult.receipt.assignment).toBe("a1");
    expect(firstResult.receipt.ticket).toBe("t1");
    expect(secondResult.report.summary).toBe("second");
    expect(secondResult.receipt.assignment).toBe("a2");
    expect(secondResult.receipt.ticket).toBe("t2");
  });

  it("processes a repeated escalation id only once", async () => {
    const resolveEscalation = vi.fn(async () => ({ kind: "answer" as const, text: "ok" }));
    const gateway = fakeGateway();
    const bridge = createWsLaunchPort({ gateway, now: () => NOW, resolveEscalation });

    const resultPromise = bridge.launchPort.launch(request);
    bridge.handleFrame(conn, escalationFrame("a1"));
    bridge.handleFrame(conn, escalationFrame("a1"));
    await flush();

    expect(resolveEscalation).toHaveBeenCalledTimes(1);
    expect(resolutionFrames(gateway)).toHaveLength(1);

    bridge.handleFrame(conn, reportFrame("a1"));
    const result = await resultPromise;
    expect(result.escalations).toHaveLength(1);
  });

  it("is structurally satisfied by the production WorkerGateway and defaults to 10 min", () => {
    // Compile-time assertion only: `satisfies` fails if @gethen/transport-ws
    // drifts from the structural surface this adapter needs.
    const gateway = {} as WorkerGateway;
    expect(gateway satisfies WorkerGatewayLike).toBe(gateway);
    expect(DEFAULT_LAUNCH_TIMEOUT_MS).toBe(600_000);
    // Referenced so the production connection type stays part of this check.
    const connection = {} as WorkerConnection;
    expect(connection satisfies WorkerConnectionLike).toBe(connection);
  });
});

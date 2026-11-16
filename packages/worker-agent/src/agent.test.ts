import { once } from "node:events";
import type { IncomingMessage } from "node:http";
import {
  type CancelFrame,
  type ControlFrame,
  type DispatchFrame,
  type DownFrame,
  encodeFrame,
  type HeartbeatFrame,
  PROTOCOL_VERSION,
  parseUpstreamFrame,
  type ResolutionFrame,
  type UpFrame,
  type UpstreamFrameType,
} from "@gethen/worker-protocol";
import { afterEach, describe, expect, it } from "vitest";
import type WebSocket from "ws";
import { WebSocketServer } from "ws";
import { createWorkerAgent, type WorkerAgent, type WorkerAgentOptions } from "./agent";
import {
  type ExecutionHandle,
  type ExecutorSink,
  type ExecutorTask,
  FakeExecutor,
  type SessionExecutor,
} from "./executor";

type Server = InstanceType<typeof WebSocketServer>;
type ServerSocket = WebSocket;

const ASSIGNMENT_ID = "assignment-1";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitUntil(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for a condition");
    await delay(5);
  }
}

function rawText(data: WebSocket.RawData): string {
  if (typeof data === "string") return data;
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString("utf8");
  return data.toString("utf8");
}

interface FrameWaiter {
  predicate: (frame: UpFrame) => boolean;
  resolve: (frame: UpFrame) => void;
}

interface FakeMasterOptions {
  /** When false the Master never answers hello with hello_ok. */
  helloOk?: boolean;
  /** heartbeatMs advertised in hello_ok. */
  heartbeatMs?: number;
}

/** A minimal Master endpoint that records validated upstream frames. */
class FakeMaster {
  readonly frames: UpFrame[] = [];
  readonly sockets: ServerSocket[] = [];
  readonly authorizations: Array<string | undefined> = [];
  readonly invalidFrames: unknown[] = [];
  readonly url: string;

  private readonly server: Server;
  private readonly options: FakeMasterOptions;
  private waiters: FrameWaiter[] = [];

  private constructor(server: Server, url: string, options: FakeMasterOptions) {
    this.server = server;
    this.url = url;
    this.options = options;
  }

  static async start(options: FakeMasterOptions = {}): Promise<FakeMaster> {
    const server = new WebSocketServer({ port: 0 });
    await once(server, "listening");
    const address = server.address();
    if (address === null || typeof address === "string") {
      throw new Error("fake master did not bind a TCP port");
    }
    const master = new FakeMaster(server, `ws://127.0.0.1:${address.port}`, options);
    server.on("connection", (socket: ServerSocket, request: IncomingMessage) => {
      master.sockets.push(socket);
      master.authorizations.push(request.headers.authorization);
      socket.on("message", (data) => master.onMessage(socket, data));
    });
    return master;
  }

  get lastSocket(): ServerSocket {
    const socket = this.sockets.at(-1);
    if (socket === undefined) throw new Error("fake master has no connection");
    return socket;
  }

  get connectionCount(): number {
    return this.sockets.length;
  }

  send(frame: DownFrame): void {
    this.lastSocket.send(encodeFrame(frame));
  }

  sendRaw(text: string): void {
    this.lastSocket.send(text);
  }

  waitForType<T extends UpstreamFrameType>(
    type: T,
    timeoutMs = 2_000,
  ): Promise<Extract<UpFrame, { type: T }>> {
    return this.waitFor((frame) => frame.type === type, timeoutMs).then(
      (frame) => frame as Extract<UpFrame, { type: T }>,
    );
  }

  waitForCount(type: UpstreamFrameType, count: number, timeoutMs = 2_000): Promise<void> {
    return waitUntil(
      () => this.frames.filter((frame) => frame.type === type).length >= count,
      timeoutMs,
    );
  }

  waitForConnections(count: number, timeoutMs = 2_000): Promise<void> {
    return waitUntil(() => this.sockets.length >= count, timeoutMs);
  }

  async close(): Promise<void> {
    for (const socket of this.sockets) socket.terminate();
    await new Promise<void>((resolve) => {
      this.server.close(() => resolve());
    });
  }

  private waitFor(predicate: (frame: UpFrame) => boolean, timeoutMs: number): Promise<UpFrame> {
    const found = this.frames.find(predicate);
    if (found !== undefined) return Promise.resolve(found);
    return new Promise<UpFrame>((resolve, reject) => {
      const waiter: FrameWaiter = {
        predicate,
        resolve: (frame) => {
          clearTimeout(timer);
          resolve(frame);
        },
      };
      const timer = setTimeout(() => {
        this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
        reject(new Error(`timed out after ${timeoutMs}ms waiting for a frame`));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }

  private onMessage(socket: ServerSocket, data: WebSocket.RawData): void {
    let json: unknown;
    try {
      json = JSON.parse(rawText(data));
    } catch {
      this.invalidFrames.push(rawText(data));
      return;
    }
    const parsed = parseUpstreamFrame(json);
    if (!parsed.ok) {
      this.invalidFrames.push(json);
      return;
    }
    const frame = parsed.value;
    this.frames.push(frame);
    if (frame.type === "hello" && this.options.helloOk !== false) {
      const helloOk: DownFrame = {
        v: PROTOCOL_VERSION,
        type: "hello_ok",
        workerId: frame.workerId,
        serverTime: new Date().toISOString(),
        heartbeatMs: this.options.heartbeatMs ?? 10_000,
      };
      socket.send(encodeFrame(helloOk));
    }
    this.waiters = this.waiters.filter((waiter) => {
      if (!waiter.predicate(frame)) return true;
      waiter.resolve(frame);
      return false;
    });
  }
}

/** Executor whose sink is driven by the test, for offline/queue scenarios. */
class ManualExecutor implements SessionExecutor {
  readonly tasks: ExecutorTask[] = [];
  sink: ExecutorSink | undefined;

  async start(task: ExecutorTask, sink: ExecutorSink): Promise<ExecutionHandle> {
    this.tasks.push(task);
    this.sink = sink;
    return {
      sendResolution: async (): Promise<void> => {},
      cancel: async (): Promise<void> => {},
    };
  }
}

const masters: FakeMaster[] = [];
const agents: WorkerAgent[] = [];

async function startMaster(options: FakeMasterOptions = {}): Promise<FakeMaster> {
  const master = await FakeMaster.start(options);
  masters.push(master);
  return master;
}

function agentOptions(
  master: FakeMaster,
  overrides: Partial<WorkerAgentOptions> = {},
): WorkerAgentOptions {
  return {
    masterUrl: master.url,
    token: "worker-token",
    workerId: "worker-1",
    machine: "machine-a",
    platform: "linux",
    runtimes: ["dsh"],
    workspaces: ["ws-gethen"],
    models: ["deepseek-v4"],
    capabilities: { gpu: false },
    quota: 2,
    executor: new FakeExecutor(),
    heartbeatMs: 60_000,
    reconnect: { initialDelayMs: 10, maxDelayMs: 50, jitter: 0 },
    ...overrides,
  };
}

function startAgent(master: FakeMaster, overrides: Partial<WorkerAgentOptions> = {}): WorkerAgent {
  const agent = createWorkerAgent(agentOptions(master, overrides));
  agents.push(agent);
  return agent;
}

function dispatchFrame(
  assignmentId = ASSIGNMENT_ID,
  overrides: Partial<DispatchFrame> = {},
): DispatchFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "dispatch",
    assignmentId,
    ticketId: "ticket-1",
    workspace: "/tmp/gethen-ws",
    packDigest: "sha256:pack-1",
    envelope: { proto: "gethen/assignment/v1", objective: "fix the failing test" },
    model: "deepseek-v4",
    ...overrides,
  };
}

function resolutionFrame(escalationId: string, assignmentId = ASSIGNMENT_ID): ResolutionFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "resolution",
    assignmentId,
    escalationId,
    resolution: { kind: "answer", option: "yes", text: "go ahead" },
  };
}

function cancelFrame(reason?: string): CancelFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "cancel",
    assignmentId: ASSIGNMENT_ID,
    ...(reason === undefined ? {} : { reason }),
  };
}

function downstreamError(
  overrides: Partial<Extract<DownFrame, { type: "error" }>> = {},
): DownFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "error",
    code: "assignment_failed",
    message: "executor crashed",
    ...overrides,
  };
}

afterEach(async () => {
  for (const agent of agents.splice(0)) await agent.close();
  for (const master of masters.splice(0)) await master.close();
});

describe("worker agent", () => {
  it("connects with the bearer token and sends a valid hello", async () => {
    const master = await startMaster();
    const agent = startAgent(master);
    const hello = await master.waitForType("hello");

    expect(master.authorizations[0]).toBe("Bearer worker-token");
    expect(hello).toEqual({
      v: PROTOCOL_VERSION,
      type: "hello",
      workerId: "worker-1",
      protocol: PROTOCOL_VERSION,
      machine: "machine-a",
      platform: "linux",
      runtimes: ["dsh"],
      workspaces: ["ws-gethen"],
      capabilities: { gpu: false },
      load: { running: 0, quota: 2 },
      models: ["deepseek-v4"],
    });
    await agent.whenConnected();
    expect(agent.state).toBe("online");
  });

  it("adopts hello_ok's heartbeat interval and reports load", async () => {
    const master = await startMaster({ heartbeatMs: 20 });
    startAgent(master, { heartbeatMs: 60_000 });
    await master.waitForCount("heartbeat", 2);

    const heartbeats = master.frames.filter(
      (frame): frame is HeartbeatFrame => frame.type === "heartbeat",
    );
    expect(heartbeats.length).toBeGreaterThanOrEqual(2);
    expect(heartbeats[0]?.load).toEqual({ running: 0, quota: 2 });
    const at = heartbeats[0]?.at;
    expect(at).toBeDefined();
    expect(new Date(at ?? "").toISOString()).toBe(at);
  });

  it("streams accepted, event and escalation for one dispatch", async () => {
    const master = await startMaster();
    const executor = new FakeExecutor();
    const agent = startAgent(master, { executor });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    const accepted = await master.waitForType("accepted");
    const event = await master.waitForType("event");
    const escalation = await master.waitForType("escalation");

    expect(accepted.assignmentId).toBe(ASSIGNMENT_ID);
    expect(event).toMatchObject({
      assignmentId: ASSIGNMENT_ID,
      seq: 0,
      event: { type: "message", text: "fake executor started" },
    });
    expect(new Date(event.at).toISOString()).toBe(event.at);
    expect(escalation).toMatchObject({
      assignmentId: ASSIGNMENT_ID,
      escalationId: "escalation-assignment-1",
      kind: "decision",
      question: "Proceed with the fake plan?",
      blocking: true,
    });
    expect(escalation.options).toEqual([
      { id: "yes", label: "Yes" },
      { id: "no", label: "No" },
    ]);
    expect(executor.tasks).toEqual([
      {
        assignmentId: ASSIGNMENT_ID,
        ticketId: "ticket-1",
        workspace: "/tmp/gethen-ws",
        packDigest: "sha256:pack-1",
        envelope: { proto: "gethen/assignment/v1", objective: "fix the failing test" },
        model: "deepseek-v4",
      },
    ]);
    expect(agent.activeAssignments).toBe(1);

    const order = master.frames.map((frame) => frame.type);
    expect(order.indexOf("accepted")).toBeLessThan(order.indexOf("event"));
    expect(order.indexOf("event")).toBeLessThan(order.indexOf("escalation"));
  });

  it("answers an escalation and reports the finished assignment", async () => {
    const master = await startMaster();
    const executor = new FakeExecutor();
    const agent = startAgent(master, { executor });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    const escalation = await master.waitForType("escalation");
    master.send(resolutionFrame(escalation.escalationId));
    const report = await master.waitForType("report");

    expect(report).toMatchObject({
      assignmentId: ASSIGNMENT_ID,
      kind: "ready",
      summary: "fake executor finished assignment-1",
      receipt: { packDigest: "sha256:pack-1", events: 1 },
    });
    expect(executor.resolutions).toEqual([{ kind: "answer", option: "yes", text: "go ahead" }]);
    expect(agent.activeAssignments).toBe(0);
  });

  it("treats a duplicate in-flight dispatch as idempotent", async () => {
    const master = await startMaster();
    const executor = new FakeExecutor();
    startAgent(master, { executor });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    const escalation = await master.waitForType("escalation");
    master.send(dispatchFrame());
    await master.waitForCount("accepted", 2);

    expect(executor.tasks).toHaveLength(1);
    master.send(resolutionFrame(escalation.escalationId));
    await master.waitForCount("report", 1);
    expect(master.frames.filter((frame) => frame.type === "report")).toHaveLength(1);
    expect(executor.resolutions).toHaveLength(1);
  });

  it("re-sends the stored report when a finished assignment is dispatched again", async () => {
    const master = await startMaster();
    const executor = new FakeExecutor();
    startAgent(master, { executor });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    const escalation = await master.waitForType("escalation");
    master.send(resolutionFrame(escalation.escalationId));
    const first = await master.waitForType("report");

    master.send(dispatchFrame());
    await master.waitForCount("report", 2);
    const reports = master.frames.filter((frame) => frame.type === "report");
    expect(reports[1]).toEqual(first);
    expect(executor.tasks).toHaveLength(1);
  });

  it("forwards a cancel to the execution and reports failure", async () => {
    const master = await startMaster();
    const executor = new FakeExecutor();
    startAgent(master, { executor });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    await master.waitForType("escalation");
    master.send(cancelFrame("stop"));
    const report = await master.waitForType("report");

    expect(report).toMatchObject({ kind: "failed", summary: "cancelled: stop" });
    expect(executor.cancels).toEqual(["stop"]);
  });

  it("ignores a resolution whose escalation id does not match", async () => {
    const master = await startMaster();
    const executor = new FakeExecutor();
    startAgent(master, { executor });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    const escalation = await master.waitForType("escalation");
    master.send(resolutionFrame("escalation-from-another-assignment"));
    await delay(20);
    expect(master.frames.filter((frame) => frame.type === "report")).toHaveLength(0);

    master.send(resolutionFrame(escalation.escalationId));
    await master.waitForType("report");
    expect(executor.resolutions).toHaveLength(1);
  });

  it("reconnects, re-sends hello and keeps the running assignment alive", async () => {
    const master = await startMaster();
    const executor = new FakeExecutor();
    const agent = startAgent(master, { executor });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    const escalation = await master.waitForType("escalation");
    master.lastSocket.terminate();
    await master.waitForConnections(2);
    await master.waitForCount("hello", 2);
    await master.waitForCount("escalation", 2);

    expect(agent.state).toBe("online");
    expect(agent.activeAssignments).toBe(1);
    expect(executor.tasks).toHaveLength(1);
    expect(master.frames.filter((frame) => frame.type === "accepted")).toHaveLength(1);
    // The unanswered escalation is replayed on the new connection.
    expect(master.frames.filter((frame) => frame.type === "escalation")).toHaveLength(2);

    master.send(resolutionFrame(escalation.escalationId));
    const report = await master.waitForType("report");
    expect(report.assignmentId).toBe(ASSIGNMENT_ID);
    expect(report.kind).toBe("ready");
  });

  it("queues a report produced while offline and flushes it after hello", async () => {
    const master = await startMaster();
    const executor = new ManualExecutor();
    const agent = startAgent(master, {
      executor,
      reconnect: { initialDelayMs: 150, maxDelayMs: 150, jitter: 0 },
    });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    await master.waitForType("accepted");
    expect(executor.sink).toBeDefined();

    master.lastSocket.terminate();
    await waitUntil(() => agent.state === "connecting");
    executor.sink?.report({ kind: "ready", summary: "finished while offline" });
    await master.waitForConnections(2);
    const report = await master.waitForType("report");

    expect(report).toMatchObject({
      assignmentId: ASSIGNMENT_ID,
      kind: "ready",
      summary: "finished while offline",
    });
  });

  it("uses a fresh bearer token from tokenProvider on every connection", async () => {
    const master = await startMaster();
    let issued = 0;
    startAgent(master, {
      token: undefined,
      tokenProvider: () => {
        issued += 1;
        return `token-${issued}`;
      },
    });
    await master.waitForType("hello");
    master.lastSocket.terminate();
    await master.waitForCount("hello", 2);

    expect(master.authorizations).toEqual(["Bearer token-1", "Bearer token-2"]);
  });

  it("answers an unsupported control frame with a failed control_result", async () => {
    const master = await startMaster();
    startAgent(master);
    await master.waitForType("hello");

    const control: ControlFrame = {
      v: PROTOCOL_VERSION,
      type: "control",
      operationId: "op-1",
      op: { op: "snapshot" },
    };
    master.send(control);
    const result = await master.waitForType("control_result");
    expect(result).toMatchObject({ operationId: "op-1", ok: false });
  });

  it("reports an error frame for a malformed downstream frame", async () => {
    const master = await startMaster();
    startAgent(master);
    await master.waitForType("hello");

    master.sendRaw("{not json");
    const error = await master.waitForType("error");
    expect(error.code).toBe("invalid_frame");
  });

  it("logs an assignment-scoped downstream error without dropping the connection", async () => {
    const master = await startMaster();
    const logs: string[] = [];
    const agent = startAgent(master, {
      logger: { error: (message) => logs.push(message) },
    });
    await master.waitForType("hello");
    await agent.whenConnected();

    master.send(
      downstreamError({
        code: "ASSIGNMENT_FAILED",
        message: "executor crashed",
        assignmentId: ASSIGNMENT_ID,
      }),
    );

    await waitUntil(() => logs.length === 1);
    expect(logs[0]).toContain("ASSIGNMENT_FAILED");
    expect(logs[0]).toContain(ASSIGNMENT_ID);
    await delay(40);
    expect(agent.state).toBe("online");
    expect(master.connectionCount).toBe(1);
  });

  it("reconnects after a protocol-level downstream error", async () => {
    const master = await startMaster();
    const logs: string[] = [];
    const agent = startAgent(master, {
      logger: { error: (message) => logs.push(message) },
    });
    await master.waitForType("hello");
    await agent.whenConnected();

    master.send(downstreamError({ code: "invalid_frame", message: "frame is not valid JSON" }));

    await master.waitForCount("hello", 2);
    await agent.whenConnected();
    await waitUntil(() => logs.some((line) => line.includes("invalid_frame")));
    expect(master.connectionCount).toBeGreaterThanOrEqual(2);
    expect(agent.state).toBe("online");
  });

  it("handles a downstream error during the handshake and retries", async () => {
    const master = await startMaster({ helloOk: false });
    const logs: string[] = [];
    startAgent(master, { logger: { error: (message) => logs.push(message) } });
    await master.waitForType("hello");

    master.send(
      downstreamError({
        code: "hello_required",
        message: 'the first frame must be "hello"',
      }),
    );

    await master.waitForCount("hello", 2);
    await waitUntil(() => logs.some((line) => line.includes("hello_required")));
    expect(master.connectionCount).toBeGreaterThanOrEqual(2);
  });

  it("resolves whenConnected on connect and rejects it after an early close", async () => {
    const master = await startMaster();
    const agent = startAgent(master);
    await agent.whenConnected();
    expect(agent.state).toBe("online");

    const unconnected = createWorkerAgent({
      ...agentOptions(master),
      masterUrl: "ws://127.0.0.1:1/gethen/worker",
    });
    agents.push(unconnected);
    const outcome = unconnected.whenConnected().then(
      () => "resolved",
      (error: Error) => error.message,
    );
    await unconnected.close();
    expect(await outcome).toMatch(/closed/);
  });

  it("stops reconnecting after close and cancels running executions", async () => {
    const master = await startMaster();
    const executor = new FakeExecutor();
    const agent = startAgent(master, { executor });
    await master.waitForType("hello");

    master.send(dispatchFrame());
    await master.waitForType("escalation");
    await agent.close();

    expect(agent.state).toBe("closed");
    expect(activeHandleWasCancelled(executor)).toBe(true);
    const connections = master.connectionCount;
    const hellos = master.frames.filter((frame) => frame.type === "hello").length;
    await delay(150);
    expect(master.connectionCount).toBe(connections);
    expect(master.frames.filter((frame) => frame.type === "hello")).toHaveLength(hellos);
  });
});

function activeHandleWasCancelled(executor: FakeExecutor): boolean {
  return executor.cancels.length === 1;
}

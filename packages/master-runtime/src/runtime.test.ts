import { mkdtempSync, rmSync } from "node:fs";
import type { IncomingMessage } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Duplex } from "node:stream";
import type {
  Assignment,
  EscalationRecord,
  LaunchRequest,
  Policy,
  Ticket,
  Trigger,
} from "@gethen/master";
import {
  type DownFrame,
  type EscalationFrame,
  type HeartbeatFrame,
  type HelloFrame,
  PROTOCOL_VERSION,
  type ReportFrame,
} from "@gethen/worker-protocol";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createMasterRuntime,
  DEFAULT_MASTER_POLICY,
  DEFAULT_PACK_DIGEST,
  defaultSelectWorker,
  type MasterRuntime,
  type MasterRuntimeOptions,
  type WorkerConnectionLike,
  type WorkerGatewayLike,
  type WorkerSnapshot,
} from "./runtime";

const NOW = new Date("2026-11-16T05:00:00.000Z");
const AT = NOW.toISOString();

interface FakeConnection extends WorkerConnectionLike {
  id: string;
  workerId?: string;
  isOpen: boolean;
}

interface FakeDispatch {
  workerId: string;
  frame: DownFrame;
}

interface FakeUpgrade {
  request: IncomingMessage;
  socket: Duplex;
  head: Buffer;
}

/**
 * Gateway double: no sockets. `events` lets the close-order test observe the
 * teardown sequence; `upgrades` proves `runtime.handleUpgrade` forwards.
 */
class FakeGateway implements WorkerGatewayLike {
  readonly connectionsList: FakeConnection[];
  readonly dispatched: FakeDispatch[] = [];
  readonly upgrades: FakeUpgrade[] = [];
  closeCalls = 0;

  constructor(
    workers: Array<{ id: string; workerId?: string; isOpen?: boolean }> = [],
    private readonly events?: string[],
  ) {
    this.connectionsList = workers.map((worker) => ({
      id: worker.id,
      ...(worker.workerId === undefined ? {} : { workerId: worker.workerId }),
      isOpen: worker.isOpen ?? true,
    }));
  }

  connections(): readonly WorkerConnectionLike[] {
    return this.connectionsList;
  }

  get(workerId: string): WorkerConnectionLike | undefined {
    return this.connectionsList.find((connection) => connection.workerId === workerId);
  }

  async dispatch(workerId: string, frame: DownFrame): Promise<void> {
    this.dispatched.push({ workerId, frame });
  }

  async close(): Promise<void> {
    this.closeCalls += 1;
    this.events?.push("gateway");
  }

  handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void {
    this.upgrades.push({ request, socket, head });
  }
}

function firstConnection(gateway: FakeGateway): FakeConnection {
  const connection = gateway.connectionsList[0];
  if (connection === undefined) throw new Error("fake gateway has no connections");
  return connection;
}

function helloFrame(workerId: string, overrides: Partial<HelloFrame> = {}): HelloFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "hello",
    workerId,
    protocol: PROTOCOL_VERSION,
    machine: `machine-${workerId}`,
    platform: "linux",
    runtimes: ["dsh"],
    workspaces: ["/ws"],
    capabilities: { promptInjection: "system" },
    load: { running: 0, quota: 2 },
    models: ["deepseek"],
    ...overrides,
  };
}

function heartbeatFrame(load: { running: number; quota: number }, at = AT): HeartbeatFrame {
  return { v: PROTOCOL_VERSION, type: "heartbeat", at, load };
}

function reportFrame(assignmentId: string, overrides: Partial<ReportFrame> = {}): ReportFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "report",
    assignmentId,
    kind: "ready",
    summary: "done",
    receipt: { packDigest: "sha256:pack", events: 2 },
    ...overrides,
  };
}

function escalationFrame(assignmentId: string): EscalationFrame {
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
    blocking: true,
  };
}

let triggerSeq = 0;

function localTrigger(
  text = "fix the failing test",
  options: { workspace?: string; scope?: string } = {},
): Trigger {
  triggerSeq += 1;
  return {
    id: `trigger-${triggerSeq}`,
    source: "local",
    thread: "local",
    actor: "local-user",
    bot: false,
    addressed: "mention",
    text,
    at: new Date().toISOString(),
    meta: { scope: options.scope ?? "local", workspace: options.workspace ?? "/ws" },
  };
}

/** Trigger whose Assignment keeps the engine's empty workspace, so host defaults apply. */
function defaultedTrigger(): Trigger {
  return localTrigger("fix the failing test", { workspace: "", scope: "" });
}

function launchRequest(workspace = "/ws"): LaunchRequest {
  const ticket: Ticket = {
    id: "ticket-1",
    mandate: "mandate-1",
    source: "local",
    threads: ["thread-1"],
    subscriptions: ["thread-1"],
    channels: [],
    inputs: [],
    assignments: ["assignment-1"],
    openedAt: AT,
  };
  const assignment: Assignment = {
    id: "assignment-1",
    ticket: ticket.id,
    worker: "worker-a",
    workspace,
    packDigest: "sha256:pack",
    state: "pending",
    operationId: "launch-0123456789abcdef",
  };
  return { ticket, assignment, packDigest: "sha256:pack", prompt: "prompt" };
}

function snapshot(overrides: Partial<WorkerSnapshot> & { workerId: string }): WorkerSnapshot {
  return {
    machine: `machine-${overrides.workerId}`,
    online: true,
    workspaces: ["/ws"],
    runtimes: ["dsh"],
    models: ["deepseek"],
    running: 0,
    quota: 2,
    lastSeenAt: AT,
    ...overrides,
  };
}

async function waitFor(predicate: () => boolean, timeoutMs = 1_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("timed out waiting for condition");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

let dir: string;
let runtimeCount = 0;
const runtimes: MasterRuntime[] = [];

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "gethen-master-runtime-"));
});

afterEach(async () => {
  while (runtimes.length > 0) {
    const runtime = runtimes.pop();
    if (runtime !== undefined) await runtime.close();
  }
  rmSync(dir, { recursive: true, force: true });
});

function makeRuntime(overrides: Partial<MasterRuntimeOptions> = {}): MasterRuntime {
  runtimeCount += 1;
  const runtime = createMasterRuntime({
    dbPath: join(dir, `runtime-${runtimeCount}`, "master.sqlite"),
    now: () => NOW,
    ...overrides,
  });
  runtimes.push(runtime);
  return runtime;
}

describe("createMasterRuntime", () => {
  it("falls back to the fake launch and completes a ticket with host defaults", async () => {
    const runtime = makeRuntime({ workspace: "/ws", packDigest: "sha256:test" });
    expect(runtime.launch).toBe("fake");
    expect(runtime.gateway).toBeUndefined();
    expect(runtime.bridge).toBeUndefined();
    expect(runtime.handleUpgrade).toBeUndefined();
    expect(runtime.workers()).toEqual([]);
    expect(runtime.health()).toEqual({
      launch: "fake",
      workers: 0,
      tickets: 0,
      open: 0,
      receipts: 0,
      escalations: 0,
    });

    const result = await runtime.service.submit(defaultedTrigger());
    expect(result.decision?.outcome).toBe("accept");
    expect(runtime.health()).toEqual({
      launch: "fake",
      workers: 0,
      tickets: 1,
      open: 1,
      receipts: 1,
      escalations: 0,
    });
    const receipt = runtime.service.store.listReceipts().at(0);
    expect(receipt).toMatchObject({
      worker: "",
      workspace: "/ws",
      packDigest: "sha256:test",
      outcome: "completed",
    });
    expect(runtime.service.store.policy.current()).toEqual(DEFAULT_MASTER_POLICY);
  });

  it("builds the ws gateway and bridge from in-memory worker tokens", () => {
    const runtime = makeRuntime({
      tokens: [{ workerId: "worker-a", token: "secret-a" }],
      workspace: "/ws",
    });
    expect(runtime.launch).toBe("ws");
    expect(runtime.gateway).toBeDefined();
    expect(runtime.bridge).toBeDefined();
    expect(runtime.handleUpgrade).toBeTypeOf("function");
    expect(runtime.health()).toMatchObject({ launch: "ws", workers: 0 });
  });

  it("builds the ws gateway from a verifyToken callback without a token list", () => {
    const runtime = makeRuntime({
      verifyToken: async (token) => (token === "secret" ? { workerId: "worker-a" } : undefined),
    });
    expect(runtime.launch).toBe("ws");
    expect(runtime.gateway).toBeDefined();
    expect(runtime.bridge).toBeDefined();
  });

  it("derives worker snapshots from hello and heartbeat frames", () => {
    const gateway = new FakeGateway([
      { id: "conn-1" },
      { id: "conn-2", workerId: "worker-b" },
      { id: "conn-3", workerId: "worker-a" },
    ]);
    const runtime = makeRuntime({ gateway, workspace: "/ws" });
    // Pre-hello connections and unknown hello state are excluded / empty.
    expect(runtime.workers().map((worker) => worker.workerId)).toEqual(["worker-a", "worker-b"]);
    const before = runtime.workers().at(0);
    expect(before).toMatchObject({ workerId: "worker-a", machine: "", online: true, quota: 0 });

    const connection = gateway.connectionsList.at(2);
    if (connection === undefined) throw new Error("missing fake connection");
    runtime.handleFrame(
      connection,
      helloFrame("worker-a", { machine: "box-a", load: { running: 1, quota: 4 } }),
    );
    expect(runtime.workers().at(0)).toEqual({
      workerId: "worker-a",
      machine: "box-a",
      online: true,
      workspaces: ["/ws"],
      runtimes: ["dsh"],
      models: ["deepseek"],
      running: 1,
      quota: 4,
      lastSeenAt: AT,
    });

    runtime.handleFrame(
      connection,
      heartbeatFrame({ running: 3, quota: 4 }, "2026-11-16T05:01:00.000Z"),
    );
    expect(runtime.workers().at(0)).toMatchObject({
      running: 3,
      quota: 4,
      lastSeenAt: "2026-11-16T05:01:00.000Z",
    });
  });

  it("uses the least-loaded eligible worker for a dispatch and records the receipt", async () => {
    const gateway = new FakeGateway([
      { id: "conn-1", workerId: "worker-a" },
      { id: "conn-2", workerId: "worker-b" },
    ]);
    const runtime = makeRuntime({ gateway, workspace: "/ws", packDigest: "sha256:test" });
    const connectionA = gateway.connectionsList[0];
    const connectionB = gateway.connectionsList[1];
    if (connectionA === undefined || connectionB === undefined) {
      throw new Error("missing fake connections");
    }
    runtime.handleFrame(connectionA, helloFrame("worker-a", { load: { running: 1, quota: 2 } }));
    runtime.handleFrame(connectionB, helloFrame("worker-b", { load: { running: 0, quota: 4 } }));

    const submitPromise = runtime.service.submit(localTrigger());
    await waitFor(() => gateway.dispatched.length === 1);
    const dispatch = gateway.dispatched.at(0);
    expect(dispatch?.workerId).toBe("worker-b");
    const frame = dispatch?.frame;
    expect(frame).toMatchObject({
      v: PROTOCOL_VERSION,
      type: "dispatch",
      workspace: "/ws",
      packDigest: "sha256:test",
      envelope: { prompt: expect.any(String) },
    });
    const assignmentId = frame?.type === "dispatch" ? frame.assignmentId : "";
    const ticketId = frame?.type === "dispatch" ? frame.ticketId : "";

    runtime.handleFrame(
      connectionB,
      reportFrame(assignmentId, { receipt: { packDigest: "sha256:test", events: 3 } }),
    );
    await submitPromise;

    expect(runtime.health()).toEqual({
      launch: "ws",
      workers: 2,
      tickets: 1,
      open: 1,
      receipts: 1,
      escalations: 0,
    });
    expect(runtime.service.store.tickets.get(ticketId)?.latestReport).toBe("ready");
    expect(runtime.service.store.listReceipts().at(0)).toMatchObject({
      assignment: assignmentId,
      ticket: ticketId,
      worker: "worker-b",
      workspace: "/ws",
      packDigest: "sha256:test",
      outcome: "completed",
      events: 3,
    });
  });

  it("honours a custom selectWorker hook", async () => {
    const gateway = new FakeGateway([
      { id: "conn-1", workerId: "worker-a" },
      { id: "conn-2", workerId: "worker-b" },
    ]);
    const runtime = makeRuntime({
      gateway,
      workspace: "/ws",
      selectWorker: (_request, workers) => workers.find((worker) => worker.workerId === "worker-b"),
    });
    const connectionB = gateway.connectionsList[1];
    if (connectionB === undefined) throw new Error("missing fake connection");
    runtime.handleFrame(connectionB, helloFrame("worker-b"));

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a ws bridge");
    await waitFor(() => gateway.dispatched.length === 1);
    expect(gateway.dispatched.at(0)?.workerId).toBe("worker-b");
    runtime.handleFrame(connectionB, reportFrame("assignment-1"));
    const result = await resultPromise;
    expect(result.receipt.worker).toBe("worker-b");
    expect(result.report.kind).toBe("ready");
  });

  it("rejects a launch when no worker fits or the chosen worker is disconnected", async () => {
    const empty = makeRuntime({ gateway: new FakeGateway(), workspace: "/ws" });
    const noWorkerBridge = empty.bridge;
    if (noWorkerBridge === undefined) throw new Error("expected a ws bridge");
    await expect(noWorkerBridge.launchPort.launch(launchRequest())).rejects.toThrow(/WS_NO_WORKER/);

    const connected = new FakeGateway([{ id: "conn-1", workerId: "worker-a" }]);
    const picky = makeRuntime({
      gateway: connected,
      workspace: "/ws",
      selectWorker: () => snapshot({ workerId: "worker-ghost" }),
    });
    const pickyBridge = picky.bridge;
    if (pickyBridge === undefined) throw new Error("expected a ws bridge");
    await expect(pickyBridge.launchPort.launch(launchRequest())).rejects.toThrow(
      /WS_WORKER_UNAVAILABLE/,
    );
  });

  it("forwards escalations to the injected resolver and observers", async () => {
    const gateway = new FakeGateway([{ id: "conn-1", workerId: "worker-a" }]);
    const escalationRecords: EscalationRecord[] = [];
    const escalationStatuses: string[] = [];
    const resolutionRecords: EscalationRecord[] = [];
    const resolutionStatuses: string[] = [];
    const resolveEscalation = vi.fn(async () => ({
      kind: "answer" as const,
      text: "Merge it.",
      option: "merge",
    }));
    const runtime = makeRuntime({
      gateway,
      workspace: "/ws",
      resolveEscalation,
      onEscalation: (record) => {
        escalationRecords.push(record);
        escalationStatuses.push(record.status);
      },
      onResolution: (record) => {
        resolutionRecords.push(record);
        resolutionStatuses.push(record.status);
      },
    });
    const connection = firstConnection(gateway);
    runtime.handleFrame(connection, helloFrame("worker-a"));

    const resultPromise = runtime.bridge?.launchPort.launch(launchRequest());
    if (resultPromise === undefined) throw new Error("expected a ws bridge");
    await waitFor(() => gateway.dispatched.length === 1);

    runtime.handleFrame(connection, escalationFrame("assignment-1"));
    await waitFor(() => gateway.dispatched.length === 2);
    expect(escalationRecords.at(0)).toMatchObject({ id: "esc-1" });
    expect(escalationStatuses).toEqual(["waiting"]);
    expect(resolutionRecords.at(0)).toMatchObject({ id: "esc-1" });
    expect(resolutionStatuses).toEqual(["resolved"]);
    expect(gateway.dispatched.at(1)?.frame).toMatchObject({
      type: "resolution",
      assignmentId: "assignment-1",
      escalationId: "esc-1",
      resolution: { kind: "answer", text: "Merge it.", option: "merge" },
    });

    runtime.handleFrame(connection, reportFrame("assignment-1"));
    const result = await resultPromise;
    expect(result.escalations?.at(0)).toMatchObject({ status: "resolved" });
  });

  it("forwards the upgrade triple to the gateway when one is attached", () => {
    const gateway = new FakeGateway();
    const runtime = makeRuntime({ gateway });
    const request = {} as IncomingMessage;
    const socket = {} as Duplex;
    const head = Buffer.from("head");
    runtime.handleUpgrade?.(request, socket, head);
    expect(gateway.upgrades).toHaveLength(1);
    expect(gateway.upgrades.at(0)).toEqual({ request, socket, head });
  });

  it("closes the gateway before stopping the service, once", async () => {
    const events: string[] = [];
    const gateway = new FakeGateway([], events);
    const runtime = makeRuntime({ gateway });
    const originalStop = runtime.service.stop.bind(runtime.service);
    const stopSpy = vi.spyOn(runtime.service, "stop").mockImplementation(() => {
      events.push("service");
      originalStop();
    });

    await runtime.close();
    expect(events).toEqual(["gateway", "service"]);
    expect(gateway.closeCalls).toBe(1);
    expect(stopSpy).toHaveBeenCalledTimes(1);

    await runtime.close();
    expect(events).toEqual(["gateway", "service"]);
    expect(gateway.closeCalls).toBe(1);
  });

  it("passes a host policy through to the Master service", () => {
    const policy: Policy = {
      version: 7,
      acceptMode: "observe",
      rules: [{ id: "r1", name: "rule", instruction: "do it", enabled: true }],
      sources: {},
    };
    const runtime = makeRuntime({ policy });
    expect(runtime.service.store.policy.current()).toEqual(policy);
    expect(DEFAULT_MASTER_POLICY).toEqual({
      version: 1,
      acceptMode: "auto",
      rules: [],
      sources: { local: { allowed: ["local-user"], bots: [] } },
    });
    expect(DEFAULT_PACK_DIGEST).toBe("sha256:local-unpinned");
  });

  it("counts persisted escalations in health", () => {
    const runtime = makeRuntime();
    runtime.service.store.saveEscalation({
      id: "esc-1",
      ticket: "ticket-1",
      assignment: "assignment-1",
      session: "worker-a",
      kind: "decision",
      question: "Which branch?",
      options: [],
      status: "waiting",
      at: AT,
    });
    expect(runtime.health().escalations).toBe(1);
  });
});

describe("defaultSelectWorker", () => {
  it("filters offline, full and wrong-workspace workers, then sorts by load and id", () => {
    const workers: WorkerSnapshot[] = [
      snapshot({ workerId: "worker-a", workspaces: ["/ws"], running: 1, quota: 2 }), // 0.5
      snapshot({ workerId: "worker-b", workspaces: ["/ws"], running: 1, quota: 4 }), // 0.25
      snapshot({ workerId: "worker-c", workspaces: ["/other"], running: 0, quota: 2 }),
      snapshot({ workerId: "worker-d", workspaces: ["/ws"], running: 0, quota: 2, online: false }),
      snapshot({ workerId: "worker-e", workspaces: ["/ws"], running: 2, quota: 2 }),
      snapshot({ workerId: "worker-f", workspaces: ["*"], running: 1, quota: 4 }), // 0.25, id tie
    ];
    expect(defaultSelectWorker(launchRequest("/ws"), workers)?.workerId).toBe("worker-b");
    expect(defaultSelectWorker(launchRequest("/elsewhere"), workers)?.workerId).toBe("worker-f");
    expect(defaultSelectWorker(launchRequest("/elsewhere"), workers, "/elsewhere")?.workerId).toBe(
      "worker-f",
    );
    expect(defaultSelectWorker(launchRequest("/ws"), [])).toBeUndefined();
    expect(
      defaultSelectWorker(launchRequest("/ws"), [
        snapshot({ workerId: "worker-a", running: 0, quota: 0 }),
      ]),
    ).toBeUndefined();
  });

  it("falls back to the host workspace when the assignment has none", () => {
    const workers: WorkerSnapshot[] = [
      snapshot({ workerId: "worker-a", workspaces: ["/host-ws"], running: 0, quota: 2 }),
      snapshot({ workerId: "worker-b", workspaces: ["/other"], running: 0, quota: 2 }),
    ];
    expect(defaultSelectWorker(launchRequest(""), workers, "/host-ws")?.workerId).toBe("worker-a");
  });
});

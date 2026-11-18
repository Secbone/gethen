import type { LaunchPort, LaunchRequest, LaunchResult, Ticket } from "@gethen/master";
import type { WorkerManifest } from "@gethen/worker-bridge";
import { describe, expect, it } from "vitest";
import { createRoutedLaunchPort, evidenceFromReceipts, WorkerDirectory } from "./fleet";

const AT = "2026-11-18T00:00:00.000Z";

function manifest(
  id: string,
  workspace: string,
  overrides: Partial<WorkerManifest> = {},
): WorkerManifest {
  return {
    id,
    machine: id,
    runtime: "dsh",
    workspaces: [{ id: workspace }],
    capabilities: {
      promptInjection: "system",
      skills: "filesystem",
      mcp: false,
      permissions: "approval",
      streaming: true,
      steer: true,
      fork: false,
    },
    models: ["deepseek-v4.1-flash"],
    costClass: "mid",
    load: { running: 0, quota: 1 },
    health: { state: "online", lastHeartbeatAt: AT },
    ...overrides,
  };
}

function request(assignmentId: string, workspace: string): LaunchRequest {
  const ticket: Ticket = {
    id: `ticket-${assignmentId}`,
    mandate: `mandate-${assignmentId}`,
    source: "local",
    threads: ["local"],
    subscriptions: ["local"],
    channels: [],
    inputs: [],
    assignments: [assignmentId],
    openedAt: AT,
  };
  return {
    assignment: {
      id: assignmentId,
      ticket: ticket.id,
      worker: "",
      workspace,
      packDigest: "sha256:pack",
      state: "running",
      operationId: `launch-${assignmentId}`,
    },
    ticket,
    packDigest: "sha256:pack",
    prompt: "do the work",
  };
}

function result(worker: string): LaunchResult {
  return {
    report: { kind: "ready", summary: `${worker} done`, at: AT },
    receipt: {
      assignment: "assignment",
      ticket: "ticket",
      worker,
      workspace: "/ws",
      packDigest: "sha256:pack",
      at: AT,
      outcome: "completed",
      summary: `${worker} done`,
      events: 0,
    },
  };
}

function deferredPort(): {
  port: LaunchPort;
  calls: Array<{ request: LaunchRequest; resolve(result: LaunchResult): void }>;
} {
  const calls: Array<{ request: LaunchRequest; resolve(result: LaunchResult): void }> = [];
  return {
    calls,
    port: {
      launch(input) {
        return new Promise<LaunchResult>((resolve) => {
          calls.push({ request: input, resolve });
        });
      },
    },
  };
}

describe("WorkerDirectory", () => {
  it("registers hello manifests, accounts load and remembers session affinity", () => {
    const directory = new WorkerDirectory(["worker-a", "worker-b"]);
    expect(directory.snapshots().every((worker) => !worker.registered)).toBe(true);
    directory.connect("worker-a");
    directory.register(manifest("worker-a", "/ws-a", { load: { running: 0, quota: 2 } }), AT);
    expect(directory.onlineCount()).toBe(1);
    expect(directory.manifest("worker-a")?.load).toEqual({ running: 0, quota: 2 });

    directory.acquire("worker-a", "assignment-1");
    expect(directory.manifest("worker-a")?.load.running).toBe(1);
    directory.release("worker-a", "assignment-1");
    expect(directory.manifest("worker-a")?.load.running).toBe(0);

    directory.recordSession("worker-a", { mode: "reuse", key: "project-x" });
    expect(directory.sessionWorker({ mode: "reuse", key: "project-x" })).toBe("worker-a");

    directory.disconnect("worker-a", AT);
    expect(directory.manifest("worker-a")?.health.state).toBe("offline");
  });

  it("derives pooled evidence from receipts", () => {
    const evidence = evidenceFromReceipts([
      {
        assignment: "a1",
        ticket: "t1",
        worker: "worker-a",
        workspace: "/ws",
        packDigest: "p",
        at: AT,
        outcome: "completed",
        summary: "",
        events: 0,
      },
      {
        assignment: "a2",
        ticket: "t2",
        worker: "worker-a",
        workspace: "/ws",
        packDigest: "p",
        at: AT,
        outcome: "failed",
        summary: "",
        events: 0,
      },
    ]);
    expect(evidence["worker-a"]).toEqual({ attempts: 2, successes: 1 });
  });
});

describe("createRoutedLaunchPort", () => {
  it("dispatches to the workspace-affine worker and records the route", async () => {
    const directory = new WorkerDirectory(["worker-a", "worker-b"]);
    directory.register(manifest("worker-a", "/ws-a"), AT);
    directory.register(manifest("worker-b", "/ws-b"), AT);
    const a = deferredPort();
    const b = deferredPort();
    const routed: string[] = [];
    const port = createRoutedLaunchPort({
      directory,
      ports: new Map([
        ["worker-a", a.port],
        ["worker-b", b.port],
      ]),
      onRoute: (input) => routed.push(input.assignment.worker ?? ""),
    });

    const launched = port.launch(request("assignment-a", "/ws-a"));
    expect(a.calls).toHaveLength(1);
    expect(b.calls).toHaveLength(0);
    expect(routed).toEqual(["worker-a"]);
    a.calls[0]?.resolve(result("worker-a"));
    await launched;
    expect(directory.manifest("worker-a")?.load.running).toBe(0);
  });

  it("queues while every eligible worker is at capacity, then dispatches", async () => {
    // Two known workers force the routing path instead of the single-worker fast path.
    const directory = new WorkerDirectory(["worker-a", "worker-b"]);
    directory.register(manifest("worker-a", "/ws"), AT);
    const first = deferredPort();
    const port = createRoutedLaunchPort({
      directory,
      ports: new Map([["worker-a", first.port]]),
      routing: { queuePollMs: 1, queueTimeoutMs: 500 },
    });
    const one = port.launch(request("assignment-1", "/ws"));
    const two = port.launch(request("assignment-2", "/ws"));
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(first.calls).toHaveLength(1);

    first.calls[0]?.resolve(result("worker-a"));
    await one;
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(first.calls).toHaveLength(2);
    expect(first.calls[1]?.request.assignment.id).toBe("assignment-2");
    first.calls[1]?.resolve(result("worker-a"));
    await two;
  });

  it("queues a second launch on a single Worker with quota 1", async () => {
    const directory = new WorkerDirectory(["worker-a"]);
    directory.register(manifest("worker-a", "/ws", { load: { running: 0, quota: 1 } }), AT);
    const first = deferredPort();
    const port = createRoutedLaunchPort({
      directory,
      ports: new Map([["worker-a", first.port]]),
      routing: { queuePollMs: 1, queueTimeoutMs: 500 },
    });

    const one = port.launch(request("assignment-1", "/ws"));
    const two = port.launch(request("assignment-2", "/ws"));
    await new Promise((resolve) => setTimeout(resolve, 10));
    // Exactly one dispatch: the second waits for the quota slot instead of
    // being pushed at the busy Worker (where its delivery would be dropped).
    expect(first.calls).toHaveLength(1);

    first.calls[0]?.resolve(result("worker-a"));
    await one;
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(first.calls).toHaveLength(2);
    expect(first.calls[1]?.request.assignment.id).toBe("assignment-2");
    first.calls[1]?.resolve(result("worker-a"));
    await two;
  });

  it("soft-falls back to an available worker when the workspace is not advertised", async () => {
    const directory = new WorkerDirectory(["worker-a", "worker-b"]);
    directory.register(manifest("worker-a", "/ws-a"), AT);
    directory.register(manifest("worker-b", "/ws-b"), AT);
    const a = deferredPort();
    const b = deferredPort();
    const plans: string[] = [];
    const port = createRoutedLaunchPort({
      directory,
      ports: new Map([
        ["worker-a", a.port],
        ["worker-b", b.port],
      ]),
      routing: { allowWorkspaceFallback: true },
      onRoute: (_request, plan) => plans.push(plan.reason),
    });
    const launched = port.launch(request("assignment-fallback", "/missing"));
    const call = a.calls[0] ?? b.calls[0];
    expect(call).toBeDefined();
    expect(plans[0]).toContain("workspace-fallback:workspace-missing:/missing");
    call?.resolve(result(call.request.assignment.worker));
    await launched;
  });

  it("keeps strict workspace parking by default", async () => {
    const directory = new WorkerDirectory(["worker-a", "worker-b"]);
    directory.register(manifest("worker-a", "/ws-a"), AT);
    directory.register(manifest("worker-b", "/ws-b"), AT);
    const port = createRoutedLaunchPort({ directory, ports: new Map() });
    const outcome = await port.launch(request("assignment-strict", "/missing"));
    expect(outcome.report.summary).toContain("ROUTER_PARKED:workspace-missing");
  });

  it("returns a terminal failed receipt when no worker can serve the workspace", async () => {
    const directory = new WorkerDirectory(["worker-a", "worker-b"]);
    directory.register(manifest("worker-a", "/ws-a"), AT);
    directory.register(manifest("worker-b", "/ws-b"), AT);
    const port = createRoutedLaunchPort({
      directory,
      ports: new Map(),
    });
    const outcome = await port.launch(request("assignment-x", "/elsewhere"));
    expect(outcome.report.kind).toBe("failed");
    expect(outcome.report.summary).toContain("ROUTER_PARKED:workspace-missing:/elsewhere");
    expect(outcome.receipt.worker).toBe("");
  });
});

describe("createRoutedLaunchPort scheduling metrics", () => {
  it("counts queued depth, running slots and dispatches", async () => {
    const directory = new WorkerDirectory(["worker-a"]);
    directory.register(manifest("worker-a", "/ws", { load: { running: 0, quota: 1 } }), AT);
    const ports = deferredPort();
    const port = createRoutedLaunchPort({
      directory,
      ports: new Map([["worker-a", ports.port]]),
      routing: { queuePollMs: 1, queueTimeoutMs: 500 },
    });

    const one = port.launch(request("assignment-1", "/ws"));
    const two = port.launch(request("assignment-2", "/ws"));
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(port.metrics()).toMatchObject({
      queued: 1,
      running: 1,
      dispatched: 1,
      queueEntries: 1,
      maxQueueDepth: 1,
      queueTimeouts: 0,
    });

    ports.calls[0]?.resolve(result("worker-a"));
    await one;
    await new Promise((resolve) => setTimeout(resolve, 10));
    ports.calls[1]?.resolve(result("worker-a"));
    await two;
    expect(port.metrics()).toMatchObject({
      queued: 0,
      running: 0,
      dispatched: 2,
      queueEntries: 1,
      queueTimeouts: 0,
    });
  });

  it("counts a bounded queue timeout as a failed receipt", async () => {
    const directory = new WorkerDirectory(["worker-a"]);
    directory.register(manifest("worker-a", "/ws", { load: { running: 0, quota: 1 } }), AT);
    const ports = deferredPort();
    const port = createRoutedLaunchPort({
      directory,
      ports: new Map([["worker-a", ports.port]]),
      routing: { queuePollMs: 1, queueTimeoutMs: 5 },
    });

    const held = port.launch(request("assignment-1", "/ws"));
    const timedOut = await port.launch(request("assignment-2", "/ws"));
    expect(timedOut.report.kind).toBe("failed");
    expect(timedOut.report.summary).toContain("ROUTER_QUEUE_TIMEOUT");
    expect(port.metrics()).toMatchObject({ queueTimeouts: 1, queued: 0 });
    ports.calls[0]?.resolve(result("worker-a"));
    await held;
  });

  it("counts a session-affine dispatch as reused", async () => {
    const directory = new WorkerDirectory(["worker-a", "worker-b"]);
    directory.register(manifest("worker-a", "/ws"), AT);
    directory.register(manifest("worker-b", "/ws"), AT);
    directory.recordSession("worker-b", { mode: "reuse", key: "project-x" });
    const a = deferredPort();
    const b = deferredPort();
    const port = createRoutedLaunchPort({
      directory,
      ports: new Map([
        ["worker-a", a.port],
        ["worker-b", b.port],
      ]),
    });
    const input = request("assignment-1", "/ws");
    input.assignment.session = { mode: "reuse", key: "project-x" };

    const launched = port.launch(input);
    expect(b.calls).toHaveLength(1);
    expect(a.calls).toHaveLength(0);
    b.calls[0]?.resolve(result("worker-b"));
    await launched;
    expect(port.metrics()).toMatchObject({ dispatched: 1, reused: 1, running: 0 });
  });
});

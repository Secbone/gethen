import type { DispatchRequest } from "@gethen/worker-bridge";
import { describe, expect, it } from "vitest";
import { createRemoteBridge } from "./bridge";
import type { RemoteClientLike, RemoteDevice, RemoteWorkspace } from "./client";

const HOST = "host-1";
const WORKSPACE = "ws-1";
const T = "2026-11-16T10:00:00.000Z";

const CAPABILITIES = {
  promptInjection: "system",
  skills: "none",
  mcp: false,
  permissions: "approval",
  streaming: true,
  steer: true,
  fork: false,
} as const;

interface DevicesCall {
  method: "devices";
  signal?: AbortSignal;
}

interface WorkspacesCall {
  method: "listRemoteWorkspaces";
  deviceId: string;
  signal?: AbortSignal;
}

interface ToolCall {
  method: "runSessionTool";
  operation: string;
  args: unknown;
  signal?: AbortSignal;
}

type ClientCall = DevicesCall | WorkspacesCall | ToolCall;

class FakeRemoteClient implements RemoteClientLike {
  readonly calls: ClientCall[] = [];
  devicesReply: RemoteDevice[] = [];
  workspacesReply: Record<string, RemoteWorkspace[]> = {};
  toolHandler: (operation: string, args: unknown) => unknown = () => ({});

  async devices(signal?: AbortSignal): Promise<RemoteDevice[]> {
    this.calls.push({ method: "devices", signal });
    return this.devicesReply;
  }

  async listRemoteWorkspaces(deviceId: string, signal?: AbortSignal): Promise<RemoteWorkspace[]> {
    this.calls.push({ method: "listRemoteWorkspaces", deviceId, signal });
    return this.workspacesReply[deviceId] ?? [];
  }

  async runSessionTool(operation: string, args: unknown, signal?: AbortSignal): Promise<unknown> {
    this.calls.push({ method: "runSessionTool", operation, args, signal });
    return this.toolHandler(operation, args);
  }
}

function setup(
  options: {
    client?: FakeRemoteClient;
    model?: string;
    ids?: { next(prefix: string): string };
    signal?: AbortSignal;
  } = {},
) {
  const client = options.client ?? new FakeRemoteClient();
  const bridge = createRemoteBridge({
    client,
    hostId: HOST,
    workspaceId: WORKSPACE,
    model: options.model,
    ids: options.ids,
    signal: options.signal,
  });
  return { client, bridge };
}

function dispatch(overrides: Partial<DispatchRequest> = {}): DispatchRequest {
  return {
    ticket: "ticket-1",
    assignment: "assignment-1",
    worker: "device-a",
    workspace: WORKSPACE,
    prompt: "fix the failing test",
    pack: {},
    ...overrides,
  };
}

function toolCalls(client: FakeRemoteClient, operation: string): ToolCall[] {
  return client.calls.filter(
    (call): call is ToolCall => call.method === "runSessionTool" && call.operation === operation,
  );
}

function toolCall(client: FakeRemoteClient, operation: string): ToolCall {
  const call = toolCalls(client, operation)[0];
  if (call === undefined) throw new Error(`no ${operation} call recorded`);
  return call;
}

function operationIdOf(call: ToolCall): string {
  const args = call.args as { operationId?: unknown };
  if (typeof args.operationId !== "string") throw new Error("call has no operationId");
  return args.operationId;
}

function expectValidOperationId(value: string): void {
  expect(value.length).toBeGreaterThanOrEqual(16);
  expect(value.length).toBeLessThanOrEqual(128);
}

describe("createRemoteBridge list", () => {
  it("maps Remote devices to DSH manifests", async () => {
    const controller = new AbortController();
    const client = new FakeRemoteClient();
    client.devicesReply = [
      { deviceId: "dev-a", name: "laptop", online: true, platform: "linux" },
      { deviceId: "dev-b", name: "cloud", online: false },
    ];
    client.workspacesReply = {
      "dev-a": [{ workspaceId: "ws-app", path: "/srv/app", title: "App" }],
    };
    const { bridge } = setup({ client, model: "deepseek-v4", signal: controller.signal });

    expect(bridge.id).toBe("remote");
    const manifests = await bridge.list();

    expect(manifests).toHaveLength(2);
    expect(manifests[0]).toMatchObject({
      id: "dev-a",
      machine: "laptop",
      runtime: "dsh",
      workspaces: [{ id: "ws-app" }],
      capabilities: CAPABILITIES,
      models: ["deepseek-v4"],
      costClass: "mid",
      load: { running: 0, quota: 1 },
      health: { state: "online" },
    });
    const heartbeat = manifests[0]?.health.lastHeartbeatAt ?? "";
    expect(Number.isNaN(Date.parse(heartbeat))).toBe(false);
    expect(manifests[1]).toMatchObject({
      id: "dev-b",
      machine: "cloud",
      health: { state: "offline" },
      workspaces: [],
      models: ["deepseek-v4"],
    });

    // Offline devices are not asked for workspaces.
    const workspaceCalls = client.calls.filter(
      (call): call is WorkspacesCall => call.method === "listRemoteWorkspaces",
    );
    expect(workspaceCalls.map((call) => call.deviceId)).toEqual(["dev-a"]);
    expect(workspaceCalls[0]?.signal).toBe(controller.signal);
    const devicesCall = client.calls[0];
    expect(devicesCall?.method).toBe("devices");
    if (devicesCall?.method === "devices") {
      expect(devicesCall.signal).toBe(controller.signal);
    }
  });

  it("defaults to no models and no workspaces", async () => {
    const client = new FakeRemoteClient();
    const { bridge } = setup({ client });

    expect(await bridge.list()).toEqual([]);

    client.devicesReply = [{ deviceId: "dev-1", name: "node-1", online: true }];
    const manifests = await bridge.list();
    expect(manifests[0]?.models).toEqual([]);
    expect(manifests[0]?.workspaces).toEqual([]);
  });
});

describe("createRemoteBridge open", () => {
  it("creates a session then sends the prompt", async () => {
    const controller = new AbortController();
    const client = new FakeRemoteClient();
    const order: string[] = [];
    client.toolHandler = (operation) => {
      order.push(operation);
      return operation === "remote_create_session" ? { sessionId: "sess-1" } : { ok: true };
    };
    const { bridge } = setup({ client, signal: controller.signal });

    const ref = await bridge.open(dispatch());

    expect(ref).toEqual({ session: "sess-1", worker: "device-a", workspace: WORKSPACE });
    expect(order).toEqual(["remote_create_session", "remote_send_message"]);
    const create = toolCall(client, "remote_create_session");
    expect(create.args).toEqual({
      hostId: HOST,
      backend: "harness",
      workspaceId: WORKSPACE,
      operationId: expect.any(String),
    });
    expect(create.signal).toBe(controller.signal);
    const send = toolCall(client, "remote_send_message");
    expect(send.args).toEqual({
      hostId: HOST,
      backend: "harness",
      workspaceId: WORKSPACE,
      sessionId: "sess-1",
      message: "fix the failing test",
      operationId: expect.any(String),
    });
    expect(send.signal).toBe(controller.signal);
    expect(operationIdOf(create)).not.toBe(operationIdOf(send));
  });

  it("extracts a session id from every supported reply shape", async () => {
    const replies: unknown[] = [
      { sessionId: "sess-direct" },
      { session: { id: "sess-nested" } },
      "sess-raw",
    ];
    const client = new FakeRemoteClient();
    let createCount = 0;
    client.toolHandler = (operation) => {
      if (operation !== "remote_create_session") return {};
      const reply = replies[createCount] ?? {};
      createCount += 1;
      return reply;
    };
    const { bridge } = setup({ client });

    expect((await bridge.open(dispatch())).session).toBe("sess-direct");
    expect((await bridge.open(dispatch())).session).toBe("sess-nested");
    expect((await bridge.open(dispatch())).session).toBe("sess-raw");
  });

  it("throws SESSION_MISSING and does not send when create has no session", async () => {
    const client = new FakeRemoteClient();
    client.toolHandler = (operation) =>
      operation === "remote_create_session" ? { session: {} } : {};
    const { bridge } = setup({ client });

    await expect(bridge.open(dispatch())).rejects.toThrow(/SESSION_MISSING/);
    expect(toolCalls(client, "remote_send_message")).toHaveLength(0);
  });

  it("generates valid unique operation ids and normalizes injected ones", async () => {
    const client = new FakeRemoteClient();
    let sessions = 0;
    client.toolHandler = (operation) => {
      if (operation !== "remote_create_session") return {};
      sessions += 1;
      return { sessionId: `sess-${sessions}` };
    };
    const { bridge } = setup({ client });

    await bridge.open(dispatch());
    await bridge.open(dispatch());

    const createIds = toolCalls(client, "remote_create_session").map(operationIdOf);
    const sendIds = toolCalls(client, "remote_send_message").map(operationIdOf);
    expect(createIds).toHaveLength(2);
    for (const id of [...createIds, ...sendIds]) expectValidOperationId(id);
    expect(new Set(createIds).size).toBe(2);
    expect(new Set(sendIds).size).toBe(2);

    // Short injected ids are padded to the DSH minimum of 16 characters.
    const shortClient = new FakeRemoteClient();
    shortClient.toolHandler = (operation) =>
      operation === "remote_create_session" ? "sess-short" : {};
    const short = setup({ client: shortClient, ids: { next: () => "x" } });
    await short.bridge.open(dispatch());
    expect(operationIdOf(toolCall(shortClient, "remote_create_session"))).toBe("x".padEnd(16, "0"));

    // Over-long injected ids are rejected before any tool call.
    const longClient = new FakeRemoteClient();
    const long = setup({ client: longClient, ids: { next: () => "x".repeat(129) } });
    await expect(long.bridge.open(dispatch())).rejects.toThrow(/OPERATION_ID_INVALID/);
    expect(longClient.calls).toHaveLength(0);
  });
});

describe("createRemoteBridge send", () => {
  it("posts input through remote_send_message", async () => {
    const client = new FakeRemoteClient();
    client.toolHandler = () => ({ ok: true });
    const { bridge } = setup({ client });

    await expect(bridge.send("sess-1", "steer left", "op-0123456789abcdef")).resolves.toBe(
      "accepted",
    );

    expect(toolCall(client, "remote_send_message").args).toEqual({
      hostId: HOST,
      backend: "harness",
      workspaceId: WORKSPACE,
      sessionId: "sess-1",
      message: "steer left",
      operationId: "op-0123456789abcdef",
    });
  });

  it("propagates tool failures unchanged", async () => {
    const createFailure = new Error("create exploded");
    const createClient = new FakeRemoteClient();
    createClient.toolHandler = (operation) => {
      if (operation === "remote_create_session") throw createFailure;
      return {};
    };
    await expect(setup({ client: createClient }).bridge.open(dispatch())).rejects.toBe(
      createFailure,
    );
    expect(toolCalls(createClient, "remote_send_message")).toHaveLength(0);

    const sendFailure = new Error("send exploded");
    const sendClient = new FakeRemoteClient();
    sendClient.toolHandler = (operation) => {
      if (operation === "remote_send_message") throw sendFailure;
      return { sessionId: "sess-1" };
    };
    await expect(
      setup({ client: sendClient }).bridge.send("sess-1", "hi", "op-0123456789abcdef"),
    ).rejects.toBe(sendFailure);
  });
});

const STATE_CASES: Array<[string, unknown, "running" | "idle" | "unknown"]> = [
  ["a running snapshot", { state: "running" }, "running"],
  ["an idle snapshot", { state: "idle" }, "idle"],
  ["an unsupported state", { state: "waiting" }, "unknown"],
  [
    "a multi-session reply",
    {
      sessions: [
        { sessionId: "other", state: "running" },
        { sessionId: "sess-1", state: "idle" },
      ],
    },
    "idle",
  ],
];

describe("createRemoteBridge state", () => {
  it.each(STATE_CASES)("maps %s to a bridge state", async (_label, reply, expected) => {
    const client = new FakeRemoteClient();
    client.toolHandler = () => reply;
    const { bridge } = setup({ client });

    await expect(bridge.state("sess-1")).resolves.toBe(expected);
    expect(toolCall(client, "remote_wait_sessions").args).toEqual({
      targets: [{ hostId: HOST, backend: "harness", workspaceId: WORKSPACE, sessionId: "sess-1" }],
      timeoutMs: 0,
    });
  });
});

describe("createRemoteBridge read", () => {
  it("reads a transcript page and forwards the page size", async () => {
    const client = new FakeRemoteClient();
    client.toolHandler = () => ({
      events: [
        { seq: 7, at: T, type: "message", text: "hello", extra: "dropped" },
        { seq: 8, at: T, type: "tool" },
      ],
      next: { beforeSeq: 6, throughSeq: 7 },
      truncated: true,
    });
    const { bridge } = setup({ client });

    const page = await bridge.read("sess-1");

    expect(toolCall(client, "remote_read_session").args).toEqual({
      hostId: HOST,
      backend: "harness",
      workspaceId: WORKSPACE,
      sessionId: "sess-1",
      maxMessages: 20,
    });
    expect(page).toEqual({
      session: "sess-1",
      events: [
        { seq: 7, at: T, type: "message", text: "hello" },
        { seq: 8, at: T, type: "tool" },
      ],
      next: { beforeSeq: 6, throughSeq: 7 },
      truncated: true,
    });

    const broken = new FakeRemoteClient();
    broken.toolHandler = () => "not a page";
    await expect(setup({ client: broken }).bridge.read("sess-1")).rejects.toThrow(
      /TRANSCRIPT_INVALID/,
    );
  });

  it("forwards the transcript cursor", async () => {
    const client = new FakeRemoteClient();
    client.toolHandler = () => ({ events: [], truncated: false });
    const { bridge } = setup({ client });

    const page = await bridge.read("sess-1", { beforeSeq: 5, throughSeq: 6 });

    expect(toolCall(client, "remote_read_session").args).toEqual({
      hostId: HOST,
      backend: "harness",
      workspaceId: WORKSPACE,
      sessionId: "sess-1",
      maxMessages: 20,
      cursor: { beforeSeq: 5, throughSeq: 6 },
    });
    expect(page).toEqual({ session: "sess-1", events: [], truncated: false });
  });
});

describe("createRemoteBridge interrupt and receipt", () => {
  it("interrupts a session with a generated operation id", async () => {
    const client = new FakeRemoteClient();
    const { bridge } = setup({ client });

    await expect(bridge.interrupt("sess-1")).resolves.toBeUndefined();

    const call = toolCall(client, "remote_interrupt_session");
    const args = call.args as {
      hostId: string;
      backend: string;
      sessionId: string;
      operationId: string;
    };
    expect(args).toMatchObject({ hostId: HOST, backend: "harness", sessionId: "sess-1" });
    expectValidOperationId(args.operationId);
  });

  it("refuses to build receipts in P3", async () => {
    const client = new FakeRemoteClient();
    const { bridge } = setup({ client });

    await expect(bridge.receipt("sess-1")).rejects.toThrow(/RECEIPT_UNAVAILABLE/);
    expect(client.calls).toHaveLength(0);
  });
});

import { createServer, type Server } from "node:http";
import { connect as netConnect } from "node:net";
import {
  type DispatchFrame,
  type DownFrame,
  encodeFrame,
  type HeartbeatFrame,
  type HelloFrame,
  parseDownstreamFrame,
  parseUpstreamFrame,
  type UpFrame,
} from "@gethen/worker-protocol";
import { afterEach, describe, expect, it } from "vitest";
import { type RawData, WebSocket } from "ws";
import {
  createMemoryTokenStore,
  createWorkerGateway,
  GATEWAY_CLOSE_CODES,
  GatewayProtocolError,
  type WorkerGateway,
  type WorkerGatewayOptions,
} from "./index";

const TOKEN_1 = "token-one";
const TOKEN_2 = "token-two";

function hello(overrides: Partial<HelloFrame> = {}): HelloFrame {
  return {
    v: 1,
    type: "hello",
    workerId: "worker-1",
    protocol: 1,
    machine: "build-box",
    platform: "linux",
    runtimes: ["dsh"],
    workspaces: ["/srv/app"],
    capabilities: { promptInjection: "system" },
    load: { running: 0, quota: 1 },
    models: ["deepseek-v4"],
    ...overrides,
  };
}

function heartbeat(overrides: Partial<HeartbeatFrame> = {}): HeartbeatFrame {
  return {
    v: 1,
    type: "heartbeat",
    at: "2026-11-16T12:00:00.000Z",
    load: { running: 0, quota: 1 },
    ...overrides,
  };
}

function dispatchFrame(overrides: Partial<DispatchFrame> = {}): DispatchFrame {
  return {
    v: 1,
    type: "dispatch",
    assignmentId: "assignment-1",
    ticketId: "ticket-1",
    workspace: "/srv/app",
    packDigest: "sha256:pack",
    envelope: { proto: "gethen/assignment/v1" },
    ...overrides,
  };
}

interface RecordedHello {
  id: string;
  frame: HelloFrame;
}

interface RecordedFrame {
  id: string;
  workerId: string | undefined;
  frame: UpFrame;
}

interface RecordedClose {
  id: string;
  workerId: string | undefined;
  reason: string;
}

interface Harness {
  gateway: WorkerGateway;
  server: Server;
  port: number;
  url: string;
  hellos: RecordedHello[];
  frames: RecordedFrame[];
  closes: RecordedClose[];
  errors: Error[];
}

const harnesses: Harness[] = [];

afterEach(async () => {
  while (harnesses.length > 0) {
    const harness = harnesses.pop();
    if (harness === undefined) continue;
    await harness.gateway.close();
    await new Promise<void>((resolve) => {
      harness.server.close(() => resolve());
    });
  }
});

async function startHarness(overrides: Partial<WorkerGatewayOptions> = {}): Promise<Harness> {
  const store = createMemoryTokenStore([
    { workerId: "worker-1", token: TOKEN_1 },
    { workerId: "worker-2", token: TOKEN_2 },
  ]);
  const hellos: RecordedHello[] = [];
  const frames: RecordedFrame[] = [];
  const closes: RecordedClose[] = [];
  const errors: Error[] = [];
  const server = createServer((_request, response) => {
    response.statusCode = 404;
    response.end("not found");
  });
  const gateway = createWorkerGateway({
    verifyToken: (token) => store.lookup(token),
    onHello: (conn, frame) => {
      hellos.push({ id: conn.id, frame });
    },
    onFrame: (conn, frame) => {
      frames.push({ id: conn.id, workerId: conn.workerId, frame });
    },
    onClose: (conn, reason) => {
      closes.push({ id: conn.id, workerId: conn.workerId, reason });
    },
    onError: (error) => {
      errors.push(error);
    },
    heartbeatMs: 60_000,
    heartbeatTimeoutMs: 60_000,
    ...overrides,
  });
  server.on("upgrade", (request, socket, head) => {
    gateway.handleUpgrade(request, socket, head);
  });
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("expected the gateway server to listen on a TCP port");
  }
  const harness: Harness = {
    gateway,
    server,
    port: address.port,
    url: `ws://127.0.0.1:${address.port}/gethen/worker`,
    hellos,
    frames,
    closes,
    errors,
  };
  harnesses.push(harness);
  return harness;
}

interface SocketOptions {
  autoPong?: boolean;
}

function openSocket(
  harness: Harness,
  authorization = `Bearer ${TOKEN_1}`,
  options: SocketOptions = {},
): Promise<WebSocket> {
  const ws = new WebSocket(harness.url, "gethen.worker.v1", {
    headers: { authorization },
    ...(options.autoPong === undefined ? {} : { autoPong: options.autoPong }),
  });
  ws.on("error", () => {});
  return new Promise<WebSocket>((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.terminate();
      reject(new Error("websocket did not open in time"));
    }, 2_000);
    timer.unref();
    ws.once("open", () => {
      clearTimeout(timer);
      resolve(ws);
    });
  });
}

interface HandshakeOptions extends SocketOptions {
  token?: string;
  workerId?: string;
  hello?: Partial<HelloFrame>;
}

async function handshake(
  harness: Harness,
  options: HandshakeOptions = {},
): Promise<{ ws: WebSocket; helloOk: DownFrame | UpFrame }> {
  const authorization = `Bearer ${options.token ?? TOKEN_1}`;
  const ws = await openSocket(harness, authorization, { autoPong: options.autoPong });
  const helloOk = nextFrame(ws, (frame) => frame.type === "hello_ok");
  ws.send(encodeFrame(hello({ workerId: options.workerId ?? "worker-1", ...options.hello })));
  return { ws, helloOk: await helloOk };
}

function nextFrame(
  ws: WebSocket,
  predicate: (frame: DownFrame | UpFrame) => boolean = () => true,
): Promise<DownFrame | UpFrame> {
  return new Promise<DownFrame | UpFrame>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("timed out waiting for a frame"));
    }, 2_000);
    timer.unref();
    const onMessage = (data: RawData): void => {
      const parsed = parseFrame(rawDataToString(data));
      if (parsed === undefined) {
        cleanup();
        reject(new Error("received a frame that is neither downstream nor upstream"));
        return;
      }
      if (!predicate(parsed)) return;
      cleanup();
      resolve(parsed);
    };
    const cleanup = (): void => {
      clearTimeout(timer);
      ws.off("message", onMessage);
    };
    ws.on("message", onMessage);
  });
}

function parseFrame(text: string): DownFrame | UpFrame | undefined {
  const downstream = parseDownstreamFrame(text);
  if (downstream.ok) return downstream.value;
  const upstream = parseUpstreamFrame(text);
  return upstream.ok ? upstream.value : undefined;
}

function waitForClose(ws: WebSocket): Promise<number> {
  return new Promise<number>((resolve) => {
    if (ws.readyState === WebSocket.CLOSED) {
      resolve(1006);
      return;
    }
    ws.once("close", (code: number) => resolve(code));
  });
}

function rawDataToString(data: RawData): string {
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return Buffer.from(data).toString("utf8");
}

const delay = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition was not met in time");
    await delay(5);
  }
}

/** Perform a raw HTTP upgrade handshake and return the status line. */
async function rawUpgradeStatus(
  harness: Harness,
  headers: Record<string, string>,
): Promise<string> {
  const socket = netConnect(harness.port, "127.0.0.1");
  socket.on("error", () => {});
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  const requestLines = [
    "GET /gethen/worker HTTP/1.1",
    `Host: 127.0.0.1:${harness.port}`,
    "Connection: Upgrade",
    "Upgrade: websocket",
    "Sec-WebSocket-Version: 13",
    `Sec-WebSocket-Key: ${Buffer.from("0123456789abcdef").toString("base64")}`,
    ...Object.entries(headers).map(([name, value]) => `${name}: ${value}`),
    "",
    "",
  ];
  socket.write(requestLines.join("\r\n"));
  const chunk = await new Promise<Buffer>((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error("no upgrade response"));
    }, 2_000);
    timer.unref();
    socket.once("data", (data: Buffer) => {
      clearTimeout(timer);
      resolve(data);
    });
  });
  socket.destroy();
  return chunk.toString("utf8").split("\r\n")[0] ?? "";
}

describe("createWorkerGateway authentication", () => {
  it("rejects an upgrade with no bearer token", async () => {
    const harness = await startHarness();

    expect(await rawUpgradeStatus(harness, {})).toBe("HTTP/1.1 401 Unauthorized");
    await delay(20);
    expect(harness.gateway.connections()).toHaveLength(0);
    expect(harness.errors).toHaveLength(0);
  });

  it("rejects an upgrade with an unknown bearer token", async () => {
    const harness = await startHarness();

    expect(await rawUpgradeStatus(harness, { authorization: "Bearer nope" })).toBe(
      "HTTP/1.1 401 Unauthorized",
    );
    await delay(20);
    expect(harness.gateway.connections()).toHaveLength(0);
  });

  it("treats a throwing verifier as an auth failure and reports it", async () => {
    const failure = new Error("token store unavailable");
    const harness = await startHarness({
      verifyToken: async () => {
        throw failure;
      },
    });

    expect(await rawUpgradeStatus(harness, { authorization: "Bearer any" })).toBe(
      "HTTP/1.1 401 Unauthorized",
    );
    await waitFor(() => harness.errors.length === 1);
    expect(harness.errors[0]).toBe(failure);
    expect(harness.gateway.connections()).toHaveLength(0);
  });
});

describe("createWorkerGateway handshake", () => {
  it("accepts hello, replies hello_ok and registers the worker", async () => {
    const harness = await startHarness();
    const { ws, helloOk } = await handshake(harness);

    expect(helloOk).toMatchObject({
      v: 1,
      type: "hello_ok",
      workerId: "worker-1",
      heartbeatMs: 60_000,
    });
    if (helloOk.type !== "hello_ok") throw new Error("expected hello_ok");
    expect(Number.isNaN(Date.parse(helloOk.serverTime))).toBe(false);

    const connection = harness.gateway.get("worker-1");
    expect(connection).toBeDefined();
    expect(connection?.workerId).toBe("worker-1");
    expect(connection?.remoteAddress).toBe("127.0.0.1");
    expect(harness.gateway.connections().map((conn) => conn.id)).toEqual([connection?.id]);
    await waitFor(() => harness.hellos.length === 1);
    expect(harness.hellos[0]?.frame.workerId).toBe("worker-1");
    expect(harness.hellos[0]?.frame.machine).toBe("build-box");
    ws.close();
  });

  it("rejects a hello whose workerId does not match the token identity", async () => {
    const harness = await startHarness();
    const ws = await openSocket(harness);
    const errorFrame = nextFrame(ws, (frame) => frame.type === "error");
    const closed = waitForClose(ws);

    ws.send(encodeFrame(hello({ workerId: "worker-2" })));

    expect(await errorFrame).toMatchObject({ type: "error", code: "worker_mismatch" });
    expect(await closed).toBe(GATEWAY_CLOSE_CODES.protocolError);
    await waitFor(() => harness.closes.length === 1);
    expect(harness.closes[0]?.reason).toBe("worker_mismatch");
    expect(harness.gateway.get("worker-1")).toBeUndefined();
    expect(harness.gateway.get("worker-2")).toBeUndefined();
  });

  it("requires hello as the first frame", async () => {
    const harness = await startHarness();
    const ws = await openSocket(harness);
    const errorFrame = nextFrame(ws, (frame) => frame.type === "error");
    const closed = waitForClose(ws);

    ws.send(encodeFrame(heartbeat()));

    expect(await errorFrame).toMatchObject({ type: "error", code: "hello_required" });
    expect(await closed).toBe(GATEWAY_CLOSE_CODES.protocolError);
    await waitFor(() => harness.closes.length === 1);
    expect(harness.closes[0]?.reason).toBe("hello_required");
    expect(harness.frames).toHaveLength(0);
  });

  it("rejects an unsupported protocol version with an error frame", async () => {
    const harness = await startHarness();

    // Envelope version mismatch.
    const envelope = await openSocket(harness);
    const envelopeError = nextFrame(envelope, (frame) => frame.type === "error");
    const envelopeClose = waitForClose(envelope);
    envelope.send(JSON.stringify({ ...hello(), v: 2 }));
    expect(await envelopeError).toMatchObject({ type: "error", code: "unsupported_version" });
    expect(await envelopeClose).toBe(GATEWAY_CLOSE_CODES.protocolError);

    // `hello.protocol` mismatch inside a v:1 envelope.
    const helloVersion = await openSocket(harness);
    const helloError = nextFrame(helloVersion, (frame) => frame.type === "error");
    const helloClose = waitForClose(helloVersion);
    helloVersion.send(JSON.stringify({ ...hello(), protocol: 2 }));
    expect(await helloError).toMatchObject({ type: "error", code: "unsupported_version" });
    expect(await helloClose).toBe(GATEWAY_CLOSE_CODES.protocolError);

    await waitFor(() => harness.gateway.connections().length === 0);
  });

  it("reports an invalid frame without dropping an established connection", async () => {
    const harness = await startHarness();
    const { ws } = await handshake(harness);
    const errorFrame = nextFrame(ws, (frame) => frame.type === "error");

    ws.send("not json at all");

    expect(await errorFrame).toMatchObject({ type: "error", code: "invalid_frame" });
    await waitFor(() => harness.errors.length === 1);
    expect(harness.errors[0]).toBeInstanceOf(GatewayProtocolError);
    expect(ws.readyState).toBe(WebSocket.OPEN);
    expect(harness.gateway.get("worker-1")).toBeDefined();
    ws.close();
  });

  it("replaces the previous connection of the same worker", async () => {
    const harness = await startHarness();
    const first = await handshake(harness);
    const firstConnection = harness.gateway.get("worker-1");
    const firstClosed = waitForClose(first.ws);

    const second = await handshake(harness);

    expect(await firstClosed).toBe(GATEWAY_CLOSE_CODES.replaced);
    await waitFor(() =>
      harness.closes.some(
        (close) => close.id === firstConnection?.id && close.reason === "replaced",
      ),
    );
    const current = harness.gateway.get("worker-1");
    expect(current?.id).not.toBe(firstConnection?.id);
    expect(harness.gateway.connections()).toHaveLength(1);
    second.ws.close();
  });
});

describe("createWorkerGateway dispatch and frames", () => {
  it("dispatches a downstream frame to the connected worker", async () => {
    const harness = await startHarness();
    const { ws } = await handshake(harness);
    const frame = dispatchFrame();
    const received = nextFrame(ws, (candidate) => candidate.type === "dispatch");

    await harness.gateway.dispatch("worker-1", frame);

    expect(await received).toEqual(frame);
    ws.close();
  });

  it("rejects dispatch for a worker that is not connected", async () => {
    const harness = await startHarness();

    await expect(harness.gateway.dispatch("worker-1", dispatchFrame())).rejects.toThrow(
      /not connected/,
    );
  });

  it("broadcasts to every connected worker", async () => {
    const harness = await startHarness();
    const first = await handshake(harness);
    const second = await handshake(harness, { token: TOKEN_2, workerId: "worker-2" });
    const frame = dispatchFrame({ assignmentId: "assignment-broadcast" });
    const firstReceived = nextFrame(first.ws, (candidate) => candidate.type === "dispatch");
    const secondReceived = nextFrame(second.ws, (candidate) => candidate.type === "dispatch");

    harness.gateway.broadcast(frame);

    expect(await firstReceived).toEqual(frame);
    expect(await secondReceived).toEqual(frame);
    first.ws.close();
    second.ws.close();
  });

  it("forwards worker heartbeat frames to onFrame", async () => {
    const harness = await startHarness();
    const { ws } = await handshake(harness);

    ws.send(encodeFrame(heartbeat({ load: { running: 1, quota: 2 } })));

    await waitFor(() => harness.frames.length === 1);
    expect(harness.frames[0]?.workerId).toBe("worker-1");
    expect(harness.frames[0]?.frame).toMatchObject({
      type: "heartbeat",
      load: { running: 1, quota: 2 },
    });
    ws.close();
  });
});

describe("createWorkerGateway heartbeat", () => {
  it("pings idle workers and keeps ponging connections alive", async () => {
    const harness = await startHarness({ heartbeatMs: 20, heartbeatTimeoutMs: 60 });
    const { ws } = await handshake(harness);
    let pings = 0;
    ws.on("ping", () => {
      pings += 1;
    });

    await waitFor(() => pings >= 2, 3_000);

    expect(ws.readyState).toBe(WebSocket.OPEN);
    expect(harness.gateway.get("worker-1")).toBeDefined();
    expect(harness.closes).toHaveLength(0);
    ws.close();
  });

  it("terminates a worker that misses the pong deadline", async () => {
    const harness = await startHarness({ heartbeatMs: 20, heartbeatTimeoutMs: 40 });
    const { ws } = await handshake(harness, { autoPong: false });
    const closed = waitForClose(ws);

    await closed;
    await waitFor(() => harness.closes.length === 1, 3_000);

    expect(harness.closes[0]?.reason).toBe("heartbeat_timeout");
    expect(harness.gateway.get("worker-1")).toBeUndefined();
    expect(harness.gateway.connections()).toHaveLength(0);
  });
});

describe("createWorkerGateway close", () => {
  it("closes every connection and releases the registry", async () => {
    const harness = await startHarness();
    const first = await handshake(harness);
    const second = await handshake(harness, { token: TOKEN_2, workerId: "worker-2" });
    const firstClosed = waitForClose(first.ws);
    const secondClosed = waitForClose(second.ws);
    const connection = harness.gateway.get("worker-1");

    await harness.gateway.close();

    expect(await firstClosed).toBe(GATEWAY_CLOSE_CODES.goingAway);
    expect(await secondClosed).toBe(GATEWAY_CLOSE_CODES.goingAway);
    await waitFor(() => harness.closes.length === 2);
    expect(harness.closes.every((close) => close.reason === "gateway_close")).toBe(true);
    expect(harness.gateway.connections()).toHaveLength(0);
    await expect(connection?.send(dispatchFrame())).rejects.toThrow(/not open/);
    await expect(harness.gateway.dispatch("worker-1", dispatchFrame())).rejects.toThrow(
      /not connected/,
    );

    // Closing twice is harmless.
    await expect(harness.gateway.close()).resolves.toBeUndefined();
  });
});

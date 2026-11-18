import { once } from "node:events";
import { createServer, type Server } from "node:http";
import { createServer as createNetServer, connect as netConnect } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocket, WebSocketServer } from "ws";
import {
  createTransport,
  createWsTransport,
  createWsTransportServer,
  TRANSPORT_CLOSE_CODES,
  type Transport,
  type TransportAuth,
  type TransportMessage,
  type TransportSession,
  type TransportSessionState,
  type WsTransportOptions,
  type WsTransportServer,
  type WsTransportServerOptions,
  type WsTransportSessionContext,
} from "../../index";

interface TestIdentity {
  workerId: string;
}

const TOKEN = "worker-token";
const BEARER: TransportAuth = { kind: "bearer", token: TOKEN };

interface ServerHarness {
  url: string;
  port: number;
  transportServer: WsTransportServer;
  httpServer: Server;
  sessions: TransportSession[];
  contexts: WsTransportSessionContext<TestIdentity>[];
  close(): Promise<void>;
}

const harnesses: ServerHarness[] = [];
const liveSessions: TransportSession[] = [];
const rawServers: WebSocketServer[] = [];

afterEach(async () => {
  for (const session of liveSessions.splice(0)) {
    await session.close("test_cleanup").catch(() => {});
  }
  for (const server of rawServers.splice(0)) {
    for (const client of server.clients) client.terminate();
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  }
  for (const harness of harnesses.splice(0)) await harness.close();
});

function delay(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition was not met in time");
    await delay(5);
  }
}

function waitForState(
  session: TransportSession,
  state: TransportSessionState,
  timeoutMs = 2_000,
): Promise<void> {
  if (session.state === state) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    let off: (() => void) | undefined;
    const timer = setTimeout(() => {
      off?.();
      reject(new Error(`timed out waiting for session state "${state}"`));
    }, timeoutMs);
    timer.unref();
    off = session.onState((next) => {
      if (next !== state) return;
      clearTimeout(timer);
      off?.();
      resolve();
    });
  });
}

function nextMessage(session: TransportSession, timeoutMs = 2_000): Promise<TransportMessage> {
  return new Promise<TransportMessage>((resolve, reject) => {
    let off: (() => void) | undefined;
    const timer = setTimeout(() => {
      off?.();
      reject(new Error("timed out waiting for a message"));
    }, timeoutMs);
    timer.unref();
    off = session.onMessage((message) => {
      clearTimeout(timer);
      off?.();
      resolve(message);
    });
  });
}

function textMessage(payload: string): TransportMessage {
  return {
    id: `text-${payload}`,
    payload,
    contentType: "text/plain",
    at: new Date().toISOString(),
  };
}

function binaryMessage(payload: Uint8Array): TransportMessage {
  return {
    id: "binary",
    payload,
    contentType: "application/octet-stream",
    at: new Date().toISOString(),
  };
}

function makeTransport(overrides: WsTransportOptions = {}): Transport {
  return createTransport({ driver: "ws", heartbeat: false, ...overrides });
}

async function connectSession(
  harness: ServerHarness,
  transportOptions: WsTransportOptions = {},
  auth: TransportAuth = BEARER,
): Promise<TransportSession> {
  const session = await makeTransport(transportOptions).connect({ url: harness.url }, auth);
  liveSessions.push(session);
  return session;
}

function serverSession(harness: ServerHarness, index = 0): TransportSession {
  const session = harness.sessions[index];
  if (session === undefined) throw new Error(`no server session at index ${index}`);
  return session;
}

type StartServerOptions = Omit<Partial<WsTransportServerOptions<TestIdentity>>, "onSession"> & {
  /** Test hook run after the harness has recorded the session/context. */
  handleSession?: (
    session: TransportSession,
    context: WsTransportSessionContext<TestIdentity>,
  ) => void;
};

async function startServer(overrides: StartServerOptions = {}): Promise<ServerHarness> {
  const { handleSession, ...serverOptions } = overrides;
  const sessions: TransportSession[] = [];
  const contexts: WsTransportSessionContext<TestIdentity>[] = [];
  const transportServer = createWsTransportServer<TestIdentity>({
    verifyAuth: async (auth) =>
      auth.kind === "bearer" && auth.token === TOKEN ? { workerId: "worker-1" } : undefined,
    onSession: (session, context) => {
      sessions.push(session);
      contexts.push(context);
      handleSession?.(session, context);
    },
    heartbeatMs: 60_000,
    heartbeatTimeoutMs: 60_000,
    ...serverOptions,
  });
  const httpServer = createServer((_request, response) => {
    response.statusCode = 404;
    response.end("not found");
  });
  httpServer.on("upgrade", (request, socket, head) => {
    transportServer.handleUpgrade(request, socket, head);
  });
  await new Promise<void>((resolve) => {
    httpServer.listen(0, "127.0.0.1", resolve);
  });
  const address = httpServer.address();
  if (address === null || typeof address === "string") {
    throw new Error("expected the harness to listen on a TCP port");
  }
  const harness: ServerHarness = {
    url: `ws://127.0.0.1:${address.port}/gethen`,
    port: address.port,
    transportServer,
    httpServer,
    sessions,
    contexts,
    close: async () => {
      await transportServer.close();
      await new Promise<void>((resolve) => {
        httpServer.close(() => resolve());
      });
    },
  };
  harnesses.push(harness);
  return harness;
}

async function startRawServer(
  options: { autoPong?: boolean; onConnection?: (socket: WebSocket) => void } = {},
): Promise<{ url: string; server: WebSocketServer }> {
  const server = new WebSocketServer({ port: 0, autoPong: options.autoPong });
  await once(server, "listening");
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("expected the raw server to listen on a TCP port");
  }
  server.on("connection", (socket) => {
    socket.on("error", () => {});
    options.onConnection?.(socket);
  });
  rawServers.push(server);
  return { url: `ws://127.0.0.1:${address.port}`, server };
}

/** Send a raw HTTP upgrade and return its status line. */
async function rawUpgradeStatus(port: number, headers: Record<string, string>): Promise<string> {
  const socket = netConnect(port, "127.0.0.1");
  socket.on("error", () => {});
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  const lines = [
    "GET /gethen HTTP/1.1",
    `Host: 127.0.0.1:${port}`,
    "Connection: Upgrade",
    "Upgrade: websocket",
    "Sec-WebSocket-Version: 13",
    `Sec-WebSocket-Key: ${Buffer.from("0123456789abcdef").toString("base64")}`,
    ...Object.entries(headers).map(([name, value]) => `${name}: ${value}`),
    "",
    "",
  ];
  socket.write(lines.join("\r\n"));
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

describe("ws transport connection failures", () => {
  it("rejects a handshake timeout without leaking an unhandled WebSocket error", async () => {
    // Accept the TCP connection but never complete the HTTP upgrade, then
    // terminate the client from the timeout path. The terminating socket emits
    // an asynchronous error that used to crash the worker during reconnect.
    const sockets = new Set<import("node:net").Socket>();
    const server = createNetServer((socket) => {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    const address = server.address();
    const port = typeof address === "object" && address !== null ? address.port : 0;
    try {
      const transport = createWsTransport({ connectTimeoutMs: 30, heartbeat: false });
      await expect(
        transport.connect({ url: `ws://127.0.0.1:${port}` }, BEARER),
      ).rejects.toMatchObject({ code: "timeout" });
      // Give the terminated socket's error event time to fire. A missing
      // listener would fail this suite as an unhandled error.
      await delay(50);
    } finally {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});

describe("ws transport sessions", () => {
  it("resolves a hostname override while keeping the URL hostname", async () => {
    const harness = await startServer();
    const session = await makeTransport().connect(
      {
        url: `ws://gethen.internal:${harness.port}/gethen`,
        resolve: { "gethen.internal": "127.0.0.1" },
      },
      BEARER,
    );
    liveSessions.push(session);
    await waitForState(session, "open");
    expect(harness.sessions).toHaveLength(1);
  });

  it("connects, echoes text messages and preserves order", async () => {
    const received: string[] = [];
    const harness = await startServer({
      handleSession: (session) => {
        session.onMessage((message) => {
          if (typeof message.payload !== "string") return;
          received.push(message.payload);
          void session.send({ ...message, id: `echo-${message.id}` });
        });
      },
    });
    const session = await connectSession(harness);
    expect(session.state).toBe("open");

    const echoed: string[] = [];
    session.onMessage((message) => {
      if (typeof message.payload === "string") echoed.push(message.payload);
    });
    const messages = Array.from({ length: 20 }, (_, index) => textMessage(`message-${index}`));
    for (const message of messages) await session.send(message);

    await waitFor(() => echoed.length === messages.length);
    expect(echoed).toEqual(messages.map((message) => message.payload));
    expect(received).toEqual(messages.map((message) => message.payload));
  });

  it("round-trips binary payloads and their content type", async () => {
    const serverContentTypes: string[] = [];
    const harness = await startServer({
      handleSession: (session) => {
        session.onMessage((message) => {
          serverContentTypes.push(message.contentType);
          if (message.payload instanceof Uint8Array) {
            void session.send({ ...message, id: `echo-${message.id}` });
          }
        });
      },
    });
    const session = await connectSession(harness);
    const received = nextMessage(session);

    await session.send(binaryMessage(new Uint8Array([0, 1, 2, 253, 254, 255])));

    const message = await received;
    expect(message.payload).toBeInstanceOf(Uint8Array);
    expect(Array.from(message.payload as Uint8Array)).toEqual([0, 1, 2, 253, 254, 255]);
    expect(message.contentType).toBe("application/octet-stream");
    await waitFor(() => serverContentTypes.length === 1);
    expect(serverContentTypes).toEqual(["application/octet-stream"]);
  });

  it("rejects an invalid or missing bearer token with a raw 401", async () => {
    const harness = await startServer();

    expect(await rawUpgradeStatus(harness.port, { authorization: "Bearer wrong" })).toBe(
      "HTTP/1.1 401 Unauthorized",
    );
    expect(await rawUpgradeStatus(harness.port, {})).toBe("HTTP/1.1 401 Unauthorized");

    await expect(
      makeTransport().connect({ url: harness.url }, { kind: "bearer", token: "wrong" }),
    ).rejects.toMatchObject({ code: "auth_failed" });
    await expect(
      makeTransport().connect({ url: harness.url }, { kind: "none" }),
    ).rejects.toMatchObject({ code: "auth_failed" });
    expect(harness.sessions).toHaveLength(0);
  });

  it("hands verifyAuth's identity to onSession", async () => {
    const harness = await startServer();
    const session = await connectSession(harness);

    await waitFor(() => harness.sessions.length === 1);
    const context = harness.contexts[0];
    expect(context?.identity).toEqual({ workerId: "worker-1" });
    expect(context?.auth).toEqual(BEARER);
    expect(context?.remoteAddress).toBe("127.0.0.1");
    expect(harness.transportServer.sessions()).toHaveLength(1);
    expect(session.id).toBe("ws-1");
    expect(serverSession(harness).id).toBe("ws-server-1");
  });

  it("pings the peer in both directions", async () => {
    const harness = await startServer();
    const session = await connectSession(harness);
    await waitFor(() => harness.sessions.length === 1);

    await expect(session.ping()).resolves.toBeUndefined();
    await expect(serverSession(harness).ping()).resolves.toBeUndefined();
  });

  it("closes a server session when the peer misses the pong deadline", async () => {
    const harness = await startServer({ heartbeatMs: 20, heartbeatTimeoutMs: 40 });
    const client = new WebSocket(harness.url, {
      headers: { authorization: `Bearer ${TOKEN}` },
      autoPong: false,
    });
    client.on("error", () => {});
    await once(client, "open");
    await waitFor(() => harness.sessions.length === 1);

    const session = serverSession(harness);
    await waitForState(session, "closed", 3_000);
    expect(session.closeReason).toBe("heartbeat_timeout");
    expect(harness.transportServer.sessions()).toHaveLength(0);
    client.terminate();
  });

  it("closes a client session when the peer misses pings", async () => {
    const raw = await startRawServer({ autoPong: false });
    const session = await makeTransport({
      heartbeat: { intervalMs: 20, timeoutMs: 40 },
    }).connect({ url: raw.url }, { kind: "none" });
    liveSessions.push(session);

    await waitForState(session, "closed", 3_000);
    expect(session.closeReason).toBe("heartbeat_timeout");
  });

  it("tracks state transitions and close reasons on both ends", async () => {
    const harness = await startServer();
    const session = await connectSession(harness);
    await waitFor(() => harness.sessions.length === 1);
    const remote = serverSession(harness);
    const clientStates: TransportSessionState[] = [];
    const serverStates: TransportSessionState[] = [];
    session.onState((state) => clientStates.push(state));
    remote.onState((state) => serverStates.push(state));

    await session.close("done");

    expect(session.state).toBe("closed");
    expect(session.closeReason).toBe("done");
    expect(clientStates).toEqual(["closed"]);
    await waitForState(remote, "closed");
    await waitFor(() => serverStates.length === 1);
    expect(serverStates).toEqual(["closed"]);
    expect(remote.closeReason).toBe("done");
    await expect(session.close()).resolves.toBeUndefined();
  });

  it("rejects sends and pings after close", async () => {
    const harness = await startServer();
    const session = await connectSession(harness);
    await session.close("bye");

    await expect(session.send(textMessage("late"))).rejects.toMatchObject({
      code: "session_closed",
    });
    await expect(session.ping()).rejects.toMatchObject({ code: "session_closed" });
  });

  it("unsubscribes message and state listeners", async () => {
    const harness = await startServer({
      handleSession: (session) => {
        session.onMessage((message) => {
          void session.send({ ...message, id: `echo-${message.id}` });
        });
      },
    });
    const session = await connectSession(harness);
    await waitFor(() => harness.sessions.length === 1);
    const server = serverSession(harness);
    const seen: TransportMessage[] = [];
    const states: TransportSessionState[] = [];
    const offMessage = session.onMessage((message) => seen.push(message));
    const offState = session.onState((state) => states.push(state));
    offMessage();
    offState();

    await session.send(textMessage("ignored"));
    await waitFor(() => server.state === "open");
    await delay(50);
    await session.close();

    expect(seen).toHaveLength(0);
    expect(states).toHaveLength(0);
  });

  it("closes every session and releases the server", async () => {
    const harness = await startServer();
    const first = await connectSession(harness);
    const second = await connectSession(harness);
    await waitFor(() => harness.sessions.length === 2);
    const firstClosed = waitForState(first, "closed");
    const secondClosed = waitForState(second, "closed");

    await harness.transportServer.close();

    await Promise.all([firstClosed, secondClosed]);
    expect(first.state).toBe("closed");
    expect(second.state).toBe("closed");
    expect(first.closeReason).toBe("server_close");
    expect(harness.transportServer.sessions()).toHaveLength(0);
    await expect(harness.transportServer.close()).resolves.toBeUndefined();
    await expect(makeTransport().connect({ url: harness.url }, BEARER)).rejects.toMatchObject({
      code: "connect_failed",
    });
  });

  it("applies buffered-amount backpressure when the peer stops reading", async () => {
    const raw = await startRawServer({ onConnection: (socket) => socket.pause() });
    const session = await makeTransport({
      maxBufferedBytes: 1_024,
      sendTimeoutMs: 100,
      maxPayloadBytes: 32 * 1_048_576,
    }).connect({ url: raw.url }, { kind: "none" });
    liveSessions.push(session);

    const pending = session
      .send(binaryMessage(new Uint8Array(8 * 1_048_576)))
      .catch(() => undefined);
    await waitFor(() => session.bufferedAmount > 1_024, 5_000);

    await expect(session.send(textMessage("small"))).rejects.toMatchObject({
      code: "backpressure",
    });

    for (const client of raw.server.clients) client.terminate();
    await pending;
    await waitForState(session, "closed");
  });

  it("removes a session from the server registry when the peer disconnects", async () => {
    const harness = await startServer();
    const session = await connectSession(harness);
    await waitFor(() => harness.sessions.length === 1);

    await session.close();

    await waitFor(() => harness.transportServer.sessions().length === 0);
    expect(serverSession(harness).state).toBe("closed");
    expect(TRANSPORT_CLOSE_CODES.normal).toBe(1000);
  });
});

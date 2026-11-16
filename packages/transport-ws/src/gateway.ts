/**
 * Master-side WebSocket gateway for the `gethen.worker/v1` control plane.
 *
 * The gateway owns the transport concerns only: bearer-token authentication
 * before the upgrade completes, the `hello` handshake, connection registry
 * (one connection per `workerId`; a reconnect replaces the old one), ping/pong
 * liveness and frame validation. Every accepted upstream frame goes to
 * `onFrame`; dispatch/event/escalation/report semantics stay in the Master.
 *
 * The gateway does not create an HTTP server: a host (a standalone
 * `node:http` process or the DSH `webServer.registerUpgrade` route) hands it the
 * upgrade triple via `handleUpgrade`, so the same connection layer works in
 * both shapes.
 */
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import {
  type DownFrame,
  type ErrorFrame,
  encodeFrame,
  type HelloFrame,
  type HelloOkFrame,
  MAX_FRAME_BYTES,
  PROTOCOL_VERSION,
  parseUpstreamFrame,
  type UpFrame,
} from "@gethen/worker-protocol";
import { type RawData, WebSocket, WebSocketServer } from "ws";
import type { WorkerTokenIdentity } from "./token";

/** Close codes the gateway uses on its own closes (application range). */
export const GATEWAY_CLOSE_CODES = {
  replaced: 4001,
  helloRequired: 4002,
  protocolError: 4003,
  workerMismatch: 4004,
  goingAway: 1001,
} as const;

export const DEFAULT_HEARTBEAT_MS = 30_000;
export const DEFAULT_HEARTBEAT_TIMEOUT_MS = 10_000;
export const DEFAULT_MAX_BUFFERED_BYTES = 1_048_576;
export const DEFAULT_SEND_TIMEOUT_MS = 10_000;

/** One live, authenticated worker connection. */
export interface WorkerConnection {
  /** Gateway-local connection id, stable for the socket's lifetime. */
  readonly id: string;
  /** Set once a valid `hello` binds this connection to a worker. */
  workerId?: string;
  readonly remoteAddress: string | undefined;
  readonly isOpen: boolean;
  /** Frames queued in the socket buffer (backpressure signal). */
  readonly bufferedAmount: number;
  /** Encode and send one downstream frame; rejects when the socket is gone. */
  send(frame: DownFrame): Promise<void>;
  /** Close the socket; `reason` defaults to `"client_close"` for `onClose`. */
  close(code?: number, reason?: string): void;
}

export interface WorkerGatewayOptions {
  /** Resolve a bearer token to its worker identity; `undefined` rejects 401. */
  verifyToken(token: string): Promise<WorkerTokenIdentity | undefined>;
  /** Called after a hello is accepted and the worker is registered. */
  onHello(conn: WorkerConnection, frame: HelloFrame): void;
  /** Called for every valid upstream frame received after the hello. */
  onFrame(conn: WorkerConnection, frame: UpFrame): void;
  /** Called exactly once per connection when it closes. */
  onClose(conn: WorkerConnection, reason: string): void;
  /** Transport/protocol problems; protocol errors are `GatewayProtocolError`. */
  onError?(error: Error): void;
  /** Ping interval in milliseconds (default {@link DEFAULT_HEARTBEAT_MS}). */
  heartbeatMs?: number;
  /** Pong deadline per ping in milliseconds (default {@link DEFAULT_HEARTBEAT_TIMEOUT_MS}). */
  heartbeatTimeoutMs?: number;
  /** Buffered bytes above which `send` waits for `drain` (default 1 MiB). */
  maxBufferedBytes?: number;
  /** How long `send` waits for `drain` before rejecting (default 10 s). */
  sendTimeoutMs?: number;
}

export interface WorkerGateway {
  /** Upgrade handler for a host HTTP server's `"upgrade"` event. */
  handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void;
  /** Every open, authenticated connection (pre-hello included). */
  connections(): WorkerConnection[];
  /** The connection registered to `workerId`, if any. */
  get(workerId: string): WorkerConnection | undefined;
  /** Send one frame to a worker; rejects if it is not connected. */
  dispatch(workerId: string, frame: DownFrame): Promise<void>;
  /** Best-effort send to every open connection. */
  broadcast(frame: DownFrame): void;
  /** Stop heartbeats, close every connection and the underlying server. */
  close(): Promise<void>;
  readonly wss: WebSocketServer;
}

/** A frame that failed validation or violated the handshake rules. */
export class GatewayProtocolError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(`${code}: ${message}`);
    this.name = "GatewayProtocolError";
    this.code = code;
  }
}

interface ConnectionHooks {
  onMessage(conn: WorkerConnectionImpl, data: RawData, isBinary: boolean): void;
  onClose(conn: WorkerConnectionImpl, reason: string): void;
  onError(error: Error): void;
  heartbeatTimeoutMs: number;
  maxBufferedBytes: number;
  sendTimeoutMs: number;
}

/** The gateway's connection implementation; also exposed as `WorkerConnection`. */
class WorkerConnectionImpl implements WorkerConnection {
  workerId?: string;
  readonly authenticatedWorkerId: string;
  readonly id: string;
  readonly remoteAddress: string | undefined;

  private readonly ws: WebSocket;
  private readonly hooks: ConnectionHooks;
  private closeReason: string | undefined;
  private closeReported = false;
  private awaitingPong = false;
  private pongTimer: NodeJS.Timeout | undefined;
  private readonly closeWaiters: (() => void)[] = [];

  constructor(
    ws: WebSocket,
    id: string,
    remoteAddress: string | undefined,
    authenticatedWorkerId: string,
    hooks: ConnectionHooks,
  ) {
    this.ws = ws;
    this.id = id;
    this.remoteAddress = remoteAddress;
    this.authenticatedWorkerId = authenticatedWorkerId;
    this.hooks = hooks;

    ws.on("message", (data, isBinary) => hooks.onMessage(this, data, isBinary));
    ws.on("pong", () => this.markAlive());
    ws.on("close", (_code, reason) => this.handleClose(reason));
    ws.on("error", (error) => hooks.onError(error));
  }

  get isOpen(): boolean {
    return this.ws.readyState === WebSocket.OPEN;
  }

  get bufferedAmount(): number {
    return this.ws.bufferedAmount;
  }

  get isClosed(): boolean {
    return this.closeReported;
  }

  async send(frame: DownFrame): Promise<void> {
    const payload = encodeFrame(frame);
    if (!this.isOpen) {
      throw new Error(`gateway: connection ${this.id} is not open`);
    }
    if (this.ws.bufferedAmount > this.hooks.maxBufferedBytes) {
      await this.waitForDrain();
    }
    if (!this.isOpen) {
      throw new Error(`gateway: connection ${this.id} closed while sending`);
    }
    await new Promise<void>((resolve, reject) => {
      this.ws.send(payload, (error) => {
        if (error !== undefined && error !== null) reject(error);
        else resolve();
      });
    });
  }

  close(code = 1000, reason = ""): void {
    this.closeReason ??= reason.length > 0 ? reason : "client_close";
    this.clearHeartbeat();
    this.closeQuietly(code, reason);
  }

  /** Gateway-owned close (replacement, heartbeat timeout, shutdown). */
  shutdown(reason: string, code: number): void {
    this.closeReason = reason;
    this.clearHeartbeat();
    this.closeQuietly(code, reason);
  }

  terminate(reason: string): void {
    this.closeReason = reason;
    this.ws.terminate();
  }

  ping(): void {
    if (!this.isOpen || this.awaitingPong) return;
    this.awaitingPong = true;
    try {
      this.ws.ping();
    } catch (error) {
      this.hooks.onError(error instanceof Error ? error : new Error(String(error)));
      this.terminate("heartbeat_timeout");
      return;
    }
    this.pongTimer = setTimeout(() => {
      this.pongTimer = undefined;
      if (this.awaitingPong) this.terminate("heartbeat_timeout");
    }, this.hooks.heartbeatTimeoutMs);
    this.pongTimer.unref();
  }

  /** Resolve when the close event has been reported to the gateway. */
  onceClosed(): Promise<void> {
    if (this.closeReported) return Promise.resolve();
    return new Promise((resolve) => {
      this.closeWaiters.push(resolve);
    });
  }

  private markAlive(): void {
    this.clearHeartbeat();
  }

  private clearHeartbeat(): void {
    this.awaitingPong = false;
    if (this.pongTimer !== undefined) {
      clearTimeout(this.pongTimer);
      this.pongTimer = undefined;
    }
  }

  private closeQuietly(code: number, reason: string): void {
    if (this.ws.readyState === WebSocket.OPEN) this.ws.close(code, reason);
    else if (this.ws.readyState === WebSocket.CONNECTING) this.ws.terminate();
  }

  private handleClose(reason: Buffer): void {
    if (this.closeReported) return;
    this.closeReported = true;
    if (this.pongTimer !== undefined) {
      clearTimeout(this.pongTimer);
      this.pongTimer = undefined;
    }
    const peerReason = reason.toString("utf8");
    const reported = this.closeReason ?? (peerReason.length > 0 ? peerReason : "client_close");
    for (const wake of this.closeWaiters.splice(0)) wake();
    this.hooks.onClose(this, reported);
  }

  private waitForDrain(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const onDrain = (): void => {
        clearTimeout(timer);
        this.ws.off("drain", onDrain);
        resolve();
      };
      const timer = setTimeout(() => {
        this.ws.off("drain", onDrain);
        reject(new Error(`gateway: connection ${this.id} send buffer did not drain`));
      }, this.hooks.sendTimeoutMs);
      timer.unref();
      this.ws.once("drain", onDrain);
    });
  }
}

export function createWorkerGateway(options: WorkerGatewayOptions): WorkerGateway {
  const heartbeatMs = positiveNumber(options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS, "heartbeatMs");
  const heartbeatTimeoutMs = positiveNumber(
    options.heartbeatTimeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS,
    "heartbeatTimeoutMs",
  );
  const maxBufferedBytes = positiveNumber(
    options.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES,
    "maxBufferedBytes",
  );
  const sendTimeoutMs = positiveNumber(
    options.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS,
    "sendTimeoutMs",
  );

  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: MAX_FRAME_BYTES,
    perMessageDeflate: false,
  });
  const all = new Set<WorkerConnectionImpl>();
  const byWorker = new Map<string, WorkerConnectionImpl>();
  const pendingSockets = new Set<Duplex>();
  let sequence = 0;
  let shuttingDown = false;
  let heartbeatTimer: NodeJS.Timeout | undefined;

  function reportError(error: unknown): void {
    if (options.onError === undefined) return;
    try {
      options.onError(toError(error));
    } catch {
      // A throwing error hook must not break the transport loop.
    }
  }

  function invoke(callback: () => void): void {
    try {
      callback();
    } catch (error) {
      reportError(error);
    }
  }

  function sendFrame(conn: WorkerConnectionImpl, frame: DownFrame): void {
    void conn.send(frame).catch(reportError);
  }

  function sendErrorFrame(conn: WorkerConnectionImpl, code: string, message: string): void {
    const frame: ErrorFrame = { v: PROTOCOL_VERSION, type: "error", code, message };
    void conn.send(frame).catch(reportError);
  }

  function rejectConnection(conn: WorkerConnectionImpl, code: string, message: string): void {
    reportError(new GatewayProtocolError(code, message));
    sendErrorFrame(conn, code, message);
    conn.shutdown(code, GATEWAY_CLOSE_CODES.protocolError);
  }

  function removeConnection(conn: WorkerConnectionImpl, reason: string): void {
    all.delete(conn);
    if (conn.workerId !== undefined && byWorker.get(conn.workerId) === conn) {
      byWorker.delete(conn.workerId);
    }
    invoke(() => options.onClose(conn, reason));
  }

  function acceptHello(conn: WorkerConnectionImpl, frame: HelloFrame): void {
    if (frame.workerId !== conn.authenticatedWorkerId) {
      rejectConnection(
        conn,
        "worker_mismatch",
        `hello workerId "${frame.workerId}" does not match the token identity "${conn.authenticatedWorkerId}"`,
      );
      return;
    }
    const previous = byWorker.get(frame.workerId);
    if (previous !== undefined && previous !== conn) {
      // Last connection wins: a reconnecting Worker replaces the stale socket.
      previous.shutdown("replaced", GATEWAY_CLOSE_CODES.replaced);
    }
    conn.workerId = frame.workerId;
    byWorker.set(frame.workerId, conn);
    const helloOk: HelloOkFrame = {
      v: PROTOCOL_VERSION,
      type: "hello_ok",
      workerId: frame.workerId,
      serverTime: new Date().toISOString(),
      heartbeatMs,
    };
    sendFrame(conn, helloOk);
    invoke(() => options.onHello(conn, frame));
  }

  function handleMessage(conn: WorkerConnectionImpl, data: RawData, isBinary: boolean): void {
    if (isBinary) {
      if (conn.workerId === undefined) {
        rejectConnection(conn, "invalid_frame", "binary frames are not supported");
      } else {
        reportError(new GatewayProtocolError("invalid_frame", "binary frames are not supported"));
        sendErrorFrame(conn, "invalid_frame", "binary frames are not supported");
      }
      return;
    }

    const parsed = parseUpstreamFrame(rawDataToString(data));
    if (!parsed.ok) {
      reportError(new GatewayProtocolError(parsed.code, parsed.message));
      sendErrorFrame(conn, parsed.code, parsed.message);
      if (conn.workerId === undefined) {
        conn.shutdown(parsed.code, GATEWAY_CLOSE_CODES.protocolError);
      }
      return;
    }

    const frame = parsed.value;
    if (conn.workerId === undefined) {
      if (frame.type !== "hello") {
        rejectConnection(conn, "hello_required", 'the first frame must be "hello"');
        return;
      }
      acceptHello(conn, frame);
      return;
    }
    invoke(() => options.onFrame(conn, frame));
  }

  function onConnection(ws: WebSocket, request: IncomingMessage, workerId: string): void {
    const conn = new WorkerConnectionImpl(
      ws,
      `conn-${++sequence}`,
      request.socket.remoteAddress,
      workerId,
      {
        onMessage: handleMessage,
        onClose: removeConnection,
        onError: reportError,
        heartbeatTimeoutMs,
        maxBufferedBytes,
        sendTimeoutMs,
      },
    );
    all.add(conn);
  }

  function handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void {
    if (shuttingDown) {
      rejectSocket(socket, "503 Service Unavailable");
      return;
    }
    // The client may disconnect while the async token lookup is in flight.
    socket.on("error", () => {});
    pendingSockets.add(socket);
    socket.once("close", () => pendingSockets.delete(socket));

    void (async () => {
      try {
        const token = bearerToken(request);
        if (token === undefined) {
          rejectSocket(socket, "401 Unauthorized");
          return;
        }
        let identity: { workerId: string } | undefined;
        try {
          identity = await options.verifyToken(token);
        } catch (error) {
          reportError(error);
          rejectSocket(socket, "401 Unauthorized");
          return;
        }
        if (identity === undefined) {
          rejectSocket(socket, "401 Unauthorized");
          return;
        }
        if (socket.destroyed) return;
        wss.handleUpgrade(request, socket, head, (ws) =>
          onConnection(ws, request, identity.workerId),
        );
      } catch (error) {
        reportError(error);
        socket.destroy();
      } finally {
        pendingSockets.delete(socket);
      }
    })();
  }

  function dispatch(workerId: string, frame: DownFrame): Promise<void> {
    const conn = byWorker.get(workerId);
    if (conn === undefined) {
      return Promise.reject(new Error(`gateway: worker "${workerId}" is not connected`));
    }
    return conn.send(frame);
  }

  function broadcast(frame: DownFrame): void {
    for (const conn of all) {
      if (conn.isOpen) sendFrame(conn, frame);
    }
  }

  async function close(): Promise<void> {
    if (shuttingDown) return;
    shuttingDown = true;
    if (heartbeatTimer !== undefined) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = undefined;
    }
    for (const socket of pendingSockets) socket.destroy();
    pendingSockets.clear();

    const conns = [...all];
    const closed = conns.map((conn) => conn.onceClosed());
    for (const conn of conns) conn.shutdown("gateway_close", GATEWAY_CLOSE_CODES.goingAway);
    await Promise.all(closed);
    await new Promise<void>((resolve) => {
      wss.close(() => resolve());
    });
  }

  heartbeatTimer = setInterval(() => {
    for (const conn of all) conn.ping();
  }, heartbeatMs);
  heartbeatTimer.unref();

  wss.on("error", reportError);
  wss.on("wsClientError", (error, socket) => {
    reportError(error);
    socket.destroy();
  });

  return {
    handleUpgrade,
    connections: () => [...all],
    get: (workerId: string) => byWorker.get(workerId),
    dispatch,
    broadcast,
    close,
    wss,
  };
}

function bearerToken(request: IncomingMessage): string | undefined {
  const header = request.headers.authorization;
  if (typeof header !== "string") return undefined;
  const match = /^Bearer\s+(.+)$/i.exec(header.trim());
  const token = match?.[1]?.trim();
  return token !== undefined && token.length > 0 ? token : undefined;
}

function rejectSocket(socket: Duplex, statusLine: string): void {
  if (!socket.destroyed && socket.writable) {
    socket.write(`HTTP/1.1 ${statusLine}\r\n\r\n`);
  }
  socket.destroy();
}

function rawDataToString(data: RawData): string {
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return Buffer.from(data).toString("utf8");
}

function positiveNumber(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`gateway: ${name} must be a positive finite number`);
  }
  return value;
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

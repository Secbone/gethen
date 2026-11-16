/**
 * WebSocket server adapter for the L1 transport.
 *
 * A host (`node:http` or the DSH web server) forwards its `upgrade` event to
 * {@link WsTransportServer.handleUpgrade}. The adapter authenticates the peer
 * before upgrading (raw 401 + destroy on failure), then delivers a plain
 * {@link TransportSession} to `onSession`. It never interprets payloads: no
 * hello, no dispatch, no business frames.
 */
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { type WebSocket, WebSocketServer } from "ws";
import type { TransportAuth, TransportSession } from "../../session";
import { DEFAULT_MAX_PAYLOAD_BYTES, heartbeatSettings, WsTransportSession } from "./session";

/** What `onSession` learns about the handshake. */
export interface WsTransportSessionContext<TIdentity = unknown> {
  request: IncomingMessage;
  auth: TransportAuth;
  identity: TIdentity;
  remoteAddress: string | undefined;
}

export interface WsTransportServerOptions<TIdentity = unknown> {
  /**
   * Authenticate the upgrade. Return the peer identity to accept, or
   * `undefined`/`false` to reject with 401. L1 authenticates; it never decides
   * what the identity may do.
   */
  verifyAuth(auth: TransportAuth, request: IncomingMessage): Promise<TIdentity | undefined | false>;
  /** Called once per accepted connection, after the upgrade completes. */
  onSession(session: TransportSession, context: WsTransportSessionContext<TIdentity>): void;
  /** Server ping interval; defaults to 30s. */
  heartbeatMs?: number;
  /** Pong deadline; defaults to 10s. */
  heartbeatTimeoutMs?: number;
  /** Frames larger than this are rejected by the ws layer. */
  maxPayloadBytes?: number;
  /** Buffered bytes above which a session `send` waits for `drain`. */
  maxBufferedBytes?: number;
  /** Maximum queued messages per session before `send` rejects. */
  maxSendQueue?: number;
  /** How long `send` waits for `drain` before rejecting with `backpressure`. */
  sendTimeoutMs?: number;
  /** How long sessions get to finish the closing handshake. */
  closeTimeoutMs?: number;
  /** Transport-level problems (bad handshake, rejected verifyAuth, hook throws). */
  onError?(error: Error): void;
}

export interface WsTransportServer {
  /** Upgrade handler for a host HTTP server's `"upgrade"` event. */
  handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void;
  /** Every open session, in accept order. */
  sessions(): TransportSession[];
  /** Close all sessions (going-away) and the underlying ws server. */
  close(): Promise<void>;
  readonly wss: WebSocketServer;
}

export function createWsTransportServer<TIdentity = unknown>(
  options: WsTransportServerOptions<TIdentity>,
): WsTransportServer {
  const heartbeat = heartbeatSettings(options.heartbeatMs, options.heartbeatTimeoutMs);
  const sessionOptions = {
    heartbeat,
    maxBufferedBytes: options.maxBufferedBytes,
    maxSendQueue: options.maxSendQueue,
    sendTimeoutMs: options.sendTimeoutMs,
    closeTimeoutMs: options.closeTimeoutMs,
  };
  const wss = new WebSocketServer({
    noServer: true,
    perMessageDeflate: false,
    maxPayload: options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES,
  });
  const live = new Set<WsTransportSession>();
  const pendingSockets = new Set<Duplex>();
  let sequence = 0;
  let shuttingDown = false;

  function reportError(error: unknown): void {
    if (options.onError === undefined) return;
    try {
      options.onError(error instanceof Error ? error : new Error(String(error)));
    } catch {
      // A throwing hook must not break the accept loop.
    }
  }

  function rejectSocket(socket: Duplex, statusLine: string): void {
    if (!socket.destroyed && socket.writable) {
      socket.write(`HTTP/1.1 ${statusLine}\r\n\r\n`);
    }
    socket.destroy();
  }

  function accept(
    ws: WebSocket,
    request: IncomingMessage,
    context: {
      auth: TransportAuth;
      identity: TIdentity;
    },
  ): void {
    let session: WsTransportSession;
    try {
      session = new WsTransportSession(ws, { id: `ws-server-${++sequence}`, ...sessionOptions });
    } catch (error) {
      reportError(error);
      ws.terminate();
      return;
    }
    live.add(session);
    session.onState((state) => {
      if (state === "closed") live.delete(session);
    });
    try {
      options.onSession(session, {
        request,
        auth: context.auth,
        identity: context.identity,
        remoteAddress: request.socket.remoteAddress,
      });
    } catch (error) {
      reportError(error);
    }
  }

  function handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): void {
    if (shuttingDown) {
      rejectSocket(socket, "503 Service Unavailable");
      return;
    }
    // The peer may disconnect while the async auth lookup is in flight.
    socket.on("error", () => {});
    pendingSockets.add(socket);
    socket.once("close", () => pendingSockets.delete(socket));

    void (async () => {
      try {
        const auth = requestAuth(request);
        let identity: TIdentity | undefined | false;
        try {
          identity = await options.verifyAuth(auth, request);
        } catch (error) {
          reportError(error);
          rejectSocket(socket, "401 Unauthorized");
          return;
        }
        if (identity === undefined || identity === false) {
          rejectSocket(socket, "401 Unauthorized");
          return;
        }
        if (socket.destroyed) return;
        wss.handleUpgrade(request, socket, head, (ws) => {
          accept(ws, request, { auth, identity });
        });
      } catch (error) {
        reportError(error);
        socket.destroy();
      } finally {
        pendingSockets.delete(socket);
      }
    })();
  }

  async function close(): Promise<void> {
    if (shuttingDown) return;
    shuttingDown = true;
    for (const socket of pendingSockets) socket.destroy();
    pendingSockets.clear();
    const sessions = [...live];
    await Promise.all(
      sessions.map((session) =>
        session.close("server_close").catch((error: unknown) => {
          reportError(error);
        }),
      ),
    );
    await new Promise<void>((resolve) => {
      wss.close(() => resolve());
    });
  }

  wss.on("error", reportError);
  wss.on("wsClientError", (error, socket) => {
    reportError(error);
    socket.destroy();
  });

  return {
    handleUpgrade,
    sessions: () => [...live],
    close,
    wss,
  };
}

/** Extract the L1 auth material from the upgrade request headers. */
export function requestAuth(request: IncomingMessage): TransportAuth {
  const header = request.headers.authorization;
  if (typeof header === "string") {
    const match = /^Bearer\s+(.+)$/i.exec(header.trim());
    const token = match?.[1]?.trim();
    if (token !== undefined && token.length > 0) return { kind: "bearer", token };
  }
  return { kind: "none" };
}

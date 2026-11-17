/**
 * WebSocket client adapter for the L1 transport.
 *
 * `createWsTransport(options)` returns a {@link Transport}: `connect()` dials the
 * URL, applies the endpoint headers and the `TransportAuth` as an
 * `Authorization: Bearer` header, waits for the WebSocket handshake and then
 * hands the open socket to {@link WsTransportSession}.
 *
 * Wire mapping: text payloads are sent as WebSocket text frames and `Uint8Array`
 * payloads as binary frames. `TransportMessage` metadata (`id`, `headers`,
 * `replyTo`, `contentType`, `at`) is transport-local in this adapter; envelope
 * metadata that must cross the wire belongs inside `payload` (L2's job).
 */
import { lookup as dnsLookup } from "node:dns";
import { isIP } from "node:net";
import { WebSocket } from "ws";
import {
  type TransportAuth,
  type TransportEndpoint,
  TransportError,
  type TransportSession,
} from "../../session";
import type { Transport, TransportCapabilities } from "../../transport";
import {
  DEFAULT_MAX_PAYLOAD_BYTES,
  resolveHeartbeat,
  type WsHeartbeatOptions,
  type WsHeartbeatSettings,
  WsTransportSession,
} from "./session";

export const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;

export interface WsTransportOptions {
  /** Automatic ping/pong; defaults to true (30s interval, 10s pong deadline). */
  heartbeat?: boolean | WsHeartbeatOptions;
  /** WebSocket subprotocol(s) to offer. */
  protocols?: string | string[];
  /** Handshake timeout; defaults to {@link DEFAULT_CONNECT_TIMEOUT_MS}. */
  connectTimeoutMs?: number;
  /** Frames larger than this are rejected by the ws layer. */
  maxPayloadBytes?: number;
  /** Buffered bytes above which `send` waits for `drain`. */
  maxBufferedBytes?: number;
  /** Maximum queued messages before `send` rejects with `queue_full`. */
  maxSendQueue?: number;
  /** How long `send` waits for `drain` before rejecting with `backpressure`. */
  sendTimeoutMs?: number;
  /** How long `close()` waits for the peer before terminating the socket. */
  closeTimeoutMs?: number;
}

export function wsTransportCapabilities(options: WsTransportOptions = {}): TransportCapabilities {
  return {
    driver: "ws",
    framing: "message",
    binary: true,
    heartbeat: options.heartbeat !== false,
    backpressure: true,
    resume: false,
  };
}

/** Build the ws adapter directly (usually obtained through `createTransport`). */
export function createWsTransport(options: WsTransportOptions = {}): Transport {
  let sequence = 0;
  const heartbeat = resolveHeartbeat(options.heartbeat);
  return {
    capabilities: wsTransportCapabilities(options),
    connect(endpoint: TransportEndpoint, auth: TransportAuth): Promise<TransportSession> {
      return connectWs(endpoint, auth, options, heartbeat, `ws-${++sequence}`);
    },
  };
}

async function connectWs(
  endpoint: TransportEndpoint,
  auth: TransportAuth,
  options: WsTransportOptions,
  heartbeat: WsHeartbeatSettings | undefined,
  id: string,
): Promise<TransportSession> {
  if (auth.kind === "mtls") {
    throw new TransportError(
      "unsupported_auth",
      "the ws adapter cannot attach a client certificate; terminate TLS in front of the endpoint instead",
    );
  }
  if (auth.kind === "bearer" && auth.token.length === 0) {
    throw new TransportError("invalid_option", "bearer auth needs a non-empty token");
  }

  const headers: Record<string, string> = { ...endpoint.headers };
  if (auth.kind === "bearer") headers.Authorization = `Bearer ${auth.token}`;

  const resolve = endpoint.resolve;
  const clientOptions = {
    headers,
    perMessageDeflate: false,
    maxPayload: options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES,
    ...(resolve === undefined ? {} : { lookup: createResolveLookup(resolve) }),
  };
  const ws =
    options.protocols === undefined
      ? new WebSocket(endpoint.url, clientOptions)
      : new WebSocket(endpoint.url, options.protocols, clientOptions);

  try {
    await waitForOpen(ws, options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS);
  } catch (error) {
    ws.terminate();
    throw error;
  }
  return new WsTransportSession(ws, {
    id,
    heartbeat,
    maxBufferedBytes: options.maxBufferedBytes,
    maxSendQueue: options.maxSendQueue,
    sendTimeoutMs: options.sendTimeoutMs,
    closeTimeoutMs: options.closeTimeoutMs,
    pingTimeoutMs: heartbeat === undefined ? undefined : heartbeat.timeoutMs,
  });
}

/** A `net`-compatible lookup that prefers the configured hostname overrides. */
function createResolveLookup(
  resolve: Record<string, string>,
): (hostname: string, options: unknown, callback: (...args: unknown[]) => void) => void {
  const table = new Map(
    Object.entries(resolve).map(([host, address]) => [host.toLowerCase(), address]),
  );
  return (hostname, options, callback) => {
    const address = table.get(hostname.toLowerCase());
    if (address !== undefined) {
      const family = isIP(address);
      if (family === 0) {
        callback(new Error(`invalid address for ${hostname}: ${address}`));
        return;
      }
      const all = (options as { all?: boolean } | undefined)?.all === true;
      if (all) callback(null, [{ address, family }]);
      else callback(null, address, family);
      return;
    }
    dnsLookup(hostname, options as never, callback as never);
  };
}

function waitForOpen(ws: WebSocket, timeoutMs: number): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new TransportError("timeout", `websocket handshake did not finish in ${timeoutMs}ms`));
    }, timeoutMs);
    timer.unref();
    const cleanup = (): void => {
      clearTimeout(timer);
      ws.off("open", onOpen);
      ws.off("error", onError);
      ws.off("close", onClose);
    };
    const onOpen = (): void => {
      cleanup();
      resolve();
    };
    const onError = (error: Error): void => {
      cleanup();
      reject(mapConnectError(error));
    };
    const onClose = (_code: number, reason: Buffer): void => {
      cleanup();
      const text = reason.toString("utf8");
      reject(
        new TransportError(
          "connect_failed",
          text.length > 0
            ? `socket closed during handshake: ${text}`
            : "socket closed during handshake",
        ),
      );
    };
    ws.once("open", onOpen);
    ws.once("error", onError);
    ws.once("close", onClose);
  });
}

function mapConnectError(error: Error): TransportError {
  if (/\b401\b/.test(error.message)) {
    return new TransportError("auth_failed", error.message, { cause: error });
  }
  return new TransportError("connect_failed", error.message, { cause: error });
}

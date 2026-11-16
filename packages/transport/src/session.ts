/**
 * L1 transport contract: connection/session, auth, framing, heartbeat and
 * backpressure. This module deliberately knows nothing about agents, tasks,
 * reports, mailboxes or `gethen.worker/v1` — it only moves opaque payloads
 * between two peers and reports liveness.
 *
 * Adapters (`ws` is the first) implement {@link Transport}; the L2 mailbox or
 * the legacy `gethen.worker/v1` frames ride inside {@link TransportMessage.payload}.
 */

export type TransportSessionState = "connecting" | "open" | "closed";

/** An opaque L1 payload: text frames stay strings, binary frames stay bytes. */
export type TransportPayload = string | Uint8Array;

/**
 * One L1 message.
 *
 * The ws adapter carries `payload` only: `id`, `headers`, `replyTo` and `at` are
 * transport-local metadata. Application layers that need envelope metadata on
 * the wire (CloudEvents ids, correlation ids) must put it inside `payload`
 * themselves — that is exactly what the L2 mailbox envelope does.
 */
export interface TransportMessage {
  /** Transport-level message id (unique per session, never resent implicitly). */
  id: string;
  payload: TransportPayload;
  /** MIME-ish description of the payload, e.g. `application/json`. */
  contentType: string;
  headers?: Record<string, string>;
  /** Message id this one replies to, when the adapter can carry it. */
  replyTo?: string;
  /** RFC 3339 timestamp assigned by the sender/receiver. */
  at: string;
}

/** One live connection between two peers. */
export interface TransportSession {
  /** Transport-level session id, stable for the connection. */
  readonly id: string;
  readonly state: TransportSessionState;
  /** Bytes queued in the underlying socket buffer (backpressure signal). */
  readonly bufferedAmount: number;
  /** Why the session closed, once `state === "closed"`. */
  readonly closeReason: string | undefined;
  /** Send one message; resolves once it is handed to the transport. */
  send(message: TransportMessage): Promise<void>;
  /** Subscribe to inbound messages; the returned function unsubscribes. */
  onMessage(handler: (message: TransportMessage) => void): () => void;
  /** Subscribe to state changes; the returned function unsubscribes. */
  onState(handler: (state: TransportSessionState) => void): () => void;
  /** Liveness round-trip; rejects when closed or no pong arrives in time. */
  ping(): Promise<void>;
  /** Close the session; resolves once the peer/transport is closed. */
  close(reason?: string): Promise<void>;
}

/**
 * Authentication the transport performs. It is authentication only: L1 never
 * decides what the peer may do.
 */
export type TransportAuth = { kind: "bearer"; token: string } | { kind: "mtls" } | { kind: "none" };

/** Where to connect. Adapter-specific TLS/SSH options live in the URL/proxy. */
export interface TransportEndpoint {
  url: string;
  headers?: Record<string, string>;
}

/** Stable failure codes; callers branch on the code, not the message text. */
export type TransportErrorCode =
  | "auth_failed"
  | "connect_failed"
  | "session_closed"
  | "not_open"
  | "heartbeat_timeout"
  | "backpressure"
  | "queue_full"
  | "send_failed"
  | "timeout"
  | "unsupported_auth"
  | "unsupported_driver"
  | "protocol_error"
  | "invalid_option";

export interface TransportErrorOptions {
  cause?: unknown;
}

/** Every transport failure is a `TransportError` with a stable `code`. */
export class TransportError extends Error {
  readonly code: TransportErrorCode;

  constructor(code: TransportErrorCode, message: string, options: TransportErrorOptions = {}) {
    super(
      `${code}: ${message}`,
      options.cause === undefined ? undefined : { cause: options.cause },
    );
    this.name = "TransportError";
    this.code = code;
  }
}

/**
 * Close codes the transport uses. 1000-1011 mirror the WebSocket/IANA range;
 * 4000-4999 are adapter-private so a peer can tell a normal shutdown from an
 * auth/heartbeat/backpressure rejection.
 */
export const TRANSPORT_CLOSE_CODES = {
  normal: 1000,
  goingAway: 1001,
  protocolError: 1002,
  unsupportedData: 1003,
  noStatus: 1005,
  abnormal: 1006,
  policyViolation: 1008,
  messageTooBig: 1009,
  internalError: 1011,
  authFailed: 4001,
  heartbeatTimeout: 4002,
  backpressure: 4003,
} as const;

export type TransportCloseCode = (typeof TRANSPORT_CLOSE_CODES)[keyof typeof TRANSPORT_CLOSE_CODES];

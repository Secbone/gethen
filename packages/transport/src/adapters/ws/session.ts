import { type RawData, WebSocket } from "ws";
import {
  TRANSPORT_CLOSE_CODES,
  TransportError,
  type TransportMessage,
  type TransportSession,
  type TransportSessionState,
} from "../../session";

export const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;
export const DEFAULT_HEARTBEAT_TIMEOUT_MS = 10_000;
export const DEFAULT_MAX_BUFFERED_BYTES = 1_048_576;
export const DEFAULT_SEND_TIMEOUT_MS = 10_000;
export const DEFAULT_MAX_SEND_QUEUE = 1_024;
export const DEFAULT_CLOSE_TIMEOUT_MS = 5_000;
export const DEFAULT_MAX_PAYLOAD_BYTES = 16 * 1_048_576;

export interface WsHeartbeatOptions {
  /** Ping interval in milliseconds. */
  intervalMs?: number;
  /** How long to wait for a pong before declaring the connection dead. */
  timeoutMs?: number;
}

export interface WsHeartbeatSettings {
  intervalMs: number;
  timeoutMs: number;
}

export interface WsSessionOptions {
  id: string;
  /** Automatic ping/pong; `undefined` disables it (explicit `ping()` still works). */
  heartbeat?: WsHeartbeatSettings;
  maxBufferedBytes?: number;
  maxSendQueue?: number;
  sendTimeoutMs?: number;
  /** How long `close()` waits for the peer before terminating the socket. */
  closeTimeoutMs?: number;
  /** Pong deadline for explicit `ping()`. Defaults to the heartbeat timeout. */
  pingTimeoutMs?: number;
  now?: () => Date;
}

interface QueuedSend {
  message: TransportMessage;
  resolve: () => void;
  reject: (error: Error) => void;
}

interface PingWaiter {
  onPong: () => void;
  timer: NodeJS.Timeout;
  reject: (error: Error) => void;
}

/**
 * Shared ws session used by both the client and server adapters.
 *
 * It owns the transport-level state machine (`open` -> `closed`), framing of
 * text vs binary payloads, a bounded FIFO send queue with `bufferedAmount`
 * backpressure, ping/pong liveness and close semantics. Adapters only create
 * the socket and hand it over.
 */
export class WsTransportSession implements TransportSession {
  readonly id: string;

  private readonly ws: WebSocket;
  private readonly heartbeat: WsHeartbeatSettings | undefined;
  private readonly maxBufferedBytes: number;
  private readonly maxSendQueue: number;
  private readonly sendTimeoutMs: number;
  private readonly closeTimeoutMs: number;
  private readonly pingTimeoutMs: number;
  private readonly now: () => Date;

  private stateValue: TransportSessionState = "open";
  private closeReasonValue: string | undefined;
  private readonly messageListeners = new Set<(message: TransportMessage) => void>();
  private readonly stateListeners = new Set<(state: TransportSessionState) => void>();
  private readonly queue: QueuedSend[] = [];
  private readonly pingWaiters = new Set<PingWaiter>();
  private pumping = false;
  private inboundSeq = 0;
  private heartbeatTimer: NodeJS.Timeout | undefined;
  private pongTimer: NodeJS.Timeout | undefined;
  private awaitingPong = false;
  private readonly closeWaiters: Array<() => void> = [];

  constructor(ws: WebSocket, options: WsSessionOptions) {
    if (ws.readyState !== WebSocket.OPEN) {
      throw new TransportError("not_open", "the ws session needs an open socket");
    }
    this.ws = ws;
    this.id = options.id;
    this.heartbeat = options.heartbeat;
    this.maxBufferedBytes = positive(
      options.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES,
      "maxBufferedBytes",
    );
    this.maxSendQueue = integer(options.maxSendQueue ?? DEFAULT_MAX_SEND_QUEUE, "maxSendQueue");
    this.sendTimeoutMs = positive(
      options.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS,
      "sendTimeoutMs",
    );
    this.closeTimeoutMs = positive(
      options.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS,
      "closeTimeoutMs",
    );
    this.pingTimeoutMs = positive(
      options.pingTimeoutMs ?? this.heartbeat?.timeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS,
      "pingTimeoutMs",
    );
    this.now = options.now ?? (() => new Date());
    ws.on("message", (data, isBinary) => this.handleMessage(data, isBinary));
    ws.on("pong", () => this.markAlive());
    ws.on("close", (code, reason) => this.handleClose(code, reason));
    ws.on("error", (error) => {
      // A send/close failure surfaces through `send`/`close`; the socket always
      // emits `close` afterwards, which drives the state machine.
      void error;
    });

    this.startHeartbeat();
  }

  get state(): TransportSessionState {
    return this.stateValue;
  }

  get bufferedAmount(): number {
    return this.ws.bufferedAmount;
  }

  get closeReason(): string | undefined {
    return this.closeReasonValue;
  }

  private get isClosed(): boolean {
    return this.stateValue === "closed";
  }

  async send(message: TransportMessage): Promise<void> {
    this.ensureOpen();
    if (this.queue.length >= this.maxSendQueue) {
      throw new TransportError(
        "queue_full",
        `send queue is full (${this.maxSendQueue} pending messages)`,
      );
    }
    if (this.ws.bufferedAmount > this.maxBufferedBytes) {
      await this.waitForDrain();
      this.ensureOpen();
      if (this.ws.bufferedAmount > this.maxBufferedBytes) {
        throw new TransportError(
          "backpressure",
          `socket buffered amount stayed above ${this.maxBufferedBytes} bytes`,
        );
      }
    }
    return new Promise<void>((resolve, reject) => {
      this.queue.push({ message, resolve, reject });
      void this.pump();
    });
  }

  onMessage(handler: (message: TransportMessage) => void): () => void {
    this.messageListeners.add(handler);
    return () => {
      this.messageListeners.delete(handler);
    };
  }

  onState(handler: (state: TransportSessionState) => void): () => void {
    this.stateListeners.add(handler);
    return () => {
      this.stateListeners.delete(handler);
    };
  }

  async ping(): Promise<void> {
    this.ensureOpen();
    return new Promise<void>((resolve, reject) => {
      const waiter: PingWaiter = {
        onPong: () => {
          done();
          resolve();
        },
        timer: setTimeout(() => {
          done();
          reject(new TransportError("timeout", "no pong received within the ping timeout"));
        }, this.pingTimeoutMs),
        reject,
      };
      const done = (): void => {
        clearTimeout(waiter.timer);
        this.ws.off("pong", waiter.onPong);
        this.pingWaiters.delete(waiter);
      };
      this.pingWaiters.add(waiter);
      this.ws.once("pong", waiter.onPong);
      try {
        this.ws.ping();
      } catch (error) {
        done();
        reject(new TransportError("send_failed", messageOf(error), { cause: error }));
      }
    });
  }

  async close(reason?: string): Promise<void> {
    if (this.isClosed) return;
    this.closeReasonValue ??= reason ?? "local_close";
    this.stopHeartbeat();
    if (this.ws.readyState === WebSocket.OPEN) {
      this.ws.close(TRANSPORT_CLOSE_CODES.normal, reason);
      // Do not wait forever for a half-open peer: terminate after the grace
      // period so `close()` always settles.
      const timer = setTimeout(() => {
        if (this.stateValue !== "closed") this.ws.terminate();
      }, this.closeTimeoutMs);
      timer.unref();
    } else if (this.ws.readyState === WebSocket.CONNECTING) {
      this.ws.terminate();
    }
    if (this.isClosed) return;
    return new Promise<void>((resolve) => {
      this.closeWaiters.push(resolve);
    });
  }

  /** Adapter hook: terminate the socket after an unrecoverable transport fault. */
  terminate(reason: string): void {
    if (this.isClosed) return;
    this.closeReasonValue ??= reason;
    this.stopHeartbeat();
    this.ws.terminate();
  }

  private pump(): Promise<void> {
    if (this.pumping || this.stateValue !== "open") return Promise.resolve();
    this.pumping = true;
    return (async () => {
      try {
        while (this.queue.length > 0 && this.stateValue === "open") {
          const item = this.queue.shift();
          if (item === undefined) break;
          try {
            await this.write(item.message);
            item.resolve();
          } catch (error) {
            item.reject(toTransportError(error, "send_failed"));
          }
        }
      } finally {
        this.pumping = false;
      }
    })();
  }

  private write(message: TransportMessage): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const onSent = (error?: Error | null): void => {
        if (error !== undefined && error !== null) {
          reject(new TransportError("send_failed", error.message, { cause: error }));
        } else {
          resolve();
        }
      };
      try {
        if (typeof message.payload === "string") {
          this.ws.send(message.payload, onSent);
        } else {
          this.ws.send(Buffer.from(message.payload), onSent);
        }
      } catch (error) {
        reject(new TransportError("send_failed", messageOf(error), { cause: error }));
      }
    });
  }

  private waitForDrain(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const cleanup = (): void => {
        clearTimeout(timer);
        this.ws.off("drain", onDrain);
        this.ws.off("close", onClose);
      };
      const onDrain = (): void => {
        cleanup();
        resolve();
      };
      const onClose = (): void => {
        cleanup();
        reject(new TransportError("session_closed", "session closed while waiting for drain"));
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(
          new TransportError(
            "backpressure",
            `socket buffered amount did not drain within ${this.sendTimeoutMs}ms`,
          ),
        );
      }, this.sendTimeoutMs);
      timer.unref();
      this.ws.once("drain", onDrain);
      this.ws.once("close", onClose);
    });
  }

  private handleMessage(data: RawData, isBinary: boolean): void {
    if (this.stateValue !== "open") return;
    const message: TransportMessage = {
      id: `${this.id}-in-${++this.inboundSeq}`,
      payload: isBinary ? toUint8Array(data) : toText(data),
      contentType: isBinary ? "application/octet-stream" : "text/plain; charset=utf-8",
      headers: {},
      at: this.now().toISOString(),
    };
    for (const listener of [...this.messageListeners]) {
      try {
        listener(message);
      } catch {
        // A throwing application listener must not break the transport loop.
      }
    }
  }

  private startHeartbeat(): void {
    if (this.heartbeat === undefined) return;
    this.heartbeatTimer = setInterval(() => this.heartbeatTick(), this.heartbeat.intervalMs);
    this.heartbeatTimer.unref();
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
    this.markAlive();
  }

  private heartbeatTick(): void {
    if (this.stateValue !== "open" || this.awaitingPong) return;
    this.awaitingPong = true;
    try {
      this.ws.ping();
    } catch (error) {
      void error;
      this.terminate("heartbeat_timeout");
      return;
    }
    this.pongTimer = setTimeout(() => {
      this.pongTimer = undefined;
      if (this.awaitingPong) this.terminate("heartbeat_timeout");
    }, this.heartbeat?.timeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS);
    this.pongTimer.unref();
  }

  private markAlive(): void {
    this.awaitingPong = false;
    if (this.pongTimer !== undefined) {
      clearTimeout(this.pongTimer);
      this.pongTimer = undefined;
    }
  }

  private handleClose(code: number, reason: Buffer): void {
    if (this.stateValue === "closed") return;
    const peerReason = reason.toString("utf8");
    this.closeReasonValue ??= peerReason.length > 0 ? peerReason : closeCodeReason(code);
    this.stopHeartbeat();
    for (const waiter of [...this.pingWaiters]) {
      clearTimeout(waiter.timer);
      this.ws.off("pong", waiter.onPong);
      this.pingWaiters.delete(waiter);
      waiter.reject(new TransportError("session_closed", "session closed before pong"));
    }
    this.failQueue(
      new TransportError("session_closed", `session closed: ${this.closeReasonValue}`),
    );
    this.stateValue = "closed";
    for (const listener of [...this.stateListeners]) {
      try {
        listener("closed");
      } catch {
        // ignore listener failures
      }
    }
    for (const wake of this.closeWaiters.splice(0)) wake();
  }

  private failQueue(error: Error): void {
    for (const item of this.queue.splice(0)) item.reject(error);
  }

  private ensureOpen(): void {
    if (this.stateValue === "closed") {
      throw new TransportError(
        "session_closed",
        `session is closed${this.closeReasonValue === undefined ? "" : `: ${this.closeReasonValue}`}`,
      );
    }
    if (this.stateValue === "connecting") {
      throw new TransportError("not_open", "session has not finished connecting");
    }
  }
}

export function resolveHeartbeat(
  value: boolean | WsHeartbeatOptions | undefined,
  defaults: WsHeartbeatSettings = {
    intervalMs: DEFAULT_HEARTBEAT_INTERVAL_MS,
    timeoutMs: DEFAULT_HEARTBEAT_TIMEOUT_MS,
  },
): WsHeartbeatSettings | undefined {
  if (value === false) return undefined;
  const options = value === true || value === undefined ? {} : value;
  return {
    intervalMs: positive(options.intervalMs ?? defaults.intervalMs, "heartbeat.intervalMs"),
    timeoutMs: positive(options.timeoutMs ?? defaults.timeoutMs, "heartbeat.timeoutMs"),
  };
}

export function heartbeatSettings(
  intervalMs: number | undefined,
  timeoutMs: number | undefined,
): WsHeartbeatSettings {
  return {
    intervalMs: positive(intervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS, "heartbeatMs"),
    timeoutMs: positive(timeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS, "heartbeatTimeoutMs"),
  };
}

function closeCodeReason(code: number): string {
  switch (code) {
    case TRANSPORT_CLOSE_CODES.normal:
      return "normal";
    case TRANSPORT_CLOSE_CODES.goingAway:
      return "going_away";
    case TRANSPORT_CLOSE_CODES.abnormal:
      return "abnormal";
    default:
      return `close_${code}`;
  }
}

function toText(data: RawData): string {
  if (Buffer.isBuffer(data)) return data.toString("utf8");
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return Buffer.from(data).toString("utf8");
}

function toUint8Array(data: RawData): Uint8Array {
  if (Array.isArray(data)) return new Uint8Array(Buffer.concat(data));
  if (Buffer.isBuffer(data)) return new Uint8Array(data);
  return new Uint8Array(data);
}

function positive(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new TransportError("invalid_option", `${name} must be a positive finite number`);
  }
  return value;
}

function integer(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new TransportError("invalid_option", `${name} must be a positive integer`);
  }
  return value;
}

function toTransportError(error: unknown, code: "send_failed"): TransportError {
  return error instanceof TransportError
    ? error
    : new TransportError(code, messageOf(error), { cause: error });
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

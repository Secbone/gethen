/**
 * Client side of `@gethen/mailbox-link`: the worker's remote {@link MailboxClient}
 * over an L1 {@link TransportSession}.
 *
 * The worker only dials out, so this link turns the mailbox API into frames:
 * `subscribe` opens the delivery stream, `receive` pulls from the locally
 * buffered deliveries, and `ack` / `nack` / `renewLeases` refer to message ids
 * the Master-side store leased. `send` / `reply` route envelopes through the
 * master; `request` sends an envelope with a fresh `correlationId`/`replyTo`
 * and waits for the matching delivery (auto-acking it).
 *
 * Reconnect is the caller's job: create a new L1 session and call
 * `attach(session)`; the link re-sends `hello` + `subscribe` and any messages
 * that were leased but unacked are redelivered by the store after lease expiry.
 */
import { randomUUID } from "node:crypto";

import {
  type EnqueueResult,
  type Envelope,
  type ReceivedMessage,
  validateEnvelope,
} from "@gethen/mailbox";
import type { TransportMessage, TransportSession, TransportSessionState } from "@gethen/transport";
import type { WorkerManifest } from "@gethen/worker-bridge";
import {
  createMailboxFrame,
  decodeMailboxFrame,
  MailboxLinkError,
  type MailboxLinkErrorCode,
  type MailboxLinkFrame,
  type MailboxLinkOp,
  type ProtocolResult,
  parseCountResult,
  parseDeliverPayload,
  parseErrorPayload,
  parseHelloResult,
  parseRouteResult,
  parseSubscribeResult,
  toTransportMessage,
  type WireDeliveredMessage,
} from "./protocol";

export const DEFAULT_LINK_BATCH_SIZE = 10;
export const DEFAULT_LINK_VISIBILITY_TIMEOUT_MS = 30_000;
export const DEFAULT_LINK_REQUEST_TIMEOUT_MS = 30_000;
export const DEFAULT_LINK_RECEIVE_TIMEOUT_MS = 5_000;

export interface MailboxClientLinkOptions {
  mailbox: string;
  /** Worker registration reported in the mailbox `hello` (optional). */
  worker?: WorkerManifest;
  /** Durable subscription name forwarded to the master link. */
  name?: string;
  batchSize?: number;
  visibilityTimeoutMs?: number;
  requestTimeoutMs?: number;
  /** How long `receive()` waits for a delivery before returning an empty batch. */
  receiveTimeoutMs?: number;
  now?: () => Date;
  /** Protocol/link problems that are not tied to a pending request. */
  onError?: (error: MailboxLinkError) => void;
  /** Connection lifecycle for callers that render link state. */
  onState?: (state: "connected" | "disconnected" | "closed") => void;
}

export interface MailboxReceiveOptions {
  /** Maximum messages to return; defaults to the configured batch size. */
  limit?: number;
  /** Overrides `receiveTimeoutMs` for this call; 0 returns immediately. */
  timeoutMs?: number;
}

export interface MailboxNackOptions {
  action?: "requeue" | "dead";
  delayMs?: number;
}

export interface MailboxRequestOptions {
  timeoutMs?: number;
}

/** The remote subset of `MailboxClient` that makes sense over one outbound link. */
export interface MailboxClientLink {
  readonly mailbox: string;
  readonly connected: boolean;
  /** Attach (or re-attach after reconnect) to a transport session. */
  attach(session: TransportSession): Promise<void>;
  /** Refresh the Master's Worker registration heartbeat without resubscribing. */
  heartbeat(): Promise<void>;
  /**
   * Replace the Worker registration and, when connected, re-send `hello`.
   * This is how a Worker refreshes observation fields (e.g. session stats)
   * without opening a new transport session. A later `heartbeat()` reuses it.
   */
  refreshWorker(worker: WorkerManifest): Promise<void>;
  receive(options?: MailboxReceiveOptions): Promise<ReceivedMessage[]>;
  ack(ids: string[]): Promise<number>;
  nack(ids: string[], options?: MailboxNackOptions): Promise<number>;
  renewLeases(ids: string[], visibilityTimeoutMs?: number): Promise<number>;
  send(envelope: Envelope): Promise<EnqueueResult>;
  reply(request: Envelope, replyEnvelope: Envelope): Promise<EnqueueResult>;
  request(envelope: Envelope, options?: MailboxRequestOptions): Promise<Envelope>;
  /** Detach from the session and reject outstanding operations. */
  close(): void;
}

interface PendingRequest {
  resolve: (payload: unknown) => void;
  reject: (error: MailboxLinkError) => void;
  timer: NodeJS.Timeout;
}

interface ReceiveWaiter {
  limit: number;
  resolve: (messages: ReceivedMessage[]) => void;
  reject: (error: MailboxLinkError) => void;
  timer: NodeJS.Timeout;
}

interface ReplyWaiter {
  resolve: (envelope: Envelope) => void;
  reject: (error: MailboxLinkError) => void;
  timer: NodeJS.Timeout;
}

class MailboxClientLinkImpl implements MailboxClientLink {
  readonly mailbox: string;

  private readonly name: string | undefined;
  private worker: WorkerManifest | undefined;
  private readonly batchSize: number;
  private readonly visibilityTimeoutMs: number;
  private readonly requestTimeoutMs: number;
  private readonly receiveTimeoutMs: number;
  private readonly now: () => Date;

  private session: TransportSession | undefined;
  private readonly offs: Array<() => void> = [];
  private readonly pending = new Map<string, PendingRequest>();
  private readonly replyWaiters = new Map<string, ReplyWaiter>();
  private readonly receiveWaiters: ReceiveWaiter[] = [];
  private readonly deliveries: ReceivedMessage[] = [];
  private sequence = 0;
  private subscribed = false;
  private closed = false;

  constructor(options: MailboxClientLinkOptions) {
    if (options.mailbox.trim() === "") {
      throw new MailboxLinkError("invalid_payload", "mailbox must be a non-empty string");
    }
    this.mailbox = options.mailbox;
    this.name = options.name;
    this.worker = options.worker;
    this.batchSize = positiveInteger(options.batchSize ?? DEFAULT_LINK_BATCH_SIZE, "batchSize");
    this.visibilityTimeoutMs = positiveNumber(
      options.visibilityTimeoutMs ?? DEFAULT_LINK_VISIBILITY_TIMEOUT_MS,
      "visibilityTimeoutMs",
    );
    this.requestTimeoutMs = positiveNumber(
      options.requestTimeoutMs ?? DEFAULT_LINK_REQUEST_TIMEOUT_MS,
      "requestTimeoutMs",
    );
    this.receiveTimeoutMs = nonNegativeNumber(
      options.receiveTimeoutMs ?? DEFAULT_LINK_RECEIVE_TIMEOUT_MS,
      "receiveTimeoutMs",
    );
    this.now = options.now ?? (() => new Date());
    this.onError = options.onError;
    this.onState = options.onState;
  }

  private readonly onError: ((error: MailboxLinkError) => void) | undefined;
  private readonly onState: ((state: "connected" | "disconnected" | "closed") => void) | undefined;

  get connected(): boolean {
    return !this.closed && this.subscribed && this.session?.state === "open";
  }

  async attach(session: TransportSession): Promise<void> {
    this.assertOpen();
    this.detach("disconnected", "mailbox link re-attached");
    this.session = session;
    this.offs.push(session.onMessage((message) => this.handleMessage(message)));
    this.offs.push(session.onState((state) => this.handleSessionState(state)));
    try {
      const hello = parseOrThrow(
        await this.call(
          "hello",
          {
            protocol: 1,
            mailbox: this.mailbox,
            ...(this.worker === undefined ? {} : { worker: this.worker }),
          },
          this.requestTimeoutMs,
        ),
        parseHelloResult,
      );
      void hello;
      const subscription = parseOrThrow(
        await this.call(
          "subscribe",
          {
            mailbox: this.mailbox,
            ...(this.name === undefined ? {} : { name: this.name }),
            batchSize: this.batchSize,
            visibilityTimeoutMs: this.visibilityTimeoutMs,
          },
          this.requestTimeoutMs,
        ),
        parseSubscribeResult,
      );
      if (subscription.mailbox !== this.mailbox) {
        throw new MailboxLinkError(
          "invalid_payload",
          `master subscribed mailbox "${subscription.mailbox}" instead of "${this.mailbox}"`,
        );
      }
      this.subscribed = true;
      this.emitState("connected");
    } catch (error) {
      this.detach("disconnected", "mailbox link attach failed");
      throw toLinkError(error, "disconnected");
    }
  }

  async heartbeat(): Promise<void> {
    this.assertOpen();
    if (!this.connected) {
      throw new MailboxLinkError("disconnected", "mailbox link is not attached to an open session");
    }
    parseOrThrow(
      await this.call(
        "hello",
        {
          protocol: 1,
          mailbox: this.mailbox,
          ...(this.worker === undefined ? {} : { worker: this.worker }),
        },
        this.requestTimeoutMs,
      ),
      parseHelloResult,
    );
  }

  async refreshWorker(worker: WorkerManifest): Promise<void> {
    this.assertOpen();
    this.worker = worker;
    if (!this.connected) return;
    parseOrThrow(
      await this.call(
        "hello",
        { protocol: 1, mailbox: this.mailbox, worker },
        this.requestTimeoutMs,
      ),
      parseHelloResult,
    );
  }

  receive(options: MailboxReceiveOptions = {}): Promise<ReceivedMessage[]> {
    if (this.closed) {
      return Promise.reject(new MailboxLinkError("closed", "mailbox link is closed"));
    }
    const limit = positiveInteger(options.limit ?? this.batchSize, "limit");
    if (this.deliveries.length > 0) {
      return Promise.resolve(this.deliveries.splice(0, limit));
    }
    if (!this.connected) {
      return Promise.reject(
        new MailboxLinkError("disconnected", "mailbox link is not attached to an open session"),
      );
    }
    const timeoutMs = nonNegativeNumber(options.timeoutMs ?? this.receiveTimeoutMs, "timeoutMs");
    if (timeoutMs === 0) return Promise.resolve([]);
    return new Promise<ReceivedMessage[]>((resolve, reject) => {
      const timer = setTimeout(() => {
        const index = this.receiveWaiters.findIndex((candidate) => candidate.timer === timer);
        if (index >= 0) this.receiveWaiters.splice(index, 1);
        resolve([]);
      }, timeoutMs);
      timer.unref();
      this.receiveWaiters.push({ limit, resolve, reject, timer });
    });
  }

  async ack(ids: string[]): Promise<number> {
    const idList = requireIds(ids);
    const value = await this.call(
      "ack",
      { mailbox: this.mailbox, ids: idList },
      this.requestTimeoutMs,
    );
    return parseOrThrow(value, (payload) => parseCountResult(payload, "acked"));
  }

  async nack(ids: string[], options: MailboxNackOptions = {}): Promise<number> {
    const idList = requireIds(ids);
    const value = await this.call(
      "nack",
      {
        mailbox: this.mailbox,
        ids: idList,
        ...(options.action === undefined ? {} : { action: options.action }),
        ...(options.delayMs === undefined ? {} : { delayMs: options.delayMs }),
      },
      this.requestTimeoutMs,
    );
    return parseOrThrow(value, (payload) => parseCountResult(payload, "nacked"));
  }

  async renewLeases(ids: string[], visibilityTimeoutMs?: number): Promise<number> {
    const idList = requireIds(ids);
    const value = await this.call(
      "renew",
      {
        mailbox: this.mailbox,
        ids: idList,
        ...(visibilityTimeoutMs === undefined ? {} : { visibilityTimeoutMs }),
      },
      this.requestTimeoutMs,
    );
    return parseOrThrow(value, (payload) => parseCountResult(payload, "renewed"));
  }

  send(envelope: Envelope): Promise<EnqueueResult> {
    requireEnvelope(envelope, "send.envelope");
    return this.sendRoute(envelope);
  }

  async reply(request: Envelope, replyEnvelope: Envelope): Promise<EnqueueResult> {
    requireEnvelope(request, "reply.request");
    requireEnvelope(replyEnvelope, "reply.envelope");
    const value = await this.call(
      "reply",
      { request, envelope: replyEnvelope },
      this.requestTimeoutMs,
    );
    return parseOrThrow(value, parseRouteResult);
  }

  async request(envelope: Envelope, options: MailboxRequestOptions = {}): Promise<Envelope> {
    this.assertOpen();
    requireEnvelope(envelope, "request.envelope");
    const correlationId = envelope.correlationId ?? randomUUID();
    const replyTo = envelope.replyTo ?? this.mailbox;
    const routed: Envelope = { ...envelope, correlationId, replyTo };
    const timeoutMs = positiveNumber(options.timeoutMs ?? this.requestTimeoutMs, "timeoutMs");

    const response = new Promise<Envelope>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.replyWaiters.delete(correlationId);
        reject(
          new MailboxLinkError(
            "timeout",
            `request ${correlationId} timed out after ${timeoutMs}ms`,
          ),
        );
      }, timeoutMs);
      timer.unref();
      this.replyWaiters.set(correlationId, { resolve, reject, timer });
    });
    // The reply waiter can reject (disconnect/timeout) before the send call
    // resolves; keep that path handled until the promise is returned below.
    response.catch(() => {});

    try {
      await this.sendRoute(routed);
    } catch (error) {
      const waiter = this.replyWaiters.get(correlationId);
      if (waiter !== undefined) {
        clearTimeout(waiter.timer);
        this.replyWaiters.delete(correlationId);
      }
      throw error;
    }
    return response;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.detach("closed", "mailbox link closed");
    this.emitState("closed");
  }

  private async sendRoute(envelope: Envelope): Promise<EnqueueResult> {
    const value = await this.call("send", { envelope }, this.requestTimeoutMs);
    return parseOrThrow(value, parseRouteResult);
  }

  private call(op: MailboxLinkOp, payload: unknown, timeoutMs: number): Promise<unknown> {
    if (this.closed) {
      return Promise.reject(new MailboxLinkError("closed", "mailbox link is closed"));
    }
    const session = this.session;
    if (session === undefined || session.state !== "open") {
      return Promise.reject(
        new MailboxLinkError("disconnected", "mailbox link has no open transport session"),
      );
    }
    const id = `${this.mailbox}-${op}-${++this.sequence}`;
    const frame = createMailboxFrame(op, id, payload);
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new MailboxLinkError("timeout", `${op} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      timer.unref();
      this.pending.set(id, { resolve, reject, timer });
      void session
        .send(toTransportMessage(frame, this.now().toISOString()))
        .catch((error: unknown) => {
          const current = this.pending.get(id);
          if (current === undefined) return;
          clearTimeout(current.timer);
          this.pending.delete(id);
          reject(new MailboxLinkError("disconnected", messageOf(error), { cause: error }));
        });
    });
  }

  private handleMessage(message: TransportMessage): void {
    if (this.closed) return;
    const decoded = decodeMailboxFrame(message.payload);
    if (!decoded.ok) {
      this.reportError(new MailboxLinkError(decoded.code, decoded.message));
      return;
    }
    const frame = decoded.value;
    switch (frame.op) {
      case "deliver":
        this.handleDeliver(frame);
        return;
      case "payload":
        this.settle(frame.id, frame.payload);
        return;
      case "error": {
        const parsed = parseErrorPayload(frame.payload);
        const error = parsed.ok
          ? new MailboxLinkError(parsed.value.code, parsed.value.message)
          : new MailboxLinkError("invalid_payload", parsed.message);
        if (!this.settleError(frame.id, error)) this.reportError(error);
        return;
      }
      default:
        this.reportError(
          new MailboxLinkError("unknown_op", `unexpected server op "${frame.op}" on the link`),
        );
    }
  }

  private handleDeliver(frame: MailboxLinkFrame): void {
    const parsed = parseDeliverPayload(frame.payload);
    if (!parsed.ok) {
      this.reportError(new MailboxLinkError(parsed.code, parsed.message));
      return;
    }
    const { mailbox, messages } = parsed.value;
    for (const wire of messages) {
      const envelope = wire.envelope;
      if (envelope.correlationId !== undefined && this.replyWaiters.has(envelope.correlationId)) {
        const waiter = this.replyWaiters.get(envelope.correlationId);
        if (waiter !== undefined) {
          this.replyWaiters.delete(envelope.correlationId);
          clearTimeout(waiter.timer);
          void this.ack([envelope.id]).catch((error: unknown) =>
            this.reportError(toLinkError(error, "disconnected")),
          );
          waiter.resolve(envelope);
          continue;
        }
      }
      this.deliveries.push(toReceivedMessage(mailbox, wire));
    }
    this.flushReceivers();
  }

  private handleSessionState(state: TransportSessionState): void {
    if (state !== "closed") return;
    this.detach("disconnected", "transport session closed");
    this.emitState("disconnected");
  }

  private settle(id: string, payload: unknown): void {
    const pending = this.pending.get(id);
    if (pending === undefined) {
      this.reportError(
        new MailboxLinkError("invalid_payload", `no pending request for frame "${id}"`),
      );
      return;
    }
    clearTimeout(pending.timer);
    this.pending.delete(id);
    pending.resolve(payload);
  }

  private settleError(id: string, error: MailboxLinkError): boolean {
    const pending = this.pending.get(id);
    if (pending === undefined) return false;
    clearTimeout(pending.timer);
    this.pending.delete(id);
    pending.reject(error);
    return true;
  }

  private flushReceivers(): void {
    while (this.deliveries.length > 0 && this.receiveWaiters.length > 0) {
      const waiter = this.receiveWaiters.shift();
      if (waiter === undefined) break;
      clearTimeout(waiter.timer);
      waiter.resolve(this.deliveries.splice(0, waiter.limit));
    }
  }

  private detach(code: MailboxLinkErrorCode, message: string): void {
    for (const off of this.offs.splice(0)) off();
    this.session = undefined;
    this.subscribed = false;
    const error = new MailboxLinkError(code, message);
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
    for (const waiter of this.replyWaiters.values()) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    this.replyWaiters.clear();
    for (const waiter of this.receiveWaiters.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    this.deliveries.length = 0;
  }

  private assertOpen(): void {
    if (this.closed) throw new MailboxLinkError("closed", "mailbox link is closed");
  }

  private emitState(state: "connected" | "disconnected" | "closed"): void {
    try {
      this.onState?.(state);
    } catch (error) {
      this.reportError(toLinkError(error, "internal"));
    }
  }

  private reportError(error: MailboxLinkError): void {
    try {
      this.onError?.(error);
    } catch {
      // A throwing hook must not break the link.
    }
  }
}

/** Create and attach a remote mailbox client for one L1 session. */
export async function createMailboxClientLink(
  options: MailboxClientLinkOptions & { session: TransportSession },
): Promise<MailboxClientLink> {
  const link = new MailboxClientLinkImpl(options);
  await link.attach(options.session);
  return link;
}

/** Build a link without attaching yet; call `attach(session)` when connected. */
export function createDetachedMailboxClientLink(
  options: MailboxClientLinkOptions,
): MailboxClientLink {
  return new MailboxClientLinkImpl(options);
}

function parseOrThrow<T>(value: unknown, parser: (payload: unknown) => ProtocolResult<T>): T {
  const parsed = parser(value);
  if (!parsed.ok) throw new MailboxLinkError(parsed.code, parsed.message);
  return parsed.value;
}

function toReceivedMessage(mailbox: string, wire: WireDeliveredMessage): ReceivedMessage {
  return {
    mailbox,
    envelope: wire.envelope,
    attempts: wire.attempts,
    leaseUntil: wire.leaseUntil,
  };
}

function requireEnvelope(value: unknown, label: string): asserts value is Envelope {
  const validation = validateEnvelope(value);
  if (!validation.valid) {
    throw new MailboxLinkError("invalid_envelope", `${label}: ${validation.errors.join("; ")}`);
  }
}

function requireIds(ids: string[]): string[] {
  if (
    !Array.isArray(ids) ||
    ids.length === 0 ||
    ids.some((id) => typeof id !== "string" || id === "")
  ) {
    throw new MailboxLinkError(
      "invalid_payload",
      "ids must be a non-empty array of non-empty strings",
    );
  }
  return [...ids];
}

function positiveNumber(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new MailboxLinkError("invalid_payload", `${name} must be a positive finite number`);
  }
  return value;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new MailboxLinkError("invalid_payload", `${name} must be a positive integer`);
  }
  return value;
}

function nonNegativeNumber(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new MailboxLinkError("invalid_payload", `${name} must be a non-negative finite number`);
  }
  return value;
}

function toLinkError(error: unknown, fallback: MailboxLinkErrorCode): MailboxLinkError {
  if (error instanceof MailboxLinkError) return error;
  return new MailboxLinkError(fallback, messageOf(error), { cause: error });
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

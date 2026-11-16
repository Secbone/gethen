/**
 * Transport-agnostic L2 mailbox semantics.
 *
 * A `MailboxStore` owns durability and atomicity; a `MailboxClient` adds the
 * delivery policy (visibility timeouts, retry limits, dedupe window), envelope
 * routing, request/reply and topic subscriptions. Swapping the store (memory
 * vs sqlite) must not change observable semantics, which the contract tests
 * enforce by running the same suite against both implementations.
 */
import { randomUUID } from "node:crypto";

import { assertValidEnvelope, type Envelope, EnvelopeError } from "./envelope";

export const DEFAULT_VISIBILITY_TIMEOUT_MS = 30_000;
export const DEFAULT_MAX_ATTEMPTS = 5;
export const DEFAULT_DEDUPE_WINDOW_MS = 5 * 60_000;
export const DEFAULT_RETRY_DELAY_MS = 1_000;
export const DEFAULT_POLL_INTERVAL_MS = 50;
export const DEFAULT_RECEIVE_BATCH_SIZE = 10;
export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

export type MessageState = "ready" | "leased" | "done" | "dead";

/** A message handed to a consumer; `attempts` counts deliveries. */
export interface ReceivedMessage {
  mailbox: string;
  envelope: Envelope;
  attempts: number;
  leaseUntil: string;
}

export interface EnqueueResult {
  mailbox: string;
  /** True when an envelope with the same id is still tracked (dedupe). */
  duplicate: boolean;
}

/** Wall-clock override so tests and replay tooling stay deterministic. */
export interface TimeOptions {
  now?: string;
}

export interface ReceiveOptions extends TimeOptions {
  limit?: number;
  visibilityTimeoutMs?: number;
  /** Deliveries allowed before a message is dead-lettered. */
  maxAttempts?: number;
}

export interface AckOptions extends TimeOptions {
  /** Keep acked ids for duplicate suppression; 0 deletes them immediately. */
  dedupeWindowMs?: number;
}

export interface NackOptions extends TimeOptions {
  action: "requeue" | "dead";
  delayMs?: number;
  maxAttempts?: number;
}

export interface PurgeResult {
  purged: number;
}

export interface MailboxStats {
  mailbox: string;
  ready: number;
  leased: number;
  done: number;
  dead: number;
  /** ready + leased: work that has not been acked or dead-lettered. */
  pending: number;
  total: number;
  oldestReadyAt?: string;
  oldestReadyAgeMs?: number;
}

export interface DeadLetter {
  mailbox: string;
  envelope: Envelope;
  attempts: number;
  createdAt: string;
}

export interface DeadLetterOptions {
  limit?: number;
  offset?: number;
}

/** Durable checkpoint of one subscription; `cursor` is the last handled id. */
export interface SubscriptionRecord {
  topic: string;
  name: string;
  cursor?: string;
  createdAt: string;
}

/**
 * Persistence port for the mailbox layer. Implementations must be atomic per
 * call: `receive` leases the whole returned batch in one transaction.
 *
 * Dedupe uses the envelope id as a global key (the sqlite schema makes it the
 * primary key), so the same envelope cannot live in two mailboxes.
 */
export interface MailboxStore {
  /** Returns `duplicate: true` when `envelope.id` is still tracked (not expired). */
  enqueue(mailbox: string, envelope: Envelope, options?: TimeOptions): EnqueueResult;
  /** Leases ready messages: highest priority first, then FIFO. Expired leases redeliver. */
  receive(mailbox: string, options?: ReceiveOptions): ReceivedMessage[];
  /** Marks leased messages done (or deletes them when the dedupe window is 0). */
  ack(mailbox: string, ids: string[], options?: AckOptions): number;
  /** Requeues or dead-letters leased messages; requeue past `maxAttempts` becomes dead. */
  nack(mailbox: string, ids: string[], options: NackOptions): number;
  /** Extends the lease of still-leased messages; returns how many were renewed. */
  renewLeases(
    mailbox: string,
    ids: string[],
    visibilityTimeoutMs: number,
    options?: TimeOptions,
  ): number;
  /** Deletes expired rows (TTL and expired dedupe windows); dead letters are kept. */
  purge(options?: TimeOptions): PurgeResult;
  stats(mailbox: string, options?: TimeOptions): MailboxStats;
  deadLetters(mailbox: string, options?: DeadLetterOptions): DeadLetter[];
  getSubscription(topic: string, name: string): SubscriptionRecord | undefined;
  saveSubscription(record: SubscriptionRecord): void;
  listSubscriptions(topic?: string): SubscriptionRecord[];
}

export class MailboxTimeoutError extends Error {
  readonly correlationId: string;
  readonly timeoutMs: number;

  constructor(correlationId: string, timeoutMs: number) {
    super(`mailbox request ${correlationId} timed out after ${timeoutMs}ms`);
    this.name = "MailboxTimeoutError";
    this.correlationId = correlationId;
    this.timeoutMs = timeoutMs;
  }
}

export type EnvelopeHandler = (
  envelope: Envelope,
  message: ReceivedMessage,
) => Promise<void> | void;

export interface MailboxClientOptions {
  store: MailboxStore;
  now?: () => Date;
  /** Default lease duration for receive/subscribe/request. */
  visibilityTimeoutMs?: number;
  /** Deliveries allowed before dead-lettering. */
  maxAttempts?: number;
  /** How long acked ids suppress duplicates; 0 deletes on ack. */
  dedupeWindowMs?: number;
  /** Delay before a nacked (handler-failed) message is redelivered. */
  retryDelayMs?: number;
  pollIntervalMs?: number;
  receiveBatchSize?: number;
  requestTimeoutMs?: number;
}

export interface RequestOptions {
  timeoutMs?: number;
  pollIntervalMs?: number;
}

export interface SubscribeOptions {
  /** Durable subscription name; defaults to a random name. */
  name?: string;
  cursor?: string;
  /** Start the poll loop immediately (default true). */
  autoStart?: boolean;
  batchSize?: number;
  pollIntervalMs?: number;
  visibilityTimeoutMs?: number;
  onError?: (error: unknown, envelope: Envelope) => void;
}

export interface SubscriptionHandle {
  readonly topic: string;
  readonly name: string;
  readonly mailbox: string;
  /** Last handled envelope id, read from the durable subscription record. */
  cursor(): string | undefined;
  start(): void;
  stop(): void;
  /** Polls once; returns how many messages the handler accepted. */
  runOnce(): Promise<number>;
}

/**
 * High-level mailbox facade. All policy defaults live here; stores only
 * persist and enforce the mechanics they are given per call.
 */
export class MailboxClient {
  readonly store: MailboxStore;
  readonly now: () => Date;
  readonly visibilityTimeoutMs: number;
  readonly maxAttempts: number;
  readonly dedupeWindowMs: number;
  readonly retryDelayMs: number;
  readonly pollIntervalMs: number;
  readonly receiveBatchSize: number;
  readonly requestTimeoutMs: number;

  constructor(options: MailboxClientOptions) {
    this.store = options.store;
    this.now = options.now ?? (() => new Date());
    this.visibilityTimeoutMs = options.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    this.dedupeWindowMs = options.dedupeWindowMs ?? DEFAULT_DEDUPE_WINDOW_MS;
    this.retryDelayMs = options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.receiveBatchSize = options.receiveBatchSize ?? DEFAULT_RECEIVE_BATCH_SIZE;
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  }

  /** Routes by `envelope.to`; a missing destination is a caller bug. */
  send(envelope: Envelope): EnqueueResult {
    assertValidEnvelope(envelope);
    const mailbox = this.targetMailbox(envelope);
    return this.store.enqueue(mailbox, envelope, { now: this.nowIso() });
  }

  receive(mailbox: string, options: ReceiveOptions = {}): ReceivedMessage[] {
    return this.store.receive(mailbox, {
      limit: options.limit ?? this.receiveBatchSize,
      visibilityTimeoutMs: options.visibilityTimeoutMs ?? this.visibilityTimeoutMs,
      maxAttempts: options.maxAttempts ?? this.maxAttempts,
      now: options.now ?? this.nowIso(),
    });
  }

  ack(mailbox: string, ids: string[], options: AckOptions = {}): number {
    return this.store.ack(mailbox, ids, {
      dedupeWindowMs: options.dedupeWindowMs ?? this.dedupeWindowMs,
      now: options.now ?? this.nowIso(),
    });
  }

  nack(mailbox: string, ids: string[], options: NackOptions = { action: "requeue" }): number {
    return this.store.nack(mailbox, ids, {
      ...options,
      maxAttempts: options.maxAttempts ?? this.maxAttempts,
      now: options.now ?? this.nowIso(),
    });
  }

  renewLeases(
    mailbox: string,
    ids: string[],
    visibilityTimeoutMs: number = this.visibilityTimeoutMs,
  ): number {
    return this.store.renewLeases(mailbox, ids, visibilityTimeoutMs, { now: this.nowIso() });
  }

  purge(): PurgeResult {
    return this.store.purge({ now: this.nowIso() });
  }

  stats(mailbox: string): MailboxStats {
    return this.store.stats(mailbox, { now: this.nowIso() });
  }

  deadLetters(mailbox: string, options: DeadLetterOptions = {}): DeadLetter[] {
    return this.store.deadLetters(mailbox, options);
  }

  listSubscriptions(topic?: string): SubscriptionRecord[] {
    return this.store.listSubscriptions(topic);
  }

  /**
   * Sends a request and waits for the reply matched by `correlationId`.
   *
   * `replyTo` defaults to a private mailbox named after the correlation id.
   * Resolves with the reply envelope; rejects with `MailboxTimeoutError` when
   * neither `timeoutMs` nor `envelope.deadline` is reached in time.
   */
  async request(envelope: Envelope, options: RequestOptions = {}): Promise<Envelope> {
    assertValidEnvelope(envelope);
    const target = this.targetMailbox(envelope);
    const correlationId = envelope.correlationId ?? randomUUID();
    const replyTo = envelope.replyTo ?? `reply:${correlationId}`;
    const routed: Envelope = { ...envelope, correlationId, replyTo };
    this.store.enqueue(target, routed, { now: this.nowIso() });

    const timeoutMs = options.timeoutMs ?? this.requestTimeoutMs;
    const pollIntervalMs = options.pollIntervalMs ?? this.pollIntervalMs;
    const startedAt = Date.now();
    const deadlineAt =
      envelope.deadline === undefined ? Number.POSITIVE_INFINITY : Date.parse(envelope.deadline);
    const effectiveTimeoutMs = Math.max(
      0,
      Math.min(
        timeoutMs,
        Number.isFinite(deadlineAt) ? deadlineAt - startedAt : Number.POSITIVE_INFINITY,
      ),
    );

    for (;;) {
      const replies = this.receive(replyTo);
      for (const reply of replies) {
        if (reply.envelope.correlationId === correlationId) {
          this.ack(replyTo, [reply.envelope.id]);
          return reply.envelope;
        }
        // A shared reply mailbox may hold someone else's reply; leave its lease
        // alone and let it redeliver to the actual waiter after expiry.
      }
      const elapsed = Date.now() - startedAt;
      if (elapsed >= effectiveTimeoutMs)
        throw new MailboxTimeoutError(correlationId, effectiveTimeoutMs);
      await sleep(Math.min(pollIntervalMs, effectiveTimeoutMs - elapsed));
    }
  }

  /** Routes an answer back to `request.replyTo` and links the causation chain. */
  reply(request: Envelope, replyEnvelope: Envelope): EnqueueResult {
    assertValidEnvelope(replyEnvelope);
    if (request.replyTo === undefined || request.replyTo.trim() === "") {
      throw new EnvelopeError(["request has no replyTo"]);
    }
    const routed: Envelope = {
      ...replyEnvelope,
      to: request.replyTo,
      correlationId: request.correlationId ?? request.id,
      causationId: replyEnvelope.causationId ?? request.id,
    };
    if (replyEnvelope.thread === undefined && request.thread !== undefined) {
      routed.thread = request.thread;
    }
    return this.store.enqueue(request.replyTo, routed, { now: this.nowIso() });
  }

  /**
   * Durable topic subscription. The topic maps to the mailbox `topic:<topic>`;
   * the durable record stores the last handled envelope id under (topic, name),
   * so resubscribing with the same name resumes where the previous handle
   * stopped (acked messages are already gone from the mailbox).
   */
  subscribe(
    topic: string,
    handler: EnvelopeHandler,
    options: SubscribeOptions = {},
  ): SubscriptionHandle {
    const normalizedTopic = topic.startsWith("topic:") ? topic.slice("topic:".length) : topic;
    if (normalizedTopic.trim() === "") throw new Error("topic must be a non-empty string");
    const mailbox = `topic:${normalizedTopic}`;
    const name = options.name ?? `sub-${randomUUID()}`;
    const existing = this.store.getSubscription(normalizedTopic, name);
    const record: SubscriptionRecord = existing ?? {
      topic: normalizedTopic,
      name,
      createdAt: this.nowIso(),
    };
    if (existing === undefined) this.store.saveSubscription(record);
    if (options.cursor !== undefined && record.cursor === undefined) {
      record.cursor = options.cursor;
      this.store.saveSubscription(record);
    }
    const loop = new SubscriptionLoop(this, normalizedTopic, mailbox, name, handler, options);
    if (options.autoStart ?? true) loop.start();
    return loop;
  }

  private targetMailbox(envelope: Envelope): string {
    if (envelope.to === undefined || envelope.to.trim() === "") {
      throw new EnvelopeError(["to is required to route an envelope"]);
    }
    return envelope.to;
  }

  private nowIso(): string {
    return this.now().toISOString();
  }
}

class SubscriptionLoop implements SubscriptionHandle {
  readonly topic: string;
  readonly name: string;
  readonly mailbox: string;

  private readonly client: MailboxClient;
  private readonly handler: EnvelopeHandler;
  private readonly options: SubscribeOptions;
  private timer: NodeJS.Timeout | undefined;
  private ticking = false;
  private stopped = false;

  constructor(
    client: MailboxClient,
    topic: string,
    mailbox: string,
    name: string,
    handler: EnvelopeHandler,
    options: SubscribeOptions,
  ) {
    this.client = client;
    this.topic = topic;
    this.mailbox = mailbox;
    this.name = name;
    this.handler = handler;
    this.options = options;
  }

  cursor(): string | undefined {
    return this.client.store.getSubscription(this.topic, this.name)?.cursor;
  }

  start(): void {
    if (this.timer !== undefined || this.stopped) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.options.pollIntervalMs ?? this.client.pollIntervalMs);
    this.timer.unref();
  }

  stop(): void {
    this.stopped = true;
    if (this.timer === undefined) return;
    clearInterval(this.timer);
    this.timer = undefined;
  }

  async runOnce(): Promise<number> {
    const batch = this.client.receive(this.mailbox, {
      limit: this.options.batchSize ?? this.client.receiveBatchSize,
      visibilityTimeoutMs: this.options.visibilityTimeoutMs ?? this.client.visibilityTimeoutMs,
    });
    let handled = 0;
    for (const message of batch) {
      try {
        await this.handler(message.envelope, message);
        this.client.ack(this.mailbox, [message.envelope.id]);
        this.advanceCursor(message.envelope.id);
        handled += 1;
      } catch (error) {
        this.options.onError?.(error, message.envelope);
        this.client.nack(this.mailbox, [message.envelope.id], {
          action: "requeue",
          delayMs: this.client.retryDelayMs,
        });
      }
    }
    return handled;
  }

  private advanceCursor(cursor: string): void {
    const record = this.client.store.getSubscription(this.topic, this.name);
    if (record === undefined) return;
    this.client.store.saveSubscription({ ...record, cursor });
  }

  private async tick(): Promise<void> {
    if (this.ticking || this.stopped) return;
    this.ticking = true;
    try {
      await this.runOnce();
    } catch {
      // Store-level errors are retried on the next tick; messages stay leased.
    } finally {
      this.ticking = false;
    }
  }
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

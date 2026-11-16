/**
 * In-memory `MailboxStore`: the reference implementation of the semantics.
 * Useful for tests, local runs and as documentation of the contract that
 * `SqliteMailboxStore` mirrors.
 */
import type { Envelope } from "./envelope";
import {
  type AckOptions,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_RECEIVE_BATCH_SIZE,
  DEFAULT_VISIBILITY_TIMEOUT_MS,
  type DeadLetter,
  type DeadLetterOptions,
  type EnqueueResult,
  type MailboxStats,
  type MailboxStore,
  type MessageState,
  type NackOptions,
  type PurgeResult,
  type ReceivedMessage,
  type ReceiveOptions,
  type SubscriptionRecord,
  type TimeOptions,
} from "./mailbox";

export interface MemoryMailboxStoreOptions {
  now?: () => Date;
}

interface MemoryMessage {
  envelope: Envelope;
  mailbox: string;
  state: MessageState;
  priority: number;
  availableAt: string;
  leaseUntil?: string;
  attempts: number;
  createdAt: string;
  expiresAt?: string;
  seq: number;
}

export class MemoryMailboxStore implements MailboxStore {
  private readonly rows = new Map<string, MemoryMessage>();
  private readonly subscriptions = new Map<string, SubscriptionRecord>();
  private readonly now: () => Date;
  private seq = 0;

  constructor(options: MemoryMailboxStoreOptions = {}) {
    this.now = options.now ?? (() => new Date());
  }

  enqueue(mailbox: string, envelope: Envelope, options: TimeOptions = {}): EnqueueResult {
    const now = this.timestamp(options.now);
    const existing = this.rows.get(envelope.id);
    if (existing !== undefined) {
      const expired = existing.expiresAt !== undefined && existing.expiresAt <= now;
      if (!expired) return { mailbox, duplicate: true };
      this.rows.delete(envelope.id);
    }
    this.seq += 1;
    this.rows.set(envelope.id, {
      envelope: structuredClone(envelope),
      mailbox,
      state: "ready",
      priority: envelope.priority ?? 0,
      availableAt: now,
      attempts: 0,
      createdAt: now,
      ...(envelope.ttlMs === undefined ? {} : { expiresAt: plus(now, envelope.ttlMs) }),
      seq: this.seq,
    });
    return { mailbox, duplicate: false };
  }

  receive(mailbox: string, options: ReceiveOptions = {}): ReceivedMessage[] {
    const now = this.timestamp(options.now);
    const limit = Math.max(0, Math.floor(options.limit ?? DEFAULT_RECEIVE_BATCH_SIZE));
    const visibilityTimeoutMs = options.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    if (limit === 0) return [];
    this.purgeExpired(now);

    for (const message of this.rows.values()) {
      if (message.mailbox !== mailbox || message.state !== "leased") continue;
      if (message.leaseUntil === undefined || message.leaseUntil > now) continue;
      if (message.attempts >= maxAttempts) this.markDead(message);
      else {
        message.state = "ready";
        message.availableAt = now;
        message.leaseUntil = undefined;
      }
    }

    const candidates = [...this.rows.values()]
      .filter(
        (message) =>
          message.mailbox === mailbox && message.state === "ready" && message.availableAt <= now,
      )
      .sort(compareMessages);

    const leaseUntil = plus(now, visibilityTimeoutMs);
    const received: ReceivedMessage[] = [];
    for (const message of candidates) {
      if (received.length >= limit) break;
      if (message.attempts >= maxAttempts) {
        this.markDead(message);
        continue;
      }
      message.state = "leased";
      message.attempts += 1;
      message.leaseUntil = leaseUntil;
      received.push({
        mailbox,
        envelope: structuredClone(message.envelope),
        attempts: message.attempts,
        leaseUntil,
      });
    }
    return received;
  }

  ack(mailbox: string, ids: string[], options: AckOptions = {}): number {
    const now = this.timestamp(options.now);
    const dedupeWindowMs = options.dedupeWindowMs ?? 0;
    let count = 0;
    for (const id of ids) {
      const message = this.rows.get(id);
      if (message === undefined || message.mailbox !== mailbox || message.state !== "leased") {
        continue;
      }
      count += 1;
      if (dedupeWindowMs > 0) {
        message.state = "done";
        message.leaseUntil = undefined;
        message.expiresAt = plus(now, dedupeWindowMs);
      } else {
        this.rows.delete(id);
      }
    }
    return count;
  }

  nack(mailbox: string, ids: string[], options: NackOptions): number {
    const now = this.timestamp(options.now);
    const delayMs = options.delayMs ?? 0;
    const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    let count = 0;
    for (const id of ids) {
      const message = this.rows.get(id);
      if (message === undefined || message.mailbox !== mailbox || message.state !== "leased") {
        continue;
      }
      count += 1;
      if (options.action === "dead" || message.attempts >= maxAttempts) {
        this.markDead(message);
      } else {
        message.state = "ready";
        message.availableAt = plus(now, delayMs);
        message.leaseUntil = undefined;
      }
    }
    return count;
  }

  renewLeases(
    mailbox: string,
    ids: string[],
    visibilityTimeoutMs: number,
    options: TimeOptions = {},
  ): number {
    const now = this.timestamp(options.now);
    const leaseUntil = plus(now, visibilityTimeoutMs);
    let count = 0;
    for (const id of ids) {
      const message = this.rows.get(id);
      if (message === undefined || message.mailbox !== mailbox || message.state !== "leased") {
        continue;
      }
      message.leaseUntil = leaseUntil;
      count += 1;
    }
    return count;
  }

  purge(options: TimeOptions = {}): PurgeResult {
    return { purged: this.purgeExpired(this.timestamp(options.now)) };
  }

  stats(mailbox: string, options: TimeOptions = {}): MailboxStats {
    const now = this.timestamp(options.now);
    const counts: Record<MessageState, number> = { ready: 0, leased: 0, done: 0, dead: 0 };
    let oldestReadyAt: string | undefined;
    for (const message of this.rows.values()) {
      if (message.mailbox !== mailbox) continue;
      counts[message.state] += 1;
      if (
        message.state === "ready" &&
        (oldestReadyAt === undefined || message.createdAt < oldestReadyAt)
      ) {
        oldestReadyAt = message.createdAt;
      }
    }
    return {
      mailbox,
      ready: counts.ready,
      leased: counts.leased,
      done: counts.done,
      dead: counts.dead,
      pending: counts.ready + counts.leased,
      total: counts.ready + counts.leased + counts.done + counts.dead,
      ...(oldestReadyAt === undefined
        ? {}
        : {
            oldestReadyAt,
            oldestReadyAgeMs: Math.max(0, Date.parse(now) - Date.parse(oldestReadyAt)),
          }),
    };
  }

  deadLetters(mailbox: string, options: DeadLetterOptions = {}): DeadLetter[] {
    const offset = Math.max(0, Math.floor(options.offset ?? 0));
    const limit = Math.max(0, Math.floor(options.limit ?? Number.POSITIVE_INFINITY));
    return [...this.rows.values()]
      .filter((message) => message.mailbox === mailbox && message.state === "dead")
      .sort(compareMessages)
      .slice(offset, offset + limit)
      .map((message) => ({
        mailbox,
        envelope: structuredClone(message.envelope),
        attempts: message.attempts,
        createdAt: message.createdAt,
      }));
  }

  getSubscription(topic: string, name: string): SubscriptionRecord | undefined {
    const record = this.subscriptions.get(subscriptionKey(topic, name));
    return record === undefined ? undefined : { ...record };
  }

  saveSubscription(record: SubscriptionRecord): void {
    const key = subscriptionKey(record.topic, record.name);
    const existing = this.subscriptions.get(key);
    const cursor = record.cursor ?? existing?.cursor;
    this.subscriptions.set(key, {
      topic: record.topic,
      name: record.name,
      ...(cursor === undefined ? {} : { cursor }),
      createdAt: existing?.createdAt ?? record.createdAt,
    });
  }

  listSubscriptions(topic?: string): SubscriptionRecord[] {
    return [...this.subscriptions.values()]
      .filter((record) => topic === undefined || record.topic === topic)
      .sort((left, right) =>
        `${left.topic}\u0000${left.name}`.localeCompare(`${right.topic}\u0000${right.name}`),
      )
      .map((record) => ({ ...record }));
  }

  private timestamp(now: string | undefined): string {
    return now === undefined ? this.now().toISOString() : rfc3339(now);
  }

  private purgeExpired(now: string): number {
    let purged = 0;
    for (const [id, message] of this.rows) {
      if (message.state === "dead" || message.expiresAt === undefined) continue;
      if (message.expiresAt <= now) {
        this.rows.delete(id);
        purged += 1;
      }
    }
    return purged;
  }

  private markDead(message: MemoryMessage): void {
    message.state = "dead";
    message.leaseUntil = undefined;
    message.expiresAt = undefined;
  }
}

function compareMessages(left: MemoryMessage, right: MemoryMessage): number {
  if (left.priority !== right.priority) return right.priority - left.priority;
  if (left.createdAt !== right.createdAt) return left.createdAt < right.createdAt ? -1 : 1;
  return left.seq - right.seq;
}

function subscriptionKey(topic: string, name: string): string {
  return `${topic}\u0000${name}`;
}

function plus(base: string, milliseconds: number): string {
  return new Date(Date.parse(base) + milliseconds).toISOString();
}

function rfc3339(value: string): string {
  const millis = Date.parse(value);
  if (Number.isNaN(millis)) throw new Error(`invalid RFC 3339 timestamp: ${value}`);
  return new Date(millis).toISOString();
}

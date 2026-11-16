/**
 * SQLite `MailboxStore` on top of `node:sqlite` (same conventions as
 * `packages/master/src/store.ts`: WAL, RFC 3339 UTC text columns, idempotent
 * schema, prepared-statement cache, BEGIN IMMEDIATE transactions).
 *
 * `receive` is transactional: expired leases are recovered and the returned
 * batch is leased atomically, so two consumers can never lease the same id.
 */
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type { StatementSync } from "node:sqlite";
import { DatabaseSync } from "node:sqlite";

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

const SCHEMA = `
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  mailbox TEXT NOT NULL,
  envelope_json TEXT NOT NULL,
  state TEXT NOT NULL,
  priority INTEGER NOT NULL,
  available_at TEXT NOT NULL,
  lease_until TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  expires_at TEXT
);
CREATE INDEX IF NOT EXISTS messages_mailbox_state_available
  ON messages (mailbox, state, available_at);
CREATE INDEX IF NOT EXISTS messages_expires ON messages (expires_at);

CREATE TABLE IF NOT EXISTS subscriptions (
  topic TEXT NOT NULL,
  name TEXT NOT NULL,
  cursor TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY (topic, name)
);
`;

export interface SqliteMailboxStoreOptions {
  now?: () => Date;
}

type Row = Record<string, unknown>;
type ColumnValue = string | number | null;

export class SqliteMailboxStore implements MailboxStore {
  private readonly db: DatabaseSync;
  private readonly statements = new Map<string, StatementSync>();
  private readonly now: () => Date;
  private closed = false;

  constructor(path: string, options: SqliteMailboxStoreOptions = {}) {
    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
    this.now = options.now ?? (() => new Date());
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA synchronous = NORMAL");
    this.db.exec("PRAGMA busy_timeout = 5000");
    this.db.exec(SCHEMA);
  }

  enqueue(mailbox: string, envelope: Envelope, options: TimeOptions = {}): EnqueueResult {
    const now = this.timestamp(options.now);
    const priority = envelope.priority ?? 0;
    const expiresAt = envelope.ttlMs === undefined ? null : plus(now, envelope.ttlMs);
    return this.transaction(() => {
      const existing = this.one("SELECT expires_at FROM messages WHERE id = ?", envelope.id);
      if (existing !== undefined) {
        const existingExpiry = optionalText(existing, "expires_at");
        if (existingExpiry === undefined || existingExpiry > now) {
          return { mailbox, duplicate: true };
        }
        this.run("DELETE FROM messages WHERE id = ?", envelope.id);
      }
      this.run(
        `INSERT INTO messages
           (id, mailbox, envelope_json, state, priority, available_at, lease_until, attempts, created_at, expires_at)
         VALUES (?, ?, ?, 'ready', ?, ?, NULL, 0, ?, ?)`,
        envelope.id,
        mailbox,
        toJson(envelope),
        priority,
        now,
        now,
        expiresAt,
      );
      return { mailbox, duplicate: false };
    });
  }

  receive(mailbox: string, options: ReceiveOptions = {}): ReceivedMessage[] {
    const now = this.timestamp(options.now);
    const limit = Math.max(0, Math.floor(options.limit ?? DEFAULT_RECEIVE_BATCH_SIZE));
    const visibilityTimeoutMs = options.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    if (limit === 0) return [];
    return this.transaction(() => {
      this.purgeExpired(now);

      const expired = this.all(
        `SELECT * FROM messages
         WHERE mailbox = ? AND state = 'leased' AND lease_until IS NOT NULL AND lease_until <= ?
         ORDER BY rowid`,
        mailbox,
        now,
      );
      for (const row of expired) {
        const id = text(row, "id");
        if (integer(row, "attempts") >= maxAttempts) this.markDead(id);
        else {
          this.run(
            "UPDATE messages SET state = 'ready', available_at = ?, lease_until = NULL WHERE id = ?",
            now,
            id,
          );
        }
      }

      const candidates = this.all(
        `SELECT * FROM messages
         WHERE mailbox = ? AND state = 'ready' AND available_at <= ?
         ORDER BY priority DESC, created_at, rowid
         LIMIT ?`,
        mailbox,
        now,
        limit,
      );
      const leaseUntil = plus(now, visibilityTimeoutMs);
      const received: ReceivedMessage[] = [];
      for (const row of candidates) {
        const id = text(row, "id");
        const attempts = integer(row, "attempts");
        if (attempts >= maxAttempts) {
          this.markDead(id);
          continue;
        }
        const changes = this.run(
          `UPDATE messages SET state = 'leased', attempts = attempts + 1, lease_until = ?
           WHERE id = ? AND state = 'ready'`,
          leaseUntil,
          id,
        );
        if (changes !== 1) continue;
        received.push({
          mailbox,
          envelope: jsonField<Envelope>(row, "envelope_json"),
          attempts: attempts + 1,
          leaseUntil,
        });
      }
      return received;
    });
  }

  ack(mailbox: string, ids: string[], options: AckOptions = {}): number {
    const now = this.timestamp(options.now);
    const dedupeWindowMs = options.dedupeWindowMs ?? 0;
    return this.transaction(() => {
      let count = 0;
      for (const id of ids) {
        count +=
          dedupeWindowMs > 0
            ? this.run(
                `UPDATE messages SET state = 'done', lease_until = NULL, expires_at = ?
                 WHERE id = ? AND mailbox = ? AND state = 'leased'`,
                plus(now, dedupeWindowMs),
                id,
                mailbox,
              )
            : this.run(
                "DELETE FROM messages WHERE id = ? AND mailbox = ? AND state = 'leased'",
                id,
                mailbox,
              );
      }
      return count;
    });
  }

  nack(mailbox: string, ids: string[], options: NackOptions): number {
    const now = this.timestamp(options.now);
    const delayMs = options.delayMs ?? 0;
    const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    return this.transaction(() => {
      let count = 0;
      for (const id of ids) {
        const row = this.one(
          "SELECT attempts FROM messages WHERE id = ? AND mailbox = ? AND state = 'leased'",
          id,
          mailbox,
        );
        if (row === undefined) continue;
        count += 1;
        if (options.action === "dead" || integer(row, "attempts") >= maxAttempts) {
          this.markDead(id);
        } else {
          this.run(
            `UPDATE messages SET state = 'ready', available_at = ?, lease_until = NULL
             WHERE id = ? AND state = 'leased'`,
            plus(now, delayMs),
            id,
          );
        }
      }
      return count;
    });
  }

  renewLeases(
    mailbox: string,
    ids: string[],
    visibilityTimeoutMs: number,
    options: TimeOptions = {},
  ): number {
    const now = this.timestamp(options.now);
    const leaseUntil = plus(now, visibilityTimeoutMs);
    return this.transaction(() => {
      let count = 0;
      for (const id of ids) {
        count += this.run(
          `UPDATE messages SET lease_until = ?
           WHERE id = ? AND mailbox = ? AND state = 'leased'`,
          leaseUntil,
          id,
          mailbox,
        );
      }
      return count;
    });
  }

  purge(options: TimeOptions = {}): PurgeResult {
    return { purged: this.purgeExpired(this.timestamp(options.now)) };
  }

  stats(mailbox: string, options: TimeOptions = {}): MailboxStats {
    const now = this.timestamp(options.now);
    const counts: Record<MessageState, number> = { ready: 0, leased: 0, done: 0, dead: 0 };
    for (const row of this.all(
      "SELECT state, COUNT(*) AS total FROM messages WHERE mailbox = ? GROUP BY state",
      mailbox,
    )) {
      const state = text(row, "state");
      if (state === "ready" || state === "leased" || state === "done" || state === "dead") {
        counts[state] = integer(row, "total");
      }
    }
    const oldest = this.one(
      "SELECT MIN(created_at) AS oldest FROM messages WHERE mailbox = ? AND state = 'ready'",
      mailbox,
    );
    const oldestReadyAt = oldest === undefined ? undefined : optionalText(oldest, "oldest");
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
    const limit = options.limit === undefined ? -1 : Math.max(0, Math.floor(options.limit));
    return this.all(
      `SELECT * FROM messages
       WHERE mailbox = ? AND state = 'dead'
       ORDER BY created_at, rowid
       LIMIT ? OFFSET ?`,
      mailbox,
      limit,
      offset,
    ).map((row) => ({
      mailbox,
      envelope: jsonField<Envelope>(row, "envelope_json"),
      attempts: integer(row, "attempts"),
      createdAt: text(row, "created_at"),
    }));
  }

  getSubscription(topic: string, name: string): SubscriptionRecord | undefined {
    const row = this.one("SELECT * FROM subscriptions WHERE topic = ? AND name = ?", topic, name);
    return row === undefined ? undefined : mapSubscription(row);
  }

  saveSubscription(record: SubscriptionRecord): void {
    this.run(
      `INSERT INTO subscriptions (topic, name, cursor, created_at) VALUES (?, ?, ?, ?)
       ON CONFLICT(topic, name) DO UPDATE SET cursor = excluded.cursor`,
      record.topic,
      record.name,
      record.cursor ?? null,
      rfc3339(record.createdAt),
    );
  }

  listSubscriptions(topic?: string): SubscriptionRecord[] {
    const rows =
      topic === undefined
        ? this.all("SELECT * FROM subscriptions ORDER BY topic, name")
        : this.all("SELECT * FROM subscriptions WHERE topic = ? ORDER BY topic, name", topic);
    return rows.map((row) => mapSubscription(row));
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.statements.clear();
    this.db.close();
  }

  private transaction<T>(fn: () => T): T {
    this.ensureOpen();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = fn();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        // keep the original failure
      }
      throw error;
    }
  }

  private purgeExpired(now: string): number {
    return this.run(
      `DELETE FROM messages
       WHERE state != 'dead' AND expires_at IS NOT NULL AND expires_at <= ?`,
      now,
    );
  }

  private markDead(id: string): void {
    this.run(
      "UPDATE messages SET state = 'dead', lease_until = NULL, expires_at = NULL WHERE id = ?",
      id,
    );
  }

  private timestamp(now: string | undefined): string {
    return now === undefined ? this.now().toISOString() : rfc3339(now);
  }

  private statement(sql: string): StatementSync {
    this.ensureOpen();
    const cached = this.statements.get(sql);
    if (cached !== undefined) return cached;
    const prepared = this.db.prepare(sql);
    this.statements.set(sql, prepared);
    return prepared;
  }

  private one(sql: string, ...parameters: ColumnValue[]): Row | undefined {
    return this.statement(sql).get(...parameters) as Row | undefined;
  }

  private all(sql: string, ...parameters: ColumnValue[]): Row[] {
    return this.statement(sql).all(...parameters) as Row[];
  }

  private run(sql: string, ...parameters: ColumnValue[]): number {
    return Number(this.statement(sql).run(...parameters).changes);
  }

  private ensureOpen(): void {
    if (this.closed) throw new Error("mailbox store is closed");
  }
}

function mapSubscription(row: Row): SubscriptionRecord {
  const value: SubscriptionRecord = {
    topic: text(row, "topic"),
    name: text(row, "name"),
    createdAt: text(row, "created_at"),
  };
  const cursor = optionalText(row, "cursor");
  if (cursor !== undefined) value.cursor = cursor;
  return value;
}

function text(row: Row, column: string): string {
  const value = row[column];
  if (typeof value !== "string") throw new Error(`corrupt column ${column}: expected text`);
  return value;
}

function optionalText(row: Row, column: string): string | undefined {
  const value = row[column];
  if (value === null || value === undefined) return undefined;
  if (typeof value !== "string") throw new Error(`corrupt column ${column}: expected text or null`);
  return value;
}

function integer(row: Row, column: string): number {
  const value = row[column];
  if (typeof value !== "number") throw new Error(`corrupt column ${column}: expected integer`);
  return value;
}

function jsonField<T>(row: Row, column: string): T {
  const encoded = text(row, column);
  try {
    return JSON.parse(encoded) as T;
  } catch {
    throw new Error(`corrupt JSON in column ${column}`);
  }
}

function toJson(value: unknown): string {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new Error("value is not JSON serializable");
  return encoded;
}

function plus(base: string, milliseconds: number): string {
  return new Date(Date.parse(base) + milliseconds).toISOString();
}

/** Normalizes to RFC 3339 UTC so string comparison in SQL is chronological. */
function rfc3339(value: string): string {
  const millis = Date.parse(value);
  if (Number.isNaN(millis)) throw new Error(`invalid RFC 3339 timestamp: ${value}`);
  return new Date(millis).toISOString();
}

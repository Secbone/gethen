/**
 * SQLite persistence for the master control plane (P1).
 *
 * - one writer per file (see `lock.ts`; the store itself does not lock)
 * - every JSON-valued field is stored as TEXT and parsed on read
 * - timestamps are stored as RFC 3339 UTC strings so SQLite can compare them
 * - business writes and `effects` inserts share one `transaction()` boundary
 *
 * Schema follows `docs/design/p1-master-research.md` §3 where the model still
 * matches it. Two derived values from that sketch are deliberately not stored:
 * `evals.status` (a Decision or deadline determines it) and `tickets.stage`
 * (computed by `ticketStage()`); see the tests for the round-tripped fields.
 */
import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type { StatementSync } from "node:sqlite";
import { DatabaseSync } from "node:sqlite";

import type {
  Addressed,
  Assignment,
  AssignmentReceipt,
  Decision,
  DecisionOutcome,
  Effect,
  Eval,
  Input,
  Mandate,
  MandateBasis,
  Policy,
  Reason,
  ReportKind,
  Strength,
  Thread,
  Ticket,
  TicketAttention,
  TicketOutcome,
  Trigger,
} from "./model";
import type { PendingEffect, PolicyPort, ReceiptSink, Repo, Repositories } from "./ports";

/** Fails after this many failed attempts (`attempt` counts from 1). */
export const MAX_EFFECT_ATTEMPTS = 5;

/** Conservative default: no source is admitted until a Policy is written. */
export const DEFAULT_POLICY: Policy = {
  version: 1,
  acceptMode: "off",
  rules: [],
  sources: {},
};

const SCHEMA = `
CREATE TABLE IF NOT EXISTS threads (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  created_at TEXT NOT NULL,
  triggers_json TEXT NOT NULL,
  pending_json TEXT NOT NULL,
  subscriptions_json TEXT NOT NULL,
  active_eval TEXT,
  expired INTEGER
);

CREATE TABLE IF NOT EXISTS triggers (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  thread TEXT NOT NULL,
  actor TEXT NOT NULL,
  bot INTEGER NOT NULL,
  addressed TEXT NOT NULL,
  "text" TEXT NOT NULL,
  "at" TEXT NOT NULL,
  meta_json TEXT,
  UNIQUE (source, id)
);
CREATE INDEX IF NOT EXISTS triggers_thread ON triggers (thread);

CREATE TABLE IF NOT EXISTS evals (
  id TEXT PRIMARY KEY,
  thread TEXT NOT NULL,
  trigger_ids_json TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  deadline_at TEXT NOT NULL,
  delivered_at TEXT
);
CREATE INDEX IF NOT EXISTS evals_thread ON evals (thread);

CREATE TABLE IF NOT EXISTS decisions (
  id TEXT PRIMARY KEY,
  eval TEXT NOT NULL,
  outcome TEXT NOT NULL,
  strength TEXT NOT NULL,
  reasons_json TEXT NOT NULL,
  "at" TEXT NOT NULL,
  dismissed_at TEXT
);
CREATE INDEX IF NOT EXISTS decisions_eval ON decisions (eval);

CREATE TABLE IF NOT EXISTS tickets (
  id TEXT PRIMARY KEY,
  mandate TEXT NOT NULL,
  source TEXT NOT NULL,
  threads_json TEXT NOT NULL,
  channels_json TEXT NOT NULL,
  inputs_json TEXT NOT NULL,
  assignments_json TEXT NOT NULL,
  opened_at TEXT NOT NULL,
  ended_at TEXT,
  outcome TEXT,
  latest_report TEXT,
  attention_json TEXT,
  delivery_json TEXT
);
CREATE INDEX IF NOT EXISTS tickets_mandate ON tickets (mandate);

CREATE TABLE IF NOT EXISTS mandates (
  id TEXT PRIMARY KEY,
  ticket TEXT NOT NULL,
  policy_version INTEGER NOT NULL,
  strength TEXT NOT NULL,
  scope TEXT NOT NULL,
  basis_json TEXT NOT NULL,
  matched_rules_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  withdrawn_at TEXT
);
CREATE INDEX IF NOT EXISTS mandates_ticket ON mandates (ticket);

CREATE TABLE IF NOT EXISTS assignments (
  id TEXT PRIMARY KEY,
  ticket TEXT NOT NULL,
  worker TEXT NOT NULL,
  workspace TEXT NOT NULL,
  pack_digest TEXT NOT NULL,
  state TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  started_at TEXT,
  ended_at TEXT
);
CREATE INDEX IF NOT EXISTS assignments_ticket ON assignments (ticket);

CREATE TABLE IF NOT EXISTS inputs (
  id TEXT PRIMARY KEY,
  ticket TEXT NOT NULL,
  kind TEXT NOT NULL,
  "text" TEXT,
  "trigger" TEXT,
  delivered_at TEXT,
  read_at TEXT,
  from_person INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS inputs_ticket ON inputs (ticket);

CREATE TABLE IF NOT EXISTS subscriptions (
  ticket TEXT NOT NULL,
  thread TEXT NOT NULL,
  own INTEGER NOT NULL DEFAULT 0,
  context_only INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ticket, thread)
);
CREATE INDEX IF NOT EXISTS subscriptions_thread ON subscriptions (thread);

CREATE TABLE IF NOT EXISTS policy_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  version INTEGER NOT NULL,
  json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS effects (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  "key" TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  due_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  done_at TEXT,
  claimed_at TEXT
);
CREATE INDEX IF NOT EXISTS effects_due ON effects (status, due_at, created_at);

CREATE TABLE IF NOT EXISTS operations (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  result_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket TEXT NOT NULL,
  assignment TEXT NOT NULL,
  worker TEXT NOT NULL,
  workspace TEXT NOT NULL,
  pack_digest TEXT NOT NULL,
  outcome TEXT NOT NULL,
  summary TEXT NOT NULL,
  events INTEGER NOT NULL,
  usage_json TEXT,
  "at" TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS receipts_ticket ON receipts (ticket);
`;

export interface StoredOperation {
  id: string;
  kind: string;
  requestHash: string;
  result: unknown;
  createdAt: string;
}

export interface StoreOptions {
  /** Policy written when the database is new; defaults to `DEFAULT_POLICY`. */
  policy?: Policy;
}

type ColumnValue = string | number | null;
type Row = Record<string, unknown>;

/** SQLite persistence for every entity in `Repositories`. */
export class SqliteStore implements Repositories, ReceiptSink {
  readonly threads: Repo<Thread>;
  readonly triggers: Repo<Trigger>;
  readonly evals: Repo<Eval>;
  readonly decisions: Repo<Decision>;
  readonly tickets: Repo<Ticket>;
  readonly mandates: Repo<Mandate>;
  readonly assignments: Repo<Assignment>;
  readonly inputs: Repo<Input>;
  readonly policy: PolicyPort;

  private readonly db: DatabaseSync;
  private readonly statements = new Map<string, StatementSync>();
  private depth = 0;
  private closed = false;

  constructor(path: string, options: StoreOptions = {}) {
    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA synchronous = NORMAL");
    this.db.exec("PRAGMA busy_timeout = 5000");
    this.db.exec(SCHEMA);

    this.threads = this.mapped(
      "threads",
      (row) => mapThread(row),
      (value) => {
        this.saveThread(value);
      },
    );
    this.triggers = this.mapped(
      "triggers",
      (row) => mapTrigger(row),
      (value) => {
        this.upsert("triggers", {
          id: value.id,
          source: value.source,
          thread: value.thread,
          actor: value.actor,
          bot: value.bot ? 1 : 0,
          addressed: value.addressed,
          text: value.text,
          at: rfc3339(value.at),
          meta_json: value.meta === undefined ? null : toJson(value.meta),
        });
      },
    );
    this.evals = this.mapped(
      "evals",
      (row) => mapEval(row),
      (value) => {
        this.upsert("evals", {
          id: value.id,
          thread: value.thread,
          trigger_ids_json: toJson(value.triggers),
          policy_version: value.policyVersion,
          started_at: rfc3339(value.startedAt),
          deadline_at: rfc3339(value.deadlineAt),
          delivered_at: value.deliveredAt === undefined ? null : rfc3339(value.deliveredAt),
        });
      },
    );
    this.decisions = this.mapped(
      "decisions",
      (row) => mapDecision(row),
      (value) => {
        this.upsert("decisions", {
          id: value.id,
          eval: value.eval,
          outcome: value.outcome,
          strength: value.strength,
          reasons_json: toJson(value.reasons),
          at: rfc3339(value.at),
          dismissed_at: value.dismissedAt === undefined ? null : rfc3339(value.dismissedAt),
        });
      },
    );
    this.tickets = this.mapped(
      "tickets",
      (row) => this.mapTicket(row),
      (value) => {
        this.saveTicket(value);
      },
    );
    this.mandates = this.mapped(
      "mandates",
      (row) => mapMandate(row),
      (value) => {
        this.upsert("mandates", {
          id: value.id,
          ticket: value.ticket,
          policy_version: value.policyVersion,
          strength: value.strength,
          scope: value.scope,
          basis_json: toJson(value.basis),
          matched_rules_json: toJson(value.matchedRules),
          created_at: rfc3339(value.createdAt),
          withdrawn_at: value.withdrawnAt === undefined ? null : rfc3339(value.withdrawnAt),
        });
      },
    );
    this.assignments = this.mapped(
      "assignments",
      (row) => mapAssignment(row),
      (value) => {
        this.upsert("assignments", {
          id: value.id,
          ticket: value.ticket,
          worker: value.worker,
          workspace: value.workspace,
          pack_digest: value.packDigest,
          state: value.state,
          operation_id: value.operationId,
          started_at: value.startedAt === undefined ? null : rfc3339(value.startedAt),
          ended_at: value.endedAt === undefined ? null : rfc3339(value.endedAt),
        });
      },
    );
    this.inputs = this.mapped(
      "inputs",
      (row) => mapInput(row),
      (value) => {
        this.upsert("inputs", {
          id: value.id,
          ticket: value.ticket,
          kind: value.kind,
          text: value.text ?? null,
          trigger: value.trigger ?? null,
          delivered_at: value.deliveredAt === undefined ? null : rfc3339(value.deliveredAt),
          read_at: value.readAt === undefined ? null : rfc3339(value.readAt),
          from_person: value.fromPerson ? 1 : 0,
        });
      },
    );
    this.policy = { current: () => this.currentPolicy() };

    if (this.one("SELECT version FROM policy_meta WHERE id = 1") === undefined) {
      this.setPolicy(options.policy ?? DEFAULT_POLICY);
    }
  }

  // ---------------------------------------------------------------- transactions

  /**
   * Runs `fn` inside `BEGIN IMMEDIATE` / `COMMIT`, rolling back on throw.
   * Nested calls become savepoints, so repos may persist composites atomically.
   */
  transaction<T>(fn: () => T): T {
    this.ensureOpen();
    const savepoint = this.depth > 0 ? `sp_${this.depth}` : undefined;
    this.db.exec(savepoint === undefined ? "BEGIN IMMEDIATE" : `SAVEPOINT ${savepoint}`);
    this.depth += 1;
    try {
      const result = fn();
      this.db.exec(savepoint === undefined ? "COMMIT" : `RELEASE ${savepoint}`);
      return result;
    } catch (error) {
      try {
        this.db.exec(savepoint === undefined ? "ROLLBACK" : `ROLLBACK TO ${savepoint}`);
        if (savepoint !== undefined) this.db.exec(`RELEASE ${savepoint}`);
      } catch {
        // keep the original failure
      }
      throw error;
    } finally {
      this.depth = Math.max(0, this.depth - 1);
    }
  }

  // --------------------------------------------------------------------- policy

  currentPolicy(): Policy {
    const row = this.one("SELECT json FROM policy_meta WHERE id = 1");
    if (row === undefined) throw new Error("policy is not initialized");
    return jsonField<Policy>(row, "json");
  }

  setPolicy(policy: Policy): void {
    if (!Number.isSafeInteger(policy.version) || policy.version < 1) {
      throw new Error("policy.version must be a positive integer");
    }
    this.run(
      `INSERT INTO policy_meta (id, version, json, updated_at) VALUES (1, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET
         version = excluded.version, json = excluded.json, updated_at = excluded.updated_at`,
      policy.version,
      toJson(policy),
      rfc3339(new Date().toISOString()),
    );
  }

  // ---------------------------------------------------------------------- effects

  /** Inserts durable effects. Call inside `transaction()` with the business write. */
  enqueue(effects: PendingEffect[], nowIso: string = new Date().toISOString()): string[] {
    const createdAt = rfc3339(nowIso);
    const insert = this.statement(
      `INSERT INTO effects (id, kind, "key", payload_json, status, attempt, due_at, created_at)
       VALUES (?, ?, ?, ?, 'waiting', 0, ?, ?)`,
    );
    const ids: string[] = [];
    for (const effect of effects) {
      const id = randomUUID();
      insert.run(
        id,
        effect.kind,
        effect.key,
        toJson(effect.payload),
        rfc3339(effect.dueAt),
        createdAt,
      );
      ids.push(id);
    }
    return ids;
  }

  /**
   * Claims up to `limit` due effects, oldest `dueAt` first. Claiming flips
   * `waiting → running` in one statement per row, so no effect is handed out twice.
   */
  claimDue(nowIso: string, limit: number = 100): Effect[] {
    const now = rfc3339(nowIso);
    const batch = Math.max(0, Math.floor(limit));
    if (batch === 0) return [];
    return this.transaction(() => {
      const rows = this.all(
        `SELECT * FROM effects
         WHERE status = 'waiting' AND due_at <= ?
         ORDER BY due_at, created_at, rowid
         LIMIT ?`,
        now,
        batch,
      );
      const claim = this.statement(
        "UPDATE effects SET status = 'running', claimed_at = ? WHERE id = ? AND status = 'waiting'",
      );
      const claimed: Effect[] = [];
      for (const row of rows) {
        const id = text(row, "id");
        if (Number(claim.run(now, id).changes) !== 1) continue;
        claimed.push(mapEffect({ ...row, status: "running" }));
      }
      return claimed;
    });
  }

  /** Marks a claimed effect done. Unknown or already finished ids are ignored. */
  complete(id: string, doneAt: string = new Date().toISOString()): void {
    this.run(
      `UPDATE effects SET status = 'done', done_at = ?, claimed_at = NULL
       WHERE id = ? AND status IN ('waiting', 'running')`,
      rfc3339(doneAt),
      id,
    );
  }

  /**
   * Records a failed attempt: requeues with `dueAt = now + retryDelayMs`, or
   * marks the effect `failed` once more than `MAX_EFFECT_ATTEMPTS` attempts ran.
   */
  fail(id: string, retryDelayMs: number, nowIso: string = new Date().toISOString()): void {
    if (!Number.isFinite(retryDelayMs) || retryDelayMs < 0) {
      throw new Error("retryDelayMs must be a non-negative finite number");
    }
    const now = rfc3339(nowIso);
    this.transaction(() => {
      const row = this.one("SELECT status, attempt FROM effects WHERE id = ?", id);
      if (row === undefined) throw new Error(`unknown effect: ${id}`);
      const status = text(row, "status");
      if (status === "done" || status === "failed") return;
      const attempt = integer(row, "attempt") + 1;
      if (attempt > MAX_EFFECT_ATTEMPTS) {
        this.run(
          "UPDATE effects SET status = 'failed', attempt = ?, claimed_at = NULL WHERE id = ?",
          attempt,
          id,
        );
        return;
      }
      const dueAt = new Date(Date.parse(now) + retryDelayMs).toISOString();
      this.run(
        `UPDATE effects SET status = 'waiting', attempt = ?, due_at = ?, claimed_at = NULL
         WHERE id = ?`,
        attempt,
        dueAt,
        id,
      );
    });
  }

  /** Crash recovery: `running` effects untouched for `runningTimeoutMs` are retried. */
  resetStale(nowIso: string, runningTimeoutMs: number): number {
    if (!Number.isFinite(runningTimeoutMs) || runningTimeoutMs < 0) {
      throw new Error("runningTimeoutMs must be a non-negative finite number");
    }
    const now = Date.parse(rfc3339(nowIso));
    const cutoff = new Date(now - runningTimeoutMs).toISOString();
    return this.run(
      `UPDATE effects SET status = 'waiting', claimed_at = NULL
       WHERE status = 'running' AND claimed_at IS NOT NULL AND claimed_at < ?`,
      cutoff,
    );
  }

  // ------------------------------------------------------------------- operations

  findOperation(id: string): StoredOperation | undefined {
    const row = this.one("SELECT * FROM operations WHERE id = ?", id);
    return row === undefined ? undefined : mapOperation(row);
  }

  /**
   * Records the receipt for an idempotent operation. Recording the same id with
   * the same request hash is a no-op; a different hash is a conflict.
   */
  recordOperation(id: string, kind: string, requestHash: string, result: unknown): StoredOperation {
    const existing = this.findOperation(id);
    if (existing !== undefined) {
      if (existing.requestHash !== requestHash) {
        throw new Error(`OPERATION_CONFLICT: ${id} was recorded with a different request hash`);
      }
      return existing;
    }
    const createdAt = new Date().toISOString();
    this.run(
      `INSERT INTO operations (id, kind, request_hash, result_json, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      id,
      kind,
      requestHash,
      toJson(result),
      createdAt,
    );
    return { id, kind, requestHash, result, createdAt };
  }

  /** Idempotent wrapper: same id + hash returns the recorded result. */
  runOperation<T>(id: string, kind: string, requestHash: string, compute: () => T): T {
    return this.transaction(() => {
      const existing = this.findOperation(id);
      if (existing !== undefined) {
        if (existing.requestHash !== requestHash) {
          throw new Error(`OPERATION_CONFLICT: ${id} was recorded with a different request hash`);
        }
        return existing.result as T;
      }
      const result = compute();
      this.recordOperation(id, kind, requestHash, result);
      return result;
    });
  }

  // --------------------------------------------------------------------- receipts

  append(receipt: AssignmentReceipt): void {
    this.run(
      `INSERT INTO receipts
         (ticket, assignment, worker, workspace, pack_digest, outcome, summary, events, usage_json, "at")
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      receipt.ticket,
      receipt.assignment,
      receipt.worker,
      receipt.workspace,
      receipt.packDigest,
      receipt.outcome,
      receipt.summary,
      receipt.events,
      receipt.usage === undefined ? null : toJson(receipt.usage),
      rfc3339(receipt.at),
    );
  }

  listReceipts(): AssignmentReceipt[] {
    return this.all("SELECT * FROM receipts ORDER BY id").map((row) => mapReceipt(row));
  }

  // ------------------------------------------------------------------------ close

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.statements.clear();
    this.db.close();
  }

  // --------------------------------------------------------------------- internals

  private mapped<T>(table: string, map: (row: Row) => T, save: (value: T) => void): Repo<T> {
    return {
      get: (id: string) => {
        const row = this.one(`SELECT * FROM ${table} WHERE id = ?`, id);
        return row === undefined ? undefined : map(row);
      },
      list: () => this.all(`SELECT * FROM ${table} ORDER BY rowid`).map((row) => map(row)),
      save,
    };
  }

  private saveThread(thread: Thread): void {
    this.upsert(
      "threads",
      {
        id: thread.id,
        source: thread.source,
        created_at: new Date().toISOString(),
        triggers_json: toJson(thread.triggers),
        pending_json: toJson(thread.pending),
        subscriptions_json: toJson(thread.subscriptions),
        active_eval: thread.activeEval ?? null,
        expired: thread.expired ?? null,
      },
      ["created_at"],
    );
  }

  private saveTicket(ticket: Ticket): void {
    this.transaction(() => {
      this.upsert("tickets", {
        id: ticket.id,
        mandate: ticket.mandate,
        source: ticket.source,
        threads_json: toJson(ticket.threads),
        channels_json: toJson(ticket.channels),
        inputs_json: toJson(ticket.inputs),
        assignments_json: toJson(ticket.assignments),
        opened_at: rfc3339(ticket.openedAt),
        ended_at: ticket.endedAt === undefined ? null : rfc3339(ticket.endedAt),
        outcome: ticket.outcome ?? null,
        latest_report: ticket.latestReport ?? null,
        attention_json: ticket.attention === undefined ? null : toJson(ticket.attention),
        delivery_json: ticket.delivery === undefined ? null : toJson(ticket.delivery),
      });
      this.run("DELETE FROM subscriptions WHERE ticket = ?", ticket.id);
      const insert = this.statement(
        "INSERT INTO subscriptions (ticket, thread, own, context_only) VALUES (?, ?, ?, ?)",
      );
      for (const thread of ticket.subscriptions) {
        const own = ticket.threads.includes(thread);
        insert.run(ticket.id, thread, own ? 1 : 0, own ? 0 : 1);
      }
    });
  }

  private mapTicket(row: Row): Ticket {
    const id = text(row, "id");
    const ticket: Ticket = {
      id,
      mandate: text(row, "mandate"),
      source: text(row, "source"),
      threads: jsonField<string[]>(row, "threads_json"),
      subscriptions: this.ticketSubscriptions(id),
      channels: jsonField<string[]>(row, "channels_json"),
      inputs: jsonField<string[]>(row, "inputs_json"),
      assignments: jsonField<string[]>(row, "assignments_json"),
      openedAt: text(row, "opened_at"),
    };
    const endedAt = optionalText(row, "ended_at");
    if (endedAt !== undefined) ticket.endedAt = endedAt;
    const outcome = optionalText(row, "outcome");
    if (outcome !== undefined) ticket.outcome = outcome as TicketOutcome;
    const latestReport = optionalText(row, "latest_report");
    if (latestReport !== undefined) ticket.latestReport = latestReport as ReportKind;
    const attention = optionalJsonField<TicketAttention>(row, "attention_json");
    if (attention !== undefined) ticket.attention = attention;
    const delivery = optionalJsonField<NonNullable<Ticket["delivery"]>>(row, "delivery_json");
    if (delivery !== undefined) ticket.delivery = delivery;
    return ticket;
  }

  private ticketSubscriptions(ticketId: string): string[] {
    return this.all(
      "SELECT thread FROM subscriptions WHERE ticket = ? ORDER BY rowid",
      ticketId,
    ).map((row) => text(row, "thread"));
  }

  private upsert(table: string, row: Record<string, ColumnValue>, keep: string[] = []): void {
    const columns = Object.keys(row);
    const names = columns.map((column) => `"${column}"`).join(", ");
    const values = columns.map((column) => `$${column}`).join(", ");
    const updates = columns
      .filter((column) => column !== "id" && !keep.includes(column))
      .map((column) => `"${column}" = excluded."${column}"`)
      .join(", ");
    const conflict = updates === "" ? "DO NOTHING" : `DO UPDATE SET ${updates}`;
    this.statement(
      `INSERT INTO ${table} (${names}) VALUES (${values}) ON CONFLICT("id") ${conflict}`,
    ).run(row);
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
    if (this.closed) throw new Error("store is closed");
  }
}

// ------------------------------------------------------------------- row mapping

function mapThread(row: Row): Thread {
  const thread: Thread = {
    id: text(row, "id"),
    source: text(row, "source"),
    triggers: jsonField<string[]>(row, "triggers_json"),
    pending: jsonField<string[]>(row, "pending_json"),
    subscriptions: jsonField<string[]>(row, "subscriptions_json"),
  };
  const activeEval = optionalText(row, "active_eval");
  if (activeEval !== undefined) thread.activeEval = activeEval;
  const expired = optionalInteger(row, "expired");
  if (expired !== undefined) thread.expired = expired;
  return thread;
}

function mapTrigger(row: Row): Trigger {
  const trigger: Trigger = {
    id: text(row, "id"),
    source: text(row, "source"),
    thread: text(row, "thread"),
    actor: text(row, "actor"),
    bot: integer(row, "bot") !== 0,
    addressed: text(row, "addressed") as Addressed,
    text: text(row, "text"),
    at: text(row, "at"),
  };
  const meta = optionalJsonField<Record<string, string>>(row, "meta_json");
  if (meta !== undefined) trigger.meta = meta;
  return trigger;
}

function mapEval(row: Row): Eval {
  const value: Eval = {
    id: text(row, "id"),
    thread: text(row, "thread"),
    triggers: jsonField<string[]>(row, "trigger_ids_json"),
    policyVersion: integer(row, "policy_version"),
    startedAt: text(row, "started_at"),
    deadlineAt: text(row, "deadline_at"),
  };
  const deliveredAt = optionalText(row, "delivered_at");
  if (deliveredAt !== undefined) value.deliveredAt = deliveredAt;
  return value;
}

function mapDecision(row: Row): Decision {
  const value: Decision = {
    id: text(row, "id"),
    eval: text(row, "eval"),
    outcome: text(row, "outcome") as DecisionOutcome,
    strength: text(row, "strength") as Strength,
    reasons: jsonField<Reason[]>(row, "reasons_json"),
    at: text(row, "at"),
  };
  const dismissedAt = optionalText(row, "dismissed_at");
  if (dismissedAt !== undefined) value.dismissedAt = dismissedAt;
  return value;
}

function mapMandate(row: Row): Mandate {
  const value: Mandate = {
    id: text(row, "id"),
    ticket: text(row, "ticket"),
    policyVersion: integer(row, "policy_version"),
    strength: text(row, "strength") as Strength,
    scope: text(row, "scope"),
    basis: jsonField<MandateBasis>(row, "basis_json"),
    matchedRules: jsonField<string[]>(row, "matched_rules_json"),
    createdAt: text(row, "created_at"),
  };
  const withdrawnAt = optionalText(row, "withdrawn_at");
  if (withdrawnAt !== undefined) value.withdrawnAt = withdrawnAt;
  return value;
}

function mapAssignment(row: Row): Assignment {
  const value: Assignment = {
    id: text(row, "id"),
    ticket: text(row, "ticket"),
    worker: text(row, "worker"),
    workspace: text(row, "workspace"),
    packDigest: text(row, "pack_digest"),
    state: text(row, "state") as Assignment["state"],
    operationId: text(row, "operation_id"),
  };
  const startedAt = optionalText(row, "started_at");
  if (startedAt !== undefined) value.startedAt = startedAt;
  const endedAt = optionalText(row, "ended_at");
  if (endedAt !== undefined) value.endedAt = endedAt;
  return value;
}

function mapInput(row: Row): Input {
  const value: Input = {
    id: text(row, "id"),
    ticket: text(row, "ticket"),
    kind: text(row, "kind") as Input["kind"],
    fromPerson: integer(row, "from_person") !== 0,
  };
  const body = optionalText(row, "text");
  if (body !== undefined) value.text = body;
  const trigger = optionalText(row, "trigger");
  if (trigger !== undefined) value.trigger = trigger;
  const deliveredAt = optionalText(row, "delivered_at");
  if (deliveredAt !== undefined) value.deliveredAt = deliveredAt;
  const readAt = optionalText(row, "read_at");
  if (readAt !== undefined) value.readAt = readAt;
  return value;
}

function mapReceipt(row: Row): AssignmentReceipt {
  const outcome = text(row, "outcome");
  if (outcome !== "completed" && outcome !== "failed" && outcome !== "aborted") {
    throw corrupt("outcome", "receipt outcome");
  }
  const value: AssignmentReceipt = {
    assignment: text(row, "assignment"),
    ticket: text(row, "ticket"),
    worker: text(row, "worker"),
    workspace: text(row, "workspace"),
    packDigest: text(row, "pack_digest"),
    at: text(row, "at"),
    outcome,
    summary: text(row, "summary"),
    events: integer(row, "events"),
  };
  const usage = optionalJsonField<NonNullable<AssignmentReceipt["usage"]>>(row, "usage_json");
  if (usage !== undefined) value.usage = usage;
  return value;
}

function mapEffect(row: Row): Effect {
  const status = text(row, "status");
  if (status !== "waiting" && status !== "running" && status !== "done" && status !== "failed") {
    throw corrupt("status", "effect status");
  }
  const value: Effect = {
    id: text(row, "id"),
    kind: text(row, "kind"),
    key: text(row, "key"),
    payload: jsonField<unknown>(row, "payload_json"),
    status,
    attempt: integer(row, "attempt"),
    dueAt: text(row, "due_at"),
    createdAt: text(row, "created_at"),
  };
  const doneAt = optionalText(row, "done_at");
  if (doneAt !== undefined) value.doneAt = doneAt;
  return value;
}

function mapOperation(row: Row): StoredOperation {
  return {
    id: text(row, "id"),
    kind: text(row, "kind"),
    requestHash: text(row, "request_hash"),
    result: jsonField<unknown>(row, "result_json"),
    createdAt: text(row, "created_at"),
  };
}

// ----------------------------------------------------------------------- helpers

function text(row: Row, column: string): string {
  const value = row[column];
  if (typeof value !== "string") throw corrupt(column, "text");
  return value;
}

function optionalText(row: Row, column: string): string | undefined {
  const value = row[column];
  if (value === null || value === undefined) return undefined;
  if (typeof value !== "string") throw corrupt(column, "text or null");
  return value;
}

function integer(row: Row, column: string): number {
  const value = row[column];
  if (typeof value !== "number") throw corrupt(column, "integer");
  return value;
}

function optionalInteger(row: Row, column: string): number | undefined {
  const value = row[column];
  if (value === null || value === undefined) return undefined;
  if (typeof value !== "number") throw corrupt(column, "integer or null");
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

function optionalJsonField<T>(row: Row, column: string): T | undefined {
  const encoded = optionalText(row, column);
  if (encoded === undefined) return undefined;
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

/** Normalizes to RFC 3339 UTC so string comparison in SQL is chronological. */
function rfc3339(value: string): string {
  const millis = Date.parse(value);
  if (Number.isNaN(millis)) throw new Error(`invalid RFC 3339 timestamp: ${value}`);
  return new Date(millis).toISOString();
}

function corrupt(column: string, expected: string): Error {
  return new Error(`corrupt column ${column}: expected ${expected}`);
}

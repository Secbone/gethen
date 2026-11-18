import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { Engine } from "./engine";
import { createOutboxHandlers, type LaunchFailure } from "./executor";
import { FileLock } from "./lock";
import type { Policy, Ticket, Trigger } from "./model";
import { OutboxRunner } from "./outbox";
import type { Clock, Ids, LaunchPort, LaunchRequest, ReceiptSink } from "./ports";
import { SqliteStore } from "./store";

export interface MasterServiceOptions {
  /** SQLite file for tickets, effects, receipts and policy. */
  dbPath: string;
  /** Defaults to `<dbDir>/master.lock`. */
  lockPath?: string;
  /** Written when the database is created. Missing means default deny. */
  policy?: Policy;
  launch: LaunchPort;
  clock?: Clock;
  ids?: Ids;
  /** Defaults to the store, which also persists receipts. */
  receipts?: ReceiptSink;
  outboxIntervalMs?: number;
  objective?: (request: LaunchRequest) => string;
  /** Notified when a launch is settled locally as failed (silence timeout). */
  onLaunchFailure?: (failure: LaunchFailure) => void;
}

export interface SubmitResult {
  stored: "stored" | "duplicate";
  decision?: ReturnType<Engine["decide"]>;
}

/** Snapshot returned by {@link MasterService.waitForTicket}. */
export interface TicketSettlement {
  ticket: Ticket | undefined;
  /** A receipt for the ticket is persisted. */
  receipt: boolean;
  /** No launch effect is pending/running and the ticket reports terminal state. */
  settled: boolean;
  /** Assignments still pending/running (parked escalations are not live). */
  liveAssignments: number;
}

export interface WaitForTicketOptions {
  /** Upper bound; defaults to 10 minutes. */
  timeoutMs?: number;
  /** Poll interval; defaults to 100ms. */
  pollMs?: number;
}

/** Default bound for `waitForTicket` (one gethen_run may legitimately take this long). */
export const DEFAULT_TICKET_WAIT_MS = 10 * 60 * 1000;

/**
 * The process-level assembly: one writer holds the file lock, the engine writes
 * through the store, and the outbox runs the `launch` effect against the
 * injected LaunchPort. DSH wiring stays outside this class.
 */
export class MasterService {
  readonly store: SqliteStore;
  readonly engine: Engine;
  readonly outbox: OutboxRunner;

  private readonly lock: FileLock;
  private readonly outboxIntervalMs: number;
  private stopped = false;

  constructor(options: MasterServiceOptions) {
    const dbDir = dirname(options.dbPath);
    mkdirSync(dbDir, { recursive: true });
    this.lock = new FileLock(options.lockPath ?? join(dbDir, "master.lock"));
    this.outboxIntervalMs = options.outboxIntervalMs ?? 1_000;
    this.lock.acquire();
    try {
      this.store = new SqliteStore(
        options.dbPath,
        options.policy ? { policy: options.policy } : {},
      );
      if (options.policy !== undefined) this.store.setPolicy(options.policy);
      const clock = options.clock ?? { now: () => new Date() };
      const ids = options.ids ?? { next: (prefix: string) => `${prefix}-${crypto.randomUUID()}` };
      this.engine = new Engine({ repos: this.store, clock, ids });
      this.outbox = new OutboxRunner({
        store: this.store,
        handlers: createOutboxHandlers({
          engine: this.engine,
          launch: options.launch,
          receipts: options.receipts ?? this.store,
          ...(options.clock ? { clock: options.clock } : {}),
          ...(options.objective ? { objective: options.objective } : {}),
          ...(options.onLaunchFailure ? { onSettledFailure: options.onLaunchFailure } : {}),
        }),
        ...(options.clock ? { now: () => options.clock?.now() ?? new Date() } : {}),
      });
    } catch (error) {
      this.lock.release();
      throw error;
    }
  }

  start(): void {
    this.outbox.start(this.outboxIntervalMs);
  }

  /** Accept one Trigger end to end: intake, eval, accept, persist and run effects. */
  async submit(trigger: Trigger): Promise<SubmitResult> {
    this.assertRunning();
    const stored = this.engine.receive(trigger);
    if (stored === "duplicate") return { stored };
    this.engine.evaluateDue();
    const candidate = this.engine.candidate(trigger.id);
    if (candidate === undefined) return { stored };
    const decision = this.engine.decide(trigger.id, "accept", [{ code: "master-accept" }]);
    this.flush();
    await this.outbox.runDue();
    return { stored, decision };
  }

  /**
   * Wait until the ticket's launch effect has run to completion and a receipt is
   * persisted (or the ticket ends / the wait times out). Readers of `submit()`
   * must use this instead of immediately snapshotting `tickets`/`receipts`:
   * a failed first launch attempt is retried by the outbox, so an immediate
   * read can observe `failed` while the retry later completes successfully.
   */
  async waitForTicket(
    ticketId: string,
    options: WaitForTicketOptions = {},
  ): Promise<TicketSettlement> {
    const timeoutMs = Math.max(0, options.timeoutMs ?? DEFAULT_TICKET_WAIT_MS);
    const pollMs = Math.max(1, options.pollMs ?? 100);
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const ticket = this.store.tickets.get(ticketId);
      const receipt = this.store.listReceipts().some((row) => row.ticket === ticketId);
      const assignments = this.store.assignments
        .list()
        .filter((assignment) => assignment.ticket === ticketId);
      const liveAssignments = assignments.filter(
        (assignment) => assignment.state === "pending" || assignment.state === "running",
      ).length;
      const pendingLaunch = this.store.hasPendingEffects(
        assignments.map((assignment) => assignment.id),
      );
      const settled =
        receipt ||
        ticket === undefined ||
        ticket.endedAt !== undefined ||
        (!pendingLaunch && liveAssignments === 0 && ticket.latestReport !== undefined);
      if (settled || Date.now() >= deadline) {
        return { ticket, receipt, settled, liveAssignments };
      }
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
  }

  /** Persist effects buffered by engine calls; safe to call repeatedly. */
  flush(): void {
    const effects = this.engine.drain();
    if (effects.length > 0) this.store.enqueue(effects);
  }

  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.outbox.stop();
    this.lock.release();
    this.store.close();
  }

  private assertRunning(): void {
    if (this.stopped) throw new Error("master service is stopped");
  }
}

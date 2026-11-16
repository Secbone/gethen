import { mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { Engine } from "./engine";
import { createOutboxHandlers } from "./executor";
import { FileLock } from "./lock";
import type { Policy, Trigger } from "./model";
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
}

export interface SubmitResult {
  stored: "stored" | "duplicate";
  decision?: ReturnType<Engine["decide"]>;
}

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

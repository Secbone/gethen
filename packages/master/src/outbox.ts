/**
 * Durable outbox relay: claim due effects, run the registered handler, then
 * `complete`; on failure requeue with exponential backoff until the store marks
 * the effect `failed`. The store is the only source of truth; this class keeps
 * no state that must survive a restart.
 */
import type { Effect } from "./model";

/** The subset of `SqliteStore` the relay needs; useful for fakes in tests. */
export interface EffectQueue {
  claimDue(nowIso: string, limit: number): Effect[];
  complete(id: string): void;
  fail(id: string, retryDelayMs: number, nowIso?: string): void;
}

export type OutboxHandler = (payload: unknown) => Promise<void>;

export interface OutboxOptions {
  store: EffectQueue;
  /** One handler per `effect.kind`; unknown kinds fail and retry. */
  handlers: Record<string, OutboxHandler>;
  /** Clock override, mainly for deterministic tests. */
  now?: () => Date;
  /** Effects claimed per `runDue()` call. */
  maxBatch?: number;
  /** First retry delay; doubles per failed attempt. */
  retryDelayMs?: number;
  /** Upper bound for the exponential backoff (default 60s). */
  maxRetryDelayMs?: number;
  /** Observer for handler/timer failures; the relay always keeps running. */
  onError?: (error: unknown, effect: Effect) => void;
}

const DEFAULT_MAX_BATCH = 100;
const DEFAULT_RETRY_DELAY_MS = 2_000;
const DEFAULT_MAX_RETRY_DELAY_MS = 60_000;

export class OutboxRunner {
  private readonly store: EffectQueue;
  private readonly handlers: Record<string, OutboxHandler>;
  private readonly now: () => Date;
  private readonly maxBatch: number;
  private readonly retryDelayMs: number;
  private readonly maxRetryDelayMs: number;
  private readonly onError: ((error: unknown, effect: Effect) => void) | undefined;
  private timer: NodeJS.Timeout | undefined;
  private ticking = false;

  constructor(options: OutboxOptions) {
    this.store = options.store;
    this.handlers = options.handlers;
    this.now = options.now ?? (() => new Date());
    this.maxBatch = positiveInteger(options.maxBatch, DEFAULT_MAX_BATCH);
    this.retryDelayMs = nonNegative(options.retryDelayMs, DEFAULT_RETRY_DELAY_MS);
    this.maxRetryDelayMs = nonNegative(options.maxRetryDelayMs, DEFAULT_MAX_RETRY_DELAY_MS);
    this.onError = options.onError;
  }

  get started(): boolean {
    return this.timer !== undefined;
  }

  /**
   * Claims and runs one batch. Handler errors are recorded with `fail()` and do
   * not reject, so one poison effect cannot stall the rest of the batch.
   */
  async runDue(): Promise<number> {
    const nowIso = this.now().toISOString();
    const effects = this.store.claimDue(nowIso, this.maxBatch);
    for (const effect of effects) {
      const handler = this.handlers[effect.kind];
      try {
        if (handler === undefined) {
          throw new Error(`no outbox handler for kind "${effect.kind}"`);
        }
        await handler(effect.payload);
        this.store.complete(effect.id);
      } catch (error) {
        this.store.fail(effect.id, this.backoffFor(effect.attempt), nowIso);
        this.onError?.(error, effect);
      }
    }
    return effects.length;
  }

  /** Polls `runDue()` on a timer; ticks are skipped while a batch is running. */
  start(intervalMs = 1_000): void {
    if (this.timer !== undefined) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer === undefined) return;
    clearInterval(this.timer);
    this.timer = undefined;
  }

  private async tick(): Promise<void> {
    if (this.ticking) return;
    this.ticking = true;
    try {
      await this.runDue();
    } catch {
      // A store-level failure is retried on the next tick; nothing to requeue.
    } finally {
      this.ticking = false;
    }
  }

  private backoffFor(attempt: number): number {
    const delay = this.retryDelayMs * 2 ** attempt;
    return Math.min(delay, this.maxRetryDelayMs);
  }
}

function positiveInteger(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isSafeInteger(value) && value > 0 ? value : fallback;
}

function nonNegative(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isFinite(value) && value >= 0 ? value : fallback;
}

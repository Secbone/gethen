/**
 * Durable notifications for waking a Master agent.
 *
 * Worker completion and parked escalations are persisted in the Master sqlite
 * store and fanned out to in-process subscribers. The DSH plugin subscribes and
 * steers the live Master agent; `gethen_wait` is the fallback for hosts without
 * agent API access. Delivery is acknowledged so a restarted plugin neither
 * loses an event nor wakes twice.
 */
import type { NotificationInput, NotificationRow, SqliteStore } from "@gethen/master";

export interface NotificationHubOptions {
  store: SqliteStore;
  now?: () => Date;
}

export interface WaitForNotificationOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export class NotificationHub {
  private readonly store: SqliteStore;
  private readonly now: () => Date;
  private readonly listeners = new Set<(row: NotificationRow) => void>();

  constructor(options: NotificationHubOptions) {
    this.store = options.store;
    this.now = options.now ?? (() => new Date());
  }

  /** Persist and fan out one notification; returns the durable row. */
  publish(input: Omit<NotificationInput, "createdAt">): NotificationRow {
    const createdAt = this.now().toISOString();
    const id = this.store.enqueueNotification({ ...input, createdAt });
    const row: NotificationRow = {
      id,
      kind: input.kind,
      ...(input.ticket === undefined ? {} : { ticket: input.ticket }),
      ...(input.assignment === undefined ? {} : { assignment: input.assignment }),
      data: input.data ?? {},
      createdAt,
    };
    for (const listener of [...this.listeners]) {
      try {
        listener(row);
      } catch {
        // A broken subscriber must not break persistence or other subscribers.
      }
    }
    return row;
  }

  /** Undelivered notifications, oldest first. */
  pending(limit = 100): NotificationRow[] {
    return this.store.listNotifications({ pendingOnly: true, limit });
  }

  list(limit = 100): NotificationRow[] {
    return this.store.listNotifications({ limit });
  }

  markDelivered(id: string): void {
    this.store.markNotificationDelivered(id, this.now().toISOString());
  }

  /** Subscribe to newly published notifications; returns an unsubscribe. */
  subscribe(listener: (row: NotificationRow) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * Wait for the oldest undelivered notification (returning it immediately when
   * one is already queued) or the next publication. Does not mark it delivered:
   * the caller acknowledges after handling it.
   */
  async waitForNext(
    options: WaitForNotificationOptions = {},
  ): Promise<NotificationRow | undefined> {
    const queued = this.pending(1)[0];
    if (queued !== undefined) return queued;
    const timeoutMs = Math.max(0, options.timeoutMs ?? 30_000);
    const signal = options.signal;
    if (signal?.aborted) return undefined;
    return new Promise<NotificationRow | undefined>((resolve) => {
      let settled = false;
      let unsubscribe: (() => void) | undefined;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const finish = (row: NotificationRow | undefined): void => {
        if (settled) return;
        settled = true;
        if (timer !== undefined) clearTimeout(timer);
        unsubscribe?.();
        signal?.removeEventListener("abort", onAbort);
        resolve(row);
      };
      const onAbort = (): void => finish(undefined);
      unsubscribe = this.subscribe((row) => finish(row));
      signal?.addEventListener("abort", onAbort, { once: true });
      timer = setTimeout(() => finish(undefined), timeoutMs);
      timer.unref?.();
      // A publication can race with subscription setup; re-check once.
      const raced = this.pending(1)[0];
      if (raced !== undefined) finish(raced);
    });
  }
}

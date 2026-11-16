/**
 * In-memory mailbox and harness doubles for `consumer` tests. Test-only: not
 * exported from the package entry point.
 */

import type {
  HarnessEscalation,
  HarnessEvent,
  HarnessExecution,
  HarnessExecutor,
  HarnessReport,
  HarnessResolution,
  HarnessSink,
  HarnessTask,
} from "@gethen/harness";
import type { Envelope, ReceivedMessage } from "@gethen/mailbox";
import type { MailboxLike } from "./consumer";

interface AckRecord {
  mailbox: string;
  ids: string[];
}

interface NackRecord {
  mailbox: string;
  ids: string[];
  action: "requeue" | "dead";
  delayMs?: number;
}

interface RenewRecord {
  mailbox: string;
  ids: string[];
  visibilityTimeoutMs?: number;
}

/** A scripted mailbox: FIFO queues, in-flight leasing, ack/nack/renew records. */
export class FakeMailbox implements MailboxLike {
  readonly queues = new Map<string, ReceivedMessage[]>();
  readonly dead: ReceivedMessage[] = [];
  readonly sent: Envelope[] = [];
  readonly acks: AckRecord[] = [];
  readonly nacks: NackRecord[] = [];
  readonly renewals: RenewRecord[] = [];
  /** Fail this many `send` calls before succeeding again. */
  failSendTimes = 0;

  private readonly inFlight = new Map<string, Map<string, ReceivedMessage>>();

  enqueue(mailbox: string, envelope: Envelope, attempts = 1): void {
    const queue = this.queues.get(mailbox) ?? [];
    queue.push({
      mailbox,
      envelope,
      attempts,
      leaseUntil: "2030-01-01T00:00:00.000Z",
    });
    this.queues.set(mailbox, queue);
  }

  receive(
    mailbox: string,
    options: { limit?: number; visibilityTimeoutMs?: number } = {},
  ): ReceivedMessage[] {
    const queue = this.queues.get(mailbox) ?? [];
    const batch = queue.splice(0, Math.max(0, options.limit ?? 10));
    let flights = this.inFlight.get(mailbox);
    if (flights === undefined) {
      flights = new Map();
      this.inFlight.set(mailbox, flights);
    }
    for (const message of batch) flights.set(message.envelope.id, message);
    return batch;
  }

  ack(mailbox: string, ids: string[]): number {
    this.acks.push({ mailbox, ids });
    const flights = this.inFlight.get(mailbox);
    for (const id of ids) flights?.delete(id);
    return ids.length;
  }

  nack(
    mailbox: string,
    ids: string[],
    options: { action: "requeue" | "dead"; delayMs?: number; maxAttempts?: number },
  ): number {
    const record: NackRecord = { mailbox, ids, action: options.action };
    if (options.delayMs !== undefined) record.delayMs = options.delayMs;
    this.nacks.push(record);
    const flights = this.inFlight.get(mailbox);
    for (const id of ids) {
      const message = flights?.get(id);
      if (message === undefined) continue;
      flights?.delete(id);
      const next: ReceivedMessage = { ...message, attempts: message.attempts + 1 };
      if (options.action === "dead" || next.attempts > (options.maxAttempts ?? 5)) {
        this.dead.push(next);
      } else {
        this.enqueue(mailbox, next.envelope, next.attempts);
      }
    }
    return ids.length;
  }

  renewLeases(mailbox: string, ids: string[], visibilityTimeoutMs?: number): number {
    const record: RenewRecord = { mailbox, ids };
    if (visibilityTimeoutMs !== undefined) record.visibilityTimeoutMs = visibilityTimeoutMs;
    this.renewals.push(record);
    return ids.length;
  }

  send(envelope: Envelope): { mailbox: string; duplicate: boolean } {
    if (this.failSendTimes > 0) {
      this.failSendTimes -= 1;
      throw new Error("mailbox send failed");
    }
    this.sent.push(envelope);
    return { mailbox: envelope.to ?? "", duplicate: false };
  }

  // ------------------------------------------------------------- assertions

  sentByType(type: string): Envelope[] {
    return this.sent.filter((envelope) => envelope.type === type);
  }

  lastSent(type?: string): Envelope | undefined {
    const matches = type === undefined ? this.sent : this.sentByType(type);
    return matches.at(-1);
  }

  ackIds(): string[] {
    return this.acks.flatMap((record) => record.ids);
  }

  pending(mailbox: string): number {
    return this.queues.get(mailbox)?.length ?? 0;
  }
}

/** One scripted execution started by `FakeHarness`. */
export interface FakeExecution extends HarnessExecution {
  task: HarnessTask;
  sink: HarnessSink;
}

export interface FakeHarnessOptions {
  /** Throw from `start` with this error. */
  startError?: Error;
  /** Report `ready` right after `sendResolution` (default true). */
  reportOnResolution?: boolean;
}

export class FakeHarness implements HarnessExecutor {
  readonly tasks: HarnessTask[] = [];
  readonly sinks: HarnessSink[] = [];
  readonly executions: FakeExecution[] = [];
  readonly resolutions: HarnessResolution[] = [];
  readonly cancels: (string | undefined)[] = [];
  startError: Error | undefined;
  reportOnResolution: boolean;

  constructor(options: FakeHarnessOptions = {}) {
    this.startError = options.startError;
    this.reportOnResolution = options.reportOnResolution ?? true;
  }

  async start(task: HarnessTask, sink: HarnessSink): Promise<HarnessExecution> {
    this.tasks.push(task);
    this.sinks.push(sink);
    if (this.startError !== undefined) throw this.startError;
    const execution: FakeExecution = {
      task,
      sink,
      sendResolution: async (resolution: HarnessResolution): Promise<void> => {
        this.resolutions.push(resolution);
        if (this.reportOnResolution) sink.report({ kind: "ready", summary: "resolved" });
      },
      cancel: async (reason?: string): Promise<void> => {
        this.cancels.push(reason);
        sink.report({
          kind: "failed",
          summary: reason === undefined ? "cancelled" : `cancelled: ${reason}`,
        });
      },
    };
    this.executions.push(execution);
    return execution;
  }

  // ------------------------------------------------------------- scripting

  emitEvent(index: number, event: HarnessEvent): void {
    this.executions[index]?.sink.event(event);
  }

  emitEscalation(index: number, escalation: HarnessEscalation): void {
    this.executions[index]?.sink.escalation(escalation);
  }

  report(index: number, report: HarnessReport): void {
    this.executions[index]?.sink.report(report);
  }
}

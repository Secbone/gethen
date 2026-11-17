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
  HarnessSessionEvent,
  HarnessSessionHistoryOptions,
  HarnessSessionInfo,
  HarnessSink,
  HarnessTask,
  HarnessTaskState,
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
  /** Live state returned by `state()` when `withState` is on. */
  stateValue?: HarnessTaskState;
  /** Texts passed to `steer()`, in order. */
  readonly steerCalls: string[];
  /** How many times `interrupt()` was called. */
  interruptCalls: number;
}

export interface FakeHarnessOptions {
  /** Throw from `start` with this error. */
  startError?: Error;
  /** Report `ready` right after `sendResolution` (default true). */
  reportOnResolution?: boolean;
  /**
   * Install the optional control methods. All default to `false` so the doubles
   * keep modelling a harness that only has `cancel` unless a test opts in.
   */
  withState?: boolean;
  withSteer?: boolean;
  withInterrupt?: boolean;
  /** Initial `stateValue` for every execution (tests may mutate it later). */
  state?: HarnessTaskState;
  /** Throw from `steer` with this error. */
  steerError?: Error;
  /** Throw from `interrupt` with this error. */
  interruptError?: Error;
  /** Install `sessions()` returning `sessionInfos` (default false: no capability). */
  withSessions?: boolean;
  /** Session directory snapshot returned by `sessions()`. */
  sessions?: HarnessSessionInfo[];
  /** Install `sessionHistory()` (default false: no capability). */
  withSessionHistory?: boolean;
  /** Static event history returned by `sessionHistory()`. */
  historyEvents?: HarnessSessionEvent[];
  /** Throw from `sessionHistory` with this error. */
  sessionHistoryError?: Error;
}

export class FakeHarness implements HarnessExecutor {
  readonly tasks: HarnessTask[] = [];
  readonly sinks: HarnessSink[] = [];
  readonly executions: FakeExecution[] = [];
  readonly resolutions: HarnessResolution[] = [];
  readonly cancels: (string | undefined)[] = [];
  startError: Error | undefined;
  reportOnResolution: boolean;
  private readonly withState: boolean;
  private readonly withSteer: boolean;
  private readonly withInterrupt: boolean;
  private readonly initialState: HarnessTaskState | undefined;
  private readonly steerError: Error | undefined;
  private readonly interruptError: Error | undefined;
  private readonly withSessions: boolean;
  /** Mutable session directory for tests; returned by `sessions()` when enabled. */
  readonly sessionInfos: HarnessSessionInfo[];
  sessions?: () => HarnessSessionInfo[];
  readonly sessionHistoryCalls: Array<{
    sessionId: string;
    options?: HarnessSessionHistoryOptions;
  }> = [];
  historyEvents: HarnessSessionEvent[];
  sessionHistoryError: Error | undefined;
  sessionHistory?: (
    sessionId: string,
    options?: HarnessSessionHistoryOptions,
  ) => Promise<HarnessSessionEvent[]>;
  private readonly withSessionHistory: boolean;

  constructor(options: FakeHarnessOptions = {}) {
    this.startError = options.startError;
    this.reportOnResolution = options.reportOnResolution ?? true;
    this.withState = options.withState ?? false;
    this.withSteer = options.withSteer ?? false;
    this.withInterrupt = options.withInterrupt ?? false;
    this.initialState = options.state;
    this.steerError = options.steerError;
    this.interruptError = options.interruptError;
    this.withSessions = options.withSessions ?? false;
    this.sessionInfos = options.sessions ?? [];
    if (this.withSessions) this.sessions = () => this.sessionInfos;
    this.withSessionHistory = options.withSessionHistory ?? false;
    this.historyEvents = options.historyEvents ?? [];
    this.sessionHistoryError = options.sessionHistoryError;
    if (this.withSessionHistory) {
      this.sessionHistory = async (sessionId, historyOptions) => {
        this.sessionHistoryCalls.push({
          sessionId,
          ...(historyOptions === undefined ? {} : { options: historyOptions }),
        });
        if (this.sessionHistoryError !== undefined) throw this.sessionHistoryError;
        return this.historyEvents.map((event) => ({ ...event }));
      };
    }
  }

  async start(task: HarnessTask, sink: HarnessSink): Promise<HarnessExecution> {
    this.tasks.push(task);
    this.sinks.push(sink);
    if (this.startError !== undefined) throw this.startError;
    const execution: FakeExecution = {
      task,
      sink,
      steerCalls: [],
      interruptCalls: 0,
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
    if (this.withState) {
      execution.stateValue = this.initialState;
      execution.state = () => execution.stateValue ?? defaultState(task);
    }
    if (this.withSteer) {
      execution.steer = async (steerText: string): Promise<void> => {
        if (this.steerError !== undefined) throw this.steerError;
        execution.steerCalls.push(steerText);
      };
    }
    if (this.withInterrupt) {
      execution.interrupt = async (): Promise<void> => {
        if (this.interruptError !== undefined) throw this.interruptError;
        execution.interruptCalls += 1;
      };
    }
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

  /** Mutate the live state returned by execution `index` (requires `withState`). */
  setState(index: number, state: HarnessTaskState | undefined): void {
    const execution = this.executions[index];
    if (execution !== undefined) execution.stateValue = state;
  }
}

function defaultState(task: HarnessTask): HarnessTaskState {
  return {
    assignmentId: task.assignmentId,
    phase: "starting",
    step: 0,
    lastEventAt: new Date().toISOString(),
    seq: 0,
  };
}

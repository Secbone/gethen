/**
 * Worker-side execution port: how one dispatched Assignment becomes a session.
 *
 * The WS agent (`./agent`) owns transport, heartbeat and reconnect; it never
 * knows what a session is. An `SessionExecutor` implementation owns exactly
 * that: start a session for a dispatch, stream events, ask the Master to
 * resolve a blocking escalation, finish with one report.
 *
 * `FakeExecutor` is the scripted P4 in-process implementation (event ->
 * escalation -> wait for resolution -> report). A real executor backed by the
 * DSH SDK JSON-RPC daemon plugs in here without touching the transport.
 */
import type {
  EscalationKind,
  EscalationOption,
  JsonObject,
  ReportKind,
  ResolutionBody,
} from "@gethen/worker-protocol";

/** One dispatch forwarded to a session executor. */
export interface ExecutorTask {
  assignmentId: string;
  ticketId: string;
  workspace: string;
  packDigest: string;
  /** The Master's assignment envelope; opaque to the transport. */
  envelope: JsonObject;
  model?: string;
  deadlineAt?: string;
}

/** The payload of an `event` frame (the protocol `EventFrame.event` object). */
export type ExecutorEvent = JsonObject;

/** One blocking escalation a session asks the Master to resolve. */
export interface ExecutorEscalation {
  escalationId: string;
  kind: EscalationKind;
  question: string;
  options: EscalationOption[];
  defaultOption?: string;
}

/** One report a session produces; the agent adds `assignmentId` and the receipt. */
export interface ExecutorReport {
  kind: ReportKind;
  summary: string;
  question?: string;
  usage?: JsonObject;
}

/** Where a running execution delivers its observations. */
export interface ExecutorSink {
  event(event: ExecutorEvent): void;
  escalation(escalation: ExecutorEscalation): void;
  report(report: ExecutorReport): void;
}

/** The agent's control surface for one running execution. */
export interface ExecutionHandle {
  /** Answer the pending escalation. The Master dedupes by escalation id. */
  sendResolution(resolution: ResolutionBody): Promise<void>;
  /** Terminate the session; the executor is expected to report `failed`. */
  cancel(reason?: string): Promise<void>;
}

/** Starts (and owns) one session per dispatch. */
export interface SessionExecutor {
  start(task: ExecutorTask, sink: ExecutorSink): Promise<ExecutionHandle>;
}

/** Scripted report, optionally derived from the resolution that arrived. */
export type FakeReportScript = ExecutorReport | ((resolution: ResolutionBody) => ExecutorReport);

export interface FakeExecutorOptions {
  /** First event emitted by every execution. */
  event?: ExecutorEvent;
  /** Escalation emitted by every execution; the id defaults per assignment. */
  escalation?: Partial<ExecutorEscalation>;
  /** Final report after the resolution; defaults to `ready`. */
  report?: FakeReportScript;
}

const DEFAULT_EVENT: ExecutorEvent = { type: "message", text: "fake executor started" };

/**
 * Deterministic in-process executor for tests and local verification:
 *
 * 1. emit one event;
 * 2. request one blocking `decision` escalation;
 * 3. wait for the Master's resolution, then emit one `ready` report;
 * 4. if cancelled first, emit one `failed` report instead (never both).
 */
export class FakeExecutor implements SessionExecutor {
  readonly tasks: ExecutorTask[] = [];
  readonly resolutions: ResolutionBody[] = [];
  readonly cancels: string[] = [];

  private readonly options: FakeExecutorOptions;

  constructor(options: FakeExecutorOptions = {}) {
    this.options = options;
  }

  async start(task: ExecutorTask, sink: ExecutorSink): Promise<ExecutionHandle> {
    this.tasks.push(task);
    sink.event(this.options.event ?? DEFAULT_EVENT);
    sink.escalation(this.escalationFor(task));

    let settled = false;
    return {
      sendResolution: async (resolution: ResolutionBody): Promise<void> => {
        this.resolutions.push(resolution);
        if (settled) return;
        settled = true;
        sink.report(this.reportFor(task, resolution));
      },
      cancel: async (reason?: string): Promise<void> => {
        this.cancels.push(reason ?? "");
        if (settled) return;
        settled = true;
        sink.report({
          kind: "failed",
          summary: reason === undefined ? "cancelled" : `cancelled: ${reason}`,
        });
      },
    };
  }

  private escalationFor(task: ExecutorTask): ExecutorEscalation {
    const scripted = this.options.escalation ?? {};
    return {
      escalationId: scripted.escalationId ?? `escalation-${task.assignmentId}`,
      kind: scripted.kind ?? "decision",
      question: scripted.question ?? "Proceed with the fake plan?",
      options: scripted.options ?? [
        { id: "yes", label: "Yes" },
        { id: "no", label: "No" },
      ],
      ...(scripted.defaultOption === undefined ? {} : { defaultOption: scripted.defaultOption }),
    };
  }

  private reportFor(task: ExecutorTask, resolution: ResolutionBody): ExecutorReport {
    const scripted = this.options.report;
    if (typeof scripted === "function") return scripted(resolution);
    if (scripted !== undefined) return scripted;
    return { kind: "ready", summary: `fake executor finished ${task.assignmentId}` };
  }
}

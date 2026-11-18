/**
 * One control surface for operators and the DSH tools.
 *
 * The assembly owns the shared implementation of status/overview, ticket
 * lifecycle actions and assignment/session inspection. The HTTP routes in
 * `gethen serve` and the DSH tools are thin adapters over this object; they do
 * not re-derive ticket state or reimplement worker failure handling.
 */
import type {
  BridgeSettlementMetrics,
  HarnessSessionEvent,
  HarnessSessionHistory,
  HarnessSessionSummary,
  TaskEvent,
  TaskEventQuery,
  TaskState,
} from "@gethen/bridge-mailbox";
import {
  buildControlOverview,
  buildControlStatus,
  type ControlOverview,
  type ControlStatus,
  type EscalationRecord,
  type EscalationResolutionInput,
  type MasterService,
  type OverviewFilter,
  summarizeTicket,
  type Ticket,
  type TicketCloseOutcome,
  type TicketSummary,
} from "@gethen/master";
import type { SchedulingMetrics, WorkerSnapshot } from "./fleet";
import type { MasterRuntimeTasks } from "./runtime";

const TASK_ACTIONS = [
  "status",
  "steer",
  "cancel",
  "logs",
  "sessions",
  "history",
  "answer",
] as const;
export type ControlTaskAction = (typeof TASK_ACTIONS)[number];

export type ControlTaskRequest =
  | { action: "status"; assignmentId: string; timeoutMs?: number }
  | { action: "steer"; assignmentId: string; text: string }
  | { action: "cancel"; assignmentId: string; reason?: string }
  | { action: "logs"; assignmentId: string; limit?: number; afterSeq?: number; type?: string }
  | { action: "sessions"; timeoutMs?: number }
  | { action: "history"; sessionId: string; limit?: number; afterSeq?: number; timeoutMs?: number }
  | {
      action: "answer";
      assignmentId: string;
      resolution: EscalationResolutionInput;
      escalationId?: string;
    };

export interface ControlTaskResult {
  action: ControlTaskAction;
  assignmentId?: string;
  state?: TaskState | null;
  cached?: boolean;
  message?: string;
  text?: string;
  steered?: boolean;
  cancelled?: boolean;
  reason?: string;
  events?: Array<TaskEvent | HarnessSessionEvent>;
  count?: number;
  dropped?: number;
  sessions?: HarnessSessionSummary[];
  sessionId?: string;
  at?: string;
  escalation?: EscalationRecord;
  /** Live queue/running/reuse counters; present on `status` results. */
  scheduling?: SchedulingMetrics;
  /** Direct vs recovered settlement counters; present on `status` results. */
  settlements?: BridgeSettlementMetrics;
}

export type ControlTicketAction = "ack" | "close" | "archive";

export interface ControlTicketRequest {
  action: ControlTicketAction;
  ticketId: string;
  /** `close` only; defaults to `completed`. */
  outcome?: TicketCloseOutcome;
  /** `close` only; forwarded to a best-effort Worker cancel. */
  reason?: string;
}

/** A compact list of live tasks for `gethen_status` (bounded, newest first). */
export interface TaskSummary {
  count: number;
  recent: Array<{
    assignmentId: string;
    phase: string;
    step: number;
    lastEventAt: string;
    lastMessage?: string;
  }>;
}

const RECENT_TASKS_LIMIT = 5;

export interface ControlPlane {
  /** Counts and the newest Tickets. Runs the archive sweep first. */
  status(recentLimit?: number): ControlStatus;
  /** Needs-action by default; `open`/`all` are explicit operator views. */
  overview(options?: { filter?: OverviewFilter; now?: Date }): ControlOverview;
  acknowledge(ticketId: string): TicketSummary;
  close(request: {
    ticketId: string;
    outcome?: TicketCloseOutcome;
    reason?: string;
  }): Promise<TicketSummary>;
  archive(ticketId: string): TicketSummary;
  /**
   * Answer the escalation that is waiting on an open Ticket. A parked Worker
   * receives the answer and its eventual result still lands on the Ticket;
   * a report-only Attention is resolved locally without a Worker.
   */
  answer(request: ControlAnswerRequest): Promise<ControlAnswerResult>;
  /** Close stale open Tickets, apply retention and return the archived ids. */
  autoArchive(now?: Date): string[];
  /** Cached live task states, keyed by assignment id. */
  states(): Record<string, TaskState>;
  /** Per-Worker registration, health and load (empty in fake mode). */
  workers(): WorkerSnapshot[];
  /** Live queue/running/reuse counters (zeros without routing). */
  scheduling(): SchedulingMetrics;
  /** Direct vs recovered settlement counters (zeros without a mailbox bridge). */
  settlements(): BridgeSettlementMetrics;
  task(request: ControlTaskRequest): Promise<ControlTaskResult>;
  sessions(options?: { timeoutMs?: number }): Promise<HarnessSessionSummary[]>;
  sessionHistory(
    sessionId: string,
    options?: { limit?: number; afterSeq?: number; timeoutMs?: number },
  ): Promise<HarnessSessionHistory>;
}

export interface ControlAnswerRequest {
  ticketId: string;
  /** The person's answer; becomes a text Input on the Ticket. */
  text: string;
  /** One of the escalation's declared option ids, when the answer selects one. */
  option?: string;
  /** Disambiguates multiple escalations on one Ticket. */
  escalationId?: string;
}

export interface ControlAnswerResult {
  ticket: TicketSummary;
  /** The escalation now marked resolved, when the Ticket had one. */
  escalation?: EscalationRecord;
  /** True when a parked Worker received the answer over the mailbox. */
  delivered: boolean;
  assignmentId?: string;
}

export interface ControlPlaneOptions {
  service: MasterService;
  tasks: MasterRuntimeTasks;
  /** Live Worker snapshots; absent in a fake/offline host means no fleet view. */
  workers?: () => WorkerSnapshot[];
  /** Live scheduling counters; absent hosts report a zero snapshot. */
  scheduling?: () => SchedulingMetrics;
  /** Direct vs recovered settlement counters; absent hosts report zeros. */
  settlements?: () => BridgeSettlementMetrics;
  now?: () => Date;
}

/** Build the shared control surface over one master service. */
export function createControlPlane(options: ControlPlaneOptions): ControlPlane {
  const { service, tasks } = options;
  const workers = options.workers ?? (() => []);
  const scheduling =
    options.scheduling ??
    (() => ({
      queued: 0,
      running: 0,
      dispatched: 0,
      reused: 0,
      queueEntries: 0,
      queueTimeouts: 0,
      maxQueueDepth: 0,
      parked: 0,
    }));
  const settlements =
    options.settlements ??
    (() => ({
      directCompleted: 0,
      recoveredCompleted: 0,
      recoveredWithoutReport: 0,
    }));
  const now = options.now ?? (() => new Date());
  const taskControl = createTaskControl(tasks);

  function input() {
    return {
      tickets: service.store.tickets.list(),
      assignments: service.store.assignments.list(),
      receipts: service.store.listReceipts(),
      escalations: service.store.listEscalations(),
    };
  }

  function summary(ticketId: string): TicketSummary {
    const ticket = service.store.tickets.get(ticketId);
    if (ticket === undefined) throw new Error(`unknown ticket: ${ticketId}`);
    return summarizeTicket(ticket, service.store.assignments.list());
  }

  function autoArchive(at?: Date): string[] {
    const archived = service.engine.convergeDue(at ?? now(), undefined, service.archivePolicy);
    if (archived.length > 0) service.flush();
    return archived;
  }

  /** The escalation an answer targets: explicit id, else the Ticket's newest. */
  function escalationFor(request: ControlAnswerRequest): EscalationRecord | undefined {
    if (request.escalationId !== undefined) {
      const record = service.store.getEscalation(request.escalationId);
      if (record === undefined) throw new Error(`unknown escalation: ${request.escalationId}`);
      if (record.ticket !== request.ticketId) {
        throw new Error(
          `escalation ${record.id} belongs to ticket ${record.ticket}, not ${request.ticketId}`,
        );
      }
      return record;
    }
    return service.store
      .listEscalations()
      .filter((record) => record.ticket === request.ticketId)
      .at(-1);
  }

  /**
   * A bridge failure that only means "no parked Worker to resume" (fake launch,
   * a report-only Attention, or an already-finished assignment) is not fatal:
   * the engine can still record the answer locally.
   */
  function isLocalOnlyAnswerError(error: unknown): boolean {
    if (typeof error !== "object" || error === null) return false;
    const code = (error as { code?: unknown }).code;
    return (
      code === "RUNTIME_COMMS_UNAVAILABLE" ||
      code === "MAILBOX_UNKNOWN_ASSIGNMENT" ||
      code === "MAILBOX_NO_PARKED_ESCALATION"
    );
  }

  /** Resolve the engine after `tasks.answer` when the host had no hook. */
  function settleTaskAnswer(
    request: Extract<ControlTaskRequest, { action: "answer" }>,
    delivered: EscalationRecord | undefined,
  ): void {
    const assignment = service.store.assignments.get(request.assignmentId);
    if (assignment === undefined) return;
    const ticket = service.store.tickets.get(assignment.ticket);
    if (ticket?.attention === undefined) return;
    const resolved = service.engine.resolve(assignment.ticket, request.resolution);
    service.flush();
    const record = delivered ?? resolved;
    service.store.saveEscalation({
      ...record,
      status: "resolved",
      resolution: record.resolution ??
        resolved.resolution ?? {
          kind: request.resolution.kind,
          text: request.resolution.text,
          ...(request.resolution.option === undefined ? {} : { option: request.resolution.option }),
          at: now().toISOString(),
        },
    });
  }

  async function answer(request: ControlAnswerRequest): Promise<ControlAnswerResult> {
    const text = request.text.trim();
    if (text.length === 0) throw new Error("answer text must be a non-empty string");
    const ticket = service.store.tickets.get(request.ticketId);
    if (ticket === undefined) throw new Error(`unknown ticket: ${request.ticketId}`);
    if (ticket.endedAt !== undefined) throw new Error(`ticket ${ticket.id} has ended`);

    const stored = escalationFor(request);
    if (stored?.status === "resolved") {
      throw new Error(`escalation ${stored.id} is already resolved`);
    }
    if (ticket.attention === undefined && stored === undefined) {
      throw new Error(`ticket ${ticket.id} has no escalation waiting`);
    }

    const option = request.option;
    const resolution: EscalationResolutionInput = {
      kind: "answer",
      text,
      ...(option === undefined ? {} : { option }),
    };
    const assignmentId = stored?.assignment ?? ticket.assignments.at(-1);

    let deliveredRecord: EscalationRecord | undefined;
    let delivered = false;
    if (assignmentId !== undefined) {
      try {
        deliveredRecord = tasks.answer(
          assignmentId,
          resolution,
          request.escalationId === undefined ? {} : { escalationId: request.escalationId },
        );
        delivered = true;
      } catch (error) {
        if (!isLocalOnlyAnswerError(error)) throw error;
      }
    }

    // A host with an `onResolution` hook (the DSH plugin) resolves the engine
    // inside `tasks.answer`; a host without one (`gethen serve`) still has the
    // Attention, so resolve it exactly once here.
    let resolvedRecord: EscalationRecord | undefined;
    const current = service.store.tickets.get(request.ticketId);
    if (current?.attention !== undefined) {
      resolvedRecord = service.engine.resolve(request.ticketId, resolution);
      service.flush();
    }

    const base = deliveredRecord ?? resolvedRecord ?? stored;
    let record: EscalationRecord | undefined;
    if (base !== undefined) {
      record = {
        ...base,
        status: "resolved",
        resolution: base.resolution ?? {
          kind: "answer",
          text,
          ...(option === undefined ? {} : { option }),
          at: now().toISOString(),
        },
      };
      service.store.saveEscalation(record);
    }

    return {
      ticket: summary(request.ticketId),
      ...(record === undefined ? {} : { escalation: record }),
      delivered,
      ...(assignmentId === undefined ? {} : { assignmentId }),
    };
  }

  /**
   * Ask the Worker to stop every live Assignment before the Ticket closes.
   * A closed Ticket must not keep a remote task running; a fake/offline runtime
   * simply has nothing to cancel.
   */
  async function cancelLive(ticket: Ticket, reason: string | undefined): Promise<void> {
    for (const assignmentId of ticket.assignments) {
      const assignment = service.store.assignments.get(assignmentId);
      // A pending Assignment has no Worker task yet; only running/waiting work
      // can be stopped remotely.
      if (
        assignment === undefined ||
        (assignment.state !== "waiting" && assignment.state !== "running")
      ) {
        continue;
      }
      try {
        await tasks.cancel(assignmentId, reason);
      } catch {
        // Best effort: closing the local Ticket is authoritative. A Worker that
        // is offline cannot be reached, and its next state query sees no task.
      }
    }
  }

  const plane: ControlPlane = {
    status(recentLimit = 10) {
      autoArchive();
      return buildControlStatus(input(), { recentLimit });
    },
    overview(options = {}) {
      const at = options.now ?? now();
      autoArchive(at);
      return buildControlOverview(input(), {
        filter: options.filter ?? "needs-action",
        now: at,
      });
    },
    acknowledge(ticketId) {
      service.engine.acknowledge(ticketId);
      service.flush();
      return summary(ticketId);
    },
    async close(request) {
      const ticket = service.store.tickets.get(request.ticketId);
      if (ticket === undefined) throw new Error(`unknown ticket: ${request.ticketId}`);
      await cancelLive(ticket, request.reason);
      service.engine.closeTicket(request.ticketId, request.outcome ?? "completed");
      service.flush();
      return summary(request.ticketId);
    },
    archive(ticketId) {
      service.engine.archive(ticketId);
      service.flush();
      return summary(ticketId);
    },
    answer,
    autoArchive,
    states: () => tasks.states(),
    workers,
    scheduling,
    settlements,
    task: async (request) => {
      const result = await taskControl.task(request);
      // A host without an `onResolution` hook still needs the Ticket's
      // Attention cleared and the Assignment resumed after a task-level answer.
      if (request.action === "answer") settleTaskAnswer(request, result.escalation);
      if (request.action === "status") {
        result.scheduling = scheduling();
        result.settlements = settlements();
      }
      return result;
    },
    sessions: (request) => tasks.sessions(request),
    sessionHistory: (sessionId, request) => tasks.sessionHistory(sessionId, request),
  };
  return plane;
}

/**
 * Persist the escalation lifecycle into the master store and keep the engine's
 * Ticket state in sync. One implementation for both hosts: the bridge owns the
 * mailbox protocol, the runtime owns what "parked"/"resolved" means locally.
 */
export function createEscalationHooks(serviceOf: () => MasterService | undefined): {
  onEscalation(record: EscalationRecord): void;
  onResolution(record: EscalationRecord): void;
} {
  return {
    onEscalation(record) {
      const service = serviceOf();
      if (service === undefined) return;
      const saved = service.engine.escalate(record.ticket, {
        id: record.id,
        assignment: record.assignment,
        session: record.session,
        kind: record.kind,
        question: record.question,
        options: record.options,
        ...(record.defaultOption === undefined ? {} : { defaultOption: record.defaultOption }),
      });
      service.store.saveEscalation(saved);
    },
    onResolution(record) {
      const service = serviceOf();
      if (service === undefined) return;
      if (record.resolution === undefined) {
        service.store.saveEscalation(record);
        return;
      }
      const saved = service.engine.resolve(record.ticket, record.resolution);
      service.store.saveEscalation(saved ?? record);
    },
  };
}

/** The task slice of the control surface, usable without a full runtime. */
export function createTaskControl(tasks: MasterRuntimeTasks): Pick<ControlPlane, "task"> {
  return { task: (request) => runTask(tasks, request) };
}

/** Summarize cached task states for `gethen_status`; never throws. */
export function taskSummary(tasks: MasterRuntimeTasks | undefined): TaskSummary | undefined {
  if (tasks === undefined) return undefined;
  let states: Record<string, TaskState>;
  try {
    states = tasks.states();
  } catch {
    return undefined;
  }
  const entries = Object.entries(states);
  entries.sort((left, right) => {
    const leftAt = left[1].updatedAt ?? left[1].lastEventAt;
    const rightAt = right[1].updatedAt ?? right[1].lastEventAt;
    return rightAt.localeCompare(leftAt);
  });
  return {
    count: entries.length,
    recent: entries.slice(0, RECENT_TASKS_LIMIT).map(([assignmentId, state]) => ({
      assignmentId,
      phase: state.phase,
      step: state.step,
      lastEventAt: state.lastEventAt,
      ...(state.lastMessage === undefined ? {} : { lastMessage: state.lastMessage }),
    })),
  };
}

/** Execute one task-control request with human-readable transport failures. */
async function runTask(
  tasks: MasterRuntimeTasks,
  request: ControlTaskRequest,
): Promise<ControlTaskResult> {
  switch (request.action) {
    case "status": {
      let cached: TaskState | undefined;
      try {
        cached = tasks.state(request.assignmentId);
      } catch (error) {
        throw taskFailure(
          error,
          `assignment ${request.assignmentId} did not answer a status request in time; ` +
            "it may be unknown, finished, or its Worker may be offline",
        );
      }
      if (cached !== undefined) {
        return {
          action: "status",
          assignmentId: request.assignmentId,
          state: cached,
          cached: true,
        };
      }
      let live: TaskState | null;
      try {
        live = await tasks.status(
          request.assignmentId,
          request.timeoutMs === undefined ? undefined : { timeoutMs: request.timeoutMs },
        );
      } catch (error) {
        throw taskFailure(
          error,
          `assignment ${request.assignmentId} did not answer a status request in time; ` +
            "it may be unknown, finished, or its Worker may be offline",
        );
      }
      return {
        action: "status",
        assignmentId: request.assignmentId,
        state: live,
        cached: false,
        ...(live === null
          ? { message: `no live task state for assignment ${request.assignmentId}` }
          : {}),
      };
    }
    case "steer": {
      try {
        await tasks.steer(request.assignmentId, request.text);
      } catch (error) {
        throw taskFailure(error, "the Worker did not accept the steer instruction in time");
      }
      return {
        action: "steer",
        assignmentId: request.assignmentId,
        text: request.text,
        steered: true,
      };
    }
    case "cancel": {
      try {
        await tasks.cancel(request.assignmentId, request.reason);
      } catch (error) {
        throw taskFailure(error, "the Worker did not accept the cancel request in time");
      }
      return {
        action: "cancel",
        assignmentId: request.assignmentId,
        cancelled: true,
        ...(request.reason === undefined ? {} : { reason: request.reason }),
      };
    }
    case "answer": {
      try {
        const escalation = tasks.answer(request.assignmentId, request.resolution, {
          ...(request.escalationId === undefined ? {} : { escalationId: request.escalationId }),
        });
        return {
          action: "answer",
          assignmentId: request.assignmentId,
          escalation,
        };
      } catch (error) {
        throw taskFailure(
          error,
          `assignment ${request.assignmentId} has no parked escalation to answer`,
        );
      }
    }
    case "logs": {
      const query: TaskEventQuery = {
        ...(request.limit === undefined ? {} : { limit: request.limit }),
        ...(request.afterSeq === undefined ? {} : { afterSeq: request.afterSeq }),
        ...(request.type === undefined ? {} : { type: request.type }),
      };
      try {
        const events = tasks.events(
          request.assignmentId,
          Object.keys(query).length === 0 ? undefined : query,
        );
        const count = tasks.eventCount(request.assignmentId);
        const dropped = tasks.eventDropped(request.assignmentId);
        return {
          action: "logs",
          assignmentId: request.assignmentId,
          events,
          count,
          ...(dropped === 0 ? {} : { dropped }),
        };
      } catch (error) {
        throw taskFailure(error, `assignment ${request.assignmentId} has no cached task events`);
      }
    }
    case "sessions": {
      try {
        const sessions = await tasks.sessions(
          request.timeoutMs === undefined ? undefined : { timeoutMs: request.timeoutMs },
        );
        return { action: "sessions", sessions };
      } catch (error) {
        throw taskFailure(
          error,
          "the Worker did not answer the session list request in time; it may be offline",
        );
      }
    }
    case "history": {
      try {
        const history = await tasks.sessionHistory(request.sessionId, {
          ...(request.limit === undefined ? {} : { limit: request.limit }),
          ...(request.afterSeq === undefined ? {} : { afterSeq: request.afterSeq }),
          ...(request.timeoutMs === undefined ? {} : { timeoutMs: request.timeoutMs }),
        });
        return {
          action: "history",
          sessionId: history.sessionId,
          events: history.events,
          ...(history.at === undefined ? {} : { at: history.at }),
        };
      } catch (error) {
        throw taskFailure(
          error,
          `session ${request.sessionId} did not answer the history request in time; ` +
            "it may be unknown, finished, or its Worker may be offline",
        );
      }
    }
  }
}

/**
 * Translate runtime/transport failures into actionable tool errors. The fake
 * launch has no worker, so `RUNTIME_COMMS_UNAVAILABLE` is a configuration
 * answer rather than a transport bug; a request timeout usually means the
 * assignment/session is unknown or its Worker is offline.
 */
function taskFailure(error: unknown, timeoutMessage: string): Error {
  if (typeof error === "object" && error !== null) {
    const code = (error as { code?: unknown }).code;
    if (code === "RUNTIME_COMMS_UNAVAILABLE") {
      return codedError(
        "gethen_task: task state/control requires the mailbox runtime; the fake launch " +
          "has no worker to inspect or control (configure ws worker tokens)",
        code,
      );
    }
    if (code === "RUNTIME_CLOSED") {
      return codedError("gethen_task: the master runtime is closed", code);
    }
    if (code === "MAILBOX_STATUS_TIMEOUT") {
      return codedError(`gethen_task: ${timeoutMessage}`, code);
    }
    if (code === "MAILBOX_CLOSED") {
      return codedError("gethen_task: the mailbox bridge is closed", code);
    }
  }
  return error instanceof Error ? error : new Error(String(error));
}

function codedError(message: string, code: string): Error {
  return Object.assign(new Error(message), { code });
}

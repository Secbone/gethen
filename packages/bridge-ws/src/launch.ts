/**
 * Master-side WebSocket `LaunchPort`: the `gethen.worker/v1` adapter between the
 * pure Master engine and the `WorkerGateway`.
 *
 * The engine only sees `LaunchPort.launch(request)`; this module turns it into
 * one `dispatch` frame, then waits for the matching `report` (or a blocking
 * `escalation` that it resolves through the injected resolver). No engine or
 * store code is touched here: reports/receipts are composed from protocol
 * frames exactly like the Remote adapter composes them from transcripts.
 *
 * Wiring: the host must forward gateway frames to `handleFrame`, e.g.
 *
 * ```ts
 * const ws = createWsLaunchPort({ gateway, resolveEscalation });
 * createWorkerGateway({ ..., onFrame: (conn, frame) => ws.handleFrame(conn, frame) });
 * ```
 *
 * The `dispatch` frame's `envelope` follows the worker-agent convention
 * (`{ prompt }`); the agent hands it to its executor opaquely.
 *
 * Worker choice is deliberately not made here. The master-runtime (fleet
 * scheduler) picks a worker per `LaunchRequest` through `selectWorker`; this
 * adapter only validates that the chosen worker is currently connected and
 * dispatches to it. Without the hook it falls back to `options.workerId`, then
 * to the first connection that completed hello.
 */
import type {
  AssignmentReceipt,
  EscalationRecord,
  EscalationResolutionInput,
  LaunchPort,
  LaunchRequest,
  LaunchResult,
  Report,
} from "@gethen/master";
import type { WorkerConnection, WorkerGateway } from "@gethen/transport-ws";
import {
  type DispatchFrame,
  type EscalationFrame,
  PROTOCOL_VERSION,
  type ReportFrame,
  type ResolutionFrame,
  type UpFrame,
} from "@gethen/worker-protocol";

/** Default time a dispatch may take before the launch fails (`WS_LAUNCH_TIMEOUT`). */
export const DEFAULT_LAUNCH_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * Stable error codes a failed launch rejects with.
 *
 * `WS_WORKER_UNAVAILABLE` also covers a throwing/rejecting `selectWorker` hook
 * and a hook result that is not a currently connected worker.
 */
export type WsLaunchErrorCode =
  | "WS_NO_WORKER"
  | "WS_WORKER_UNAVAILABLE"
  | "WS_DISPATCH_FAILED"
  | "WS_LAUNCH_TIMEOUT"
  | "WS_RESOLUTION_FAILED";

/** A launch failure; `message` is prefixed with `code` for log-grep-ability. */
export class WsLaunchError extends Error {
  readonly code: WsLaunchErrorCode;

  constructor(code: WsLaunchErrorCode, message: string) {
    super(`${code}: ${message}`);
    this.name = "WsLaunchError";
    this.code = code;
  }
}

/** The connection fields the launch needs; a real `WorkerConnection` has more. */
export type WorkerConnectionLike = Pick<WorkerConnection, "id" | "workerId">;

/**
 * The gateway surface used here. The production `WorkerGateway` satisfies it;
 * tests can pass a small fake instead of a live WebSocket server.
 */
export type WorkerGatewayLike = Pick<WorkerGateway, "dispatch"> & {
  connections(): readonly WorkerConnectionLike[];
  get(workerId: string): WorkerConnectionLike | undefined;
};

/** The worker chosen for one launch. */
export interface WorkerSelection {
  workerId: string;
}

/**
 * Fleet-aware worker selection hook supplied by the master-runtime. It may be
 * synchronous or asynchronous; `undefined` means "no worker fits this launch".
 */
export type SelectWorker = (
  request: LaunchRequest,
) => WorkerSelection | undefined | Promise<WorkerSelection | undefined>;

export interface WsLaunchOptions {
  gateway: WorkerGatewayLike;
  /**
   * Preferred worker, used only when `selectWorker` is not configured; when
   * absent the first connection that completed hello is used.
   */
  workerId?: string;
  /**
   * Fleet-aware selection hook (master-runtime owns fleet choice): receives the
   * full `LaunchRequest` and returns the worker to dispatch to. It takes
   * precedence over `workerId`. Returning `undefined` rejects with
   * `WS_NO_WORKER`; a throw/rejection or a worker id that is not currently
   * connected rejects with `WS_WORKER_UNAVAILABLE`.
   */
  selectWorker?: SelectWorker;
  /** Answers a Worker escalation; `undefined` parks it for a person. */
  resolveEscalation?: (record: EscalationRecord) => Promise<EscalationResolutionInput | undefined>;
  /** Observer fired when an escalation is parsed (status `"waiting"`). */
  onEscalation?: (record: EscalationRecord) => void;
  /** Observer fired when an escalation is resolved or parked. */
  onResolution?: (record: EscalationRecord) => void;
  /** How long one dispatch may stay unanswered (default 10 minutes). */
  timeoutMs?: number;
  now?: () => Date;
}

export interface WsLaunchBridge {
  /** The port to hand to `MasterService`. */
  readonly launchPort: LaunchPort;
  /** Forward every gateway `onFrame` here; unrelated frames are ignored. */
  handleFrame(conn: WorkerConnectionLike, frame: UpFrame): void;
}

interface PendingLaunch {
  readonly promise: Promise<LaunchResult>;
  readonly request: LaunchRequest;
  readonly workerId: string;
  readonly escalations: EscalationRecord[];
  readonly seenEscalations: Set<string>;
  events: number;
  settled: boolean;
  timer: ReturnType<typeof setTimeout> | undefined;
  resolve(result: LaunchResult): void;
  reject(error: Error): void;
}

/**
 * Build the WebSocket `LaunchPort` plus the frame sink the gateway must call.
 * All state is per-bridge; one bridge serves any number of Workers.
 */
export function createWsLaunchPort(options: WsLaunchOptions): WsLaunchBridge {
  const gateway = options.gateway;
  const now = options.now ?? (() => new Date());
  const timeoutMs = positiveTimeout(options.timeoutMs ?? DEFAULT_LAUNCH_TIMEOUT_MS);
  const pendingByAssignment = new Map<string, PendingLaunch>();

  function notify(callback: (() => void) | undefined): void {
    // Observers (audit/persistence hooks) are best-effort: a throwing observer
    // must not turn into an unhandled rejection that breaks the transport loop.
    try {
      callback?.();
    } catch {
      // ignored on purpose; the launch state is still correct
    }
  }

  function finish(pending: PendingLaunch): void {
    pending.settled = true;
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
      pending.timer = undefined;
    }
    pendingByAssignment.delete(pending.request.assignment.id);
  }

  function resolveWith(pending: PendingLaunch, result: LaunchResult): void {
    if (pending.settled) return;
    finish(pending);
    pending.resolve(result);
  }

  function rejectWith(pending: PendingLaunch, error: Error): void {
    if (pending.settled) return;
    finish(pending);
    pending.reject(error);
  }

  function createPending(request: LaunchRequest, workerId: string): PendingLaunch {
    let resolvePromise: (result: LaunchResult) => void = () => {};
    let rejectPromise: (error: Error) => void = () => {};
    const promise = new Promise<LaunchResult>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    });
    return {
      promise,
      request,
      workerId,
      escalations: [],
      seenEscalations: new Set(),
      events: 0,
      settled: false,
      timer: undefined,
      resolve: resolvePromise,
      reject: rejectPromise,
    };
  }

  function receiptFor(pending: PendingLaunch, report: Report, events: number): AssignmentReceipt {
    return {
      assignment: pending.request.assignment.id,
      ticket: pending.request.ticket.id,
      worker: pending.workerId,
      workspace: pending.request.assignment.workspace,
      packDigest: pending.request.packDigest,
      at: report.at,
      outcome: report.kind === "failed" ? "failed" : "completed",
      summary: report.summary,
      events,
    };
  }

  function resultFor(pending: PendingLaunch, report: Report, events: number): LaunchResult {
    const result: LaunchResult = { report, receipt: receiptFor(pending, report, events) };
    if (pending.escalations.length > 0) result.escalations = [...pending.escalations];
    return result;
  }

  function park(pending: PendingLaunch, record: EscalationRecord): void {
    record.status = "parked";
    notify(() => options.onResolution?.(record));
    const at = now().toISOString();
    const report: Report = {
      kind: "waiting",
      summary: `waiting for a person: ${record.question}`,
      question: record.question,
      at,
    };
    resolveWith(pending, resultFor(pending, report, pending.events));
  }

  function handleReport(pending: PendingLaunch, frame: ReportFrame): void {
    const report: Report = {
      kind: frame.kind,
      summary: frame.summary,
      ...(frame.question === undefined ? {} : { question: frame.question }),
      at: now().toISOString(),
    };
    resolveWith(pending, resultFor(pending, report, frame.receipt.events));
  }

  async function resolveEscalation(
    pending: PendingLaunch,
    record: EscalationRecord,
  ): Promise<void> {
    const resolver = options.resolveEscalation;
    let resolution: EscalationResolutionInput | undefined;
    try {
      resolution = resolver === undefined ? undefined : await resolver(record);
    } catch {
      // A failing resolver cannot answer: fall back to parking for a person.
      resolution = undefined;
    }
    if (pending.settled) return;
    if (resolution === undefined) {
      park(pending, record);
      return;
    }

    const at = now().toISOString();
    record.status = resolution.kind === "answer" ? "resolved" : "parked";
    record.resolution = {
      kind: resolution.kind,
      text: resolution.text,
      ...(resolution.option === undefined ? {} : { option: resolution.option }),
      at,
    };
    notify(() => options.onResolution?.(record));

    const frame: ResolutionFrame = {
      v: PROTOCOL_VERSION,
      type: "resolution",
      assignmentId: pending.request.assignment.id,
      escalationId: record.id,
      resolution: {
        kind: resolution.kind,
        text: resolution.text,
        ...(resolution.option === undefined ? {} : { option: resolution.option }),
      },
    };
    try {
      await gateway.dispatch(pending.workerId, frame);
    } catch (error) {
      rejectWith(
        pending,
        new WsLaunchError(
          "WS_RESOLUTION_FAILED",
          `assignment ${pending.request.assignment.id}: ${messageOf(error)}`,
        ),
      );
    }
  }

  function beginEscalation(
    pending: PendingLaunch,
    conn: WorkerConnectionLike,
    frame: EscalationFrame,
  ): void {
    if (pending.seenEscalations.has(frame.escalationId)) return;
    pending.seenEscalations.add(frame.escalationId);

    const record: EscalationRecord = {
      id: frame.escalationId,
      ticket: pending.request.ticket.id,
      assignment: pending.request.assignment.id,
      // The WS protocol carries no session id; the worker id is the stable
      // identity a person-facing panel can show and a reconnect keeps.
      session: conn.workerId ?? pending.workerId,
      kind: frame.kind,
      question: frame.question,
      options: frame.options.map((option) => ({ id: option.id, label: option.label })),
      ...(frame.defaultOption === undefined ? {} : { defaultOption: frame.defaultOption }),
      status: "waiting",
      at: now().toISOString(),
    };
    pending.escalations.push(record);
    notify(() => options.onEscalation?.(record));

    if (options.resolveEscalation === undefined) {
      park(pending, record);
      return;
    }
    void resolveEscalation(pending, record);
  }

  /**
   * Launch one assignment. Without `selectWorker` the worker is chosen
   * synchronously, so dispatch starts in the same tick as before; with the hook
   * the choice is awaited first.
   */
  function launch(request: LaunchRequest): Promise<LaunchResult> {
    const hook = options.selectWorker;
    if (hook === undefined) {
      try {
        return beginLaunch(request, configuredWorker(gateway, options.workerId));
      } catch (error) {
        return Promise.reject(error);
      }
    }
    return launchWithHook(request, hook);
  }

  async function launchWithHook(request: LaunchRequest, hook: SelectWorker): Promise<LaunchResult> {
    return beginLaunch(request, await selectWithHook(hook, request));
  }

  /** Validate the hook's choice against the live gateway connections. */
  async function selectWithHook(hook: SelectWorker, request: LaunchRequest): Promise<string> {
    let selection: WorkerSelection | undefined;
    try {
      selection = await hook(request);
    } catch (error) {
      throw new WsLaunchError(
        "WS_WORKER_UNAVAILABLE",
        `worker selection failed for assignment ${request.assignment.id}: ${messageOf(error)}`,
      );
    }
    if (selection === undefined) {
      throw new WsLaunchError(
        "WS_NO_WORKER",
        `selectWorker found no worker for assignment ${request.assignment.id}`,
      );
    }
    const workerId: unknown = selection.workerId;
    if (
      typeof workerId !== "string" ||
      workerId.length === 0 ||
      gateway.get(workerId) === undefined
    ) {
      throw new WsLaunchError(
        "WS_WORKER_UNAVAILABLE",
        `worker "${String(workerId)}" is not connected`,
      );
    }
    return workerId;
  }

  function beginLaunch(request: LaunchRequest, workerId: string): Promise<LaunchResult> {
    const pending = createPending(request, workerId);
    pendingByAssignment.set(request.assignment.id, pending);
    pending.timer = setTimeout(() => {
      rejectWith(
        pending,
        new WsLaunchError(
          "WS_LAUNCH_TIMEOUT",
          `assignment ${request.assignment.id} produced no report within ${timeoutMs}ms`,
        ),
      );
    }, timeoutMs);
    // A pending launch must not keep the process alive on its own.
    pending.timer.unref();

    const onDispatchFailure = (error: unknown): void => {
      rejectWith(
        pending,
        new WsLaunchError(
          "WS_DISPATCH_FAILED",
          `assignment ${request.assignment.id}: ${messageOf(error)}`,
        ),
      );
    };
    // Register before sending (a very fast report must not be lost) and do not
    // block the returned promise on the send: the timeout still applies if the
    // transport's send itself never settles.
    try {
      void gateway.dispatch(workerId, dispatchFrameFor(request)).catch(onDispatchFailure);
    } catch (error) {
      onDispatchFailure(error);
    }
    return pending.promise;
  }

  function handleFrame(conn: WorkerConnectionLike, frame: UpFrame): void {
    switch (frame.type) {
      case "event": {
        const pending = pendingByAssignment.get(frame.assignmentId);
        if (pending !== undefined) pending.events += 1;
        return;
      }
      case "escalation": {
        const pending = pendingByAssignment.get(frame.assignmentId);
        if (pending !== undefined) beginEscalation(pending, conn, frame);
        return;
      }
      case "report": {
        const pending = pendingByAssignment.get(frame.assignmentId);
        if (pending !== undefined) handleReport(pending, frame);
        return;
      }
      default:
        // accepted/heartbeat/control_result/error/hello carry no launch state.
        return;
    }
  }

  return { launchPort: { launch }, handleFrame };
}

/** Compose the dispatch frame; `envelope` matches worker-agent's task contract. */
function dispatchFrameFor(request: LaunchRequest): DispatchFrame {
  return {
    v: PROTOCOL_VERSION,
    type: "dispatch",
    assignmentId: request.assignment.id,
    ticketId: request.ticket.id,
    workspace: request.assignment.workspace,
    packDigest: request.packDigest,
    envelope: { prompt: request.prompt },
  };
}

/** Pre-hook worker choice: explicit `workerId`, else the first connected worker. */
function configuredWorker(gateway: WorkerGatewayLike, preferred: string | undefined): string {
  if (preferred !== undefined) {
    if (gateway.get(preferred) === undefined) {
      throw new WsLaunchError("WS_WORKER_UNAVAILABLE", `worker "${preferred}" is not connected`);
    }
    return preferred;
  }
  const candidate = gateway.connections().find((conn) => conn.workerId !== undefined)?.workerId;
  if (candidate === undefined) {
    throw new WsLaunchError("WS_NO_WORKER", "no worker is connected");
  }
  return candidate;
}

function positiveTimeout(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error("bridge-ws: timeoutMs must be a positive finite number");
  }
  return value;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

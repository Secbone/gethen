/**
 * Worker-side `gethen.worker/v1` WebSocket client.
 *
 * The agent is deliberately outbound-only: it connects to the Master, says
 * who it is (`hello`), reports load (`heartbeat`), turns `dispatch` frames into
 * sessions through an injected `SessionExecutor`, and forwards the Master's
 * `resolution` / `cancel` decisions back to the running execution.
 *
 * Transport decisions (see docs/design/p4-ws-control-plane.md §4/§7):
 * - one socket at a time, JSON text frames validated by `@gethen/worker-protocol`;
 * - reconnect uses exponential backoff plus jitter (1s -> 30s by default) and
 *   re-sends `hello`; in-flight assignments are never replayed — the Master
 *   reconciles by `assignmentId` and re-dispatches if it needs to;
 * - a dispatch is idempotent: the same `assignmentId` reuses the running
 *   execution (or re-sends its finished report) instead of starting twice;
 * - a `report` produced while offline is queued and re-sent after the next
 *   `hello`; a still-pending `escalation` is replayed from its execution (the
 *   Master dedupes by escalation id); best-effort `event` frames are dropped
 *   while offline because the report is the source of truth.
 */

import {
  type ControlResultFrame,
  type DispatchFrame,
  type ErrorFrame,
  type EscalationFrame,
  type EventFrame,
  encodeFrame,
  type HeartbeatFrame,
  type HelloFrame,
  type JsonObject,
  type LoadSnapshot,
  PROTOCOL_VERSION,
  parseDownstreamFrame,
  type ReportFrame,
  type ResolutionBody,
  type ResolutionFrame,
  type UpFrame,
} from "@gethen/worker-protocol";
import WebSocket from "ws";
import type {
  ExecutionHandle,
  ExecutorEscalation,
  ExecutorEvent,
  ExecutorReport,
  ExecutorSink,
  ExecutorTask,
  SessionExecutor,
} from "./executor";

type ClientOptions = WebSocket.ClientOptions;
type RawData = WebSocket.RawData;
type Socket = WebSocket;

/** Constructor injection seam; tests pass `ws`' own client. */
export type WebSocketImpl = new (url: string, options?: ClientOptions) => Socket;

export interface ReconnectOptions {
  /** First retry delay; doubles per failed attempt. Defaults to 1s. */
  initialDelayMs?: number;
  /** Retry ceiling. Defaults to 30s. */
  maxDelayMs?: number;
  /** Fraction of the delay applied as random jitter, 0..1. Defaults to 0.2. */
  jitter?: number;
}

export interface WorkerAgentOptions {
  /** `wss://master.example.com/gethen/worker` (ws:// is accepted for tests). */
  masterUrl: string;
  /** Static bearer token; ignored when `tokenProvider` is set. */
  token?: string;
  /** Called for every connection attempt, so tokens can rotate. */
  tokenProvider?: () => string | Promise<string>;
  workerId: string;
  machine: string;
  platform: string;
  runtimes: string[];
  workspaces: string[];
  models: string[];
  capabilities: JsonObject;
  /** Maximum concurrent assignments advertised in hello/heartbeat. */
  quota: number;
  executor: SessionExecutor;
  /** Heartbeat period before the Master overrides it in `hello_ok`. Default 10s. */
  heartbeatMs?: number;
  reconnect?: ReconnectOptions;
  /** WebSocket constructor; defaults to the real `ws` client. */
  WebSocketImpl?: WebSocketImpl;
  /** Injectable clock for heartbeat/event timestamps. */
  now?: () => Date;
  /** Injectable randomness for jitter (0..1). */
  random?: () => number;
  logger?: { error?(message: string): void };
}

export type AgentState = "connecting" | "online" | "closed";

export interface WorkerAgent {
  readonly workerId: string;
  readonly state: AgentState;
  /** Assignments currently running a session (reported as `load.running`). */
  readonly activeAssignments: number;
  /** Resolves once a connection has sent `hello`; rejects if closed first. */
  whenConnected(): Promise<void>;
  /** Stop reconnecting, cancel running executions and close the socket. */
  close(): Promise<void>;
}

const DEFAULT_HEARTBEAT_MS = 10_000;
const DEFAULT_INITIAL_DELAY_MS = 1_000;
const DEFAULT_MAX_DELAY_MS = 30_000;
const DEFAULT_JITTER = 0.2;
const MAX_PENDING_FRAMES = 512;
const SOCKET_OPEN = 1;
const SOCKET_CLOSED = 3;

interface ExecutionEntry {
  task: ExecutorTask;
  /** Events emitted so far; also the `receipt.events` value and next `seq`. */
  events: number;
  handle?: ExecutionHandle;
  /** Escalation the Master may still answer; re-sent after every reconnect. */
  escalation?: EscalationFrame;
  /** Final report, kept so a duplicate dispatch can be acknowledged idempotently. */
  report?: ReportFrame;
  cancelled: boolean;
  cancelReason?: string;
  /** Resolutions that arrived before `start()` returned the handle. */
  pendingResolutions: ResolutionBody[];
}

class WorkerAgentImpl implements WorkerAgent {
  readonly workerId: string;

  private readonly options: WorkerAgentOptions;
  private readonly executor: SessionExecutor;
  private readonly WebSocketImpl: WebSocketImpl;
  private readonly now: () => Date;
  private readonly random: () => number;
  private readonly reconnect: Required<ReconnectOptions>;
  private readonly executions = new Map<string, ExecutionEntry>();
  private readonly pendingFrames: UpFrame[] = [];

  private socket: Socket | undefined;
  private stateValue: AgentState = "connecting";
  private closed = false;
  private heartbeatMs: number;
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectAttempt = 0;
  private connectedWaiters: Array<{ resolve: () => void; reject: (error: Error) => void }> = [];

  constructor(options: WorkerAgentOptions) {
    this.options = options;
    this.workerId = options.workerId;
    this.executor = options.executor;
    this.WebSocketImpl = options.WebSocketImpl ?? (WebSocket as WebSocketImpl);
    this.now = options.now ?? (() => new Date());
    this.random = options.random ?? Math.random;
    this.heartbeatMs = options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS;
    this.reconnect = {
      initialDelayMs: options.reconnect?.initialDelayMs ?? DEFAULT_INITIAL_DELAY_MS,
      maxDelayMs: options.reconnect?.maxDelayMs ?? DEFAULT_MAX_DELAY_MS,
      jitter: clamp(options.reconnect?.jitter ?? DEFAULT_JITTER, 0, 1),
    };
    this.connect();
  }

  get state(): AgentState {
    return this.stateValue;
  }

  get activeAssignments(): number {
    let active = 0;
    for (const entry of this.executions.values()) {
      if (entry.report === undefined) active += 1;
    }
    return active;
  }

  whenConnected(): Promise<void> {
    if (this.stateValue === "online") return Promise.resolve();
    if (this.closed) return Promise.reject(new Error("worker agent is closed"));
    return new Promise<void>((resolve, reject) => {
      this.connectedWaiters.push({ resolve, reject });
    });
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.stateValue = "closed";
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.stopHeartbeat();
    this.pendingFrames.length = 0;
    for (const waiter of this.connectedWaiters.splice(0)) {
      waiter.reject(new Error("worker agent closed before connecting"));
    }

    const handles: Array<{ assignmentId: string; handle: ExecutionHandle }> = [];
    for (const [assignmentId, entry] of this.executions) {
      entry.cancelled = true;
      if (entry.handle !== undefined) handles.push({ assignmentId, handle: entry.handle });
    }
    const socket = this.socket;
    this.socket = undefined;
    const socketClosed = closeSocket(socket);
    await Promise.all(
      handles.map(({ assignmentId, handle }) =>
        handle.cancel("worker agent closed").catch((error: unknown) => {
          this.logError(`cancelling ${assignmentId} failed: ${messageOf(error)}`);
        }),
      ),
    );
    await socketClosed;
    this.executions.clear();
  }

  // ------------------------------------------------------------------ transport

  private connect(): void {
    if (this.closed) return;
    void this.open();
  }

  private async open(): Promise<void> {
    if (this.closed) return;
    let token: string | undefined;
    try {
      token = await this.resolveToken();
    } catch (error) {
      this.logError(`token provider failed: ${messageOf(error)}`);
      this.scheduleReconnect();
      return;
    }
    if (this.closed) return;

    const headers: Record<string, string> = {};
    if (token !== undefined && token.length > 0) headers.Authorization = `Bearer ${token}`;
    let socket: Socket;
    try {
      socket = new this.WebSocketImpl(this.options.masterUrl, { headers });
    } catch (error) {
      this.logError(`connect failed: ${messageOf(error)}`);
      this.scheduleReconnect();
      return;
    }
    this.socket = socket;
    socket.on("open", () => this.handleOpen(socket));
    socket.on("message", (data) => this.handleMessage(socket, data));
    socket.on("close", () => this.handleClose(socket));
    socket.on("error", (error) => this.logError(`socket error: ${messageOf(error)}`));
  }

  private handleOpen(socket: Socket): void {
    if (socket !== this.socket || this.closed) return;
    this.reconnectAttempt = 0;
    this.stateValue = "online";
    this.send(this.helloFrame());
    // Durable outcomes produced while offline follow the hello, in order.
    for (const frame of this.pendingFrames.splice(0)) this.sendNow(frame);
    // Escalations are never queued: replay the still-unanswered one from each
    // live execution so a reconnected Master can reconcile without the Worker
    // re-running the Assignment. The Master dedupes by escalation id.
    for (const entry of this.executions.values()) {
      if (entry.escalation !== undefined) this.sendNow(entry.escalation);
    }
    this.startHeartbeat();
    for (const waiter of this.connectedWaiters.splice(0)) waiter.resolve();
  }

  private handleClose(socket: Socket): void {
    if (socket !== this.socket) return;
    this.socket = undefined;
    this.stateValue = this.closed ? "closed" : "connecting";
    this.stopHeartbeat();
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer !== undefined) return;
    const delay = this.nextReconnectDelay();
    const timer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, delay);
    timer.unref?.();
    this.reconnectTimer = timer;
  }

  private nextReconnectDelay(): number {
    const { initialDelayMs, maxDelayMs, jitter } = this.reconnect;
    const base = Math.min(maxDelayMs, initialDelayMs * 2 ** this.reconnectAttempt);
    this.reconnectAttempt += 1;
    const spread = base * jitter;
    const delay = base - spread + this.random() * spread * 2;
    return Math.max(0, Math.round(delay));
  }

  private async resolveToken(): Promise<string | undefined> {
    if (this.options.tokenProvider !== undefined) return this.options.tokenProvider();
    return this.options.token;
  }

  // -------------------------------------------------------------------- frames

  private handleMessage(socket: Socket, data: RawData): void {
    if (socket !== this.socket) return;
    const parsed = parseDownstreamFrame(rawText(data));
    if (!parsed.ok) {
      this.sendError(parsed.code, parsed.message);
      return;
    }
    const frame = parsed.value;
    switch (frame.type) {
      case "hello_ok":
        if (frame.heartbeatMs !== this.heartbeatMs) {
          this.heartbeatMs = frame.heartbeatMs;
          if (this.stateValue === "online") this.startHeartbeat();
        }
        return;
      case "dispatch":
        this.handleDispatch(frame);
        return;
      case "resolution":
        this.handleResolution(frame);
        return;
      case "cancel":
        this.handleCancel(frame);
        return;
      case "control": {
        const result: ControlResultFrame = {
          v: PROTOCOL_VERSION,
          type: "control_result",
          operationId: frame.operationId,
          ok: false,
          error: "control ops are not supported by the P4 worker agent",
        };
        this.send(result);
        return;
      }
    }
  }

  private helloFrame(): HelloFrame {
    return {
      v: PROTOCOL_VERSION,
      type: "hello",
      workerId: this.workerId,
      protocol: PROTOCOL_VERSION,
      machine: this.options.machine,
      platform: this.options.platform,
      runtimes: [...this.options.runtimes],
      workspaces: [...this.options.workspaces],
      capabilities: this.options.capabilities,
      load: this.load(),
      models: [...this.options.models],
    };
  }

  private heartbeatFrame(): HeartbeatFrame {
    return {
      v: PROTOCOL_VERSION,
      type: "heartbeat",
      at: this.now().toISOString(),
      load: this.load(),
    };
  }

  private load(): LoadSnapshot {
    return { running: this.activeAssignments, quota: this.options.quota };
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    if (this.closed || this.stateValue !== "online") return;
    const timer = setInterval(() => this.send(this.heartbeatFrame()), this.heartbeatMs);
    timer.unref?.();
    this.heartbeatTimer = timer;
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer === undefined) return;
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = undefined;
  }

  private send(frame: UpFrame): void {
    if (this.closed) return;
    if (this.socket !== undefined && this.socket.readyState === SOCKET_OPEN) {
      this.sendNow(frame);
      return;
    }
    // Only reports are queued for replay; events are best-effort and pending
    // escalations are replayed from their executions after the next hello.
    if (frame.type !== "report") return;
    this.pendingFrames.push(frame);
    if (this.pendingFrames.length > MAX_PENDING_FRAMES) this.pendingFrames.shift();
  }

  private sendNow(frame: UpFrame): void {
    const socket = this.socket;
    if (socket === undefined || socket.readyState !== SOCKET_OPEN) return;
    let text: string;
    try {
      text = encodeFrame(frame);
    } catch (error) {
      this.logError(`dropping oversized frame: ${messageOf(error)}`);
      return;
    }
    socket.send(text, (error?: Error) => {
      if (error != null) this.logError(`send failed: ${messageOf(error)}`);
    });
  }

  private sendError(code: string, message: string): void {
    const frame: ErrorFrame = { v: PROTOCOL_VERSION, type: "error", code, message };
    this.send(frame);
  }

  // --------------------------------------------------------------- dispatches

  private handleDispatch(frame: DispatchFrame): void {
    this.send({ v: PROTOCOL_VERSION, type: "accepted", assignmentId: frame.assignmentId });
    const existing = this.executions.get(frame.assignmentId);
    if (existing !== undefined) {
      // Idempotent dispatch: reuse the running handle. Re-send the report or
      // the still-open escalation when the Master missed it across a disconnect.
      if (existing.report !== undefined) this.send(existing.report);
      else if (existing.escalation !== undefined) this.send(existing.escalation);
      return;
    }

    const task: ExecutorTask = {
      assignmentId: frame.assignmentId,
      ticketId: frame.ticketId,
      workspace: frame.workspace,
      packDigest: frame.packDigest,
      envelope: frame.envelope,
      ...(frame.model === undefined ? {} : { model: frame.model }),
      ...(frame.deadlineAt === undefined ? {} : { deadlineAt: frame.deadlineAt }),
    };
    const entry: ExecutionEntry = {
      task,
      events: 0,
      cancelled: false,
      pendingResolutions: [],
    };
    this.executions.set(task.assignmentId, entry);
    const sink: ExecutorSink = {
      event: (event) => this.onEvent(entry, event),
      escalation: (escalation) => this.onEscalation(entry, escalation),
      report: (report) => this.onReport(entry, report),
    };
    try {
      void this.executor.start(task, sink).then(
        (handle) => this.handleStarted(entry, handle),
        (error: unknown) => this.handleStartFailed(entry, error),
      );
    } catch (error) {
      this.handleStartFailed(entry, error);
    }
  }

  private handleStarted(entry: ExecutionEntry, handle: ExecutionHandle): void {
    if (entry.report !== undefined) return;
    entry.handle = handle;
    if (entry.cancelled) {
      void handle.cancel(entry.cancelReason).catch((error: unknown) => {
        this.logError(`cancel failed: ${messageOf(error)}`);
      });
      return;
    }
    for (const resolution of entry.pendingResolutions.splice(0)) {
      void handle.sendResolution(resolution).catch((error: unknown) => {
        this.logError(`resolution delivery failed: ${messageOf(error)}`);
      });
    }
  }

  private handleStartFailed(entry: ExecutionEntry, error: unknown): void {
    if (entry.report !== undefined) return;
    this.onReport(entry, {
      kind: "failed",
      summary: `executor failed to start: ${messageOf(error)}`,
    });
  }

  private onEvent(entry: ExecutionEntry, event: ExecutorEvent): void {
    const frame: EventFrame = {
      v: PROTOCOL_VERSION,
      type: "event",
      assignmentId: entry.task.assignmentId,
      seq: entry.events,
      at: this.now().toISOString(),
      event,
    };
    entry.events += 1;
    this.send(frame);
  }

  private onEscalation(entry: ExecutionEntry, escalation: ExecutorEscalation): void {
    const frame: EscalationFrame = {
      v: PROTOCOL_VERSION,
      type: "escalation",
      assignmentId: entry.task.assignmentId,
      escalationId: escalation.escalationId,
      kind: escalation.kind,
      question: escalation.question,
      options: escalation.options.map((option) => ({ id: option.id, label: option.label })),
      ...(escalation.defaultOption === undefined
        ? {}
        : { defaultOption: escalation.defaultOption }),
      blocking: true,
    };
    entry.escalation = frame;
    this.send(frame);
  }

  private onReport(entry: ExecutionEntry, report: ExecutorReport): void {
    entry.escalation = undefined;
    entry.handle = undefined;
    const frame: ReportFrame = {
      v: PROTOCOL_VERSION,
      type: "report",
      assignmentId: entry.task.assignmentId,
      kind: report.kind,
      summary: report.summary,
      ...(report.question === undefined ? {} : { question: report.question }),
      ...(report.usage === undefined ? {} : { usage: report.usage }),
      receipt: { packDigest: entry.task.packDigest, events: entry.events },
    };
    entry.report = frame;
    this.send(frame);
  }

  private handleResolution(frame: ResolutionFrame): void {
    const entry = this.executions.get(frame.assignmentId);
    if (entry === undefined || entry.escalation?.escalationId !== frame.escalationId) return;
    // The escalation id matches once; later duplicates are ignored.
    entry.escalation = undefined;
    if (entry.handle === undefined) {
      entry.pendingResolutions.push(frame.resolution);
      return;
    }
    void entry.handle.sendResolution(frame.resolution).catch((error: unknown) => {
      this.logError(`resolution delivery failed: ${messageOf(error)}`);
    });
  }

  private handleCancel(frame: { assignmentId: string; reason?: string }): void {
    const entry = this.executions.get(frame.assignmentId);
    if (entry === undefined || entry.report !== undefined) return;
    entry.cancelled = true;
    entry.cancelReason = frame.reason;
    entry.escalation = undefined;
    if (entry.handle === undefined) return;
    void entry.handle.cancel(frame.reason).catch((error: unknown) => {
      this.logError(`cancel failed: ${messageOf(error)}`);
    });
  }

  private logError(message: string): void {
    this.options.logger?.error?.(message);
  }
}

/**
 * Create and start one Worker agent. The first connection attempt starts
 * immediately; reconnect attempts continue in the background until `close()`.
 */
export function createWorkerAgent(options: WorkerAgentOptions): WorkerAgent {
  return new WorkerAgentImpl(options);
}

/** Close a socket and wait for its `close` event (bounded, never rejects). */
function closeSocket(socket: Socket | undefined): Promise<void> {
  if (socket === undefined || socket.readyState === SOCKET_CLOSED) return Promise.resolve();
  return new Promise<void>((resolve) => {
    let settled = false;
    const finish = (): void => {
      if (settled) return;
      settled = true;
      resolve();
    };
    socket.once("close", finish);
    try {
      socket.close(1000, "worker agent closed");
    } catch {
      finish();
    }
  });
}

function rawText(data: RawData): string {
  if (typeof data === "string") return data;
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString("utf8");
  return data.toString("utf8");
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function clamp(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, value));
}

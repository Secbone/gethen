/**
 * DSH SDK driver: one `dsh --profile sdk` child process behind JSON-RPC.
 *
 * The driver owns the process boundary only. It spawns the runtime, speaks the
 * newline-delimited JSON-RPC protocol over its stdio, exposes the session calls
 * the Worker daemon needs, and turns server-to-client requests into callbacks.
 * Everything is injectable (`spawnImpl`, streams, request timeout) so tests run
 * against a fake in-memory child and never launch the real runtime.
 *
 * Protocol reference: DSH `@deepseek-ai/dsh-sdk-protocol` (`initialize`,
 * `session/prompt`, `session/query`, `session/wait`, `session/resume`,
 * `session/working-directory/get|set`, `shutdown`; notifications
 * `session.event` / `session.status`; server requests `approval/request` and
 * `user-question/ask`). This package re-declares the wire shapes locally and
 * imports nothing from DSH.
 */
import { spawn } from "node:child_process";
import {
  type ChildLike,
  chunkToText,
  type DuplexLike,
  JsonRpcPeer,
  type StreamListener,
} from "./rpc";

/** How a Worker asks the SDK runtime to behave at handshake time. */
export interface InitializeOptions {
  provider: string;
  model: string;
  /** Defaults to the spawn cwd, then to `process.cwd()`. */
  cwd?: string;
  maxTokens?: number;
  reasoningEffort?: string;
  /** Defaults to whether an `onApprovalRequest` handler is registered. */
  approvalRequests?: boolean;
  /** Defaults to whether an `onUserQuestion` handler is registered. */
  userQuestions?: boolean;
}

export interface ServerInfo {
  name: string;
  version: string;
}

export interface InitializeResult {
  serverInfo: ServerInfo;
}

export interface SessionPromptResult {
  /** Identity of the queued user message; not the assistant reply. */
  messageId: string;
}

export type SessionQueryResult =
  | { kind: "prompt"; messageId: string }
  | { kind: "command"; execution: unknown };

export interface SessionEventNotification {
  /** Session the event belongs to (the runtime reports every session). */
  sessionId: string;
  /** The session-log event envelope. */
  event: { type: string; [key: string]: unknown };
}

export type SessionStatus = "idle" | "running";

export interface SessionStatusNotification {
  sessionId: string;
  status: SessionStatus;
}

export type ApprovalOutcome = "allowed-once" | "rejected" | "cancelled";

export interface ApprovalRequestParams {
  sessionId: string;
  toolName: string;
  callId?: string;
  reason?: string;
}

export interface ApprovalRequestResult {
  outcome: ApprovalOutcome;
}

export type ApprovalHandler = (
  params: ApprovalRequestParams,
) => ApprovalOutcome | Promise<ApprovalOutcome>;

export interface UserQuestionOption {
  label: string;
  description?: string;
}

/** One question in a `user-question/ask` request. */
export interface UserQuestionItem {
  id: string;
  question: string;
  detail?: string;
  header?: string;
  options?: UserQuestionOption[];
  multiSelect?: boolean;
  intent?: unknown;
}

export interface UserQuestionRequest {
  requestId: string;
  sessionId: string;
  questions: UserQuestionItem[];
}

export interface UserQuestionAnswerItem {
  id: string;
  selected: string[];
  custom?: string;
}

export type UserQuestionResult = { answers: UserQuestionAnswerItem[] } | { cancelled: true };

export type UserQuestionHandler = (
  request: UserQuestionRequest,
) => UserQuestionResult | Promise<UserQuestionResult>;

/** Spawn options the driver hands to `spawnImpl` (Node `spawn` shaped). */
export interface DshSdkSpawnOptions {
  cwd?: string;
  env: Record<string, string | undefined>;
  stdio: readonly ["pipe", "pipe", "pipe"];
}

export type DshSdkSpawn = (
  command: string,
  args: string[],
  options: DshSdkSpawnOptions,
) => ChildLike;

export interface StartDshSdkOptions {
  /** Defaults to `process.env.GETHEN_DSH`, then `dsh`. */
  command?: string;
  /** Defaults to `["--profile", "sdk"]`. */
  args?: string[];
  cwd?: string;
  /** Extra environment entries merged over `process.env`. */
  env?: Record<string, string>;
  /** Injection point for tests; defaults to `node:child_process.spawn`. */
  spawnImpl?: DshSdkSpawn;
  /** Default request timeout; defaults to 30 seconds. */
  requestTimeoutMs?: number;
  /** Graceful shutdown budget before SIGTERM; defaults to 5 seconds. */
  shutdownTimeoutMs?: number;
}

/** Graceful shutdown budget: `shutdown` waits this long for the process to exit. */
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000;
/** Default command used when neither the caller nor `GETHEN_DSH` names one. */
export const DEFAULT_DSH_COMMAND = "dsh";
/** Default runtime arguments: the SDK profile mounted by `dsh`. */
export const DEFAULT_DSH_ARGS: readonly string[] = ["--profile", "sdk"];

const defaultSpawn: DshSdkSpawn = (command, args, options) => {
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: options.env,
    stdio: "pipe",
  });
  return child as unknown as ChildLike;
};

/**
 * One SDK runtime process, driven over JSON-RPC. Create it, `start()` it, then
 * `initialize()` before any session call. `shutdown()` closes it gracefully;
 * `close()` is the synchronous last-resort cleanup.
 */
export class DshSdkClient {
  private child: ChildLike | undefined;
  private peer: JsonRpcPeer | undefined;
  private launchedCwd: string | undefined;
  private shutdownTimeoutMs = DEFAULT_SHUTDOWN_TIMEOUT_MS;

  private readonly eventListeners = new Set<(notification: SessionEventNotification) => void>();
  private readonly statusListeners = new Set<(notification: SessionStatusNotification) => void>();
  private readonly stderrListeners = new Set<(text: string) => void>();
  private readonly exitWaiters = new Set<() => void>();

  private stderrStream: DuplexLike | undefined;
  private stderrListener: StreamListener | undefined;
  private approvalHandler: ApprovalHandler | undefined;
  private userQuestionHandler: UserQuestionHandler | undefined;
  private exited = false;
  private closed = false;
  private terminated = false;

  /** True while the runtime process is alive and the client is usable. */
  get running(): boolean {
    return this.child !== undefined && !this.exited && !this.closed;
  }

  /**
   * Spawn the runtime and attach the JSON-RPC peer. Throws when the client
   * already started; a daemon that needs a new process creates a new client.
   */
  start(options: StartDshSdkOptions = {}): this {
    if (this.child !== undefined || this.closed) {
      throw new Error("dsh sdk client has already been started");
    }
    const command = options.command ?? process.env.GETHEN_DSH ?? DEFAULT_DSH_COMMAND;
    const args = options.args ?? [...DEFAULT_DSH_ARGS];
    const spawnOptions: DshSdkSpawnOptions = {
      env: { ...process.env, ...options.env },
      stdio: ["pipe", "pipe", "pipe"],
    };
    if (options.cwd !== undefined) spawnOptions.cwd = options.cwd;

    const child = (options.spawnImpl ?? defaultSpawn)(command, args, spawnOptions);
    this.child = child;
    this.launchedCwd = options.cwd;
    this.shutdownTimeoutMs = options.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;

    const peer = new JsonRpcPeer({
      input: child.stdout,
      output: child.stdin,
      ...(options.requestTimeoutMs === undefined
        ? {}
        : { requestTimeoutMs: options.requestTimeoutMs }),
    });
    peer.onNotification((method, params) => {
      this.handleNotification(method, params);
    });
    peer.onRequest((method, params) => this.handleRequest(method, params));
    this.peer = peer;

    child.on("exit", (...args) => this.handleExit(args[0], args[1]));
    child.on("close", (...args) => this.handleExit(args[0], args[1]));
    child.on("error", (error) => {
      this.handleFailure(error instanceof Error ? error : new Error(String(error)));
    });
    this.attachStderr(child.stderr);
    return this;
  }

  /**
   * Perform the process-wide handshake. `capabilities.userQuestions` and
   * `approvalRequests` are only declared when the caller asks for them or the
   * matching handler is registered, so unhandled flows keep DSH's defaults.
   */
  async initialize(options: InitializeOptions): Promise<InitializeResult> {
    const peer = this.requirePeer();
    const params: Record<string, unknown> = {
      cwd: options.cwd ?? this.launchedCwd ?? process.cwd(),
      provider: options.provider,
      model: options.model,
    };
    if (options.maxTokens !== undefined) params.maxTokens = options.maxTokens;
    if (options.reasoningEffort !== undefined) {
      params.reasoningEffort = options.reasoningEffort;
    }
    if (options.approvalRequests ?? this.approvalHandler !== undefined) {
      params.approvalRequests = true;
    }
    if (options.userQuestions ?? this.userQuestionHandler !== undefined) {
      params.capabilities = { userQuestions: true };
    }
    return (await peer.request("initialize", params)) as InitializeResult;
  }

  /** Queue one text prompt; the runtime returns the durable enqueue receipt. */
  async prompt(sessionId: string, text: string): Promise<SessionPromptResult> {
    return (await this.requirePeer().request("session/prompt", {
      sessionId,
      contentBlocks: [{ type: "text", text }],
    })) as SessionPromptResult;
  }

  /** Send ordinary input through the SDK's command-aware entry. */
  async query(sessionId: string, text: string): Promise<SessionQueryResult> {
    return (await this.requirePeer().request("session/query", {
      sessionId,
      contentBlocks: [{ type: "text", text }],
    })) as SessionQueryResult;
  }

  /** Wait until the session and its managed descendants finish this round. */
  async wait(sessionId: string, timeoutMs?: number): Promise<void> {
    const peer = this.requirePeer();
    await peer.request("session/wait", { sessionId }, timeoutMs === undefined ? {} : { timeoutMs });
  }

  /** Resume a persisted session without queueing new input. */
  async resume(sessionId: string): Promise<void> {
    await this.requirePeer().request("session/resume", { sessionId });
  }

  /** Read the session's effective working directory. */
  async getWorkingDirectory(sessionId: string): Promise<string> {
    const result = (await this.requirePeer().request("session/working-directory/get", {
      sessionId,
    })) as { cwd: string };
    return result.cwd;
  }

  /** Change the session's effective working directory and return the new one. */
  async setWorkingDirectory(sessionId: string, path: string): Promise<string> {
    const result = (await this.requirePeer().request("session/working-directory/set", {
      sessionId,
      path,
    })) as { cwd: string };
    return result.cwd;
  }

  /** Subscribe to `session.event` notifications; returns an unsubscribe. */
  onEvent(listener: (notification: SessionEventNotification) => void): () => void {
    this.eventListeners.add(listener);
    return () => {
      this.eventListeners.delete(listener);
    };
  }

  /** Subscribe to `session.status` notifications; returns an unsubscribe. */
  onStatus(listener: (notification: SessionStatusNotification) => void): () => void {
    this.statusListeners.add(listener);
    return () => {
      this.statusListeners.delete(listener);
    };
  }

  /** Subscribe to raw stderr text; returns an unsubscribe. */
  onStderr(listener: (text: string) => void): () => void {
    this.stderrListeners.add(listener);
    return () => {
      this.stderrListeners.delete(listener);
    };
  }

  /**
   * Answer `approval/request`. Without a handler, or when the handler fails,
   * the driver answers `cancelled`, which denies the operation.
   */
  onApprovalRequest(handler: ApprovalHandler | undefined): void {
    this.approvalHandler = handler;
  }

  /**
   * Answer `user-question/ask`. Without a handler, or when the handler fails,
   * the driver answers `{ cancelled: true }` so the question is dismissed.
   */
  onUserQuestion(handler: UserQuestionHandler | undefined): void {
    this.userQuestionHandler = handler;
  }

  /**
   * Ask the runtime to shut down, wait up to `shutdownTimeoutMs` for the
   * process to exit, then SIGTERM it, then close the client.
   */
  async shutdown(): Promise<void> {
    if (this.closed) return;
    const child = this.child;
    const peer = this.peer;
    if (child === undefined || peer === undefined) {
      this.close();
      return;
    }
    const exited = this.waitForExit();
    try {
      await peer.request("shutdown", undefined, { timeoutMs: this.shutdownTimeoutMs });
    } catch {
      // The runtime may close stdio while exiting; the process exit is the contract.
    }
    if (!(await exited)) {
      this.terminated = true;
      child.kill("SIGTERM");
    }
    this.close();
  }

  /**
   * Synchronous last-resort cleanup: reject pending requests, stop reading,
   * detach stderr and kill a still-live runtime with SIGKILL.
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    const peer = this.peer;
    this.peer = undefined;
    peer?.close("dsh sdk client closed");

    const stream = this.stderrStream;
    const listener = this.stderrListener;
    this.stderrStream = undefined;
    this.stderrListener = undefined;
    if (stream !== undefined && listener !== undefined) stream.off?.("data", listener);

    const child = this.child;
    this.child = undefined;
    if (child !== undefined && !this.exited && !this.terminated) child.kill("SIGKILL");

    this.eventListeners.clear();
    this.statusListeners.clear();
    this.stderrListeners.clear();
    this.exitWaiters.clear();
    this.approvalHandler = undefined;
    this.userQuestionHandler = undefined;
  }

  private requirePeer(): JsonRpcPeer {
    if (this.peer === undefined) throw new Error("dsh sdk client is not started");
    return this.peer;
  }

  private attachStderr(stream: DuplexLike): void {
    const listener: StreamListener = (chunk) => {
      const text = chunkToText(chunk);
      for (const subscriber of [...this.stderrListeners]) subscriber(text);
    };
    this.stderrStream = stream;
    this.stderrListener = listener;
    stream.on("data", listener);
  }

  private handleExit(code: unknown, signal: unknown): void {
    if (this.exited) return;
    this.exited = true;
    const exitCode = typeof code === "number" ? code : null;
    const exitSignal = typeof signal === "string" ? signal : null;
    this.peer?.close(
      `dsh sdk process exited (code ${String(exitCode)}, signal ${String(exitSignal)})`,
    );
    this.flushExitWaiters();
  }

  private handleFailure(error: Error): void {
    if (this.exited) return;
    this.exited = true;
    this.peer?.close(`dsh sdk process failed: ${error.message}`);
    this.flushExitWaiters();
  }

  private flushExitWaiters(): void {
    const waiters = [...this.exitWaiters];
    this.exitWaiters.clear();
    for (const waiter of waiters) waiter();
  }

  /** Resolves `true` when the child exits within the shutdown budget. */
  private waitForExit(): Promise<boolean> {
    if (this.exited) return Promise.resolve(true);
    return new Promise<boolean>((resolve) => {
      let timer: NodeJS.Timeout | undefined;
      const waiter = (): void => {
        if (timer !== undefined) clearTimeout(timer);
        resolve(true);
      };
      this.exitWaiters.add(waiter);
      timer = setTimeout(() => {
        this.exitWaiters.delete(waiter);
        resolve(false);
      }, this.shutdownTimeoutMs);
      const withUnref = timer as NodeJS.Timeout & { unref?: () => void };
      withUnref.unref?.();
    });
  }

  private handleNotification(method: string, params: unknown): void {
    if (typeof params !== "object" || params === null) return;
    if (method === "session.event") {
      const notification = params as unknown as SessionEventNotification;
      for (const listener of [...this.eventListeners]) listener(notification);
      return;
    }
    if (method === "session.status") {
      const notification = params as unknown as SessionStatusNotification;
      for (const listener of [...this.statusListeners]) listener(notification);
    }
  }

  private async handleRequest(method: string, params: unknown): Promise<unknown> {
    if (method === "approval/request") {
      const handler = this.approvalHandler;
      if (handler === undefined) return { outcome: "cancelled" } satisfies ApprovalRequestResult;
      try {
        return {
          outcome: await handler(params as ApprovalRequestParams),
        } satisfies ApprovalRequestResult;
      } catch {
        return { outcome: "cancelled" } satisfies ApprovalRequestResult;
      }
    }
    if (method === "user-question/ask") {
      const handler = this.userQuestionHandler;
      if (handler === undefined) return { cancelled: true } satisfies UserQuestionResult;
      try {
        return await handler(params as UserQuestionRequest);
      } catch {
        return { cancelled: true } satisfies UserQuestionResult;
      }
    }
    throw new Error(`unsupported DSH SDK server request: ${method}`);
  }
}

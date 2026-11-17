/**
 * Newline-delimited JSON-RPC 2.0 peer over injected streams.
 *
 * The DSH SDK runtime (`dsh --profile sdk`) speaks one JSON-RPC message per
 * line on stdio: frames with both `id` and `method` are requests, `id` alone is
 * a response, and `method` alone is a notification. Malformed lines are
 * ignored, which keeps diagnostics accidentally written to stdout from killing
 * the channel.
 *
 * This module owns framing, request ids, pending-request bookkeeping, timeouts
 * and stream lifecycle. It never imports DSH and never touches a process: the
 * caller injects the streams, so tests can run a fully in-memory peer.
 */
export type StreamListener = (...args: unknown[]) => void;

/** The slice of a duplex/readable/writable stream this peer needs. */
export interface DuplexLike {
  /** Write one chunk; the peer appends the newline itself. */
  write(chunk: string): boolean;
  on(event: string, listener: StreamListener): unknown;
  off?(event: string, listener: StreamListener): unknown;
  once?(event: string, listener: StreamListener): unknown;
}

/** The slice of a spawned child process the driver needs. */
export interface ChildLike {
  readonly stdin: DuplexLike;
  readonly stdout: DuplexLike;
  readonly stderr: DuplexLike;
  kill(signal?: NodeJS.Signals): boolean;
  on(event: string, listener: StreamListener): unknown;
  off?(event: string, listener: StreamListener): unknown;
  once?(event: string, listener: StreamListener): unknown;
}

export type JsonRpcId = number | string;

/** A JSON-RPC error response body, preserving the wire `code` and `data`. */
export interface JsonRpcErrorObject {
  code?: number;
  message?: string;
  data?: unknown;
}

export interface JsonRpcRequestFrame {
  jsonrpc: "2.0";
  id: JsonRpcId;
  method: string;
  params?: unknown;
}

export interface JsonRpcNotificationFrame {
  jsonrpc: "2.0";
  method: string;
  params?: unknown;
}

export interface JsonRpcResponseFrame {
  jsonrpc: "2.0";
  id: JsonRpcId;
  result?: unknown;
  error?: JsonRpcErrorObject;
}

export type JsonRpcFrame = JsonRpcRequestFrame | JsonRpcNotificationFrame | JsonRpcResponseFrame;

/** Rejection for an error response; `code` and `data` are carried verbatim. */
export class JsonRpcResponseError extends Error {
  readonly code: number | undefined;
  readonly data: unknown;

  constructor(code: number | undefined, message: string, data?: unknown) {
    super(message);
    this.name = "JsonRpcResponseError";
    this.code = code;
    this.data = data;
  }
}

/** Rejection when a request outlives its timeout. */
export class JsonRpcTimeoutError extends Error {
  readonly method: string;
  readonly timeoutMs: number;

  constructor(method: string, timeoutMs: number) {
    super(`JSON-RPC request "${method}" timed out after ${timeoutMs}ms`);
    this.name = "JsonRpcTimeoutError";
    this.method = method;
    this.timeoutMs = timeoutMs;
  }
}

export type JsonRpcNotificationHandler = (method: string, params: unknown) => void;
export type JsonRpcRequestHandler = (method: string, params: unknown) => unknown | Promise<unknown>;

export interface JsonRpcPeerOptions {
  /** Stream the peer reads frames from (a child's stdout). */
  input: DuplexLike;
  /** Stream the peer writes frames to (a child's stdin). */
  output: DuplexLike;
  /** Default per-request timeout; `0` disables it. Defaults to 30 seconds. */
  requestTimeoutMs?: number;
}

/** Default request timeout; the DSH SDK server answers most calls promptly. */
export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

interface PendingRequest {
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: Error) => void;
  /** Clears the timeout and detaches the abort listener. */
  cleanup: () => void;
}

interface StreamBinding {
  readonly stream: DuplexLike;
  readonly event: string;
  readonly listener: StreamListener;
}

export interface JsonRpcRequestOptions {
  /** Override the peer's default timeout for this request; `0` disables it. */
  timeoutMs?: number;
  /** Abandon the request; it rejects and its pending entry is removed. */
  signal?: AbortSignal;
}

/**
 * One JSON-RPC endpoint. Construct it around an input/output stream pair; it
 * starts reading immediately and stops on `close()`, input end, input error or
 * a write failure. After that, pending and future requests reject with the
 * first failure.
 */
export class JsonRpcPeer {
  private readonly input: DuplexLike;
  private readonly output: DuplexLike;
  private readonly defaultTimeoutMs: number;
  private readonly pending = new Map<string, PendingRequest>();
  private readonly bindings: StreamBinding[] = [];
  private notificationHandler: JsonRpcNotificationHandler | undefined;
  private requestHandler: JsonRpcRequestHandler | undefined;
  private failure: Error | undefined;
  private closed = false;
  private buffer = "";
  private nextId = 1;

  constructor(options: JsonRpcPeerOptions) {
    this.input = options.input;
    this.output = options.output;
    this.defaultTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.bind("data", (chunk) => this.receive(chunk));
    this.bind("end", () => this.fail(new Error("JSON-RPC input ended")));
    this.bind("close", () => this.fail(new Error("JSON-RPC input closed")));
    this.bind("error", (error) => {
      this.fail(toError(error, "JSON-RPC input failed"));
    });
  }

  /** Replace the notification handler; notifications without one are dropped. */
  onNotification(handler: JsonRpcNotificationHandler | undefined): void {
    this.notificationHandler = handler;
  }

  /** Replace the server-request handler; requests without one get `-32601`. */
  onRequest(handler: JsonRpcRequestHandler | undefined): void {
    this.requestHandler = handler;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get pendingCount(): number {
    return this.pending.size;
  }

  /**
   * Send one request and await its response. Rejects with
   * {@link JsonRpcResponseError} for an error frame, {@link JsonRpcTimeoutError}
   * when it outlives its timeout, and a plain `Error` when the peer is closed.
   */
  request<T = unknown>(
    method: string,
    params?: unknown,
    options: JsonRpcRequestOptions = {},
  ): Promise<T> {
    if (this.failure !== undefined) return Promise.reject(this.failure);

    const id = this.nextId;
    this.nextId += 1;
    const timeoutMs = options.timeoutMs ?? this.defaultTimeoutMs;
    const key = String(id);
    const signal = options.signal;
    if (signal?.aborted) return Promise.reject(abortReason(signal));

    return new Promise<T>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;
      let onAbort: (() => void) | undefined;
      const cleanup = (): void => {
        if (timer !== undefined) {
          clearTimeout(timer);
          timer = undefined;
        }
        if (signal !== undefined && onAbort !== undefined) {
          signal.removeEventListener("abort", onAbort);
          onAbort = undefined;
        }
      };
      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          if (!this.pending.delete(key)) return;
          cleanup();
          reject(new JsonRpcTimeoutError(method, timeoutMs));
        }, timeoutMs);
        unrefTimer(timer);
      }
      if (signal !== undefined) {
        onAbort = () => {
          if (!this.pending.delete(key)) return;
          cleanup();
          reject(abortReason(signal));
        };
        signal.addEventListener("abort", onAbort, { once: true });
      }
      this.pending.set(key, {
        resolve: (value) => {
          resolve(value as T);
        },
        reject,
        cleanup,
      });
      try {
        this.send(frameForRequest(id, method, params));
      } catch (error) {
        this.pending.delete(key);
        cleanup();
        const failure = toError(error, `failed to write JSON-RPC request "${method}"`);
        this.fail(failure);
        reject(failure);
      }
    });
  }

  /** Send one notification (no response is expected). */
  notify(method: string, params?: unknown): void {
    if (this.failure !== undefined) throw this.failure;
    this.send(frameForNotification(method, params));
  }

  /**
   * Stop reading, reject pending requests and make future ones reject. Never
   * destroys the underlying streams: the caller owns them.
   */
  close(reason = "JSON-RPC peer closed"): void {
    if (this.closed) return;
    this.closed = true;
    if (this.failure === undefined) this.failure = new Error(reason);
    this.failPending(this.failure);
    this.detach();
  }

  private fail(error: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.failure = error;
    this.failPending(error);
    this.detach();
    this.buffer = "";
  }

  private failPending(error: Error): void {
    const pending = [...this.pending.values()];
    this.pending.clear();
    for (const request of pending) {
      request.cleanup();
      request.reject(error);
    }
  }

  private bind(event: string, listener: StreamListener): void {
    this.bindings.push({ stream: this.input, event, listener });
    this.input.on(event, listener);
  }

  private detach(): void {
    for (const binding of this.bindings.splice(0)) {
      binding.stream.off?.(binding.event, binding.listener);
    }
  }

  private send(frame: JsonRpcFrame): void {
    if (this.closed) throw this.failure ?? new Error("JSON-RPC peer is closed");
    this.output.write(`${JSON.stringify(frame)}\n`);
  }

  private receive(chunk: unknown): void {
    if (this.closed) return;
    this.buffer += chunkToText(chunk);
    let newline = this.buffer.indexOf("\n");
    while (newline !== -1) {
      const line = this.buffer.slice(0, newline).replace(/\r$/, "");
      this.buffer = this.buffer.slice(newline + 1);
      if (line.trim().length > 0) this.handleLine(line);
      newline = this.buffer.indexOf("\n");
    }
  }

  private handleLine(line: string): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      return; // Malformed lines are ignored, like the DSH transport does.
    }
    if (!isRecord(parsed)) return;

    const method = parsed.method;
    const id = parsed.id;
    if (typeof method === "string") {
      if (id === undefined || id === null) {
        this.notificationHandler?.(method, parsed.params);
      } else if (typeof id === "number" || typeof id === "string") {
        void this.answer(id, method, parsed.params);
      }
      return;
    }
    if (typeof id === "number" || typeof id === "string") this.settle(id, parsed);
  }

  private settle(id: JsonRpcId, frame: Record<string, unknown>): void {
    const pending = this.pending.get(String(id));
    if (pending === undefined) return; // Late or unknown responses are ignored.
    this.pending.delete(String(id));
    pending.cleanup();

    const error = frame.error;
    if (isRecord(error)) {
      pending.reject(
        new JsonRpcResponseError(
          typeof error.code === "number" ? error.code : undefined,
          typeof error.message === "string" ? error.message : "JSON-RPC error",
          error.data,
        ),
      );
      return;
    }
    pending.resolve(frame.result);
  }

  private async answer(id: JsonRpcId, method: string, params: unknown): Promise<void> {
    const handler = this.requestHandler;
    if (handler === undefined) {
      this.respondError(id, -32601, `Method not found: ${method}`);
      return;
    }
    try {
      const result = await handler(method, params);
      this.respondResult(id, result);
    } catch (error) {
      this.respondError(id, -32603, toError(error, "request handler failed").message);
    }
  }

  private respondResult(id: JsonRpcId, result: unknown): void {
    this.respond({ jsonrpc: "2.0", id, result: result === undefined ? null : result });
  }

  private respondError(id: JsonRpcId, code: number, message: string): void {
    this.respond({ jsonrpc: "2.0", id, error: { code, message } });
  }

  private respond(frame: JsonRpcResponseFrame): void {
    if (this.closed) return;
    try {
      this.send(frame);
    } catch {
      // The peer is going away; the request has no one left to answer.
    }
  }
}

function frameForRequest(id: number, method: string, params: unknown): JsonRpcRequestFrame {
  const frame: JsonRpcRequestFrame = { jsonrpc: "2.0", id, method };
  if (params !== undefined) frame.params = params;
  return frame;
}

function frameForNotification(method: string, params: unknown): JsonRpcNotificationFrame {
  const frame: JsonRpcNotificationFrame = { jsonrpc: "2.0", method };
  if (params !== undefined) frame.params = params;
  return frame;
}

/** Decode one stream chunk (string, Buffer or any Uint8Array) to text. */
export function chunkToText(chunk: unknown): string {
  if (typeof chunk === "string") return chunk;
  if (chunk instanceof Uint8Array) return new TextDecoder().decode(chunk);
  return String(chunk);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Rejection used when a request's AbortSignal fires. */
function abortReason(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  const error = new Error("JSON-RPC request aborted");
  error.name = "AbortError";
  return error;
}

function toError(error: unknown, fallback: string): Error {
  return error instanceof Error ? error : new Error(`${fallback}: ${String(error)}`);
}

function unrefTimer(timer: NodeJS.Timeout): void {
  const withUnref = timer as NodeJS.Timeout & { unref?: () => void };
  withUnref.unref?.();
}

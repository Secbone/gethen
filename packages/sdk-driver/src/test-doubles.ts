/**
 * In-memory stand-ins for a spawned `dsh --profile sdk` child, for tests.
 *
 * These are test doubles, not part of the public API: nothing in `index.ts`
 * exports them. They implement exactly the stream/child slice the driver needs
 * (`DuplexLike` / `ChildLike`) and record what the client writes so a test can
 * assert frames and answer them.
 */
import type { ChildLike, DuplexLike, StreamListener } from "./rpc";

/** An in-memory stream: records writes and lets a test emit events. */
export class FakeStream implements DuplexLike {
  readonly written: string[] = [];
  private readonly listeners = new Map<string, Set<StreamListener>>();

  write(chunk: string): boolean {
    this.written.push(chunk);
    return true;
  }

  on(event: string, listener: StreamListener): this {
    let bucket = this.listeners.get(event);
    if (bucket === undefined) {
      bucket = new Set();
      this.listeners.set(event, bucket);
    }
    bucket.add(listener);
    return this;
  }

  off(event: string, listener: StreamListener): void {
    this.listeners.get(event)?.delete(listener);
  }

  emit(event: string, ...args: unknown[]): void {
    const bucket = this.listeners.get(event);
    if (bucket === undefined) return;
    for (const listener of [...bucket]) listener(...args);
  }

  /** Number of listeners registered for one event (detach assertions). */
  listenerCount(event: string): number {
    return this.listeners.get(event)?.size ?? 0;
  }

  /** Every newline-framed JSON value written so far, in order. */
  frames(): Record<string, unknown>[] {
    const frames: Record<string, unknown>[] = [];
    for (const line of this.written.join("").split("\n")) {
      if (line.trim().length === 0) continue;
      frames.push(JSON.parse(line) as Record<string, unknown>);
    }
    return frames;
  }
}

/** A fake child process: the client's stdin/stdout/stderr plus lifecycle events. */
export class FakeChild implements ChildLike {
  readonly stdin = new FakeStream();
  readonly stdout = new FakeStream();
  readonly stderr = new FakeStream();
  /** Signals this child received, in order. */
  readonly signals: (NodeJS.Signals | undefined)[] = [];
  private readonly listeners = new Map<string, Set<StreamListener>>();

  on(event: string, listener: StreamListener): this {
    let bucket = this.listeners.get(event);
    if (bucket === undefined) {
      bucket = new Set();
      this.listeners.set(event, bucket);
    }
    bucket.add(listener);
    return this;
  }

  off(event: string, listener: StreamListener): void {
    this.listeners.get(event)?.delete(listener);
  }

  emit(event: string, ...args: unknown[]): void {
    const bucket = this.listeners.get(event);
    if (bucket === undefined) return;
    for (const listener of [...bucket]) listener(...args);
  }

  kill(signal?: NodeJS.Signals): boolean {
    this.signals.push(signal);
    return true;
  }

  /** Frames the client wrote to stdin, in order. */
  frames(): Record<string, unknown>[] {
    return this.stdin.frames();
  }

  /** The first frame matching a method (or an id when `method` is absent). */
  frame(method: string): Record<string, unknown> | undefined {
    return this.frames().find((frame) => frame.method === method);
  }

  frameById(id: unknown): Record<string, unknown> | undefined {
    return this.frames().find((frame) => frame.id === id && frame.method === undefined);
  }

  /** Write one frame from the "runtime" to the driver. */
  send(frame: unknown): void {
    this.stdout.emit("data", `${JSON.stringify(frame)}\n`);
  }

  /** Write raw bytes from the "runtime" to the driver. */
  sendRaw(text: string): void {
    this.stdout.emit("data", text);
  }

  notify(method: string, params: unknown): void {
    this.send({ jsonrpc: "2.0", method, params });
  }

  /** Send a server-to-client request from the runtime. */
  request(id: number, method: string, params: unknown): void {
    this.send({ jsonrpc: "2.0", id, method, params });
  }

  respond(id: unknown, result: unknown): void {
    this.send({ jsonrpc: "2.0", id, result });
  }

  respondError(id: unknown, code: number, message: string, data?: unknown): void {
    this.send({ jsonrpc: "2.0", id, error: { code, message, data } });
  }

  /** Emit the process exit (and the trailing close Node also emits). */
  exit(code: number | null = 0, signal: NodeJS.Signals | null = null): void {
    this.emit("exit", code, signal);
    this.emit("close", code, signal);
  }
}

/**
 * In-memory `SdkClientLike` for harness-adapter tests. Test-only: not exported
 * from the package entry point.
 */
import type {
  ApprovalOutcome,
  ApprovalRequestParams,
  InitializeOptions,
  InitializeResult,
  SessionEventNotification,
  SessionPromptResult,
  SessionStatusNotification,
  StartDshSdkOptions,
  UserQuestionRequest,
  UserQuestionResult,
  WaitOptions,
} from "@gethen/sdk-driver";
import type { SdkClientLike } from "./harness-adapter";

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function abortError(): Error {
  const error = new Error("wait aborted");
  error.name = "AbortError";
  return error;
}

export class FakeSdkClient implements SdkClientLike {
  running = false;
  readonly startCalls: StartDshSdkOptions[] = [];
  readonly initializeCalls: InitializeOptions[] = [];
  readonly prompts: { sessionId: string; text: string }[] = [];
  readonly waitCalls: string[] = [];
  readonly waitOptions: (WaitOptions | undefined)[] = [];
  /** Ordered side effects, for cancel/interrupt ordering assertions. */
  readonly actions: string[] = [];
  closeCount = 0;
  terminateCount = 0;
  failInitialize: Error | undefined;
  failPrompt: Error | undefined;

  private readonly eventHandlers = new Set<(notification: SessionEventNotification) => void>();
  private readonly statusHandlers = new Set<(notification: SessionStatusNotification) => void>();
  private readonly waits = new Map<string, Deferred<void>>();
  private approvalHandler:
    | ((params: ApprovalRequestParams) => ApprovalOutcome | Promise<ApprovalOutcome>)
    | undefined;
  private questionHandler:
    | ((request: UserQuestionRequest) => UserQuestionResult | Promise<UserQuestionResult>)
    | undefined;

  start(options: StartDshSdkOptions): this {
    this.startCalls.push(options);
    this.running = true;
    return this;
  }

  async initialize(options: InitializeOptions): Promise<InitializeResult> {
    this.initializeCalls.push(options);
    if (this.failInitialize !== undefined) throw this.failInitialize;
    return { serverInfo: { name: "fake-dsh-sdk", version: "test" } };
  }

  async prompt(sessionId: string, text: string): Promise<SessionPromptResult> {
    this.prompts.push({ sessionId, text });
    this.actions.push(`prompt:${text}`);
    if (this.failPrompt !== undefined) throw this.failPrompt;
    return { messageId: `msg-${this.prompts.length}` };
  }

  wait(sessionId: string, options: WaitOptions = {}): Promise<void> {
    this.waitCalls.push(sessionId);
    this.waitOptions.push(options);
    const waiting = deferred<void>();
    this.waits.set(sessionId, waiting);
    if (options.signal !== undefined) {
      if (options.signal.aborted) {
        waiting.reject(abortError());
        return waiting.promise;
      }
      options.signal.addEventListener(
        "abort",
        () => {
          if (this.waits.get(sessionId) !== waiting) return;
          this.waits.delete(sessionId);
          waiting.reject(abortError());
        },
        { once: true },
      );
    }
    return waiting.promise;
  }

  onEvent(listener: (notification: SessionEventNotification) => void): () => void {
    this.eventHandlers.add(listener);
    return () => {
      this.eventHandlers.delete(listener);
    };
  }

  onStatus(listener: (notification: SessionStatusNotification) => void): () => void {
    this.statusHandlers.add(listener);
    return () => {
      this.statusHandlers.delete(listener);
    };
  }

  onApprovalRequest(
    handler: (params: ApprovalRequestParams) => ApprovalOutcome | Promise<ApprovalOutcome>,
  ): void {
    this.approvalHandler = handler;
  }

  onUserQuestion(
    handler: (request: UserQuestionRequest) => UserQuestionResult | Promise<UserQuestionResult>,
  ): void {
    this.questionHandler = handler;
  }

  close(): void {
    this.closeCount += 1;
    this.actions.push("close");
    this.rejectWaits("fake client closed");
  }

  terminate(): void {
    this.terminateCount += 1;
    this.running = false;
    this.actions.push("terminate");
    this.rejectWaits("fake client terminated");
  }

  // ------------------------------------------------------------- test drivers

  emitEvent(notification: SessionEventNotification): void {
    for (const handler of [...this.eventHandlers]) handler(notification);
  }

  emitStatus(notification: SessionStatusNotification): void {
    for (const handler of [...this.statusHandlers]) handler(notification);
  }

  /** Emit an `assistant/message` event carrying one text block. */
  emitAssistantText(sessionId: string, text: string): void {
    this.emitEvent({
      sessionId,
      event: {
        type: "assistant/message",
        time: Date.parse("2026-11-17T00:00:00.000Z"),
        data: { message: { content: [{ type: "text", text }] } },
      },
    });
  }

  /** Emit a session event with a given type and data. */
  emitSessionEvent(sessionId: string, type: string, data: Record<string, unknown> = {}): void {
    this.emitEvent({
      sessionId,
      event: { type, time: Date.parse("2026-11-17T00:00:00.000Z"), data },
    });
  }

  /** Invoke the registered approval handler, as DSH does for `approval/request`. */
  async askApproval(params: ApprovalRequestParams): Promise<ApprovalOutcome> {
    if (this.approvalHandler === undefined) return "cancelled";
    return this.approvalHandler(params);
  }

  /** Invoke the registered question handler, as DSH does for `user-question/ask`. */
  async askQuestion(request: UserQuestionRequest): Promise<UserQuestionResult> {
    if (this.questionHandler === undefined) return { cancelled: true };
    return this.questionHandler(request);
  }

  resolveWait(sessionId: string): void {
    this.waits.get(sessionId)?.resolve(undefined);
  }

  rejectWait(sessionId: string, error: unknown): void {
    this.waits.get(sessionId)?.reject(error);
  }

  private rejectWaits(reason: string): void {
    for (const waiting of this.waits.values()) waiting.reject(new Error(reason));
    this.waits.clear();
  }
}

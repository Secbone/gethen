/**
 * In-memory `SdkClientLike` for executor tests. Not part of the public API:
 * nothing in `index.ts` exports it, and the executor only ever needs the
 * structural subset it implements.
 */
import type {
  ApprovalOutcome,
  ApprovalRequestParams,
  InitializeOptions,
  InitializeResult,
  SessionEventNotification,
  SessionPromptResult,
  StartDshSdkOptions,
  UserQuestionRequest,
  UserQuestionResult,
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

export class FakeSdkClient implements SdkClientLike {
  running = false;
  readonly startCalls: StartDshSdkOptions[] = [];
  readonly initializeCalls: InitializeOptions[] = [];
  readonly prompts: { sessionId: string; text: string }[] = [];
  readonly waitCalls: string[] = [];
  closeCount = 0;
  failInitialize: Error | undefined;
  failPrompt: Error | undefined;

  private readonly eventHandlers = new Set<(notification: SessionEventNotification) => void>();
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
    if (this.failPrompt !== undefined) throw this.failPrompt;
    return { messageId: `msg-${this.prompts.length}` };
  }

  wait(sessionId: string): Promise<void> {
    this.waitCalls.push(sessionId);
    const waiting = deferred<void>();
    this.waits.set(sessionId, waiting);
    return waiting.promise;
  }

  onEvent(listener: (notification: SessionEventNotification) => void): () => void {
    this.eventHandlers.add(listener);
    return () => {
      this.eventHandlers.delete(listener);
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
    for (const waiting of this.waits.values()) waiting.reject(new Error("fake client closed"));
    this.waits.clear();
  }

  // ------------------------------------------------------------- test drivers

  /** Emit one `session.event` notification as the runtime would. */
  emitEvent(notification: SessionEventNotification): void {
    for (const handler of [...this.eventHandlers]) handler(notification);
  }

  /** Emit an `assistant/message` event carrying one text block. */
  emitAssistantText(sessionId: string, text: string): void {
    this.emitEvent({
      sessionId,
      event: {
        type: "assistant/message",
        data: { message: { content: [{ type: "text", text }] } },
      },
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
}

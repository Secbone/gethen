import type {
  AssignmentReceipt,
  DispatchRequest,
  SessionId,
  TranscriptPage,
  WorkerManifest,
  WorkerSessionRef,
  WorkerState,
} from "./types";

/**
 * The only transport contract the Master knows. A Remote adapter drives a
 * running DSH host; a later stdio adapter drives a per-task subprocess.
 */
export interface WorkerBridge {
  /** Stable adapter id, e.g. `remote` or `stdio`. */
  readonly id: string;
  /** Workers currently selectable, with health and load. */
  list(): Promise<WorkerManifest[]>;
  /** Open a Worker session for one Assignment. */
  open(request: DispatchRequest): Promise<WorkerSessionRef>;
  /** Queue or steer input; `unknown` means the outcome must be inspected. */
  send(session: SessionId, text: string, operationId: string): Promise<"accepted" | "unknown">;
  /** Current execution state, used to resume a `waiting` Assignment. */
  state(session: SessionId): Promise<WorkerState>;
  /** Bounded read of the Worker transcript, newest page first. */
  read(
    session: SessionId,
    cursor?: { beforeSeq: number; throughSeq: number },
  ): Promise<TranscriptPage>;
  /** Request interruption; does not undo edits or delete queued input. */
  interrupt(session: SessionId): Promise<void>;
  /** Evidence for the Assignment, recorded even when it failed. */
  receipt(session: SessionId): Promise<AssignmentReceipt>;
}

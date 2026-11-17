import type {
  Assignment,
  AssignmentId,
  AssignmentReceipt,
  Decision,
  Eval,
  Input,
  Mandate,
  Policy,
  Report,
  Thread,
  Ticket,
  Trigger,
  WorkerRecord,
} from "./model";

export interface Clock {
  now(): Date;
}

export interface Ids {
  next(prefix: string): string;
}

/** A persisted entity collection; the engine never knows how it is stored. */
export interface Repo<T> {
  get(id: string): T | undefined;
  save(value: T): void;
  list(): T[];
}

export interface Repositories {
  threads: Repo<Thread>;
  triggers: Repo<Trigger>;
  evals: Repo<Eval>;
  decisions: Repo<Decision>;
  tickets: Repo<Ticket>;
  mandates: Repo<Mandate>;
  assignments: Repo<Assignment>;
  inputs: Repo<Input>;
  policy: PolicyPort;
}

export interface PolicyPort {
  current(): Policy;
}

/**
 * Persisted Worker identities. Token hashing stays in the transport; the master
 * receives opaque hashes and never sees the plaintext token.
 */
export interface WorkerRegistry {
  saveWorker(record: WorkerRecord): void;
  getWorker(id: string): WorkerRecord | undefined;
  listWorkers(): WorkerRecord[];
  findWorkerByTokenHash(hash: string): WorkerRecord | undefined;
  touchWorkerSeen(id: string, at: string): void;
  revokeWorker(id: string, at: string): void;
}

/** A stream of events; implementations own polling, translation and read positions. */
export interface SourcePort {
  readonly id: string;
  readonly label: string;
  poll(): Promise<Trigger[]>;
}

/** An effect the engine wants persisted in the same transaction as the state. */
export interface PendingEffect {
  kind: string;
  key: string;
  payload: unknown;
  dueAt: string;
}

export interface ReceiptSink {
  append(receipt: AssignmentReceipt): void;
}

/**
 * Code a `LaunchPort` rejects with when the Worker fell silent. The task was
 * dispatched, so retrying would start a second Worker on the same Assignment;
 * a later result for it is applied when it arrives.
 */
export const LAUNCH_TIMEOUT_CODE = "MAILBOX_LAUNCH_TIMEOUT";

/** True when a launch rejection only means "stop waiting", not "retry". */
export function isLaunchTimeout(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === LAUNCH_TIMEOUT_CODE
  );
}

export interface LaunchRequest {
  assignment: Assignment;
  ticket: Ticket;
  packDigest: string;
  prompt: string;
}

export interface LaunchResult {
  report: Report;
  receipt: AssignmentReceipt;
  /** Escalations seen during the launch, in order (P3b). */
  escalations?: import("./model").EscalationRecord[];
}

/**
 * P1 uses FakeLaunch; P3 wires this to WorkerBridge over the Remote channel.
 * The engine only sees the result, never the transport.
 */
export interface LaunchPort {
  launch(request: LaunchRequest): Promise<LaunchResult>;
}

export type { AssignmentId };

export type HttpMethod = "GET" | "POST" | "PATCH" | "PUT";

export interface HttpRequest {
  url: string;
  method?: HttpMethod;
  headers?: Record<string, string>;
  body?: string;
}

export interface HttpResponse {
  status: number;
  headers: Record<string, string>;
  body: string;
}

/** Injected HTTP transport; sources never call fetch directly, tests inject fakes. */
export interface HttpPort {
  send(request: HttpRequest): Promise<HttpResponse>;
}

/** Durable key-value state for a source's read positions. */
export interface SourceState {
  get<T>(key: string): T | undefined;
  set(key: string, value: unknown): void;
}

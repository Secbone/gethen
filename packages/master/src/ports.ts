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

export interface LaunchRequest {
  assignment: Assignment;
  ticket: Ticket;
  packDigest: string;
  prompt: string;
}

export interface LaunchResult {
  report: Report;
  receipt: AssignmentReceipt;
}

/**
 * P1 uses FakeLaunch; P3 wires this to WorkerBridge over the Remote channel.
 * The engine only sees the result, never the transport.
 */
export interface LaunchPort {
  launch(request: LaunchRequest): Promise<LaunchResult>;
}

export type { AssignmentId };

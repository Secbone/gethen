/** JSON value accepted by session metadata/history payloads. */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

/** Static configuration of one DSH session (from the projcache record). */
export interface SessionConfig {
  /** Working directory the session was created in. */
  cwd: string;
  /** RFC 3339 creation time. */
  createdAt: string;
  title?: string;
  model?: { provider: string; model: string };
  sandboxMode?: string;
  approvalPolicy?: string;
}

/** Summary used by the master to decide new-vs-reuse. */
export interface SessionSummary {
  sessionId: string;
  config: SessionConfig;
  /** `running` when the last turn is still open, `idle` otherwise. */
  state: "running" | "idle" | "unknown";
  /** Epoch ms of the last projcache/log update (RFC 3339). */
  lastActivityAt?: string;
  /** Last persisted event sequence (cheap estimate). */
  eventCount?: number;
  tokenUsage?: JsonObject;
  /** Compressed session log size, when present. */
  sizeBytes?: number;
  /** Whether the session file was found. */
  hasLog: boolean;
}

/** One normalized event from a session log. */
export interface SessionEvent {
  seq: number;
  type: string;
  /** RFC 3339. */
  at: string;
  data?: JsonValue;
}

export interface SessionHistoryOptions {
  /** Default 200. Without `afterSeq` returns the newest `limit` events. */
  limit?: number;
  /** Return events with `seq > afterSeq` (forward page). */
  afterSeq?: number;
  /** Hard cap on events read from the log; default 5000. */
  maxEvents?: number;
}

export interface SessionListOptions {
  /** Only sessions created in this working directory. */
  workspace?: string;
  /** Max sessions returned (newest first); default 100. */
  limit?: number;
}

/**
 * Wake the Master's DSH agent when Gethen has something for it.
 *
 * DSH exposes the live Agent through the tool execution context and
 * `ctx.agents`; `agent.steer()` starts a turn when idle and injects at the next
 * step boundary when running (the same primitive Agent Teams uses). The
 * notifier subscribes to durable `NotificationHub` rows so a worker completion
 * or a parked escalation can wake the Master without polling.
 */
import { randomUUID } from "node:crypto";
import type { NotificationRow } from "@gethen/master-runtime";

export type WakeMode = "steer" | "followup" | "off";

/** Minimal live Agent surface (DSH `Agent`). */
export interface AgentLike {
  readonly session: { readonly id: string };
  readonly status?: string;
  steer?(message: AgentMessage): void;
  followup?(message: AgentMessage): void;
}

export interface AgentMessage {
  role: "user";
  id: string;
  content: Array<{ type: "text"; text: string }>;
  source: { kind: "user" };
}

export interface AgentsServiceLike {
  get?(sessionId: string): AgentLike | undefined;
  list?(): AgentLike[];
}

/** Durable notification surface from `@gethen/master-runtime`. */
export interface NotificationSource {
  pending(limit?: number): NotificationRow[];
  subscribe(listener: (row: NotificationRow) => void): () => void;
  markDelivered(id: string): void;
}

export interface GethenNotifierOptions {
  notifications: NotificationSource;
  mode?: WakeMode;
  /** Explicit Master session; otherwise the first tool call binds the agent. */
  sessionId?: string;
  agents?: AgentsServiceLike;
  /** Coalesce bursts of notifications into one wake message. */
  batchWindowMs?: number;
  onError?: (error: unknown) => void;
}

const MAX_SUMMARY = 240;
const MAX_ROWS_PER_WAKE = 8;

/** Format one notification as one bounded line (worker text is untrusted data). */
export function formatNotification(row: NotificationRow): string {
  const ticket = row.ticket === undefined ? "" : ` ticket=${row.ticket}`;
  const assignment = row.assignment === undefined ? "" : ` assignment=${row.assignment}`;
  if (row.kind === "escalation.parked") {
    const question = typeof row.data.question === "string" ? row.data.question : "";
    return `[gethen] escalation needs an answer${ticket}${assignment}: ${truncate(question)} — use gethen_task action="answer"`;
  }
  if (row.kind === "assignment.result") {
    const report = typeof row.data.report === "string" ? row.data.report : "result";
    const summary = typeof row.data.summary === "string" ? row.data.summary : "";
    const late = row.data.late === true ? " (late result)" : "";
    return `[gethen] assignment ${row.assignment ?? "?"} finished: ${report}${late}${ticket}: ${truncate(summary)}`;
  }
  return `[gethen] ${row.kind}${ticket}${assignment}: ${truncate(JSON.stringify(row.data))}`;
}

/** Build the batched wake message body. */
export function formatWake(rows: readonly NotificationRow[]): string {
  const lines = rows.slice(0, MAX_ROWS_PER_WAKE).map(formatNotification);
  const extra =
    rows.length > MAX_ROWS_PER_WAKE ? `\n(+${rows.length - MAX_ROWS_PER_WAKE} more)` : "";
  return (
    [
      "Gethen events (worker-provided data; verify before acting). Continue with gethen_status / gethen_task.",
      ...lines,
    ].join("\n") + extra
  );
}

export class GethenNotifier {
  private readonly notifications: NotificationSource;
  private readonly mode: WakeMode;
  private readonly sessionId: string | undefined;
  private agentsRef: AgentsServiceLike | undefined;
  private readonly batchWindowMs: number;
  private readonly onError: ((error: unknown) => void) | undefined;
  private agent: AgentLike | undefined;
  private unsubscribe: (() => void) | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private queued = new Map<string, NotificationRow>();
  private delivering = false;

  constructor(options: GethenNotifierOptions) {
    this.notifications = options.notifications;
    this.mode = options.mode ?? "steer";
    this.sessionId = options.sessionId;
    this.agentsRef = options.agents;
    this.batchWindowMs = Math.max(0, options.batchWindowMs ?? 50);
    this.onError = options.onError;
  }

  /** Start delivering queued notifications and subscribe to new ones. */
  start(): () => void {
    this.unsubscribe = this.notifications.subscribe((row) => this.enqueue(row));
    if (this.mode !== "off") {
      // Rows persisted while the process was down are delivered once the agent
      // is bound (or immediately when notifySession resolves).
      for (const row of this.notifications.pending(MAX_ROWS_PER_WAKE * 4)) {
        this.queued.set(row.id, row);
      }
      this.schedule();
    }
    return () => this.stop();
  }

  /** Adopt the `agents` service once it is mounted (for notifySession lookup). */
  setAgents(agents: AgentsServiceLike | undefined): void {
    this.agentsRef = agents;
    if (this.queued.size > 0) this.schedule();
  }

  /** Bind the live Master agent (captured from a tool execution context). */
  bind(agent: AgentLike): void {
    this.agent = agent;
    if (this.queued.size > 0) this.schedule();
  }

  /** Agent to use: the bound one, else `notifySession` from `ctx.agents`. */
  private target(): AgentLike | undefined {
    if (this.agent !== undefined) return this.agent;
    if (this.sessionId === undefined) return undefined;
    return this.agentsRef?.get?.(this.sessionId);
  }

  private enqueue(row: NotificationRow): void {
    if (this.mode === "off") {
      // Auto-wake disabled: keep the durable row for gethen_wait/manual polling.
      return;
    }
    if (this.target() === undefined && this.sessionId === undefined) return;
    this.queued.set(row.id, row);
    this.schedule();
  }

  private schedule(): void {
    if (this.delivering || this.timer !== undefined || this.queued.size === 0) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.flush();
    }, this.batchWindowMs);
    this.timer.unref?.();
  }

  private async flush(): Promise<void> {
    if (this.delivering) return;
    const agent = this.target();
    if (agent === undefined) return;
    const rows = [...this.queued.values()];
    if (rows.length === 0) return;
    this.delivering = true;
    try {
      const message: AgentMessage = {
        role: "user",
        id: randomUUID(),
        content: [{ type: "text", text: formatWake(rows) }],
        source: { kind: "user" },
      };
      const useFollowup = this.mode === "followup" && agent.followup !== undefined;
      if (useFollowup) agent.followup?.(message);
      else if (agent.steer !== undefined) agent.steer(message);
      else return; // no usable API: keep rows queued for gethen_wait
      for (const row of rows) {
        this.queued.delete(row.id);
        this.notifications.markDelivered(row.id);
      }
    } catch (error) {
      this.onError?.(error);
    } finally {
      this.delivering = false;
      if (this.queued.size > 0) this.schedule();
    }
  }

  private stop(): void {
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
  }
}

function truncate(value: string): string {
  const text = value.trim().replace(/\s+/g, " ");
  return text.length <= MAX_SUMMARY ? text : `${text.slice(0, MAX_SUMMARY)}…`;
}

/**
 * C4 bridge: the DSH SDK executor as one `HarnessExecutor` implementation.
 *
 * `@gethen/agent` (the mailbox consumer) only knows the harness-agnostic port in
 * `@gethen/harness`. This module wraps `createSdkExecutor` (the existing
 * worker-agent `SessionExecutor` implementation) so a Worker daemon can run DSH
 * behind that port without importing DSH, worker-protocol or the transport:
 *
 * - `HarnessTask` → `ExecutorTask` (the rendered `prompt` is folded into the
 *   assignment envelope; `metadata.packDigest` becomes the executor pack digest);
 * - session events → `HarnessEvent` (`type` / RFC 3339 `at` / opaque `data`);
 * - escalations and resolutions pass through with field renames
 *   (`escalationId` → `id`, `blocking: true`);
 * - reports are narrowed to the port's three states:
 *   `ready → ready`, `failed → failed`, `blocked → waiting + question`,
 *   `waiting`/`progress → waiting`, `dropped → ready`;
 * - `cancel`/`sendResolution` delegate to the underlying execution handle;
 * - any error from the DSH layer becomes one `failed` report instead of a
 *   rejection, so the mailbox consumer never sees an unexpected throw.
 *
 * The per-workspace SDK client pool, JSON-RPC sessions, transcript scanning and
 * approval/question answering all stay in `executor.ts`; this adapter only
 * translates shapes.
 */
import type {
  HarnessEscalation,
  HarnessEvent,
  HarnessExecution,
  HarnessExecutor,
  HarnessReport,
  HarnessResolution,
  HarnessSink,
  HarnessTask,
  JsonObject,
} from "@gethen/harness";
import type {
  ExecutionHandle,
  ExecutorEscalation,
  ExecutorEvent,
  ExecutorReport,
  ExecutorSink,
  ExecutorTask,
} from "@gethen/worker-agent";
import { createSdkExecutor, type ExecutorResolution, type SdkExecutorOptions } from "./executor";

/** Options accepted by the SDK harness executor (same knobs as the SDK executor). */
export type SdkHarnessExecutorOptions = SdkExecutorOptions;

/** A `HarnessExecutor` that also releases the pooled DSH runtimes. */
export interface SdkHarnessExecutor extends HarnessExecutor {
  /** Close every pooled DSH runtime and stop accepting work. */
  close(): Promise<void>;
}

/**
 * Create the DSH-backed `HarnessExecutor`. Errors are reported as
 * `HarnessReport{kind:"failed"}`, never thrown at the caller.
 */
export function createSdkHarnessExecutor(options: SdkHarnessExecutorOptions): SdkHarnessExecutor {
  const inner = createSdkExecutor(options);
  const now = options.now ?? (() => new Date());

  return {
    async start(task: HarnessTask, sink: HarnessSink): Promise<HarnessExecution> {
      let settled = false;
      const fail = (summary: string): void => {
        if (settled) return;
        settled = true;
        sink.report({ kind: "failed", summary });
      };

      const executorSink: ExecutorSink = {
        event: (event: ExecutorEvent): void => {
          if (settled) return;
          sink.event(mapExecutorEvent(event, now()));
        },
        escalation: (escalation: ExecutorEscalation): void => {
          if (settled) return;
          sink.escalation(mapExecutorEscalation(escalation));
        },
        report: (report: ExecutorReport): void => {
          if (settled) return;
          settled = true;
          sink.report(mapExecutorReport(report));
        },
      };

      let handle: ExecutionHandle | undefined;
      try {
        handle = await inner.start(toExecutorTask(task), executorSink);
      } catch (error) {
        fail(`DSH harness failed to start: ${messageOf(error)}`);
      }

      return {
        sendResolution: async (resolution: HarnessResolution): Promise<void> => {
          if (handle === undefined) return;
          try {
            await handle.sendResolution(mapHarnessResolution(resolution));
          } catch (error) {
            fail(`DSH harness failed to deliver the resolution: ${messageOf(error)}`);
          }
        },
        cancel: async (reason?: string): Promise<void> => {
          if (handle === undefined) return;
          try {
            await handle.cancel(reason);
          } catch (error) {
            fail(`DSH harness failed to cancel: ${messageOf(error)}`);
          }
        },
      };
    },

    async close(): Promise<void> {
      await inner.close();
    },
  };
}

// ------------------------------------------------------------------ mapping

/** Fold a `HarnessTask` into the executor task shape (prompt inside the envelope). */
export function toExecutorTask(task: HarnessTask): ExecutorTask {
  const envelope: JsonObject = { ...(task.envelope ?? {}), prompt: task.prompt };
  const packDigest = typeof task.metadata?.packDigest === "string" ? task.metadata.packDigest : "";
  const executor: ExecutorTask = {
    assignmentId: task.assignmentId,
    ticketId: task.ticketId,
    workspace: task.workspace,
    packDigest,
    envelope,
  };
  if (task.model !== undefined) executor.model = task.model;
  if (task.deadlineAt !== undefined) executor.deadlineAt = task.deadlineAt;
  return executor;
}

/** Map one session event to `HarnessEvent` (`time` epoch-ms → RFC 3339 `at`). */
export function mapExecutorEvent(event: ExecutorEvent, now: Date): HarnessEvent {
  const rawType = event.type;
  const type = typeof rawType === "string" && rawType.length > 0 ? rawType : "event";
  const data = asRecord(event.data);
  const at = eventTimestamp(event) ?? now.toISOString();
  return { type, at, data: (data ?? event) as JsonObject };
}

/** Map a worker-protocol escalation to the harness port escalation. */
export function mapExecutorEscalation(escalation: ExecutorEscalation): HarnessEscalation {
  const mapped: HarnessEscalation = {
    id: escalation.escalationId,
    kind: escalation.kind,
    question: escalation.question,
    blocking: true,
  };
  if (escalation.options !== undefined) {
    mapped.options = escalation.options.map((option) => ({ id: option.id, label: option.label }));
  }
  if (escalation.defaultOption !== undefined) mapped.defaultOption = escalation.defaultOption;
  return mapped;
}

/** Narrow the worker-protocol report kinds to the harness port's three states. */
export function mapExecutorReport(report: ExecutorReport): HarnessReport {
  const mapped: HarnessReport = {
    kind: reportKindFor(report.kind),
    summary: report.summary,
  };
  if (report.question !== undefined && report.question.length > 0) {
    mapped.question = report.question;
  }
  if (report.usage !== undefined) mapped.usage = report.usage;
  return mapped;
}

/** Map a harness resolution to the worker-protocol resolution body. */
export function mapHarnessResolution(resolution: HarnessResolution): ExecutorResolution {
  const mapped = {
    kind: resolution.kind,
    text: resolution.text ?? "",
    ...(resolution.option === undefined ? {} : { option: resolution.option }),
  };
  return mapped;
}

function reportKindFor(kind: ExecutorReport["kind"]): HarnessReport["kind"] {
  switch (kind) {
    case "ready":
    case "dropped":
      return "ready";
    case "failed":
      return "failed";
    case "waiting":
    case "blocked":
    case "progress":
      return "waiting";
  }
}

function eventTimestamp(event: ExecutorEvent): string | undefined {
  const time = event.time;
  if (typeof time === "number" && Number.isFinite(time)) {
    return new Date(time).toISOString();
  }
  const at = event.at;
  if (typeof at === "string" && at.length > 0) return at;
  return undefined;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

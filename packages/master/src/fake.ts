import type { AssignmentReceipt, Report } from "./model";
import type { LaunchPort, LaunchRequest } from "./ports";

export interface FakeLaunchOptions {
  report?: Report;
  fail?: Error;
  events?: number;
}

/**
 * Deterministic P1 worker: reports a fixed result and writes a receipt. P3
 * replaces it with a LaunchPort backed by WorkerBridge.
 */
export function createFakeLaunch(options: FakeLaunchOptions = {}): LaunchPort {
  return {
    async launch(request: LaunchRequest) {
      if (options.fail !== undefined) throw options.fail;
      const at = new Date().toISOString();
      const report: Report = options.report ?? {
        kind: "ready",
        summary: `fake worker finished ${request.assignment.id}`,
        at,
      };
      const receipt: AssignmentReceipt = {
        assignment: request.assignment.id,
        ticket: request.ticket.id,
        worker: request.assignment.worker,
        workspace: request.assignment.workspace,
        packDigest: request.packDigest,
        at,
        outcome: report.kind === "failed" ? "failed" : "completed",
        summary: report.summary,
        events: options.events ?? 1,
      };
      return { report, receipt };
    },
  };
}

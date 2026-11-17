/**
 * Lifecycle control ops the Master may later dispatch to a Worker.
 *
 * Fleet owns this vocabulary now: the former worker-protocol frame package is
 * gone. Validation/policy (runtime allowlist, managed roots) belongs with the
 * dispatcher that executes these ops.
 */
export type ControlOp =
  | { op: "snapshot" }
  | { op: "runtime.start" | "runtime.stop" | "runtime.restart"; name: string; drain?: boolean }
  | { op: "workspace.prepare"; path: string; repo?: string; branch?: string; worktree?: boolean }
  | { op: "workspace.cleanup"; path: string }
  | {
      op: "slot.create";
      workspace: string;
      pack: string;
      kind: "resident" | "ephemeral";
      ttlMs?: number;
    }
  | { op: "slot.close"; slot: string; force?: boolean }
  | { op: "slot.drain"; slot: string }
  | { op: "quota.set"; slots: number; models?: string[] };

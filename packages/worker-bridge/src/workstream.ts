/**
 * Workstream workspace contract shared by the Master and its Workers.
 *
 * A dispatch names a logical *workstream*, never a Master filesystem path.
 * The Worker is the only party that owns and materializes a workspace (a git
 * worktree or clone under its own root); the Master only states the goal and
 * the acceptance criteria. Delivery is always a `feat/<workstream>` branch
 * pushed to the Worker's configured remote.
 *
 * This module is pure (no Node built-ins) so both the control plane and the
 * Worker-side adapter can validate the same names.
 */

/** Prefix of every delivery branch; `ws-collab` is delivered on `feat/ws-collab`. */
export const WORKSTREAM_BRANCH_PREFIX = "feat/";

/** Workstream ids double as branch components, so keep them filesystem/branch safe. */
const WORKSTREAM_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

/** True when a value looks like a filesystem path rather than a logical id. */
export function isFilesystemPath(value: string): boolean {
  return (
    value.startsWith("/") ||
    value.startsWith("./") ||
    value.startsWith("../") ||
    /^[A-Za-z]:[\\/]/.test(value)
  );
}

/** The branch a workstream must be delivered on. */
export function workstreamBranch(id: string): string {
  return `${WORKSTREAM_BRANCH_PREFIX}${id}`;
}

/** True for a logical workstream id that can safely become a branch and a dir name. */
export function isWorkstreamId(value: string): boolean {
  return WORKSTREAM_ID.test(value) && !value.includes("..");
}

/**
 * Validate a workstream id. Throws so a bad dispatch fails before any Worker
 * directory is touched; the message names the offending value.
 */
export function assertWorkstreamId(value: string): void {
  if (!isWorkstreamId(value)) {
    throw new Error(
      `workstream id "${value}" is invalid: use letters, digits, ".", "_" or "-" ` +
        "and do not start with a separator",
    );
  }
}

/**
 * What the Master expects from a delivery. The Worker owns repo/root; these
 * are the parts the Master can state without knowing the Worker's filesystem.
 */
export interface WorkstreamContract {
  /** Logical workstream id. */
  id: string;
  /** Branch the Worker must push; defaults to `feat/<id>`. */
  branch: string;
  /** Delivery transport; the only supported mode is a pushed branch. */
  delivery: "push";
}

/** Build the Master-side contract for a logical workstream id. */
export function workstreamContract(id: string, branch = workstreamBranch(id)): WorkstreamContract {
  assertWorkstreamId(id);
  return { id, branch, delivery: "push" };
}

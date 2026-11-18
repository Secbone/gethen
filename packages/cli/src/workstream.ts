/**
 * Worker-owned workstream workspaces.
 *
 * The Master only sends a logical workstream id and the acceptance criteria.
 * This module turns that id into an isolated git worktree (or clone) under a
 * Worker-owned root, and enforces the delivery contract: every change is
 * committed on `feat/<workstream>` and pushed to the Worker's configured
 * remote. The Master's repository path is never used.
 */
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { assertWorkstreamId, isFilesystemPath, workstreamBranch } from "@gethen/worker-bridge";

export type WorkstreamIsolation = "worktree" | "clone";

/** One Worker-owned workstream declaration from `worker.json`. */
export interface WorkstreamConfig {
  /** Logical id the Master dispatches; never a filesystem path. */
  id: string;
  /** Worker-owned source repository (a clone on this machine). */
  repo: string;
  /** Optional Worker-owned parent directory; defaults to `<workspaceRoot>/<id>`. */
  root?: string;
  /** Base ref the delivery branch starts from; defaults to the repo's default branch. */
  base?: string;
  /** Delivery branch; must follow `feat/<id>` when present. */
  branch?: string;
  /** Remote the delivery branch is pushed to; defaults to `origin`. */
  remote?: string;
  /** Isolation kind; defaults to an isolated git worktree. */
  isolation?: WorkstreamIsolation;
}

/** Worker-side settings needed to materialize a workstream. */
export interface WorkstreamRuntime {
  workerId: string;
  /** Parent directory for all of this Worker's workstreams. */
  workspaceRoot: string;
}

/** A materialized, Worker-owned workspace. */
export interface PreparedWorkstream {
  id: string;
  branch: string;
  repo: string;
  path: string;
  remote: string;
  isolation: WorkstreamIsolation;
  /** True when this call created the worktree/clone. */
  created: boolean;
}

/** The delivery evidence a Worker attaches to a `ready` report. */
export interface DeliveryResult {
  workstream: string;
  branch: string;
  commit: string;
  remote: string;
  pushed: boolean;
  /** True when uncommitted changes had to be committed by the verifier. */
  committed: boolean;
}

export type WorkstreamErrorCode =
  | "WORKSTREAM_CONFIG"
  | "WORKSTREAM_UNKNOWN"
  | "WORKSTREAM_NOT_OWNED"
  | "WORKSTREAM_GIT"
  | "WORKSTREAM_DELIVERY";

export class WorkstreamError extends Error {
  readonly code: WorkstreamErrorCode;

  constructor(code: WorkstreamErrorCode, message: string) {
    super(`${code}: ${message}`);
    this.name = "WorkstreamError";
    this.code = code;
  }
}

/** Default root for one Worker's worktrees, outside any source repository. */
export function defaultWorkspaceRoot(stateDir: string | undefined, workerId: string): string {
  return join(stateDir ?? join(homedir(), ".gethen", "worker"), "workstreams", workerId);
}

/** Validate the `workstreams` section of a worker config file. */
export function parseWorkstreams(value: unknown): WorkstreamConfig[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value))
    throw new WorkstreamError("WORKSTREAM_CONFIG", "workstreams must be an array");
  const seen = new Set<string>();
  return value.map((entry, index) => {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      throw new WorkstreamError("WORKSTREAM_CONFIG", `workstreams[${index}] must be an object`);
    }
    const record = entry as Record<string, unknown>;
    if (typeof record.id !== "string" || record.id.length === 0) {
      throw new WorkstreamError("WORKSTREAM_CONFIG", `workstreams[${index}].id must be a string`);
    }
    try {
      assertWorkstreamId(record.id);
    } catch (error) {
      throw new WorkstreamError("WORKSTREAM_CONFIG", messageOf(error));
    }
    if (seen.has(record.id)) {
      throw new WorkstreamError("WORKSTREAM_CONFIG", `duplicate workstream id: ${record.id}`);
    }
    seen.add(record.id);
    if (typeof record.repo !== "string" || !isAbsolute(record.repo)) {
      throw new WorkstreamError(
        "WORKSTREAM_CONFIG",
        `workstreams[${index}].repo must be an absolute path`,
      );
    }
    for (const field of ["root", "base", "branch", "remote"] as const) {
      if (record[field] !== undefined && typeof record[field] !== "string") {
        throw new WorkstreamError(
          "WORKSTREAM_CONFIG",
          `workstreams[${index}].${field} must be a string`,
        );
      }
    }
    const isolation = record.isolation ?? "worktree";
    if (isolation !== "worktree" && isolation !== "clone") {
      throw new WorkstreamError(
        "WORKSTREAM_CONFIG",
        `workstreams[${index}].isolation must be "worktree" or "clone"`,
      );
    }
    const branch = record.branch ?? workstreamBranch(record.id);
    if (branch !== workstreamBranch(record.id)) {
      throw new WorkstreamError(
        "WORKSTREAM_CONFIG",
        `workstreams[${index}].branch must be "${workstreamBranch(record.id)}"`,
      );
    }
    if (typeof record.root === "string") {
      if (!isAbsolute(record.root)) {
        throw new WorkstreamError(
          "WORKSTREAM_CONFIG",
          `workstreams[${index}].root must be an absolute path`,
        );
      }
    }
    return {
      id: record.id,
      repo: record.repo,
      ...(typeof record.root === "string" ? { root: record.root } : {}),
      ...(typeof record.base === "string" && record.base.length > 0 ? { base: record.base } : {}),
      branch,
      ...(typeof record.remote === "string" && record.remote.length > 0
        ? { remote: record.remote }
        : {}),
      isolation,
    };
  });
}

/** True when `child` is `parent` or below it. */
function within(parent: string, child: string): boolean {
  const rel = relative(resolve(parent), resolve(child));
  return rel === "" || (!rel.startsWith(`..${sep}`) && rel !== ".." && !isAbsolute(rel));
}

function git(cwd: string, args: string[], timeout = 120_000): string {
  try {
    return execFileSync("git", args, {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
      timeout,
      env: { ...process.env, GIT_TERMINAL_PROMPT: "0" },
    }).trim();
  } catch (error) {
    throw new WorkstreamError(
      "WORKSTREAM_GIT",
      `git ${args.join(" ")} failed: ${messageOf(error)}`,
    );
  }
}

function tryGit(cwd: string, args: string[]): string | undefined {
  try {
    return git(cwd, args);
  } catch {
    return undefined;
  }
}

interface WorktreeEntry {
  path: string;
  branch?: string;
}

function worktrees(repo: string): WorktreeEntry[] {
  const output = tryGit(repo, ["worktree", "list", "--porcelain"]) ?? "";
  const entries: WorktreeEntry[] = [];
  let current: WorktreeEntry | undefined;
  for (const line of output.split("\n")) {
    if (line.startsWith("worktree ")) {
      if (current !== undefined) entries.push(current);
      current = { path: line.slice("worktree ".length).trim() };
    } else if (line.startsWith("branch refs/heads/") && current !== undefined) {
      current.branch = line.slice("branch refs/heads/".length).trim();
    }
  }
  if (current !== undefined) entries.push(current);
  return entries;
}

function branchExists(repo: string, branch: string): boolean {
  return tryGit(repo, ["show-ref", "--verify", `refs/heads/${branch}`]) !== undefined;
}

function defaultBase(repo: string): string {
  const remoteHead = tryGit(repo, ["symbolic-ref", "--short", "refs/remotes/origin/HEAD"]);
  if (remoteHead !== undefined && remoteHead.length > 0) return remoteHead;
  for (const candidate of ["origin/main", "main", "HEAD"]) {
    if (tryGit(repo, ["rev-parse", "--verify", `${candidate}^{commit}`]) !== undefined) {
      return candidate;
    }
  }
  throw new WorkstreamError("WORKSTREAM_GIT", `repository ${repo} has no usable base ref`);
}

function assertRepo(repo: string): void {
  if (!existsSync(repo)) {
    throw new WorkstreamError("WORKSTREAM_CONFIG", `workstream repository does not exist: ${repo}`);
  }
  if (tryGit(repo, ["rev-parse", "--git-dir"]) === undefined) {
    throw new WorkstreamError(
      "WORKSTREAM_CONFIG",
      `workstream repository is not a git repo: ${repo}`,
    );
  }
}

/** The path a workstream is materialized at, without touching the filesystem. */
export function workstreamPath(workspace: WorkstreamConfig, runtime: WorkstreamRuntime): string {
  const root = workspace.root ?? join(runtime.workspaceRoot, workspace.id);
  if (within(workspace.repo, root)) {
    throw new WorkstreamError(
      "WORKSTREAM_NOT_OWNED",
      `workstream root ${root} is inside the source repo ${workspace.repo}; ` +
        "use a Worker-owned directory outside the repo",
    );
  }
  return resolve(root);
}

/**
 * Materialize one workstream workspace under the Worker's own root.
 *
 * Reuse is idempotent: a second dispatch of the same workstream returns the
 * same path and branch. It never creates a second worktree for a branch that
 * is already checked out elsewhere.
 */
export function prepareWorkstream(
  workspace: WorkstreamConfig,
  runtime: WorkstreamRuntime,
): PreparedWorkstream {
  assertRepo(workspace.repo);
  const path = workstreamPath(workspace, runtime);
  const branch = workspace.branch ?? workstreamBranch(workspace.id);
  const remote = workspace.remote ?? "origin";
  const isolation = workspace.isolation ?? "worktree";
  if (isolation === "clone") {
    return prepareClone(workspace, path, branch, remote);
  }

  const entries = worktrees(workspace.repo);
  const existing = entries.find((entry) => resolve(entry.path) === path);
  if (existing !== undefined) {
    if (existing.branch !== branch) {
      throw new WorkstreamError(
        "WORKSTREAM_GIT",
        `workstream ${workspace.id} path ${path} is on "${existing.branch ?? "detached"}", ` +
          `expected "${branch}"`,
      );
    }
    return { ...workspace, path, branch, remote, isolation, created: false };
  }
  if (existsSync(path)) {
    throw new WorkstreamError(
      "WORKSTREAM_GIT",
      `workstream ${workspace.id} path ${path} exists but is not a worktree of ${workspace.repo}`,
    );
  }
  const occupied = entries.find((entry) => entry.branch === branch);
  if (occupied !== undefined) {
    throw new WorkstreamError(
      "WORKSTREAM_GIT",
      `branch ${branch} is already checked out at ${occupied.path}; ` +
        `workstream ${workspace.id} cannot share another workspace`,
    );
  }
  const base = workspace.base ?? defaultBase(workspace.repo);
  if (branchExists(workspace.repo, branch)) {
    git(workspace.repo, ["worktree", "add", path, branch]);
  } else {
    git(workspace.repo, ["worktree", "add", "-b", branch, path, base]);
  }
  return { ...workspace, path, branch, remote, isolation, created: true };
}

function prepareClone(
  workspace: WorkstreamConfig,
  path: string,
  branch: string,
  remote: string,
): PreparedWorkstream {
  if (existsSync(path)) {
    if (tryGit(path, ["rev-parse", "--git-dir"]) === undefined) {
      throw new WorkstreamError(
        "WORKSTREAM_GIT",
        `workstream ${workspace.id} path ${path} exists but is not a git clone`,
      );
    }
    const current = tryGit(path, ["symbolic-ref", "--short", "HEAD"]);
    if (current !== branch) {
      throw new WorkstreamError(
        "WORKSTREAM_GIT",
        `workstream ${workspace.id} clone ${path} is on "${current ?? "detached"}", ` +
          `expected "${branch}"`,
      );
    }
    return { ...workspace, path, branch, remote, isolation: "clone", created: false };
  }
  const remoteUrl = tryGit(workspace.repo, ["remote", "get-url", remote]);
  if (remoteUrl === undefined) {
    throw new WorkstreamError(
      "WORKSTREAM_CONFIG",
      `repository ${workspace.repo} has no "${remote}" remote to clone/push`,
    );
  }
  const base = workspace.base ?? defaultBase(workspace.repo);
  git(process.cwd(), ["clone", workspace.repo, path]);
  git(path, ["remote", "set-url", remote, remoteUrl]);
  if (branchExists(path, branch)) {
    git(path, ["checkout", branch]);
  } else {
    git(path, ["checkout", "-B", branch, base]);
  }
  return { ...workspace, path, branch, remote, isolation: "clone", created: true };
}

/**
 * Enforce the delivery contract for a workstream: stay on the delivery branch,
 * commit anything still uncommitted (the workspace is Worker-owned and the
 * assignment already reported the work as done), then push the branch.
 */
export function finalizeDelivery(
  workspace: PreparedWorkstream,
  options: { message?: string } = {},
): DeliveryResult {
  const current = tryGit(workspace.path, ["symbolic-ref", "--short", "HEAD"]);
  if (current !== workspace.branch) {
    throw new WorkstreamError(
      "WORKSTREAM_DELIVERY",
      `workstream ${workspace.id} is on "${current ?? "detached"}", expected "${workspace.branch}"`,
    );
  }
  let committed = false;
  const dirty = tryGit(workspace.path, ["status", "--porcelain"]) ?? "";
  if (dirty.length > 0) {
    git(workspace.path, ["add", "-A"]);
    git(workspace.path, [
      "-c",
      "user.name=Gethen Worker",
      "-c",
      "user.email=worker@gethen.local",
      "commit",
      "--no-verify",
      "-m",
      options.message ?? `chore(workstream): deliver ${workspace.id}`,
    ]);
    committed = true;
  }
  const commit = git(workspace.path, ["rev-parse", "HEAD"]);
  try {
    git(workspace.path, ["push", "--set-upstream", workspace.remote, workspace.branch]);
  } catch (error) {
    throw new WorkstreamError(
      "WORKSTREAM_DELIVERY",
      `push of ${workspace.branch} to ${workspace.remote} failed: ${messageOf(error)}`,
    );
  }
  return {
    workstream: workspace.id,
    branch: workspace.branch,
    commit,
    remote: workspace.remote,
    pushed: true,
    committed,
  };
}

/** Prompt suffix that states the delivery and report contract to the model. */
export function deliveryInstructions(workspace: PreparedWorkstream): string {
  return [
    "",
    "[GETHEN-DELIVERY]",
    `workstream: ${workspace.id}`,
    `branch: ${workspace.branch}`,
    `remote: ${workspace.remote}`,
    `workspace: ${workspace.path}`,
    "",
    `Work only in this isolated workstream workspace. Commit every change on "${workspace.branch}"`,
    `and push it to "${workspace.remote}" before reporting ready. The Worker verifies the branch,`,
    "commits any remaining change and pushes it; a failed push fails this assignment.",
    "In the report summary include the commit SHA, the test command(s) with their results,",
    "and the remaining risks.",
  ].join("\n");
}

/** Append the verified delivery evidence to the report summary shown to the Master. */
export function deliverySummary(summary: string, delivery: DeliveryResult): string {
  return [
    summary.trim(),
    "",
    `delivery: workstream=${delivery.workstream} branch=${delivery.branch} ` +
      `commit=${delivery.commit} remote=${delivery.remote} pushed=${delivery.pushed}`,
  ].join("\n");
}

/**
 * Validate Worker-owned workstream declarations without materializing anything.
 * Used by `gethen doctor`; returns human-readable issues (empty means valid).
 */
export function validateWorkstreams(workstreams: readonly WorkstreamConfig[]): string[] {
  const issues: string[] = [];
  for (const workspace of workstreams) {
    if (!existsSync(workspace.repo)) {
      issues.push(`${workspace.id}: repo does not exist: ${workspace.repo}`);
      continue;
    }
    if (tryGit(workspace.repo, ["rev-parse", "--git-dir"]) === undefined) {
      issues.push(`${workspace.id}: repo is not a git repository: ${workspace.repo}`);
    }
    if (workspace.root !== undefined && within(workspace.repo, workspace.root)) {
      issues.push(`${workspace.id}: root is inside the source repo: ${workspace.root}`);
    }
  }
  return issues;
}

/** Reject a Master-supplied path when the Worker has opted into workstreams. */
export function assertOwnedWorkspace(value: string, mapped: string | undefined): string {
  if (mapped === undefined && isFilesystemPath(value)) {
    throw new WorkstreamError(
      "WORKSTREAM_NOT_OWNED",
      `workspace "${value}" is a Master filesystem path; configure "workspaceMap" or ` +
        "dispatch a workstream id so the Worker owns the workspace",
    );
  }
  return mapped ?? value;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

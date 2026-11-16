/**
 * Lifecycle `ControlOp`s the Master may send to a Worker.
 *
 * The Worker never accepts free-form commands, shell strings or arbitrary
 * paths: `validateControlOp` rejects unknown ops, enforces the configured
 * runtime whitelist and keeps `workspace.*` paths inside one managed root.
 * Everything here is pure (string/type checks only, no filesystem access).
 *
 * The result type and error codes mirror `./frames`; only the type is imported
 * so this module and `frames` have no runtime dependency cycle.
 */
import type { ProtocolErrorCode, Result } from "./frames";

/** One lifecycle control op in a `control` frame. */
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

/** Every accepted `op` value, in the order the design lists them. */
export const CONTROL_OPS = [
  "snapshot",
  "runtime.start",
  "runtime.stop",
  "runtime.restart",
  "workspace.prepare",
  "workspace.cleanup",
  "slot.create",
  "slot.close",
  "slot.drain",
  "quota.set",
] as const;

export type ControlOpName = (typeof CONTROL_OPS)[number];

/** Host policy a Worker applies before executing a `ControlOp`. */
export interface ControlValidationOptions {
  /**
   * Runtime/service names this host may start (systemd whitelist). Defaults to
   * `[]`, so `runtime.*` ops are rejected unless an allowlist is configured.
   */
  allowedRuntimes?: readonly string[];
  /**
   * Managed root directory. `workspace.*` paths must stay inside it; when it is
   * absent those ops are rejected rather than guessed.
   */
  root?: string;
}

const RESOLUTION_ERROR = "invalid_field";

/**
 * Validate and normalize one `ControlOp` against a Worker's policy.
 *
 * Unknown ops, wrong field types, non-whitelisted runtimes and paths escaping
 * the managed root fail as a `Result`; this function never throws. Unknown
 * extra fields are dropped from the returned op.
 */
export function validateControlOp(
  value: unknown,
  options: ControlValidationOptions = {},
): Result<ControlOp> {
  const parsed = parseControlOp(value);
  if (!parsed.ok) return parsed;

  const op = parsed.value;
  if (op.op === "runtime.start" || op.op === "runtime.stop" || op.op === "runtime.restart") {
    const allowed = options.allowedRuntimes ?? [];
    if (!allowed.includes(op.name)) {
      return fail("runtime_not_allowed", `runtime "${op.name}" is not in the allowed runtimes`);
    }
  }
  if (op.op === "workspace.prepare" || op.op === "workspace.cleanup") {
    if (options.root === undefined) {
      return fail("path_outside_root", `workspace path "${op.path}" needs a managed root`);
    }
    if (!isWithinRoot(op.path, options.root)) {
      return fail("path_outside_root", `workspace path "${op.path}" is outside the managed root`);
    }
  }
  return ok(op);
}

/**
 * Structural validation only: shape, discriminant and field types, no host
 * policy. `frames.parseDownstreamFrame` uses this so a control frame can cross
 * the wire before the receiving Worker applies `validateControlOp` with its
 * own whitelist/root.
 */
export function parseControlOp(value: unknown): Result<ControlOp> {
  try {
    return ok(shapeOf(value));
  } catch (error) {
    if (error instanceof ControlError) return fail(error.code, error.message);
    return fail("invalid_frame", `control op could not be validated: ${messageOf(error)}`);
  }
}

/**
 * Pure containment test: is `path` the managed `root` or inside it?
 *
 * Normalizes `.`/`..` segments, treats both `/` and `\` as separators and
 * rejects `..` escapes and absolute paths outside the root. A relative path is
 * resolved under the root. No filesystem access, so symlinks are not resolved.
 */
export function isWithinRoot(path: string, root: string): boolean {
  const normalizedRoot = splitPath(root);
  if (normalizedRoot === undefined || !normalizedRoot.absolute) return false;

  const normalizedPath = splitPath(path);
  if (normalizedPath === undefined) return false;
  if (!normalizedPath.absolute) return true; // already resolved under root; no escapes left
  if (normalizedPath.drive !== normalizedRoot.drive) return false;
  return startsWithSegments(normalizedPath.segments, normalizedRoot.segments);
}

function shapeOf(value: unknown): ControlOp {
  if (!isRecord(value)) throw invalid("control op must be a JSON object");
  const op = value.op;
  if (typeof op !== "string" || !isControlOpName(op)) {
    throw new ControlError(
      "unknown_control_op",
      `control op "op" must be one of: ${CONTROL_OPS.join(", ")}`,
    );
  }

  switch (op) {
    case "snapshot":
      return { op: "snapshot" };
    case "runtime.start":
      return { op: "runtime.start", ...runtimeFields(value) };
    case "runtime.stop":
      return { op: "runtime.stop", ...runtimeFields(value) };
    case "runtime.restart":
      return { op: "runtime.restart", ...runtimeFields(value) };
    case "workspace.prepare": {
      const prepared: Extract<ControlOp, { op: "workspace.prepare" }> = {
        op: "workspace.prepare",
        path: fieldString(value, "path", "workspace.prepare.path"),
      };
      const repo = optionalString(value, "repo", "workspace.prepare.repo");
      if (repo !== undefined) prepared.repo = repo;
      const branch = optionalString(value, "branch", "workspace.prepare.branch");
      if (branch !== undefined) prepared.branch = branch;
      const worktree = optionalBoolean(value, "worktree", "workspace.prepare.worktree");
      if (worktree !== undefined) prepared.worktree = worktree;
      return prepared;
    }
    case "workspace.cleanup":
      return {
        op: "workspace.cleanup",
        path: fieldString(value, "path", "workspace.cleanup.path"),
      };
    case "slot.create": {
      const kind = fieldString(value, "kind", "slot.create.kind");
      if (kind !== "resident" && kind !== "ephemeral") {
        throw invalid('slot.create "kind" must be "resident" or "ephemeral"');
      }
      const ttlMs = optionalNumber(value, "ttlMs", "slot.create.ttlMs");
      if (ttlMs !== undefined && ttlMs <= 0) {
        throw invalid('slot.create "ttlMs" must be a positive number when present');
      }
      return {
        op: "slot.create",
        workspace: fieldString(value, "workspace", "slot.create.workspace"),
        pack: fieldString(value, "pack", "slot.create.pack"),
        kind,
        ...(ttlMs === undefined ? {} : { ttlMs }),
      };
    }
    case "slot.close": {
      const force = optionalBoolean(value, "force", "slot.close.force");
      return {
        op: "slot.close",
        slot: fieldString(value, "slot", "slot.close.slot"),
        ...(force === undefined ? {} : { force }),
      };
    }
    case "slot.drain":
      return { op: "slot.drain", slot: fieldString(value, "slot", "slot.drain.slot") };
    case "quota.set": {
      const slots = fieldNumber(value, "slots", "quota.set.slots");
      if (!Number.isInteger(slots) || slots < 0) {
        throw invalid('quota.set "slots" must be a non-negative integer');
      }
      const models = optionalStringArray(value, "models", "quota.set.models");
      return { op: "quota.set", slots, ...(models === undefined ? {} : { models }) };
    }
  }
}

function runtimeFields(record: Record<string, unknown>): { name: string; drain?: boolean } {
  const drain = optionalBoolean(record, "drain", "runtime.drain");
  return {
    name: fieldString(record, "name", "runtime.name"),
    ...(drain === undefined ? {} : { drain }),
  };
}

interface SplitPath {
  drive: string;
  segments: string[];
  absolute: boolean;
}

/**
 * Split a path into a drive + normalized segments. Returns `undefined` when the
 * path is empty, contains NUL, is drive-relative (`C:foo`) or escapes above its
 * own root with `..`.
 */
function splitPath(path: string): SplitPath | undefined {
  if (path.length === 0 || path.includes("\0")) return undefined;

  let rest = path.replaceAll("\\", "/");
  let drive = "";
  const driveMatch = /^([A-Za-z]):/.exec(rest);
  if (driveMatch !== null) {
    drive = (driveMatch[1] ?? "").toUpperCase();
    rest = rest.slice(driveMatch[0].length);
    // `C:relative` has no absolute base; requiring `/` keeps the check deterministic.
    if (!rest.startsWith("/")) return undefined;
  }

  const absolute = rest.startsWith("/") || drive.length > 0;
  const segments: string[] = [];
  for (const segment of rest.split("/")) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") {
      if (segments.length === 0) return undefined;
      segments.pop();
      continue;
    }
    segments.push(segment);
  }
  return { drive, segments, absolute };
}

function startsWithSegments(segments: readonly string[], prefix: readonly string[]): boolean {
  if (prefix.length > segments.length) return false;
  return prefix.every((segment, index) => segments[index] === segment);
}

function isControlOpName(value: string): value is ControlOpName {
  return (CONTROL_OPS as readonly string[]).includes(value);
}

class ControlError extends Error {
  readonly code: ProtocolErrorCode;

  constructor(code: ProtocolErrorCode, message: string) {
    super(message);
    this.name = "ControlError";
    this.code = code;
  }
}

function invalid(message: string): never {
  throw new ControlError(RESOLUTION_ERROR, message);
}

function ok<T>(value: T): Result<T> {
  return { ok: true, value };
}

function fail(code: ProtocolErrorCode, message: string): Result<never> {
  return { ok: false, code, message };
}

function fieldString(record: Record<string, unknown>, field: string, path: string): string {
  const value = record[field];
  if (typeof value !== "string" || value.length === 0) {
    throw invalid(`${path} must be a non-empty string`);
  }
  return value;
}

function optionalString(
  record: Record<string, unknown>,
  field: string,
  path: string,
): string | undefined {
  const value = record[field];
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.length === 0) {
    throw invalid(`${path} must be a non-empty string when present`);
  }
  return value;
}

function optionalBoolean(
  record: Record<string, unknown>,
  field: string,
  path: string,
): boolean | undefined {
  const value = record[field];
  if (value === undefined) return undefined;
  if (typeof value !== "boolean") {
    throw invalid(`${path} must be a boolean when present`);
  }
  return value;
}

function fieldNumber(record: Record<string, unknown>, field: string, path: string): number {
  const value = record[field];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw invalid(`${path} must be a finite number`);
  }
  return value;
}

function optionalNumber(
  record: Record<string, unknown>,
  field: string,
  path: string,
): number | undefined {
  const value = record[field];
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw invalid(`${path} must be a finite number when present`);
  }
  return value;
}

function optionalStringArray(
  record: Record<string, unknown>,
  field: string,
  path: string,
): string[] | undefined {
  const value = record[field];
  if (value === undefined) return undefined;
  if (
    !Array.isArray(value) ||
    value.some((item) => typeof item !== "string" || item.length === 0)
  ) {
    throw invalid(`${path} must be an array of non-empty strings when present`);
  }
  return [...(value as string[])];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

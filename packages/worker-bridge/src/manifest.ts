/**
 * Validation for the Worker registration a Worker reports in its mailbox
 * `hello` (workspace/model/capability/quota/health). The Master uses the
 * declared values only to *select* a Worker; the Worker's own sandbox and
 * approval policy stay local and are never enforced from here.
 */
import type {
  CostClass,
  RuntimeKind,
  WorkerCapabilities,
  WorkerHealth,
  WorkerManifest,
} from "./types";
import { BridgeContractError } from "./validate";

export type WorkerManifestResult =
  | { ok: true; value: WorkerManifest }
  | { ok: false; reason: string };

const RUNTIMES: readonly RuntimeKind[] = ["dsh", "opencode", "pi"];
const HEALTH: readonly WorkerHealth[] = ["online", "degraded", "offline"];
const COSTS: readonly CostClass[] = ["low", "mid", "high"];
const PROMPT_INJECTION = ["system", "prefix", "none"] as const;
const SKILLS = ["filesystem", "mcp", "none"] as const;
const PERMISSIONS = ["sandbox", "approval", "none"] as const;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function filled(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isTimestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    !Number.isNaN(Date.parse(value)) &&
    /^\d{4}-\d{2}-\d{2}T/.test(value)
  );
}

function parseCapabilities(value: unknown): WorkerCapabilities | string {
  if (!isRecord(value)) return "capabilities must be an object";
  const { promptInjection, skills, mcp, permissions, streaming, steer, fork } = value;
  if (!(PROMPT_INJECTION as readonly unknown[]).includes(promptInjection))
    return "capabilities.promptInjection is invalid";
  if (!(SKILLS as readonly unknown[]).includes(skills)) return "capabilities.skills is invalid";
  if (!(PERMISSIONS as readonly unknown[]).includes(permissions))
    return "capabilities.permissions is invalid";
  for (const [name, flag] of Object.entries({ mcp, streaming, steer, fork })) {
    if (typeof flag !== "boolean") return `capabilities.${name} must be a boolean`;
  }
  return {
    promptInjection: promptInjection as WorkerCapabilities["promptInjection"],
    skills: skills as WorkerCapabilities["skills"],
    mcp: mcp as boolean,
    permissions: permissions as WorkerCapabilities["permissions"],
    streaming: streaming as boolean,
    steer: steer as boolean,
    fork: fork as boolean,
  };
}

/**
 * Validate a Worker-reported manifest. Returns reasons instead of throwing so
 * the mailbox link can reject one bad `hello` without dropping the connection.
 */
export function parseWorkerManifest(value: unknown): WorkerManifestResult {
  if (!isRecord(value)) return { ok: false, reason: "worker must be an object" };
  if (!filled(value.id)) return { ok: false, reason: "worker.id must be a non-empty string" };
  if (!filled(value.machine)) return { ok: false, reason: "worker.machine must be a string" };
  if (!(RUNTIMES as readonly unknown[]).includes(value.runtime))
    return { ok: false, reason: "worker.runtime is invalid" };
  if (!(COSTS as readonly unknown[]).includes(value.costClass))
    return { ok: false, reason: "worker.costClass is invalid" };

  if (!Array.isArray(value.workspaces))
    return { ok: false, reason: "worker.workspaces must be an array" };
  const workspaces: WorkerManifest["workspaces"] = [];
  for (const [index, entry] of value.workspaces.entries()) {
    if (!isRecord(entry) || !filled(entry.id)) {
      return { ok: false, reason: `worker.workspaces[${index}].id must be a string` };
    }
    if (entry.repo !== undefined && typeof entry.repo !== "string")
      return { ok: false, reason: `worker.workspaces[${index}].repo must be a string` };
    if (entry.branch !== undefined && typeof entry.branch !== "string")
      return { ok: false, reason: `worker.workspaces[${index}].branch must be a string` };
    workspaces.push({
      id: entry.id,
      ...(entry.repo === undefined ? {} : { repo: entry.repo }),
      ...(entry.branch === undefined ? {} : { branch: entry.branch }),
    });
  }

  const capabilities = parseCapabilities(value.capabilities);
  if (typeof capabilities === "string") return { ok: false, reason: capabilities };

  if (!Array.isArray(value.models) || value.models.length === 0)
    return { ok: false, reason: "worker.models must be a non-empty array" };
  const models: string[] = [];
  for (const [index, model] of value.models.entries()) {
    if (!filled(model)) return { ok: false, reason: `worker.models[${index}] must be a string` };
    models.push(model);
  }

  if (!isRecord(value.load)) return { ok: false, reason: "worker.load must be an object" };
  const { running, quota } = value.load;
  if (typeof running !== "number" || !Number.isInteger(running) || running < 0)
    return { ok: false, reason: "worker.load.running must be a non-negative integer" };
  if (typeof quota !== "number" || !Number.isInteger(quota) || quota < 0)
    return { ok: false, reason: "worker.load.quota must be a non-negative integer" };

  if (!isRecord(value.health)) return { ok: false, reason: "worker.health must be an object" };
  if (!(HEALTH as readonly unknown[]).includes(value.health.state))
    return { ok: false, reason: "worker.health.state is invalid" };
  if (!isTimestamp(value.health.lastHeartbeatAt))
    return { ok: false, reason: "worker.health.lastHeartbeatAt must be an RFC 3339 timestamp" };
  if (value.health.detail !== undefined && typeof value.health.detail !== "string")
    return { ok: false, reason: "worker.health.detail must be a string" };

  return {
    ok: true,
    value: {
      id: value.id,
      machine: value.machine,
      runtime: value.runtime as RuntimeKind,
      workspaces,
      capabilities,
      models,
      costClass: value.costClass as CostClass,
      load: { running, quota },
      health: {
        state: value.health.state as WorkerHealth,
        lastHeartbeatAt: value.health.lastHeartbeatAt,
        ...(value.health.detail === undefined ? {} : { detail: value.health.detail }),
      },
    },
  };
}

/** Throwing form for adapters that build a registration in-process. */
export function assertWorkerManifest(value: unknown): WorkerManifest {
  const parsed = parseWorkerManifest(value);
  if (!parsed.ok) throw new BridgeContractError(parsed.reason);
  return parsed.value;
}

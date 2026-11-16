import type { DispatchRequest, Escalation, Resolution } from "./types";

/** Raised when a Master or an adapter violates the bridge contract. */
export class BridgeContractError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BridgeContractError";
  }
}

function filled(value: string | undefined): boolean {
  return typeof value === "string" && value.trim().length > 0;
}

export function assertDispatch(request: DispatchRequest): void {
  for (const field of ["ticket", "assignment", "worker", "workspace", "prompt"] as const) {
    if (!filled(request[field])) throw new BridgeContractError(`dispatch requires ${field}`);
  }
}

export function assertEscalation(escalation: Escalation): void {
  for (const field of ["id", "ticket", "assignment", "session", "question", "at"] as const) {
    if (!filled(escalation[field])) throw new BridgeContractError(`escalation requires ${field}`);
  }
  const ids = new Set<string>();
  for (const option of escalation.options) {
    if (!filled(option.id) || !filled(option.label))
      throw new BridgeContractError("escalation options require id and label");
    if (ids.has(option.id))
      throw new BridgeContractError(`duplicate escalation option: ${option.id}`);
    ids.add(option.id);
  }
  if (escalation.defaultOption !== undefined && !ids.has(escalation.defaultOption))
    throw new BridgeContractError(`defaultOption is not an option: ${escalation.defaultOption}`);
}

export function assertResolution(resolution: Resolution, escalation?: Escalation): void {
  if (!filled(resolution.id)) throw new BridgeContractError("resolution requires id");
  if (!filled(resolution.escalation))
    throw new BridgeContractError("resolution requires escalation");
  if (escalation !== undefined && resolution.escalation !== escalation.id)
    throw new BridgeContractError(
      `resolution targets ${resolution.escalation}, expected ${escalation.id}`,
    );
  switch (resolution.kind) {
    case "answer":
      if (!filled(resolution.text)) throw new BridgeContractError("answer requires text");
      if (
        !Number.isFinite(resolution.confidence) ||
        resolution.confidence < 0 ||
        resolution.confidence > 1
      )
        throw new BridgeContractError("answer confidence must be within [0, 1]");
      if (resolution.basis.length === 0) throw new BridgeContractError("answer requires a basis");
      return;
    case "escalate":
      if (!filled(resolution.question)) throw new BridgeContractError("escalate requires question");
      return;
    case "reroute":
      if (!filled(resolution.worker)) throw new BridgeContractError("reroute requires worker");
      return;
    case "drop":
      if (!filled(resolution.reason)) throw new BridgeContractError("drop requires reason");
      return;
    default:
      throw new BridgeContractError("unknown resolution kind");
  }
}

/**
 * Capability-pack materialization for the Worker side.
 *
 * A `DispatchRequest.pack` is opaque to the bridge: it is the capability pack
 * the Master sent, shaped like `@gethen/pack`'s `CapabilityPack`. The Worker
 * only applies the parts a session can take (system prompt, role, model, skill
 * names, MCP servers); the rest of the pack (digest, memory, policy, eval
 * protocol, team sections, tool allowlists) is ignored in P3.
 *
 * P3 is deliberately strict: a payload that is not an object, misses the
 * required `prompt` section, or carries a known section in a shape the Worker
 * cannot apply throws, so a malformed pack never silently starts a Worker with
 * half its instructions. Pure module: no IO, no clock.
 */

/** The parts of a capability pack a Worker session can apply. */
export interface MaterializedPack {
  systemPrompt?: string;
  role?: string;
  model?: string;
  skills?: string[];
  mcp?: unknown;
}

/**
 * Validate an opaque capability pack and return the session settings it
 * implies. Throws on a non-object payload, a missing `prompt` section, or a
 * known section whose shape is unusable.
 */
export function materializePack(payload: unknown): MaterializedPack {
  const pack = asRecord(payload, "capability pack must be an object");
  const prompt = asRecord(pack.prompt, 'capability pack is missing required object "prompt"');

  const materialized: MaterializedPack = {};
  const system = optionalString(prompt, "system", "prompt.system");
  if (system !== undefined) materialized.systemPrompt = system;
  const role = optionalString(prompt, "role", "prompt.role");
  if (role !== undefined) materialized.role = role;

  if (pack.model !== undefined) {
    const model = asRecord(pack.model, 'capability pack field "model" must be an object');
    materialized.model = requireString(model, "model", "model.model");
  }

  if (pack.skills !== undefined) {
    if (!Array.isArray(pack.skills)) {
      throw new Error('capability pack field "skills" must be an array');
    }
    materialized.skills = pack.skills.map((skill, index) => {
      const entry = asRecord(skill, `capability pack field "skills[${index}]" must be an object`);
      return requireString(entry, "name", `skills[${index}].name`);
    });
  }

  if (pack.tools !== undefined) {
    const tools = asRecord(pack.tools, 'capability pack field "tools" must be an object');
    if (tools.mcpServers !== undefined) {
      if (!Array.isArray(tools.mcpServers)) {
        throw new Error('capability pack field "tools.mcpServers" must be an array');
      }
      materialized.mcp = tools.mcpServers;
    }
  }

  return materialized;
}

function asRecord(value: unknown, message: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(message);
  }
  return value as Record<string, unknown>;
}

function requireString(record: Record<string, unknown>, field: string, path: string): string {
  const value = record[field];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${path} must be a non-empty string`);
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
    throw new Error(`${path} must be a non-empty string when present`);
  }
  return value;
}

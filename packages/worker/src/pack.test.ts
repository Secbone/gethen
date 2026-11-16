import { describe, expect, it } from "vitest";
import { materializePack } from "./pack";

describe("materializePack", () => {
  it("maps a capability pack to the settings a Worker session applies", () => {
    const pack = {
      id: "pack-1",
      version: 3,
      digest: "sha256:pack",
      prompt: {
        system: "You are the Gethen Worker.",
        role: "worker",
        teamSections: { delivery: "Open a pull request when done." },
      },
      skills: [{ name: "debug", version: 1 }, { name: "git" }],
      model: { provider: "deepseek", model: "deepseek-v4", effort: "high" },
      tools: {
        mcpServers: [{ name: "filesystem", command: "mcp-fs" }],
        allowlist: ["shell"],
        blockedCommands: ["rm -rf /"],
      },
      memory: { snapshot: "memory-1" },
      policy: { budget: { steps: 25 } },
      evalProtocol: { id: "protocol-1", version: 1, digest: "sha256:eval" },
    };

    expect(materializePack(pack)).toEqual({
      systemPrompt: "You are the Gethen Worker.",
      role: "worker",
      model: "deepseek-v4",
      skills: ["debug", "git"],
      mcp: [{ name: "filesystem", command: "mcp-fs" }],
    });
  });

  it("accepts a minimal pack and ignores the parts a session cannot apply", () => {
    expect(materializePack({ prompt: { system: "be useful", role: "worker" } })).toEqual({
      systemPrompt: "be useful",
      role: "worker",
    });
    expect(materializePack({ prompt: {}, skills: [] })).toEqual({ skills: [] });
    expect(
      materializePack({
        id: "pack-2",
        version: 1,
        prompt: {},
        model: { provider: "deepseek", model: "deepseek-v4" },
        memory: { digest: "sha256:memory" },
      }),
    ).toEqual({ model: "deepseek-v4" });
  });

  it("rejects payloads that are not objects", () => {
    const payloads: unknown[] = [undefined, null, "pack", 42, [], ["prompt"]];
    for (const payload of payloads) {
      expect(() => materializePack(payload), String(payload)).toThrow(/object/);
    }
  });

  it("rejects a pack without a prompt section or with unusable known sections", () => {
    expect(() => materializePack({})).toThrow(/prompt/);
    expect(() => materializePack({ prompt: "be useful" })).toThrow(/prompt/);
    expect(() => materializePack({ prompt: { system: 7 } })).toThrow(/system/);
    expect(() => materializePack({ prompt: {}, model: {} })).toThrow(/model\.model/);
    expect(() => materializePack({ prompt: {}, skills: "git" })).toThrow(/skills/);
    expect(() => materializePack({ prompt: {}, skills: [{ version: 1 }] })).toThrow(/name/);
    expect(() => materializePack({ prompt: {}, tools: { mcpServers: "fs" } })).toThrow(
      /mcpServers/,
    );
  });
});

import { afterEach, expect, it, vi } from "vitest";
import bridge from "./gethen-bridge";

afterEach(() => vi.unstubAllEnvs());
it("blocks tools when the required UI permission bridge is unavailable", async () => {
  vi.stubEnv("GETHEN_PERMISSION_MODE", "ask");
  vi.stubEnv("GETHEN_MCP_SERVERS", "");
  let handler: ((event: unknown, ctx: unknown) => unknown) | undefined;
  bridge({
    on: (_event, fn) => {
      handler = fn;
    },
  });
  expect(await handler?.({ toolName: "write" }, {})).toMatchObject({ block: true });
});
it("fails startup explicitly when configured MCP registration is unavailable", () => {
  vi.stubEnv("GETHEN_MCP_SERVERS", '[{"name":"memory","command":"node"}]');
  expect(() => bridge({ on() {} })).toThrow("MCP registration is unavailable");
});

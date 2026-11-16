import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  apply,
  type DshContext,
  type DshToolDefinition,
  type GethenService,
  workerFromEnv,
} from "./index";

// The plugin reads GETHEN_* at apply time. A developer shell (or a DSH profile)
// that exports GETHEN_WORKER or GETHEN_BENCH_PLAN would otherwise spawn a real
// engine or evaluator inside these unit tests.
const savedEnv: Record<string, string> = {};

beforeAll(() => {
  for (const key of Object.keys(process.env)) {
    if (!key.startsWith("GETHEN_")) continue;
    savedEnv[key] = process.env[key] ?? "";
    delete process.env[key];
  }
});

afterAll(() => {
  for (const [key, value] of Object.entries(savedEnv)) process.env[key] = value;
});

function context() {
  const services = new Map<string, unknown>();
  const tools: DshToolDefinition[] = [];
  const ctx: DshContext = {
    provide(name, value) {
      services.set(name, value);
      return () => services.delete(name);
    },
    tools: {
      register(definition) {
        tools.push(definition);
        return () => {};
      },
    },
  };
  return { ctx, services, tools };
}

function tool(tools: DshToolDefinition[], name: string): DshToolDefinition {
  const found = tools.find((candidate) => candidate.name === name);
  if (!found) throw new Error(`missing tool ${name}`);
  return found;
}

describe("Gethen DSH plugin", () => {
  it("provides the control-plane service and its tools", async () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: mkdtempSync(join(tmpdir(), "gethen-dsh-")) });
    expect(services.get("gethen")).toMatchObject({ runtime: expect.anything() });
    expect(tools.map((entry) => entry.name)).toEqual([
      "gethen_status",
      "gethen_receive",
      "gethen_dispatch",
      "gethen_dream",
      "gethen_evolve",
      "gethen_channel",
      "gethen_overview",
      "gethen_poll",
      "gethen_run",
    ]);
    await expect(tool(tools, "gethen_status").execute({}, {})).resolves.toEqual({
      tickets: 0,
      threads: 0,
      decisions: 0,
      router: "",
    });
  });

  it("selects the configured heterogeneous worker", () => {
    expect(workerFromEnv({})).toBeUndefined();
    expect(workerFromEnv({ GETHEN_WORKER: "pi" })?.id).toBe("pi");
    expect(workerFromEnv({ GETHEN_WORKER: "opencode" })?.id).toBe("opencode");
    expect(() => workerFromEnv({ GETHEN_WORKER: "codex" })).toThrow("unknown worker: codex");
  });

  it("runs one request through the tool surface and records a receipt", async () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: mkdtempSync(join(tmpdir(), "gethen-dsh-")) });
    const result = (await tool(tools, "gethen_run").execute(
      { text: "fix the failing test" },
      {},
    )) as {
      outcome: string;
      ticket: string;
      receipt: { outcome: string; report: { kind: string } };
    };
    expect(result.outcome).toBe("accept");
    expect(result.ticket).toBeTruthy();
    expect(result.receipt).toMatchObject({ outcome: "completed", report: { kind: "ready" } });
    const service = services.get("gethen") as GethenService;
    expect(service.repos.tickets.list()).toHaveLength(1);
  });
});

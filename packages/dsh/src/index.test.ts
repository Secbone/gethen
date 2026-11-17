import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MasterService } from "@gethen/master";
import { afterEach, describe, expect, it } from "vitest";
import { apply, type DshContext, type DshToolDefinition } from "./index";

interface RunResult {
  stored: string;
  outcome: string;
  ticket: string;
  report: string;
  receipt: number;
}

interface StatusResult {
  tickets: number;
  open: number;
  receipts: number;
  running: number;
  escalations: number;
  launch: "fake" | "ws";
  workers: number;
  mailbox?: { pending: number; leased: number; dead: number };
}

interface OverviewResult {
  tickets: Array<{
    id: string;
    stage: string;
    outcome: string;
    latestReport: string;
    assignments: number;
  }>;
}

/** Cleanups registered through the fake `ctx.effect`, run after every test. */
const cleanups: Array<() => void> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) cleanup();
  // runtime.close() is started by the effect cleanup; let it settle.
  await new Promise((resolve) => setTimeout(resolve, 20));
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
    effect(setup) {
      const cleanup = setup();
      if (typeof cleanup === "function") cleanups.push(cleanup);
      return () => {};
    },
  };
  return { ctx, services, tools };
}

function stateDir(): string {
  return mkdtempSync(join(tmpdir(), "gethen-dsh-"));
}

function tool(tools: DshToolDefinition[], name: string): DshToolDefinition {
  const found = tools.find((candidate) => candidate.name === name);
  if (!found) throw new Error(`missing tool ${name}`);
  return found;
}

function serviceOf(services: Map<string, unknown>): MasterService {
  const service = services.get("gethen");
  if (!(service instanceof MasterService)) throw new Error("gethen service is not a MasterService");
  return service;
}

describe("Gethen DSH plugin", () => {
  it("provides the master service and registers the four tools", () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: stateDir() });
    serviceOf(services);
    expect(cleanups).toHaveLength(1);
    expect(tools.map((entry) => entry.name)).toEqual([
      "gethen_status",
      "gethen_run",
      "gethen_overview",
      "gethen_escalations",
    ]);
  });

  it("runs one local request into an accepted ticket with a ready report", async () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: stateDir() });
    const result = (await tool(tools, "gethen_run").execute(
      { text: "fix the failing test" },
      {},
    )) as RunResult;
    expect(result.stored).toBe("stored");
    expect(result.outcome).toBe("accept");
    expect(result.ticket).toBeTruthy();
    expect(result.report).toBe("ready");
    expect(result.receipt).toBe(1);
    const service = serviceOf(services);
    expect(service.store.tickets.list()).toHaveLength(1);
    expect(service.store.listReceipts()).toHaveLength(1);
  });

  it("reports status counts and stages the ticket for review (fake launch)", async () => {
    const { ctx, tools } = context();
    apply(ctx, { dir: stateDir() });
    await tool(tools, "gethen_run").execute({ text: "fix the failing test" }, {});
    await expect(tool(tools, "gethen_status").execute({}, {})).resolves.toEqual({
      tickets: 1,
      open: 1,
      receipts: 1,
      running: 0,
      escalations: 0,
      launch: "fake",
      workers: 0,
    } satisfies StatusResult);
    const overview = (await tool(tools, "gethen_overview").execute({}, {})) as OverviewResult;
    expect(overview.tickets).toHaveLength(1);
    const ticket = overview.tickets[0];
    expect(ticket?.latestReport).toBe("ready");
    expect(ticket?.assignments).toBe(1);
    expect(ticket?.stage).toBe("person/review");
  });

  it("keeps escalation history available through gethen_escalations", async () => {
    const { ctx, tools } = context();
    apply(ctx, { dir: stateDir() });
    await tool(tools, "gethen_run").execute({ text: "fix the failing test" }, {});
    const result = (await tool(tools, "gethen_escalations").execute({}, {})) as {
      escalations: unknown[];
    };
    expect(result.escalations).toEqual([]);
  });
});

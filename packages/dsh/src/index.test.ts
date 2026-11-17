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
  sessionId?: string;
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

/** The session binding the plugin stored on the last Trigger's meta. */
function triggerSession(services: Map<string, unknown>): unknown {
  const raw = serviceOf(services).store.triggers.list().at(-1)?.meta?.session;
  return raw === undefined ? undefined : JSON.parse(raw);
}

describe("Gethen DSH plugin", () => {
  it("provides the master service and registers the five tools", () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: stateDir() });
    serviceOf(services);
    expect(cleanups).toHaveLength(1);
    expect(tools.map((entry) => entry.name)).toEqual([
      "gethen_status",
      "gethen_run",
      "gethen_overview",
      "gethen_escalations",
      "gethen_task",
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

  it("defaults gethen_run to a fresh session", async () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: stateDir() });
    const result = (await tool(tools, "gethen_run").execute(
      { text: "fix the failing test" },
      {},
    )) as RunResult;
    expect(result.outcome).toBe("accept");
    expect(result.sessionId).toBeUndefined();
    expect(triggerSession(services)).toEqual({ mode: "new" });
  });

  it("passes a reuse sessionId through and backfills the resolved id", async () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: stateDir() });
    const result = (await tool(tools, "gethen_run").execute(
      { text: "continue the migration", session: { mode: "reuse", sessionId: "session-1" } },
      {},
    )) as RunResult;
    expect(triggerSession(services)).toEqual({ mode: "reuse", sessionId: "session-1" });
    expect(result.sessionId).toBe("session-1");
  });

  it("passes a reuse key through without a resolved sessionId", async () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: stateDir() });
    const result = (await tool(tools, "gethen_run").execute(
      { text: "continue via key", session: { mode: "reuse", key: "project-key" } },
      {},
    )) as RunResult;
    expect(triggerSession(services)).toEqual({ mode: "reuse", key: "project-key" });
    expect(result.sessionId).toBeUndefined();
  });

  it("rejects invalid session bindings before submitting", async () => {
    const { ctx, services, tools } = context();
    apply(ctx, { dir: stateDir() });
    const run = (session: unknown) => tool(tools, "gethen_run").execute({ text: "x", session }, {});
    await expect(run({ mode: "reuse" })).rejects.toThrow(/exactly one of sessionId or key/);
    await expect(run({ mode: "reuse", sessionId: "s", key: "k" })).rejects.toThrow(
      /exactly one of sessionId or key/,
    );
    await expect(run({ mode: "new", sessionId: "s" })).rejects.toThrow(
      /must not set sessionId or key/,
    );
    await expect(run({ mode: "resume" })).rejects.toThrow(/session.mode/);
    await expect(run("{not json")).rejects.toThrow(/valid JSON/);
    expect(serviceOf(services).store.triggers.list()).toHaveLength(0);
  });

  it("gethen_task fails clearly with the fake launch", async () => {
    const { ctx, tools } = context();
    apply(ctx, { dir: stateDir() });
    await expect(
      tool(tools, "gethen_task").execute({ action: "status", assignmentId: "assignment-1" }, {}),
    ).rejects.toThrow(/requires the mailbox runtime/);
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

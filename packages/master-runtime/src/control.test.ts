import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Trigger } from "@gethen/master";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createMasterRuntime, type MasterRuntime } from "./runtime";

const START = new Date("2026-11-17T10:00:00.000Z");

let dir: string;
let runtime: MasterRuntime | undefined;
let now = START;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "gethen-control-"));
  now = START;
});

afterEach(async () => {
  vi.restoreAllMocks();
  await runtime?.close();
  runtime = undefined;
  rmSync(dir, { recursive: true, force: true });
});

function makeRuntime(options: { retentionMs?: number } = {}): MasterRuntime {
  runtime = createMasterRuntime({
    dbPath: join(dir, "master.sqlite"),
    now: () => now,
    ...(options.retentionMs === undefined ? {} : { retentionMs: options.retentionMs }),
  });
  return runtime;
}

function trigger(text: string): Trigger {
  return {
    id: `trigger-${crypto.randomUUID()}`,
    source: "local",
    thread: "local",
    actor: "local-user",
    bot: false,
    addressed: "mention",
    text,
    at: now.toISOString(),
    meta: { scope: "local", workspace: "/ws" },
  };
}

async function acceptedTicket(runtime: MasterRuntime): Promise<string> {
  await runtime.service.submit(trigger("fix the failing test"));
  const ticket = runtime.service.store.tickets.list().at(-1);
  if (ticket === undefined) throw new Error("expected an accepted ticket");
  return ticket.id;
}

describe("control plane", () => {
  it("overview defaults to needs-action and ack clears it until the next report", async () => {
    const runtime = makeRuntime();
    const ticketId = await acceptedTicket(runtime);

    const before = runtime.control.overview();
    expect(before.filter).toBe("needs-action");
    expect(before.counts).toMatchObject({ tickets: 1, open: 1, needsAction: 1, archived: 0 });
    expect(before.tickets.map((ticket) => ticket.id)).toEqual([ticketId]);
    expect(before.tickets[0]).toMatchObject({ stage: "person/review", needsAction: true });

    const acknowledged = runtime.control.acknowledge(ticketId);
    expect(acknowledged.needsAction).toBe(false);
    expect(runtime.control.overview().tickets).toEqual([]);
    expect(runtime.control.status().counts.needsAction).toBe(0);
  });

  it("shows closed work only in the open/all views", async () => {
    const runtime = makeRuntime();
    const ticketId = await acceptedTicket(runtime);

    const closed = await runtime.control.close({ ticketId, outcome: "completed" });
    expect(closed).toMatchObject({ lifecycle: "closed", outcome: "completed", needsAction: false });
    expect(runtime.control.overview().tickets).toEqual([]);
    expect(runtime.control.overview({ filter: "open" }).tickets).toEqual([]);
    expect(runtime.control.overview({ filter: "all" }).tickets.map((ticket) => ticket.id)).toEqual([
      ticketId,
    ]);
  });

  it("archives terminal tickets and sweeps by outcome retention", async () => {
    const runtime = makeRuntime();
    const ticketId = await acceptedTicket(runtime);

    await runtime.control.close({ ticketId, outcome: "dropped" });
    const archived = runtime.control.archive(ticketId);
    expect(archived.lifecycle).toBe("archived");
    expect(runtime.control.overview({ filter: "all" }).counts.archived).toBe(1);

    const second = await acceptedTicket(runtime);
    await runtime.control.close({ ticketId: second, outcome: "dropped" });
    now = new Date(START.getTime() + 2 * 86_400_000);
    expect(runtime.control.autoArchive()).toContain(second);
    expect(runtime.control.overview({ filter: "all" }).counts.archived).toBe(2);
  });

  it("honours a configured retention window", async () => {
    const runtime = makeRuntime({ retentionMs: 1_000 });
    const ticketId = await acceptedTicket(runtime);
    await runtime.control.close({ ticketId, outcome: "completed" });
    now = new Date(START.getTime() + 1_500);
    expect(runtime.control.autoArchive()).toContain(ticketId);
    expect(runtime.control.overview({ filter: "all" }).counts.archived).toBe(1);
  });

  it("refuses to archive an open ticket and to acknowledge a closed one", async () => {
    const runtime = makeRuntime();
    const ticketId = await acceptedTicket(runtime);

    expect(() => runtime.control.archive(ticketId)).toThrow(/close it before archiving/);
    await runtime.control.close({ ticketId, outcome: "closed" });
    expect(() => runtime.control.acknowledge(ticketId)).toThrow(/has ended/);
  });

  it("answers a report-only escalation locally in fake mode", async () => {
    const runtime = makeRuntime();
    const ticketId = await acceptedTicket(runtime);
    runtime.service.engine.report(ticketId, {
      kind: "waiting",
      summary: "a person must choose",
      question: "Which branch should I merge?",
      at: now.toISOString(),
    });

    const overview = runtime.control.overview();
    expect(overview.tickets.map((ticket) => ticket.question)).toEqual([
      "Which branch should I merge?",
    ]);

    const answered = await runtime.control.answer({
      ticketId,
      text: "Use main.",
      option: "main",
    });
    expect(answered).toMatchObject({
      delivered: false,
      assignmentId: expect.any(String),
      escalation: {
        ticket: ticketId,
        status: "resolved",
        question: "Which branch should I merge?",
        resolution: { kind: "answer", text: "Use main.", option: "main" },
      },
      ticket: { id: ticketId, needsAction: false },
    });
    expect(answered.ticket.question).toBeUndefined();
    expect(runtime.control.overview().tickets).toEqual([]);
    expect(runtime.service.store.listEscalations()).toHaveLength(1);
    // The answer is persisted as a human input on the Ticket.
    const inputs = runtime.service.store.inputs.list().filter((input) => input.ticket === ticketId);
    expect(inputs.at(-1)).toMatchObject({ kind: "text", text: "Use main." });

    await expect(runtime.control.answer({ ticketId, text: "again" })).rejects.toThrow(
      /already resolved/,
    );
    await expect(runtime.control.answer({ ticketId: "ticket-missing", text: "x" })).rejects.toThrow(
      /unknown ticket/,
    );
  });

  it("refuses an answer once the ticket has ended", async () => {
    const runtime = makeRuntime();
    const ticketId = await acceptedTicket(runtime);
    runtime.service.engine.report(ticketId, {
      kind: "waiting",
      summary: "waiting",
      question: "Which?",
      at: now.toISOString(),
    });
    await runtime.control.close({ ticketId, outcome: "closed" });
    await expect(runtime.control.answer({ ticketId, text: "late" })).rejects.toThrow(/has ended/);
  });

  it("delegates task control to the runtime bridge with clear fake-mode errors", async () => {
    const runtime = makeRuntime();
    await expect(
      runtime.control.task({ action: "status", assignmentId: "assignment-1" }),
    ).rejects.toThrow(/requires the mailbox runtime/);
    expect(() => runtime.control.states()).toThrow(/RUNTIME_COMMS_UNAVAILABLE/);
  });
});

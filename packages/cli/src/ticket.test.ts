import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { parseTicketArgs, ticketCommand, ticketPath } from "./commands/ticket";

interface StubResult {
  status: number;
  body: unknown;
}

interface Captured {
  method: string | undefined;
  url: string | undefined;
  body: string;
}

const servers: Server[] = [];

afterEach(async () => {
  while (servers.length > 0) {
    const server = servers.pop();
    if (server !== undefined) await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

function capture(): { lines: string[]; write: (line: string) => void } {
  const lines: string[] = [];
  return { lines, write: (line: string) => lines.push(line) };
}

async function stubMaster(handler: () => StubResult): Promise<{
  base: string;
  captured: Captured[];
}> {
  const captured: Captured[] = [];
  const server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      captured.push({
        method: request.method,
        url: request.url,
        body: Buffer.concat(chunks).toString("utf8"),
      });
      const result = handler();
      const body = JSON.stringify(result.body);
      response.writeHead(result.status, {
        "content-type": "application/json",
        "content-length": Buffer.byteLength(body),
      });
      response.end(body);
    });
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  const port = typeof address === "object" && address !== null ? address.port : 0;
  return { base: `http://127.0.0.1:${port}`, captured };
}

function summary(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: "ticket-1",
    source: "local",
    stage: "person/review",
    lifecycle: "closed",
    needsAction: false,
    openedAt: "2026-11-17T10:00:00.000Z",
    assignments: 1,
    outcome: "completed",
    endedAt: "2026-11-17T11:00:00.000Z",
    ...overrides,
  };
}

describe("gethen ticket", () => {
  it("parses ack/close/archive and rejects bad invocations", () => {
    expect(parseTicketArgs(["ack", "ticket-1", "--master", "http://m:1"])).toEqual({
      action: "ack",
      ticketId: "ticket-1",
      master: "http://m:1",
      json: false,
    });
    expect(
      parseTicketArgs([
        "close",
        "ticket-1",
        "--outcome",
        "merged",
        "--reason",
        "clean merge",
        "--master",
        "http://m:1",
        "--json",
      ]),
    ).toMatchObject({
      action: "close",
      outcome: "merged",
      reason: "clean merge",
      json: true,
    });
    expect(() => parseTicketArgs(["pause", "ticket-1", "--master", "http://m:1"])).toThrow(
      /subcommand/,
    );
    expect(() => parseTicketArgs(["ack", "--master", "http://m:1"])).toThrow(/ticket id/);
    expect(parseTicketArgs(["ack", "ticket-1"], { GETHEN_MASTER_URL: "http://m:2" })).toMatchObject(
      { master: "http://m:2" },
    );
    expect(() =>
      parseTicketArgs(["close", "ticket-1", "--outcome", "stopped", "--master", "http://m:1"]),
    ).toThrow(/--outcome/);
  });

  it("builds the ticket route", () => {
    expect(
      ticketPath({ action: "close", ticketId: "ticket/1", master: "http://m:1", json: false }),
    ).toBe("/gethen/tickets/ticket%2F1/close");
  });

  it("posts a close outcome and renders the returned summary", async () => {
    const { base, captured } = await stubMaster(() => ({
      status: 200,
      body: { ok: true, action: "close", ticket: summary() },
    }));
    const out = capture();

    expect(
      await ticketCommand(
        ["close", "ticket-1", "--outcome", "merged", "--reason", "done", "--master", base],
        {
          write: out.write,
        },
      ),
    ).toBe(0);

    expect(captured[0]?.method).toBe("POST");
    expect(captured[0]?.url).toBe("/gethen/tickets/ticket-1/close");
    expect(JSON.parse(captured[0]?.body ?? "{}")).toEqual({
      outcome: "merged",
      reason: "done",
    });
    expect(out.lines.join("\n")).toContain("close: ticket-1");
    expect(out.lines.join("\n")).toContain("lifecycle: closed");
    expect(out.lines.join("\n")).toContain("outcome: completed");
  });

  it("prints the raw JSON response with --json", async () => {
    const { base } = await stubMaster(() => ({
      status: 200,
      body: { ok: true, action: "ack", ticket: summary({ lifecycle: "open", endedAt: undefined }) },
    }));
    const out = capture();

    expect(
      await ticketCommand(["ack", "ticket-1", "--master", base, "--json"], { write: out.write }),
    ).toBe(0);

    const parsed = JSON.parse(out.lines[0] ?? "{}") as { ticket?: { id?: string } };
    expect(parsed.ticket?.id).toBe("ticket-1");
  });

  it("surfaces master errors with the HTTP status", async () => {
    const { base } = await stubMaster(() => ({
      status: 409,
      body: { error: "ticket ticket-1 is open; close it before archiving" },
    }));
    const out = capture();
    const errors = capture();

    expect(
      await ticketCommand(["archive", "ticket-1", "--master", base], {
        write: out.write,
        error: errors.write,
      }),
    ).toBe(1);
    expect(errors.lines.join("\n")).toContain("HTTP 409");
    expect(errors.lines.join("\n")).toContain("close it before archiving");
  });
});

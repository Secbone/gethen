import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { parseTaskArgs, taskCommand } from "./commands/task";

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
    let raw = "";
    request.on("data", (chunk: Buffer) => {
      raw += chunk.toString("utf8");
    });
    request.on("end", () => {
      captured.push({ method: request.method, url: request.url, body: raw });
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

describe("gethen task", () => {
  it("parses status/steer/cancel and rejects bad invocations", () => {
    expect(
      parseTaskArgs(["status", "a1", "--master", "http://m:1", "--json", "--timeout-ms", "250"]),
    ).toMatchObject({
      action: "status",
      assignmentId: "a1",
      master: "http://m:1",
      json: true,
      timeoutMs: 250,
    });
    expect(
      parseTaskArgs(["steer", "a1", "focus on tests", "--master", "http://m:1"]),
    ).toMatchObject({ action: "steer", assignmentId: "a1", text: "focus on tests" });
    expect(
      parseTaskArgs(["steer", "a1", "--text", "focus", "--master", "http://m:1"]),
    ).toMatchObject({ text: "focus" });
    expect(
      parseTaskArgs(["cancel", "a1", "--reason", "stop", "--master", "http://m:1"]),
    ).toMatchObject({ assignmentId: "a1", reason: "stop" });
    expect(parseTaskArgs(["cancel", "a1", "--master", "http://m:1"])).not.toHaveProperty("reason");
    expect(parseTaskArgs(["status", "a1"], { GETHEN_MASTER_URL: "http://m:1" }).master).toBe(
      "http://m:1",
    );

    expect(() => parseTaskArgs(["status", "a1"])).toThrow(/--master/);
    expect(() => parseTaskArgs(["status", "--master", "http://m:1"])).toThrow(/assignment id/);
    expect(() => parseTaskArgs(["steer", "a1", "--master", "http://m:1"])).toThrow(/text/);
    expect(() => parseTaskArgs(["bogus", "a1", "--master", "http://m:1"])).toThrow(/subcommand/);
    expect(() =>
      parseTaskArgs(["status", "a1", "--master", "http://m:1", "--timeout-ms", "0"]),
    ).toThrow(/positive integer/);

    expect(
      parseTaskArgs([
        "logs",
        "a1",
        "--master",
        "http://m:1",
        "--limit",
        "2",
        "--after-seq",
        "5",
        "--type",
        "message",
        "--json",
      ]),
    ).toMatchObject({
      action: "logs",
      assignmentId: "a1",
      limit: 2,
      afterSeq: 5,
      type: "message",
      json: true,
    });
    expect(() => parseTaskArgs(["logs", "a1", "--master", "http://m:1", "--limit", "0"])).toThrow(
      /positive integer/,
    );
    expect(() =>
      parseTaskArgs(["logs", "a1", "--master", "http://m:1", "--after-seq", "-1"]),
    ).toThrow(/non-negative integer/);
  });

  it("gets task status from the master and prints text or raw JSON", async () => {
    const statusBody = {
      assignmentId: "a1",
      state: {
        phase: "thinking",
        step: 2,
        seq: 1,
        lastEventAt: "2026-11-17T10:00:00.000Z",
        lastMessage: "planning",
      },
    };
    const stub = await stubMaster(() => ({ status: 200, body: statusBody }));

    const text = capture();
    expect(
      await taskCommand(["status", "a1", "--master", stub.base, "--timeout-ms", "250"], {
        write: text.write,
      }),
    ).toBe(0);
    expect(stub.captured[0]).toMatchObject({
      method: "GET",
      url: "/gethen/tasks/a1?timeoutMs=250",
    });
    const rendered = text.lines.join("\n");
    expect(rendered).toContain("assignment: a1");
    expect(rendered).toContain("phase: thinking");
    expect(rendered).toContain("last message: planning");

    const raw = capture();
    expect(
      await taskCommand(["status", "a1", "--master", stub.base, "--json"], { write: raw.write }),
    ).toBe(0);
    expect(JSON.parse(raw.lines.join("\n"))).toEqual(statusBody);
  });

  it("posts steer/cancel and exits non-zero on HTTP/transport errors", async () => {
    const stub = await stubMaster(() => ({ status: 202, body: { ok: true } }));

    const steerOut = capture();
    expect(
      await taskCommand(["steer", "a1", "focus on tests", "--master", stub.base], {
        write: steerOut.write,
      }),
    ).toBe(0);
    expect(stub.captured[0]).toMatchObject({
      method: "POST",
      url: "/gethen/tasks/a1/steer",
    });
    expect(JSON.parse(stub.captured[0]?.body ?? "{}")).toEqual({ text: "focus on tests" });
    expect(steerOut.lines.join("\n")).toContain("steer sent for a1");

    expect(
      await taskCommand(["cancel", "a1", "--reason", "stop", "--master", stub.base], {
        write: capture().write,
      }),
    ).toBe(0);
    expect(stub.captured[1]).toMatchObject({
      method: "POST",
      url: "/gethen/tasks/a1/cancel",
    });
    expect(JSON.parse(stub.captured[1]?.body ?? "{}")).toEqual({ reason: "stop" });

    const failing = await stubMaster(() => ({
      status: 504,
      body: { error: "worker did not answer", code: "MAILBOX_STATUS_TIMEOUT" },
    }));
    const httpErrors = capture();
    expect(
      await taskCommand(["status", "a1", "--master", failing.base], { error: httpErrors.write }),
    ).toBe(1);
    expect(httpErrors.lines.join("\n")).toContain("504");
    expect(httpErrors.lines.join("\n")).toContain("worker did not answer");

    const badMaster = capture();
    expect(
      await taskCommand(["status", "a1", "--master", "ftp://example.com"], {
        error: badMaster.write,
      }),
    ).toBe(1);
    expect(badMaster.lines.join("\n")).toContain("http(s)");

    const unreachable = capture();
    expect(
      await taskCommand(["status", "a1", "--master", "http://127.0.0.1:1"], {
        error: unreachable.write,
      }),
    ).toBe(1);
    expect(unreachable.lines.join("\n")).toContain("cannot reach master");
  });
  it("reads task logs from the master", async () => {
    const logsBody = {
      assignmentId: "a1",
      events: [
        { seq: 0, type: "message", at: "2026-11-17T10:00:00.000Z", data: { text: "hi" } },
        { seq: 1, type: "tool.start", at: "2026-11-17T10:00:01.000Z" },
      ],
      count: 2,
      dropped: 0,
    };
    const stub = await stubMaster(() => ({ status: 200, body: logsBody }));

    const text = capture();
    expect(
      await taskCommand(["logs", "a1", "--master", stub.base, "--limit", "2", "--after-seq", "0"], {
        write: text.write,
      }),
    ).toBe(0);
    expect(stub.captured[0]?.url).toBe("/gethen/tasks/a1/events?limit=2&afterSeq=0");
    const rendered = text.lines.join("\n");
    expect(rendered).toContain("assignment: a1");
    expect(rendered).toContain("events: 2 of 2 buffered");
    expect(rendered).toContain("#0 message");
    expect(rendered).toContain("#1 tool.start");

    const raw = capture();
    expect(
      await taskCommand(["logs", "a1", "--master", stub.base, "--type", "message", "--json"], {
        write: raw.write,
      }),
    ).toBe(0);
    expect(stub.captured[1]?.url).toBe("/gethen/tasks/a1/events?type=message");
    expect(JSON.parse(raw.lines.join("\n"))).toEqual(logsBody);
  });
});

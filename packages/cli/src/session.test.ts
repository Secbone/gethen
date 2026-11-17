import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { parseSessionArgs, sessionCommand } from "./commands/session";

interface StubResult {
  status: number;
  body: unknown;
}

interface Captured {
  method: string | undefined;
  url: string | undefined;
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
    request.resume();
    request.on("end", () => {
      captured.push({ method: request.method, url: request.url });
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

describe("gethen session", () => {
  it("parses list/history and rejects bad invocations", () => {
    expect(parseSessionArgs(["list", "--master", "http://m:1", "--json"])).toMatchObject({
      action: "list",
      master: "http://m:1",
      json: true,
    });
    expect(
      parseSessionArgs([
        "history",
        "session-1",
        "--master",
        "http://m:1",
        "--limit",
        "5",
        "--after-seq",
        "0",
        "--timeout-ms",
        "250",
      ]),
    ).toMatchObject({
      action: "history",
      sessionId: "session-1",
      limit: 5,
      afterSeq: 0,
      timeoutMs: 250,
    });
    expect(parseSessionArgs(["list"], { GETHEN_MASTER_URL: "http://m:1" }).master).toBe(
      "http://m:1",
    );

    expect(() => parseSessionArgs(["list"])).toThrow(/--master/);
    expect(() => parseSessionArgs(["history", "--master", "http://m:1"])).toThrow(/session id/);
    expect(() => parseSessionArgs(["bogus", "--master", "http://m:1"])).toThrow(/subcommand/);
    expect(() =>
      parseSessionArgs(["history", "s1", "--master", "http://m:1", "--limit", "0"]),
    ).toThrow(/positive integer/);
    expect(() =>
      parseSessionArgs(["history", "s1", "--master", "http://m:1", "--after-seq", "-1"]),
    ).toThrow(/non-negative integer/);
  });

  it("lists sessions and reads history from the master", async () => {
    const at = "2026-11-17T10:00:00.000Z";
    const sessionsBody = {
      sessions: [
        {
          sessionId: "session-1",
          state: "idle",
          key: "project-a",
          workspace: "/ws",
          lastUsedAt: at,
        },
      ],
    };
    const listStub = await stubMaster(() => ({ status: 200, body: sessionsBody }));

    const listOut = capture();
    expect(
      await sessionCommand(["list", "--master", listStub.base, "--timeout-ms", "250"], {
        write: listOut.write,
      }),
    ).toBe(0);
    expect(listStub.captured[0]).toMatchObject({
      method: "GET",
      url: "/gethen/sessions?timeoutMs=250",
    });
    const listText = listOut.lines.join("\n");
    expect(listText).toContain("sessions: 1");
    expect(listText).toContain("session-1");
    expect(listText).toContain("state=idle");
    expect(listText).toContain("key=project-a");

    const listJson = capture();
    expect(
      await sessionCommand(["list", "--master", listStub.base, "--json"], {
        write: listJson.write,
      }),
    ).toBe(0);
    expect(JSON.parse(listJson.lines.join("\n"))).toEqual(sessionsBody);

    const historyBody = {
      sessionId: "session-1",
      at,
      events: [{ seq: 0, type: "message", at, data: { text: "hi" } }],
    };
    const historyStub = await stubMaster(() => ({ status: 200, body: historyBody }));
    const historyOut = capture();
    expect(
      await sessionCommand(
        ["history", "session-1", "--master", historyStub.base, "--limit", "5", "--after-seq", "0"],
        { write: historyOut.write },
      ),
    ).toBe(0);
    expect(historyStub.captured[0]?.url).toBe(
      "/gethen/sessions/session-1/history?limit=5&afterSeq=0",
    );
    const historyText = historyOut.lines.join("\n");
    expect(historyText).toContain("session: session-1");
    expect(historyText).toContain("#0 message");
  });

  it("exits non-zero on HTTP and transport errors", async () => {
    const failing = await stubMaster(() => ({
      status: 504,
      body: { error: "worker did not answer", code: "MAILBOX_STATUS_TIMEOUT" },
    }));
    const httpErrors = capture();
    expect(
      await sessionCommand(["list", "--master", failing.base], { error: httpErrors.write }),
    ).toBe(1);
    expect(httpErrors.lines.join("\n")).toContain("504");
    expect(httpErrors.lines.join("\n")).toContain("worker did not answer");

    const badMaster = capture();
    expect(
      await sessionCommand(["list", "--master", "ftp://example.com"], { error: badMaster.write }),
    ).toBe(1);
    expect(badMaster.lines.join("\n")).toContain("http(s)");

    const unreachable = capture();
    expect(
      await sessionCommand(["list", "--master", "http://127.0.0.1:1"], {
        error: unreachable.write,
      }),
    ).toBe(1);
    expect(unreachable.lines.join("\n")).toContain("cannot reach master");
  });
});

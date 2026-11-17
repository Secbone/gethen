import { mkdtempSync, rmSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { runCommand, runEndpoint } from "./commands/run";
import { type ServeConfig, type ServeHandle, startServe } from "./commands/serve";

const handles: ServeHandle[] = [];
const servers: Server[] = [];
const dirs: string[] = [];

afterEach(async () => {
  while (handles.length > 0) await handles.pop()?.close();
  while (servers.length > 0) {
    const server = servers.pop();
    if (server !== undefined) await new Promise<void>((resolve) => server.close(() => resolve()));
  }
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

async function serve(config: Partial<ServeConfig> = {}): Promise<ServeHandle> {
  const dir = mkdtempSync(join(tmpdir(), "gethen-run-"));
  dirs.push(dir);
  const handle = await startServe({ dbPath: join(dir, "master.sqlite"), port: 0, ...config });
  handles.push(handle);
  return handle;
}

function baseUrl(handle: ServeHandle): string {
  return `http://127.0.0.1:${handle.port}`;
}

function capture(): { lines: string[]; write: (line: string) => void } {
  const lines: string[] = [];
  return { lines, write: (line: string) => lines.push(line) };
}

describe("gethen run", () => {
  it("dispatches runs and prints the summary, raw JSON, or uses GETHEN_MASTER_URL", async () => {
    const handle = await serve();
    const master = baseUrl(handle);

    const summary = capture();
    expect(await runCommand(["--master", master, "hello from run"], { write: summary.write })).toBe(
      0,
    );
    const parsed = JSON.parse(summary.lines.join("\n")) as {
      ticket: string;
      report: string;
      receipt: boolean;
    };
    expect(parsed.ticket).toMatch(/^ticket-/);
    expect(parsed.report).toBe("ready");
    expect(parsed.receipt).toBe(true);

    const raw = capture();
    expect(
      await runCommand(["--master", master, "--text", "second run", "--json"], {
        write: raw.write,
      }),
    ).toBe(0);
    expect(JSON.parse(raw.lines.join("\n"))).toMatchObject({
      stored: "stored",
      outcome: "accept",
      report: "ready",
      receipt: true,
    });

    const fallback = capture();
    expect(
      await runCommand(["fallback run"], {
        write: fallback.write,
        env: { GETHEN_MASTER_URL: master },
      }),
    ).toBe(0);
    expect(JSON.parse(fallback.lines.join("\n"))).toMatchObject({ report: "ready" });
  });

  it("exits non-zero for bad invocations and unreachable masters", async () => {
    const errors: string[] = [];
    const error = (line: string) => errors.push(line);
    const write = () => undefined;

    expect(await runCommand([], { write, error })).toBe(1);
    expect(errors.join("\n")).toContain("--master");

    const handle = await serve();
    const closedPort = handle.port;
    await handle.close();
    handles.pop();

    errors.length = 0;
    expect(
      await runCommand(["--master", `http://127.0.0.1:${closedPort}`, "x"], { write, error }),
    ).toBe(1);
    expect(errors.join("\n")).toContain("cannot reach master");

    errors.length = 0;
    expect(await runCommand(["--master", "ftp://example.com", "x"], { write, error })).toBe(1);
    expect(errors.join("\n")).toContain("http(s)");

    errors.length = 0;
    expect(await runCommand(["--master", "http://127.0.0.1:1"], { write, error })).toBe(1);
    expect(errors.join("\n")).toContain("text is required");

    expect(runEndpoint("http://127.0.0.1:8080")).toBe("http://127.0.0.1:8080/gethen/run");
    expect(runEndpoint("https://master.example.com/api/")).toBe(
      "https://master.example.com/api/gethen/run",
    );
  });

  it("exits non-zero when the worker report is failed", async () => {
    // A master that stores the run but whose worker reported failure.
    const server = createServer((_request, response) => {
      const body = JSON.stringify({
        stored: "stored",
        outcome: "accept",
        ticket: "ticket-1",
        report: "failed",
        receipt: false,
      });
      response.writeHead(200, {
        "content-type": "application/json",
        "content-length": Buffer.byteLength(body),
      });
      response.end(body);
    });
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    const address = server.address();
    const port = typeof address === "object" && address !== null ? address.port : 0;

    const out = capture();
    expect(
      await runCommand(["--master", `http://127.0.0.1:${port}`, "needs a worker"], {
        write: out.write,
      }),
    ).toBe(1);
    expect(JSON.parse(out.lines.join("\n"))).toMatchObject({ report: "failed" });
  });
});

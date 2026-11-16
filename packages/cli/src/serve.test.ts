import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { type ServeHandle, startServe } from "./commands/serve";

const handles: ServeHandle[] = [];

afterEach(async () => {
  while (handles.length > 0) await handles.pop()?.close();
});

async function serve() {
  const dir = mkdtempSync(join(tmpdir(), "gethen-serve-"));
  const handle = await startServe({ dbPath: join(dir, "master.sqlite"), port: 0 });
  handles.push(handle);
  return handle;
}

describe("gethen serve", () => {
  it("exposes health and records a local run", async () => {
    const handle = await serve();
    const base = `http://127.0.0.1:${handle.port}`;

    const health = await fetch(`${base}/health`).then((response) => response.json());
    expect(health).toMatchObject({ ok: true, tickets: 0, launch: "fake" });

    const run = await fetch(`${base}/gethen/run`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: "standalone smoke" }),
    }).then((response) => response.json());
    expect(run).toMatchObject({
      stored: "stored",
      outcome: "accept",
      report: "ready",
      receipt: true,
    });

    const after = await fetch(`${base}/health`).then((response) => response.json());
    expect(after).toMatchObject({ tickets: 1, open: 1, receipts: 1 });
  });

  it("rejects malformed requests", async () => {
    const handle = await serve();
    const base = `http://127.0.0.1:${handle.port}`;
    const bad = await fetch(`${base}/gethen/run`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(bad.status).toBe(400);
    const missing = await fetch(`${base}/nope`);
    expect(missing.status).toBe(404);
  });
});

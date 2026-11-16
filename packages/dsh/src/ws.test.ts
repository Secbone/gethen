import { mkdtempSync, writeFileSync } from "node:fs";
import type { IncomingMessage } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Duplex } from "node:stream";
import { MasterService } from "@gethen/master";
import { afterEach, describe, expect, it } from "vitest";
import {
  apply,
  type DshContext,
  type DshToolDefinition,
  type DshUpgradeRoute,
  type DshWebServer,
  parseWorkerTokens,
  readWorkerTokens,
  WORKER_PATH,
} from "./index";

interface StatusResult {
  tickets: number;
  open: number;
  receipts: number;
  running: number;
  escalations: number;
  launch: "fake" | "remote" | "ws";
  workers: number;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stateDir(): string {
  return mkdtempSync(join(tmpdir(), "gethen-dsh-ws-"));
}

function tokensJson(): string {
  return JSON.stringify([{ workerId: "worker-1", token: "token-1" }]);
}

interface FakeWebServer extends DshWebServer {
  routes: DshUpgradeRoute[];
  disposed: string[];
}

function webServer(): FakeWebServer {
  const routes: DshUpgradeRoute[] = [];
  const disposed: string[] = [];
  return {
    routes,
    disposed,
    registerUpgrade(route) {
      routes.push(route);
      return () => {
        disposed.push(route.path);
        const index = routes.indexOf(route);
        if (index >= 0) routes.splice(index, 1);
      };
    },
  };
}

/** Cleanups registered through fake `ctx.effect`, run after every test. */
const cleanups: Array<() => void> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) cleanup();
  // runtime.close() is started by the effect cleanup; let it settle.
  await delay(20);
});

function context(options: { webServer?: FakeWebServer; listen?: boolean; log?: string[] } = {}) {
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
  if (options.listen !== false) {
    ctx.get = (name) => (name === "webServer" ? options.webServer : undefined);
  }
  if (options.log !== undefined) {
    const log = options.log;
    ctx.logger = { error: (message) => log.push(message) };
  }
  return { ctx, services, tools };
}

function serviceOf(services: Map<string, unknown>): MasterService {
  const service = services.get("gethen");
  if (!(service instanceof MasterService)) throw new Error("gethen service is not a MasterService");
  return service;
}

function tool(tools: DshToolDefinition[], name: string): DshToolDefinition {
  const found = tools.find((candidate) => candidate.name === name);
  if (!found) throw new Error(`missing tool ${name}`);
  return found;
}

function fakeRequest(): IncomingMessage {
  return { headers: {}, url: WORKER_PATH, method: "GET" } as unknown as IncomingMessage;
}

interface FakeSocket {
  socket: Duplex;
  writes: string[];
  isDestroyed(): boolean;
}

function fakeSocket(): FakeSocket {
  const writes: string[] = [];
  let destroyed = false;
  const socket = {
    writable: true,
    get destroyed(): boolean {
      return destroyed;
    },
    write(chunk: unknown): boolean {
      writes.push(String(chunk));
      return true;
    },
    destroy(): void {
      destroyed = true;
    },
    on(): FakeSocket["socket"] {
      return socket as unknown as Duplex;
    },
    once(): FakeSocket["socket"] {
      return socket as unknown as Duplex;
    },
  };
  return { socket: socket as unknown as Duplex, writes, isDestroyed: () => destroyed };
}

const TOOL_NAMES = ["gethen_status", "gethen_run", "gethen_overview", "gethen_escalations"];

describe("worker token config", () => {
  it("parses the gateway token format and rejects malformed input", () => {
    expect(parseWorkerTokens(tokensJson())).toEqual([{ workerId: "worker-1", token: "token-1" }]);
    expect(() => parseWorkerTokens("{not json")).toThrow(/invalid worker tokens JSON/);
    expect(() => parseWorkerTokens("{}")).toThrow(/JSON array/);
    expect(() => parseWorkerTokens(JSON.stringify([{ workerId: "worker-1" }]))).toThrow(
      /needs a non-empty string token/,
    );
    expect(() => parseWorkerTokens(JSON.stringify([{ workerId: "", token: "t" }]))).toThrow(
      /needs a non-empty string workerId/,
    );
  });

  it("reads inline JSON and a tokens file, preferring the file", () => {
    const dir = stateDir();
    const file = join(dir, "tokens.json");
    writeFileSync(file, JSON.stringify([{ workerId: "from-file", token: "file-token" }]));
    expect(readWorkerTokens({ tokensJson: tokensJson() })).toHaveLength(1);
    expect(readWorkerTokens({ tokensFile: file })).toEqual([
      { workerId: "from-file", token: "file-token" },
    ]);
    expect(readWorkerTokens({ tokensFile: file, tokensJson: tokensJson() })).toEqual([
      { workerId: "from-file", token: "file-token" },
    ]);
    expect(readWorkerTokens({})).toEqual([]);
  });
});

describe("WS control-plane mount", () => {
  it("mounts the runtime service and registers the /gethen/worker upgrade route", async () => {
    const server = webServer();
    const { ctx, services, tools } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    serviceOf(services);
    expect(tools.map((entry) => entry.name)).toEqual(TOOL_NAMES);
    expect(server.routes.map((route) => route.path)).toEqual([WORKER_PATH]);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status).toMatchObject({ launch: "ws", workers: 0, tickets: 0, receipts: 0 });
  });

  it("forwards the upgrade to the gateway (unauthenticated upgrades get 401)", async () => {
    const server = webServer();
    const { ctx } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    const route = server.routes[0];
    if (route === undefined) throw new Error("worker upgrade route was not registered");
    const pending = fakeSocket();
    route.handler(fakeRequest(), pending.socket, Buffer.alloc(0));
    await delay(20);

    expect(pending.writes.join("")).toContain("401 Unauthorized");
    expect(pending.isDestroyed()).toBe(true);
  });

  it("disposes the route and closes the runtime on cleanup", () => {
    const server = webServer();
    const { ctx } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    expect(server.routes).toHaveLength(1);
    expect(cleanups).toHaveLength(2);
    cleanups[0]?.();
    expect(server.routes).toHaveLength(0);
    expect(server.disposed).toEqual([WORKER_PATH]);
  });

  it("reads worker credentials from ws.tokensFile", async () => {
    const dir = stateDir();
    const file = join(dir, "tokens.json");
    writeFileSync(file, JSON.stringify([{ workerId: "file-worker", token: "file-token" }]));
    const server = webServer();
    const { ctx, services, tools } = context({ webServer: server });
    apply(ctx, { dir, ws: { tokensFile: file, workspace: dir } });

    serviceOf(services);
    expect(server.routes.map((route) => route.path)).toEqual([WORKER_PATH]);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status.launch).toBe("ws");
  });

  it("falls back to the fake launch when ws has no worker tokens", async () => {
    const server = webServer();
    const { ctx, services, tools } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: {} });

    serviceOf(services);
    expect(server.routes).toHaveLength(0);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status).toMatchObject({ launch: "fake", workers: 0 });
  });

  it("degrades safely when the webServer service is absent", async () => {
    const log: string[] = [];
    const { ctx, services, tools } = context({ listen: false, log });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    serviceOf(services);
    expect(tools.map((entry) => entry.name)).toEqual(TOOL_NAMES);
    expect(log.some((message) => message.includes("webServer"))).toBe(true);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status).toMatchObject({ launch: "ws", workers: 0 });
  });

  it("rejects invalid worker credentials before mounting", () => {
    const server = webServer();
    const { ctx } = context({ webServer: server });
    expect(() => apply(ctx, { dir: stateDir(), ws: { tokensJson: "{not json" } })).toThrow(
      /invalid ws.tokensJson JSON/,
    );
    expect(server.routes).toHaveLength(0);
    // Empty credentials are valid and keep the fake launch (no gateway).
    apply(ctx, { dir: stateDir(), ws: { tokensJson: "[]" } });
    expect(server.routes).toHaveLength(0);
  });
});

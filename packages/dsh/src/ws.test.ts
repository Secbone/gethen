import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
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
  launch: "fake" | "ws";
  workers: number;
  mailbox?: { pending: number; leased: number; dead: number };
}

function stateDir(): string {
  return mkdtempSync(join(tmpdir(), "gethen-dsh-ws-"));
}

function tokensJson(
  entries: ReadonlyArray<{ workerId: string; token: string }> = [
    { workerId: "worker-1", token: "token-1" },
  ],
): string {
  return JSON.stringify(entries);
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

/** Cleanups registered through fake `ctx.effect`/`ctx.inject`, from every test. */
const cleanups: Array<() => void> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) cleanup();
  // runtime.close() is started by the effect cleanup; let it settle.
  await new Promise((resolve) => setTimeout(resolve, 20));
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
    if (options.webServer !== undefined) {
      ctx.inject = (deps, callback) => {
        if (!deps.includes("webServer")) return;
        const dispose = callback(ctx);
        if (typeof dispose === "function") cleanups.push(dispose);
      };
    }
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

const TOOL_NAMES = [
  "gethen_wait",
  "gethen_status",
  "gethen_run",
  "gethen_overview",
  "gethen_escalations",
  "gethen_task",
  "gethen_ticket",
];

describe("worker token config", () => {
  it("parses the worker token format and rejects malformed input", () => {
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

describe("mailbox control-plane mount", () => {
  it("mounts the mailbox runtime and registers the /gethen/worker route", async () => {
    const server = webServer();
    const { ctx, services, tools } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    serviceOf(services);
    expect(tools.map((entry) => entry.name)).toEqual(TOOL_NAMES);
    expect(server.routes.map((route) => route.path)).toEqual([WORKER_PATH]);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status).toMatchObject({
      launch: "ws",
      workers: 0,
      tickets: 0,
      receipts: 0,
      mailbox: { pending: 0, leased: 0, dead: 0 },
      tasks: { count: 0, recent: [] },
    });
  });

  it("gethen_task reports an unanswered assignment as a clear error", async () => {
    const server = webServer();
    const { ctx, tools } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    await expect(
      tool(tools, "gethen_task").execute(
        { action: "status", assignmentId: "assignment-unknown", timeoutMs: 10 },
        {},
      ),
    ).rejects.toThrow(/assignment assignment-unknown did not answer a status request/);
  });

  it("forwards the upgrade to the mailbox transport (unauthenticated gets 401)", async () => {
    const server = webServer();
    const { ctx } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    const route = server.routes[0];
    if (route === undefined) throw new Error("worker upgrade route was not registered");
    const pending = fakeSocket();
    route.handler(fakeRequest(), pending.socket, Buffer.alloc(0));
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(pending.writes.join("")).toContain("401 Unauthorized");
    expect(pending.isDestroyed()).toBe(true);
  });

  it("disposes the route and closes the runtime on cleanup", () => {
    const server = webServer();
    const { ctx } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    expect(server.routes).toHaveLength(1);
    expect(cleanups).toHaveLength(3);
    // Route disposer first, then the runtime close effect.
    cleanups[0]?.();
    expect(server.routes).toHaveLength(0);
    expect(server.disposed).toEqual([WORKER_PATH]);
  });

  it("passes workerId, mailboxDbPath and masterMailbox to the runtime", async () => {
    const dir = stateDir();
    const tokensFile = join(dir, "tokens.json");
    const mailboxDbPath = join(dir, "custom-mailbox.sqlite");
    writeFileSync(tokensFile, tokensJson());
    const server = webServer();
    const { ctx, services, tools } = context({ webServer: server });
    apply(ctx, {
      dir,
      ws: {
        tokensFile,
        workerId: "worker-1",
        mailboxDbPath,
        masterMailbox: "custom:master",
      },
    });

    serviceOf(services);
    expect(existsSync(mailboxDbPath)).toBe(true);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status.launch).toBe("ws");
  });

  it("falls back to the fake launch when the ws config has no tokens", async () => {
    const server = webServer();
    const { ctx, services, tools } = context({ webServer: server });
    apply(ctx, { dir: stateDir(), ws: {} });

    serviceOf(services);
    expect(server.routes).toHaveLength(0);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status).toMatchObject({ launch: "fake", workers: 0 });
    expect(status.mailbox).toBeUndefined();
  });

  it("uses the fake launch and registers no route without any ws config", async () => {
    const server = webServer();
    const { ctx, tools } = context({ webServer: server });
    apply(ctx, { dir: stateDir() });

    expect(server.routes).toHaveLength(0);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status.launch).toBe("fake");
  });

  it("degrades safely when the webServer service is absent", async () => {
    const log: string[] = [];
    const { ctx, services, tools } = context({ listen: false, log });
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokensJson() } });

    serviceOf(services);
    expect(tools.map((entry) => entry.name)).toEqual(TOOL_NAMES);
    expect(log.some((message) => message.includes("webServer"))).toBe(true);
    const status = (await tool(tools, "gethen_status").execute({}, {})) as StatusResult;
    expect(status.launch).toBe("ws");
  });

  it("rejects invalid credentials and invalid mailbox names before mounting", () => {
    const server = webServer();
    const { ctx } = context({ webServer: server });
    expect(() => apply(ctx, { dir: stateDir(), ws: { tokensJson: "{not json" } })).toThrow(
      /invalid ws.tokensJson JSON/,
    );
    expect(server.routes).toHaveLength(0);
    expect(() =>
      apply(ctx, {
        dir: stateDir(),
        ws: { tokensJson: tokensJson(), masterMailbox: "mailbox:main" },
      }),
    ).toThrow(/masterMailbox must be/);
    expect(server.routes).toHaveLength(0);
  });

  it("supports several configured workers and an explicit workerId pin", () => {
    const server = webServer();
    const { ctx } = context({ webServer: server });
    const tokens = tokensJson([
      { workerId: "worker-1", token: "token-1" },
      { workerId: "worker-2", token: "token-2" },
    ]);
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokens } });
    expect(server.routes.map((route) => route.path)).toEqual([WORKER_PATH]);
    apply(ctx, { dir: stateDir(), ws: { tokensJson: tokens, workerId: "worker-2" } });
    expect(server.routes.map((route) => route.path)).toEqual([WORKER_PATH, WORKER_PATH]);
  });
});

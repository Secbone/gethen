import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Policy, Trigger } from "@gethen/master";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  createMasterRuntime,
  DEFAULT_MASTER_POLICY,
  DEFAULT_PACK_DIGEST,
  type MasterRuntime,
  type MasterRuntimeOptions,
} from "./runtime";

const NOW = new Date("2026-11-17T08:00:00.000Z");

let dir: string;
let runtimeCount = 0;
const runtimes: MasterRuntime[] = [];

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "gethen-master-runtime-"));
});

afterEach(async () => {
  while (runtimes.length > 0) {
    const runtime = runtimes.pop();
    if (runtime !== undefined) await runtime.close();
  }
  rmSync(dir, { recursive: true, force: true });
});

function makeRuntime(overrides: Partial<MasterRuntimeOptions> = {}): MasterRuntime {
  runtimeCount += 1;
  const runtime = createMasterRuntime({
    dbPath: join(dir, `runtime-${runtimeCount}`, "master.sqlite"),
    now: () => NOW,
    ...overrides,
  });
  runtimes.push(runtime);
  return runtime;
}

let triggerSeq = 0;

function localTrigger(
  text = "fix the failing test",
  options: { workspace?: string; scope?: string } = {},
): Trigger {
  triggerSeq += 1;
  return {
    id: `trigger-${triggerSeq}`,
    source: "local",
    thread: "local",
    actor: "local-user",
    bot: false,
    addressed: "mention",
    text,
    at: new Date().toISOString(),
    meta: { scope: options.scope ?? "local", workspace: options.workspace ?? "/ws" },
  };
}

/** Trigger whose Assignment keeps the engine's empty workspace, so host defaults apply. */
function defaultedTrigger(): Trigger {
  return localTrigger("fix the failing test", { workspace: "", scope: "" });
}

describe("createMasterRuntime", () => {
  it("uses only the fake launch when no credentials are configured", async () => {
    const runtime = makeRuntime({ workspace: "/ws", packDigest: "sha256:test" });
    expect(runtime.launch).toBe("fake");
    expect(runtime.bridge).toBeUndefined();
    expect(runtime.handleUpgrade).toBeUndefined();
    expect(runtime.health()).toEqual({
      launch: "fake",
      workers: 0,
      tickets: 0,
      open: 0,
      receipts: 0,
      escalations: 0,
      mailbox: { pending: 0, leased: 0, dead: 0 },
    });

    const result = await runtime.service.submit(defaultedTrigger());
    expect(result.decision?.outcome).toBe("accept");
    expect(runtime.health()).toMatchObject({ tickets: 1, open: 1, receipts: 1 });
    expect(runtime.service.store.listReceipts().at(0)).toMatchObject({
      worker: "",
      workspace: "/ws",
      packDigest: "sha256:test",
      outcome: "completed",
    });
    expect(runtime.service.store.policy.current()).toEqual(DEFAULT_MASTER_POLICY);
  });

  it("treats an empty token list as fake mode too", () => {
    const runtime = makeRuntime({ tokens: [] });
    expect(runtime.launch).toBe("fake");
    expect(runtime.bridge).toBeUndefined();
    expect(runtime.handleUpgrade).toBeUndefined();
  });

  it("assembles the mailbox transport stack from in-memory tokens", () => {
    const runtime = makeRuntime({
      tokens: [{ workerId: "worker-a", token: "secret-a" }],
      workspace: "/ws",
    });
    expect(runtime.launch).toBe("ws");
    expect(runtime.bridge).toBeDefined();
    expect(runtime.handleUpgrade).toBeTypeOf("function");
    expect(runtime.bridge?.workerMailbox).toBe("mailbox:worker-a");
    expect(runtime.bridge?.masterMailbox).toBe("mailbox:master");
    expect(runtime.health()).toEqual({
      launch: "ws",
      workers: 0,
      tickets: 0,
      open: 0,
      receipts: 0,
      escalations: 0,
      mailbox: { pending: 0, leased: 0, dead: 0 },
    });
    // The mailbox store defaults next to the master database.
    expect(existsSync(join(dir, `runtime-${runtimeCount}`, "mailbox.sqlite"))).toBe(true);
  });

  it("assembles the stack from a verifyToken callback with an explicit workerId", () => {
    const runtime = makeRuntime({
      verifyToken: async (token) => (token === "secret" ? { workerId: "worker-a" } : undefined),
      workerId: "worker-a",
    });
    expect(runtime.launch).toBe("ws");
    expect(runtime.bridge).toBeDefined();
    expect(runtime.handleUpgrade).toBeTypeOf("function");
    expect(runtime.bridge?.workerMailbox).toBe("mailbox:worker-a");
  });

  it("requires an explicit workerId when more than one token is configured", () => {
    expect(() =>
      makeRuntime({
        tokens: [
          { workerId: "worker-a", token: "secret-a" },
          { workerId: "worker-b", token: "secret-b" },
        ],
      }),
    ).toThrow(/workerId/);

    const runtime = makeRuntime({
      tokens: [
        { workerId: "worker-a", token: "secret-a" },
        { workerId: "worker-b", token: "secret-b" },
      ],
      workerId: "worker-b",
    });
    expect(runtime.bridge?.workerMailbox).toBe("mailbox:worker-b");
  });

  it("requires an explicit workerId with a custom verifyToken", () => {
    expect(() => makeRuntime({ verifyToken: async () => ({ workerId: "worker-a" }) })).toThrow(
      /workerId/,
    );
  });

  it("honours a custom masterMailbox prefix and validates it", () => {
    const runtime = makeRuntime({
      tokens: [{ workerId: "worker-a", token: "secret-a" }],
      masterMailbox: "gethen:master",
    });
    expect(runtime.bridge?.workerMailbox).toBe("gethen:worker-a");
    expect(runtime.bridge?.masterMailbox).toBe("gethen:master");

    expect(() =>
      makeRuntime({
        tokens: [{ workerId: "worker-a", token: "secret-a" }],
        masterMailbox: "mailbox:main",
      }),
    ).toThrow(/masterMailbox/);
    expect(() =>
      makeRuntime({
        tokens: [{ workerId: "worker-a", token: "secret-a" }],
        masterMailbox: "master",
      }),
    ).toThrow(/empty prefix/);
  });

  it("passes a host policy through and keeps the public defaults", () => {
    const policy: Policy = {
      version: 7,
      acceptMode: "observe",
      rules: [{ id: "r1", name: "rule", instruction: "do it", enabled: true }],
      sources: {},
    };
    const runtime = makeRuntime({ policy });
    expect(runtime.service.store.policy.current()).toEqual(policy);
    expect(DEFAULT_MASTER_POLICY).toEqual({
      version: 1,
      acceptMode: "auto",
      rules: [],
      sources: { local: { allowed: ["local-user"], bots: [] } },
    });
    expect(DEFAULT_PACK_DIGEST).toBe("sha256:local-unpinned");
  });
});

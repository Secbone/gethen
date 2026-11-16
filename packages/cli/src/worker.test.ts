import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { normalizeMasterUrl, parseWorkerConfig } from "./commands/worker";

describe("gethen worker config", () => {
  it("builds config from simple flags and normalizes a bare master origin", () => {
    const config = parseWorkerConfig(
      [
        "--master-url",
        "http://127.0.0.1:8080",
        "--token",
        "worker-token",
        "--worker-id",
        "worker-1",
        "--provider",
        "deepseek",
        "--model",
        "deepseek-v4",
        "--workspace",
        "/srv/a",
        "--workspace",
        "/srv/b",
        "--quota",
        "2",
      ],
      { cwd: "/tmp/ignored" },
    );

    expect(config).toEqual({
      masterUrl: "ws://127.0.0.1:8080/gethen/worker",
      token: "worker-token",
      workerId: "worker-1",
      provider: "deepseek",
      model: "deepseek-v4",
      workspaces: ["/srv/a", "/srv/b"],
      quota: 2,
    });
    expect(normalizeMasterUrl("wss://master.example.com/gethen/worker")).toBe(
      "wss://master.example.com/gethen/worker",
    );
    expect(normalizeMasterUrl("http://127.0.0.1:8080/gethen/worker")).toBe(
      "ws://127.0.0.1:8080/gethen/worker",
    );
    expect(() => normalizeMasterUrl("localhost:8080")).toThrow(/masterUrl/);
  });

  it("reads --config and lets flags override the file", () => {
    const dir = mkdtempSync(join(tmpdir(), "gethen-worker-"));
    const path = join(dir, "worker.json");
    writeFileSync(
      path,
      JSON.stringify({
        masterUrl: "wss://master.example.com/gethen/worker",
        token: "file-token",
        workerId: "file-worker",
        machine: "box-1",
        provider: "file-provider",
        model: "file-model",
        workspaces: ["/srv/from-file"],
        quota: 3,
        dshCommand: "/usr/local/bin/dsh",
        dshArgs: ["--profile", "sdk"],
      }),
    );

    try {
      const config = parseWorkerConfig(
        ["--config", path, "--worker-id", "flag-worker", "--token", "flag-token"],
        { cwd: "/tmp/ignored" },
      );
      expect(config).toEqual({
        masterUrl: "wss://master.example.com/gethen/worker",
        token: "flag-token",
        workerId: "flag-worker",
        machine: "box-1",
        provider: "file-provider",
        model: "file-model",
        workspaces: ["/srv/from-file"],
        quota: 3,
        dshCommand: "/usr/local/bin/dsh",
        dshArgs: ["--profile", "sdk"],
      });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("lists missing fields and rejects malformed config files", () => {
    expect(() => parseWorkerConfig(["--master-url", "ws://127.0.0.1:1/gethen/worker"])).toThrow(
      /missing: token.*workerId.*provider.*model/,
    );

    const dir = mkdtempSync(join(tmpdir(), "gethen-worker-"));
    try {
      const broken = join(dir, "broken.json");
      writeFileSync(broken, "not json");
      expect(() => parseWorkerConfig(["--config", broken])).toThrow(/not valid JSON/);

      const partial = join(dir, "partial.json");
      writeFileSync(partial, JSON.stringify({ masterUrl: "ws://127.0.0.1:1/gethen/worker" }));
      expect(() => parseWorkerConfig(["--config", partial])).toThrow(/missing/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  it("passes --dsh-args through, repeatable or comma-separated, over the config file", () => {
    const required = [
      "--master-url",
      "http://127.0.0.1:8080",
      "--token",
      "worker-token",
      "--worker-id",
      "worker-1",
      "--provider",
      "deepseek",
      "--model",
      "deepseek-v4",
    ];

    const repeated = parseWorkerConfig(
      [
        ...required,
        "--dsh-args",
        "--profile",
        "--dsh-args",
        "sdk",
        "--dsh-args",
        "--patch",
        "--dsh-args",
        "/tmp/a.yml",
      ],
      { cwd: "/tmp/ignored" },
    );
    expect(repeated.dshArgs).toEqual(["--profile", "sdk", "--patch", "/tmp/a.yml"]);

    const comma = parseWorkerConfig(
      [...required, "--dsh-args", "--profile,sdk, --patch,/tmp/b.yml "],
      { cwd: "/tmp/ignored" },
    );
    expect(comma.dshArgs).toEqual(["--profile", "sdk", "--patch", "/tmp/b.yml"]);

    const dir = mkdtempSync(join(tmpdir(), "gethen-worker-"));
    try {
      const path = join(dir, "worker.json");
      writeFileSync(
        path,
        JSON.stringify({
          masterUrl: "ws://127.0.0.1:8080/gethen/worker",
          token: "worker-token",
          workerId: "worker-1",
          provider: "deepseek",
          model: "deepseek-v4",
          workspaces: ["/tmp/ignored"],
          dshArgs: ["--profile", "sdk"],
        }),
      );
      const overridden = parseWorkerConfig(["--config", path, "--dsh-args", "--patch,/tmp/c.yml"], {
        cwd: "/tmp/ignored",
      });
      expect(overridden.dshArgs).toEqual(["--patch", "/tmp/c.yml"]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  it("parses --comms and lets it override the config file", () => {
    const required = [
      "--master-url",
      "http://127.0.0.1:8080",
      "--token",
      "worker-token",
      "--worker-id",
      "worker-1",
      "--provider",
      "deepseek",
      "--model",
      "deepseek-v4",
    ];

    const fromFlags = parseWorkerConfig([...required, "--comms", "mailbox"], {
      cwd: "/tmp/ignored",
    });
    expect(fromFlags.comms).toBe("mailbox");

    const dir = mkdtempSync(join(tmpdir(), "gethen-worker-"));
    try {
      const path = join(dir, "worker.json");
      writeFileSync(
        path,
        JSON.stringify({
          comms: "mailbox",
          workspaces: ["/tmp/ignored"],
          dshArgs: ["--profile", "sdk"],
        }),
      );
      const fromFile = parseWorkerConfig([...required, "--config", path], { cwd: "/tmp/ignored" });
      expect(fromFile.comms).toBe("mailbox");

      const overridden = parseWorkerConfig([...required, "--config", path, "--comms", "legacy"], {
        cwd: "/tmp/ignored",
      });
      expect(overridden.comms).toBe("legacy");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects an unknown comms value from flags or config", () => {
    const required = [
      "--master-url",
      "http://127.0.0.1:8080",
      "--token",
      "worker-token",
      "--worker-id",
      "worker-1",
      "--provider",
      "deepseek",
      "--model",
      "deepseek-v4",
    ];
    expect(() => parseWorkerConfig([...required, "--comms", "bogus"])).toThrow(/comms/);

    const dir = mkdtempSync(join(tmpdir(), "gethen-worker-"));
    try {
      const path = join(dir, "worker.json");
      writeFileSync(path, JSON.stringify({ comms: "bogus" }));
      expect(() => parseWorkerConfig([...required, "--config", path])).toThrow(/comms/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

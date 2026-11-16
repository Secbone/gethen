import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { type DoctorReport, doctorCommand, runDoctor } from "./commands/doctor";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function tempDir(prefix = "gethen-doctor-"): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  dirs.push(dir);
  return dir;
}

/** A stand-in runtime that answers `--version` and `--profile sdk --help` with exit 0. */
function fakeDsh(): string {
  const dir = tempDir("gethen-doctor-bin-");
  const path = join(dir, "dsh");
  writeFileSync(path, "#!/bin/sh\necho fake-dsh 1.0.0\n", { mode: 0o755 });
  return path;
}

describe("gethen doctor --json and checks", () => {
  it("keeps the line-oriented output and never fails on warnings", () => {
    const home = tempDir();
    const stateDir = join(home, "missing-state");
    const lines: string[] = [];

    const code = doctorCommand({
      home,
      stateDir,
      env: { GETHEN_DSH: fakeDsh() },
      write: (line) => lines.push(line),
    });

    expect(code).toBe(0);
    expect(lines[0]).toMatch(/^ok\s+node v/);
    for (const line of lines) expect(line).toMatch(/^(ok|warn|info)\s+\S/);
    expect(lines.some((line) => line.startsWith("warn") && line.includes("state"))).toBe(true);
    expect(lines.some((line) => line.startsWith("warn") && line.includes("credentials"))).toBe(
      true,
    );
    expect(lines.some((line) => line.includes(".dsh"))).toBe(true);
  });

  it("reports structured JSON without leaking credential values", () => {
    const home = tempDir();
    mkdirSync(join(home, ".dsh"), { recursive: true });
    const stateDir = tempDir("gethen-doctor-state-");
    const lines: string[] = [];

    const code = doctorCommand({
      home,
      stateDir,
      env: { OPENCODE_API_KEY: "super-secret-key", GETHEN_DSH: fakeDsh() },
      json: true,
      write: (line) => lines.push(line),
    });

    expect(code).toBe(0);
    expect(lines).toHaveLength(1);
    const report = JSON.parse(lines[0] ?? "{}") as DoctorReport;
    const checks = new Map(report.checks.map((check) => [check.name, check]));

    expect(report.ok).toBe(true);
    expect(report.warnings).toBe(0);
    expect(report.stateDir).toBe(stateDir);
    expect(checks.get("node")?.status).toBe("ok");
    expect(checks.get("dsh")?.status).toBe("ok");
    expect(checks.get("dsh")?.message).toContain("fake-dsh 1.0.0");
    expect(checks.get("dsh-sdk")?.status).toBe("ok");
    expect(checks.get("dsh-home")?.status).toBe("ok");
    expect(checks.get("state-dir")?.status).toBe("ok");
    expect(checks.get("credentials")?.status).toBe("ok");
    expect(checks.get("credentials")?.message).toContain("OPENCODE_API_KEY");
    expect(checks.get("gethen-env")?.status).toBe("info");
    expect(lines.join("\n")).not.toContain("super-secret-key");
  });

  it("returns 0 with a warn-only report", () => {
    const home = tempDir();
    const stateDir = join(home, "missing-state");

    const report = runDoctor({ home, stateDir, env: { GETHEN_DSH: fakeDsh() } });

    expect(report.ok).toBe(false);
    expect(report.warnings).toBeGreaterThanOrEqual(1);
    expect(
      doctorCommand({ home, stateDir, env: { GETHEN_DSH: fakeDsh() }, write: () => undefined }),
    ).toBe(0);
  });
});

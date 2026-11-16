import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { doctorCommand } from "./commands/doctor";
import { initCommand } from "./commands/init";
import { run } from "./index";

afterEach(() => {
  vi.restoreAllMocks();
});

function outputOf(spy: { mock: { calls: unknown[][] } }): string {
  return spy.mock.calls.map((call) => String(call[0])).join("");
}

it.each([["help"], ["-h"], ["--help"]])("prints usage for %s", async (command: string) => {
  const stdout = vi.spyOn(process.stdout, "write").mockReturnValue(true);

  expect(await run([command])).toBe(0);

  const output = outputOf(stdout);
  expect(output).toContain("gethen init");
  expect(output).toContain("gethen serve");
  expect(output).toContain("gethen worker");
  expect(output).toContain("gethen doctor");
  expect(output).toContain("gethen help");
  expect(output).not.toContain("bench");
});

it("returns 1 and prints usage to stderr for an unknown command", async () => {
  const stderr = vi.spyOn(process.stderr, "write").mockReturnValue(true);

  expect(await run(["nope"])).toBe(1);

  const output = outputOf(stderr);
  expect(output).toContain("unknown command: nope");
  expect(output).toContain("gethen help");
});

it("creates ~/.gethen idempotently for an injected home", () => {
  const home = mkdtempSync(join(tmpdir(), "gethen-cli-"));
  const first: string[] = [];
  const second: string[] = [];

  try {
    expect(initCommand({ home, write: (line) => first.push(line) })).toBe(0);
    expect(existsSync(join(home, ".gethen"))).toBe(true);

    expect(initCommand({ home, write: (line) => second.push(line) })).toBe(0);
    expect(existsSync(join(home, ".gethen"))).toBe(true);

    expect(first[0]).toContain("created");
    expect(second[0]).toContain("already exists");
  } finally {
    rmSync(home, { recursive: true, force: true });
  }
});

it("doctor reports warnings without failing", () => {
  const home = mkdtempSync(join(tmpdir(), "gethen-cli-"));
  const lines: string[] = [];

  try {
    expect(doctorCommand({ home, write: (line) => lines.push(line) })).toBe(0);
    expect(lines.length).toBeGreaterThanOrEqual(4);
    expect(lines[0]).toMatch(/^(ok|warn|info)\s/);
  } finally {
    rmSync(home, { recursive: true, force: true });
  }
});

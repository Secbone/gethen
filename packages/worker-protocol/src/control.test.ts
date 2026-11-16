import { describe, expect, it } from "vitest";
import { CONTROL_OPS, isWithinRoot, parseControlOp, validateControlOp } from "./control";
import type { ProtocolFailure, Result } from "./frames";

function expectOk<T>(result: Result<T>): T {
  if (!result.ok) throw new Error(`expected ok, got ${result.code}: ${result.message}`);
  return result.value;
}

function expectFail(result: Result<unknown>, code?: string): ProtocolFailure {
  if (result.ok) throw new Error("expected a failure result");
  if (code !== undefined) expect(result.code).toBe(code);
  return result;
}

describe("ControlOp validation", () => {
  it("accepts snapshot and drops unknown extra fields", () => {
    expect(CONTROL_OPS).toEqual([
      "snapshot",
      "runtime.start",
      "runtime.stop",
      "runtime.restart",
      "workspace.prepare",
      "workspace.cleanup",
      "slot.create",
      "slot.close",
      "slot.drain",
      "quota.set",
    ]);
    const op = expectOk(validateControlOp({ op: "snapshot", extra: true }));
    expect(op).toEqual({ op: "snapshot" });
    expect(op).not.toHaveProperty("extra");
  });

  it("accepts whitelisted runtime ops with optional drain", () => {
    for (const name of ["runtime.start", "runtime.stop", "runtime.restart"] as const) {
      const op = expectOk(
        validateControlOp({ op: name, name: "dsh", drain: true }, { allowedRuntimes: ["dsh"] }),
      );
      expect(op).toEqual({ op: name, name: "dsh", drain: true });
    }
    expect(
      expectOk(
        validateControlOp({ op: "runtime.stop", name: "dsh" }, { allowedRuntimes: ["dsh"] }),
      ),
    ).toEqual({ op: "runtime.stop", name: "dsh" });
  });

  it("rejects non-whitelisted or malformed runtime ops", () => {
    expect(
      expectFail(
        validateControlOp({ op: "runtime.start", name: "open-code" }, { allowedRuntimes: ["dsh"] }),
        "runtime_not_allowed",
      ).message,
    ).toContain("open-code");
    // Fail closed: no allowlist configured means no runtime may start.
    expectFail(validateControlOp({ op: "runtime.start", name: "dsh" }), "runtime_not_allowed");
    expectFail(validateControlOp({ op: "runtime.start" }, { allowedRuntimes: ["dsh"] }));
    expectFail(validateControlOp({ op: "runtime.stop", name: "" }, { allowedRuntimes: ["dsh"] }));
    expectFail(
      validateControlOp(
        { op: "runtime.stop", name: "dsh", drain: "yes" },
        { allowedRuntimes: ["dsh"] },
      ),
    );
  });

  it("accepts workspace prepare/cleanup inside the managed root", () => {
    const prepared = expectOk(
      validateControlOp(
        {
          op: "workspace.prepare",
          path: "/srv/managed/repo",
          repo: "git@example.com:org/repo.git",
          branch: "main",
          worktree: true,
        },
        { root: "/srv/managed" },
      ),
    );
    expect(prepared).toEqual({
      op: "workspace.prepare",
      path: "/srv/managed/repo",
      repo: "git@example.com:org/repo.git",
      branch: "main",
      worktree: true,
    });
    expect(
      expectOk(
        validateControlOp(
          { op: "workspace.cleanup", path: "/srv/managed/repo" },
          { root: "/srv/managed" },
        ),
      ),
    ).toEqual({ op: "workspace.cleanup", path: "/srv/managed/repo" });
    // A relative path is resolved under the managed root.
    expect(
      expectOk(
        validateControlOp({ op: "workspace.cleanup", path: "repo/src" }, { root: "/srv/managed" }),
      ),
    ).toEqual({ op: "workspace.cleanup", path: "repo/src" });
  });

  it("rejects workspace ops that escape the managed root", () => {
    const cases: Array<Record<string, unknown>> = [
      { op: "workspace.cleanup", path: "/etc/passwd" },
      { op: "workspace.cleanup", path: "/srv/managed/../etc" },
      { op: "workspace.cleanup", path: "/srv/managed-evil/repo" },
      { op: "workspace.prepare", path: "../outside" },
    ];
    for (const value of cases) {
      expectFail(validateControlOp(value, { root: "/srv/managed" }), "path_outside_root");
    }
    expectFail(
      validateControlOp(
        { op: "workspace.prepare", path: "/srv/managed", repo: 42 },
        { root: "/srv/managed" },
      ),
      "invalid_field",
    );
    // Missing root: workspace ops cannot be validated, so they fail closed.
    expectFail(validateControlOp({ op: "workspace.cleanup", path: "/srv/managed/repo" }));
    expectFail(validateControlOp({ op: "workspace.prepare" }, { root: "/srv/managed" }));
  });

  it("accepts slot ops with validated fields and defaults", () => {
    expect(
      expectOk(
        validateControlOp({
          op: "slot.create",
          workspace: "ws-1",
          pack: "pack-a",
          kind: "resident",
        }),
      ),
    ).toEqual({ op: "slot.create", workspace: "ws-1", pack: "pack-a", kind: "resident" });
    expect(
      expectOk(
        validateControlOp({
          op: "slot.create",
          workspace: "ws-1",
          pack: "pack-a",
          kind: "ephemeral",
          ttlMs: 60_000,
        }),
      ),
    ).toEqual({
      op: "slot.create",
      workspace: "ws-1",
      pack: "pack-a",
      kind: "ephemeral",
      ttlMs: 60_000,
    });
    expect(expectOk(validateControlOp({ op: "slot.close", slot: "s1", force: true }))).toEqual({
      op: "slot.close",
      slot: "s1",
      force: true,
    });
    expect(expectOk(validateControlOp({ op: "slot.drain", slot: "s1" }))).toEqual({
      op: "slot.drain",
      slot: "s1",
    });
  });

  it("rejects malformed slot ops", () => {
    const cases: Array<Record<string, unknown>> = [
      { op: "slot.create", workspace: "ws-1", pack: "pack-a", kind: "big" },
      { op: "slot.create", workspace: "ws-1", pack: "pack-a", kind: "resident", ttlMs: 0 },
      { op: "slot.create", workspace: "ws-1", pack: "pack-a", kind: "resident", ttlMs: "soon" },
      { op: "slot.create", pack: "pack-a", kind: "resident" },
      { op: "slot.close", slot: "s1", force: "yes" },
      { op: "slot.drain" },
    ];
    for (const value of cases) expectFail(validateControlOp(value), "invalid_field");
  });

  it("accepts and validates quota.set", () => {
    expect(expectOk(validateControlOp({ op: "quota.set", slots: 2 }))).toEqual({
      op: "quota.set",
      slots: 2,
    });
    expect(expectOk(validateControlOp({ op: "quota.set", slots: 0, models: ["a", "b"] }))).toEqual({
      op: "quota.set",
      slots: 0,
      models: ["a", "b"],
    });
    for (const slots of [-1, 1.5, "2", Number.NaN]) {
      expectFail(validateControlOp({ op: "quota.set", slots }), "invalid_field");
    }
    expectFail(validateControlOp({ op: "quota.set", slots: 1, models: "a" }));
    expectFail(validateControlOp({ op: "quota.set", slots: 1, models: ["a", 2] }));
  });

  it("rejects unknown ops and non-objects without throwing", () => {
    expect(
      expectFail(validateControlOp({ op: "install-rootkit" }), "unknown_control_op"),
    ).toBeDefined();
    expect(expectFail(validateControlOp({ op: 42 }), "unknown_control_op")).toBeDefined();
    expect(expectFail(validateControlOp({}), "unknown_control_op")).toBeDefined();
    for (const value of [null, [], "snapshot", 42]) {
      expectFail(parseControlOp(value), "invalid_field");
    }
    expect(() => validateControlOp({ op: "runtime.start", name: "dsh" })).not.toThrow();
  });
});

describe("isWithinRoot", () => {
  it("accepts the root itself and paths below it", () => {
    expect(isWithinRoot("/srv/managed", "/srv/managed")).toBe(true);
    expect(isWithinRoot("/srv/managed/", "/srv/managed")).toBe(true);
    expect(isWithinRoot("/srv/managed/repo/src", "/srv/managed")).toBe(true);
    expect(isWithinRoot("/srv/managed/./repo", "/srv/managed")).toBe(true);
    expect(isWithinRoot("/srv/managed/repo/../other", "/srv/managed")).toBe(true);
    expect(isWithinRoot("repo/src", "/srv/managed")).toBe(true);
    expect(isWithinRoot("/", "/")).toBe(true);
    expect(isWithinRoot("/etc/hosts", "/")).toBe(true);
  });

  it("rejects escapes, siblings and invalid roots", () => {
    expect(isWithinRoot("/srv/managed/../etc/passwd", "/srv/managed")).toBe(false);
    expect(isWithinRoot("../etc/passwd", "/srv/managed")).toBe(false);
    expect(isWithinRoot("/srv/managed-evil/repo", "/srv/managed")).toBe(false);
    expect(isWithinRoot("/etc/passwd", "/srv/managed")).toBe(false);
    expect(isWithinRoot("", "/srv/managed")).toBe(false);
    expect(isWithinRoot("/srv/managed", "")).toBe(false);
    expect(isWithinRoot("/srv/managed", "relative/root")).toBe(false);
    expect(isWithinRoot("/srv/managed/repo", "/srv/managed\0x")).toBe(false);
  });

  it("handles Windows-style absolute paths without filesystem access", () => {
    expect(isWithinRoot("C:\\srv\\managed\\repo", "C:\\srv\\managed")).toBe(true);
    expect(isWithinRoot("c:/srv/managed/repo", "C:/srv/managed")).toBe(true);
    expect(isWithinRoot("C:\\srv\\managed\\..\\etc", "C:\\srv\\managed")).toBe(false);
    expect(isWithinRoot("D:\\srv\\managed", "C:\\srv\\managed")).toBe(false);
    expect(isWithinRoot("C:relative", "C:/srv/managed")).toBe(false);
  });
});

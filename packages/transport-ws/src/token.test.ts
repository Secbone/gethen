import { describe, expect, it } from "vitest";
import { createMemoryTokenStore, hashWorkerToken } from "./token";

describe("createMemoryTokenStore", () => {
  it("resolves each configured token to its worker", async () => {
    const store = createMemoryTokenStore([
      { workerId: "worker-1", token: "secret-one" },
      { workerId: "worker-2", token: "secret-two" },
    ]);

    await expect(store.lookup("secret-one")).resolves.toEqual({ workerId: "worker-1" });
    await expect(store.lookup("secret-two")).resolves.toEqual({ workerId: "worker-2" });
  });

  it("does not match prefixes, unknown or empty tokens", async () => {
    const store = createMemoryTokenStore([{ workerId: "worker-1", token: "secret" }]);

    await expect(store.lookup("secret-long")).resolves.toBeUndefined();
    await expect(store.lookup("secre")).resolves.toBeUndefined();
    await expect(store.lookup("SECRET")).resolves.toBeUndefined();
    await expect(store.lookup("")).resolves.toBeUndefined();
  });

  it("rejects empty records and duplicate tokens eagerly", () => {
    expect(() => createMemoryTokenStore([{ workerId: "", token: "x" }])).toThrow(/workerId/);
    expect(() => createMemoryTokenStore([{ workerId: "w", token: "" }])).toThrow(/token/);
    expect(() =>
      createMemoryTokenStore([
        { workerId: "a", token: "same" },
        { workerId: "b", token: "same" },
      ]),
    ).toThrow(/duplicate/);
  });

  it("exposes the sha256 digest a durable store persists", () => {
    expect(hashWorkerToken("secret")).toBe(
      "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b",
    );
  });
});

import { describe, expect, it } from "vitest";
import { createMemoryTokenStore, hashToken } from "./auth";

describe("transport auth helpers", () => {
  it("hashes tokens deterministically without keeping the plaintext", async () => {
    expect(hashToken("abc")).toBe(hashToken("abc"));
    expect(hashToken("abc")).not.toBe(hashToken("abd"));
    const store = createMemoryTokenStore([{ workerId: "w1", token: "secret" }]);
    expect(await store.lookup("secret")).toEqual({ workerId: "w1" });
    expect(await store.lookup("wrong")).toBeUndefined();
  });

  it("resolves the last matching record and rejects empty input", async () => {
    const store = createMemoryTokenStore([
      { workerId: "w1", token: "same" },
      { workerId: "w2", token: "same" },
    ]);
    expect(await store.lookup("same")).toEqual({ workerId: "w2" });
    expect(await createMemoryTokenStore([]).lookup("x")).toBeUndefined();
  });
});

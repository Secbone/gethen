import { describe, expect, it, vi } from "vitest";
import { JsonRpcPeer, JsonRpcResponseError, JsonRpcTimeoutError } from "./rpc";
import { FakeStream } from "./test-doubles";

function pair(requestTimeoutMs?: number) {
  const input = new FakeStream();
  const output = new FakeStream();
  const peer = new JsonRpcPeer({
    input,
    output,
    ...(requestTimeoutMs === undefined ? {} : { requestTimeoutMs }),
  });
  return { input, output, peer };
}

function frameById(frames: Record<string, unknown>[], id: unknown): Record<string, unknown> {
  const frame = frames.find((item) => item.id === id && item.method === undefined);
  if (frame === undefined) throw new Error(`no frame with id ${String(id)}`);
  return frame;
}

async function tick(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

describe("JsonRpcPeer", () => {
  it("writes newline-framed requests with increasing ids and settles out of order", async () => {
    const { input, output, peer } = pair(1_000);

    const first = peer.request<{ ok: boolean }>("initialize", { provider: "p" });
    const second = peer.request("session/wait", { sessionId: "s1" });

    const frames = output.frames();
    expect(frames).toHaveLength(2);
    expect(frames[0]).toMatchObject({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { provider: "p" },
    });
    expect(frames[1]).toMatchObject({
      jsonrpc: "2.0",
      id: 2,
      method: "session/wait",
      params: { sessionId: "s1" },
    });
    expect(output.written.join("").endsWith("\n")).toBe(true);
    expect(peer.pendingCount).toBe(2);

    input.emit("data", `${JSON.stringify({ jsonrpc: "2.0", id: 2, result: {} })}\n`);
    input.emit("data", `${JSON.stringify({ jsonrpc: "2.0", id: 1, result: { ok: true } })}\n`);

    await expect(first).resolves.toEqual({ ok: true });
    await expect(second).resolves.toEqual({});
    expect(peer.pendingCount).toBe(0);

    const split = peer.request("session/prompt", { sessionId: "s1" });
    input.emit("data", '{"jsonrpc":"2.0","id":3,"result":');
    input.emit("data", '{"messageId":"m1"}}\n');
    await expect(split).resolves.toEqual({ messageId: "m1" });
    peer.close();
  });

  it("dispatches notifications in both directions and ignores malformed lines", async () => {
    const { input, output, peer } = pair();
    const seen: Array<[string, unknown]> = [];
    peer.onNotification((method, params) => {
      seen.push([method, params]);
    });

    peer.notify("session.event", { sessionId: "s1" });
    expect(output.frames()).toEqual([
      { jsonrpc: "2.0", method: "session.event", params: { sessionId: "s1" } },
    ]);

    input.emit("data", "not json at all\n");
    input.emit("data", "[]\n");
    expect(seen).toEqual([]);

    input.emit(
      "data",
      `${JSON.stringify({ jsonrpc: "2.0", method: "session.status", params: { status: "idle" } })}\n`,
    );
    expect(seen).toEqual([["session.status", { status: "idle" }]]);
    peer.close();
  });

  it("preserves the error code and data of an error response", async () => {
    const { input, peer } = pair();
    const pending = peer.request("session/query", { sessionId: "s1" });

    input.emit(
      "data",
      `${JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        error: { code: -32602, message: "invalid params", data: { field: "contentBlocks" } },
      })}\n`,
    );

    const error: unknown = await pending.catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(JsonRpcResponseError);
    expect(error).toMatchObject({
      name: "JsonRpcResponseError",
      code: -32602,
      message: "invalid params",
      data: { field: "contentBlocks" },
    });
    peer.close();
  });

  it("times out a request and ignores a late response", async () => {
    vi.useFakeTimers();
    try {
      const { input, peer } = pair(100);
      const pending = peer.request("session/wait", { sessionId: "s1" });
      const rejection = expect(pending).rejects.toBeInstanceOf(JsonRpcTimeoutError);
      vi.advanceTimersByTime(100);
      await rejection;
      expect(peer.pendingCount).toBe(0);

      input.emit("data", `${JSON.stringify({ jsonrpc: "2.0", id: 1, result: {} })}\n`);
      expect(peer.pendingCount).toBe(0);
      peer.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("answers server requests and reports unknown or failed methods", async () => {
    const { input, output, peer } = pair();
    peer.onRequest((method) => {
      if (method === "approval/request") return Promise.resolve({ outcome: "allowed-once" });
      throw new Error("boom");
    });

    input.emit(
      "data",
      `${JSON.stringify({ jsonrpc: "2.0", id: 7, method: "approval/request", params: {} })}\n`,
    );
    input.emit("data", `${JSON.stringify({ jsonrpc: "2.0", id: 8, method: "other/thing" })}\n`);
    await tick();

    const frames = output.frames();
    expect(frameById(frames, 7).result).toEqual({ outcome: "allowed-once" });
    expect(frameById(frames, 8).error).toEqual({ code: -32603, message: "boom" });

    peer.onRequest(undefined);
    input.emit("data", `${JSON.stringify({ jsonrpc: "2.0", id: 9, method: "other/thing" })}\n`);
    await tick();
    expect(frameById(output.frames(), 9).error).toEqual({
      code: -32601,
      message: "Method not found: other/thing",
    });
    peer.close();
  });

  it("rejects pending and later requests when closed or the input ends", async () => {
    const first = pair();
    expect(first.input.listenerCount("data")).toBe(1);
    const pending = first.peer.request("initialize", { provider: "p" });
    first.peer.close();
    expect(first.input.listenerCount("data")).toBe(0);
    await expect(pending).rejects.toThrow(/closed/);
    await expect(first.peer.request("initialize", { provider: "p" })).rejects.toThrow(/closed/);
    expect(first.peer.isClosed).toBe(true);

    const second = pair();
    const waiting = second.peer.request("session/wait", { sessionId: "s1" });
    second.input.emit("end");
    await expect(waiting).rejects.toThrow(/ended/);
    expect(second.peer.isClosed).toBe(true);
  });

  it("rejects each pending request when the output write fails", async () => {
    const input = new FakeStream();
    const output = new FakeStream();
    output.write = (): boolean => {
      throw new Error("EPIPE");
    };
    const peer = new JsonRpcPeer({ input, output });
    const first = peer.request("initialize", { provider: "p" });
    const second = peer.request("session/wait", { sessionId: "s1" });

    await expect(first).rejects.toThrow(/EPIPE/);
    await expect(second).rejects.toThrow(/EPIPE/);
    expect(peer.isClosed).toBe(true);
  });

  it("aborts a pending request and ignores its late response", async () => {
    const { input, peer } = pair();
    const controller = new AbortController();
    const pending = peer.request(
      "session/wait",
      { sessionId: "s1" },
      {
        signal: controller.signal,
      },
    );
    expect(peer.pendingCount).toBe(1);

    const rejection = expect(pending).rejects.toMatchObject({ name: "AbortError" });
    controller.abort();
    await rejection;
    expect(peer.pendingCount).toBe(0);

    input.emit("data", `${JSON.stringify({ jsonrpc: "2.0", id: 1, result: {} })}\n`);
    expect(peer.pendingCount).toBe(0);
    peer.close();
  });

  it("rejects immediately when the signal is already aborted", async () => {
    const { peer } = pair();
    const controller = new AbortController();
    controller.abort();
    await expect(
      peer.request("session/wait", { sessionId: "s1" }, { signal: controller.signal }),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(peer.pendingCount).toBe(0);
    peer.close();
  });
});

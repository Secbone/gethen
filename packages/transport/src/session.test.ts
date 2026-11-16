import { describe, expect, it } from "vitest";
import {
  createTransport,
  TRANSPORT_CLOSE_CODES,
  type TransportConfig,
  TransportError,
  wsTransportCapabilities,
} from "./index";

describe("@gethen/transport contract", () => {
  it("negotiates ws capabilities through createTransport", () => {
    const transport = createTransport({ driver: "ws", heartbeat: false });

    expect(transport.capabilities).toEqual({
      driver: "ws",
      framing: "message",
      binary: true,
      heartbeat: false,
      backpressure: true,
      resume: false,
    });
    expect(wsTransportCapabilities()).toMatchObject({ driver: "ws", heartbeat: true });
  });

  it("rejects an unsupported driver instead of guessing", () => {
    const unsupported = { driver: "nats" } as unknown as TransportConfig;

    expect(() => createTransport(unsupported)).toThrow(TransportError);
    try {
      createTransport(unsupported);
    } catch (error) {
      expect(error).toBeInstanceOf(TransportError);
      expect((error as TransportError).code).toBe("unsupported_driver");
    }
  });

  it("rejects mTLS auth in the ws adapter as unsupported_auth", async () => {
    const transport = createTransport({ driver: "ws", heartbeat: false });

    await expect(
      transport.connect({ url: "ws://127.0.0.1:1" }, { kind: "mtls" }),
    ).rejects.toMatchObject({ name: "TransportError", code: "unsupported_auth" });
  });

  it("rejects an empty bearer token before dialing", async () => {
    const transport = createTransport({ driver: "ws", heartbeat: false });

    await expect(
      transport.connect({ url: "ws://127.0.0.1:1" }, { kind: "bearer", token: "" }),
    ).rejects.toMatchObject({ code: "invalid_option" });
  });

  it("keeps the failure cause on TransportError", () => {
    const cause = new Error("socket exploded");
    const error = new TransportError("connect_failed", "could not connect", { cause });

    expect(error.name).toBe("TransportError");
    expect(error.code).toBe("connect_failed");
    expect(error.message).toBe("connect_failed: could not connect");
    expect((error as Error & { cause?: unknown }).cause).toBe(cause);
  });

  it("exposes stable close codes for shutdown and adapter rejections", () => {
    expect(TRANSPORT_CLOSE_CODES.normal).toBe(1000);
    expect(TRANSPORT_CLOSE_CODES.goingAway).toBe(1001);
    expect(TRANSPORT_CLOSE_CODES.abnormal).toBe(1006);
    expect(TRANSPORT_CLOSE_CODES.authFailed).toBe(4001);
    expect(TRANSPORT_CLOSE_CODES.heartbeatTimeout).toBe(4002);
    expect(TRANSPORT_CLOSE_CODES.backpressure).toBe(4003);
  });
});

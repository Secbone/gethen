import { describe, expect, it } from "vitest";

import {
  assertValidEnvelope,
  type Envelope,
  EnvelopeError,
  isEnvelope,
  newEnvelope,
  validateEnvelope,
} from "./envelope";

const AT = "2026-11-17T00:00:00.000Z";

describe("newEnvelope", () => {
  it("fills specversion, datacontenttype, id and time defaults", () => {
    const envelope = newEnvelope({
      source: "agent:router-1",
      type: "gethen.task.assign",
      data: { task: 1 },
    });
    expect(envelope.specversion).toBe("1.0");
    expect(envelope.datacontenttype).toBe("application/json");
    expect(envelope.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    expect(Number.isFinite(Date.parse(envelope.time))).toBe(true);
    expect(envelope.data).toEqual({ task: 1 });
  });

  it("copies optional attributes only when provided", () => {
    const envelope = newEnvelope({
      id: "env-1",
      source: "agent:router-1",
      type: "gethen.task.assign",
      time: AT,
      data: null,
      subject: "ticket-1",
      thread: "thread-1",
      correlationId: "corr-1",
      causationId: "cause-1",
      to: "agent:worker-1",
      replyTo: "reply:corr-1",
      deadline: AT,
      priority: 9,
      ttlMs: 1_000,
    });
    expect(envelope).toEqual({
      specversion: "1.0",
      id: "env-1",
      source: "agent:router-1",
      type: "gethen.task.assign",
      time: AT,
      datacontenttype: "application/json",
      data: null,
      subject: "ticket-1",
      thread: "thread-1",
      correlationId: "corr-1",
      causationId: "cause-1",
      to: "agent:worker-1",
      replyTo: "reply:corr-1",
      deadline: AT,
      priority: 9,
      ttlMs: 1_000,
    });

    const minimal = newEnvelope({
      id: "env-2",
      source: "agent:router-1",
      type: "t",
      time: AT,
      data: {},
    });
    expect(minimal).not.toHaveProperty("to");
    expect(minimal).not.toHaveProperty("priority");
    expect(minimal).not.toHaveProperty("ttlMs");
  });

  it("rejects bad priority, ttl and time", () => {
    expect(() => newEnvelope({ source: "s", type: "t", data: null, priority: 10 })).toThrow(
      EnvelopeError,
    );
    expect(() => newEnvelope({ source: "s", type: "t", data: null, ttlMs: 0 })).toThrow(/ttlMs/);
    expect(() => newEnvelope({ source: "s", type: "t", data: null, time: "tomorrow" })).toThrow(
      /time/,
    );
  });
});

describe("validateEnvelope", () => {
  const valid = newEnvelope({
    id: "env-1",
    source: "agent:a",
    type: "t",
    time: AT,
    data: { ok: true },
  });

  it("accepts a valid envelope and rejects non-objects", () => {
    expect(validateEnvelope(valid)).toEqual({ valid: true, errors: [] });
    expect(validateEnvelope(null).valid).toBe(false);
    expect(validateEnvelope("nope").valid).toBe(false);
    expect(validateEnvelope([]).valid).toBe(false);
  });

  it("reports every missing required attribute", () => {
    const { valid: isValid, errors } = validateEnvelope({ specversion: "1.0" });
    expect(isValid).toBe(false);
    expect(errors).toEqual(
      expect.arrayContaining([
        "id must be a non-empty string",
        "source must be a non-empty string",
        "type must be a non-empty string",
        "data is required",
        "time must be an RFC 3339 timestamp",
      ]),
    );
  });

  it("rejects empty ids and non-RFC 3339 timestamps", () => {
    expect(validateEnvelope({ ...valid, id: " " }).errors).toContain(
      "id must be a non-empty string",
    );
    expect(validateEnvelope({ ...valid, time: "2026-11-17" }).errors).toContain(
      "time must be an RFC 3339 timestamp",
    );
    expect(validateEnvelope({ ...valid, deadline: "tomorrow" }).errors).toContain(
      "deadline must be an RFC 3339 timestamp when present",
    );
  });

  it("rejects non-string optional fields, bad priority and non-positive ttl", () => {
    expect(validateEnvelope({ ...valid, to: 5 }).errors).toContain(
      "to must be a string when present",
    );
    expect(validateEnvelope({ ...valid, correlationId: false }).errors).toContain(
      "correlationId must be a string when present",
    );
    expect(validateEnvelope({ ...valid, priority: 1.5 }).errors).toContain(
      "priority must be an integer between 0 and 9",
    );
    expect(validateEnvelope({ ...valid, priority: -1 }).valid).toBe(false);
    expect(validateEnvelope({ ...valid, ttlMs: -1 }).errors).toContain(
      "ttlMs must be a positive finite number",
    );
    expect(validateEnvelope({ ...valid, ttlMs: Number.NaN }).valid).toBe(false);
  });
});

describe("assertValidEnvelope / isEnvelope", () => {
  it("throws EnvelopeError with all problems and narrows valid values", () => {
    try {
      assertValidEnvelope({ specversion: "1.0" });
      throw new Error("expected assertValidEnvelope to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(EnvelopeError);
      expect((error as EnvelopeError).errors.length).toBeGreaterThan(0);
    }
    const envelope: Envelope = newEnvelope({ source: "s", type: "t", data: null });
    expect(isEnvelope(envelope)).toBe(true);
    expect(isEnvelope({ specversion: "1.0" })).toBe(false);
  });
});

import { describe, expect, it } from "vitest";

import { type Envelope, EnvelopeError, newEnvelope } from "./envelope";
import { MailboxClient, type MailboxClientOptions, MailboxTimeoutError } from "./mailbox";
import { MemoryMailboxStore } from "./store-memory";

const AT = "2026-11-17T00:00:00.000Z";

let sequence = 0;

function envelope(overrides: Partial<Envelope> = {}): Envelope {
  sequence += 1;
  return newEnvelope({
    id: `env-${sequence}`,
    source: "agent:router-1",
    type: "gethen.task.assign",
    time: AT,
    data: { ok: true },
    ...overrides,
  });
}

function plus(base: string, milliseconds: number): string {
  return new Date(Date.parse(base) + milliseconds).toISOString();
}

function defined<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("expected a value");
  return value;
}

interface Harness {
  store: MemoryMailboxStore;
  client: MailboxClient;
  peer: MailboxClient;
  advance: (milliseconds: number) => void;
}

function createClients(options: Partial<MailboxClientOptions> = {}): Harness {
  let current = Date.parse(AT);
  const now = () => new Date(current);
  const store = new MemoryMailboxStore({ now });
  return {
    store,
    client: new MailboxClient({ store, now, ...options }),
    peer: new MailboxClient({ store, now, ...options }),
    advance: (milliseconds: number) => {
      current += milliseconds;
    },
  };
}

async function waitFor(condition: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("condition not met in time");
    await new Promise((resolve) => {
      setTimeout(resolve, 5);
    });
  }
}

describe("MailboxClient send", () => {
  it("routes by `to` and validates envelopes", () => {
    const { client } = createClients();
    const message = envelope({ id: "e1", to: "agent:worker-1" });
    expect(client.send(message)).toEqual({ mailbox: "agent:worker-1", duplicate: false });
    expect(client.receive("agent:worker-1").map((received) => received.envelope.id)).toEqual([
      "e1",
    ]);

    expect(() => client.send(envelope({ id: "e2" }))).toThrow(EnvelopeError);
    const invalid = { ...message, specversion: "0.9" } as unknown as Envelope;
    expect(() => client.send(invalid)).toThrow(EnvelopeError);
  });

  it("dedupes a repeated send through the store", () => {
    const { client } = createClients();
    const message = envelope({ id: "e1", to: "agent:worker-1" });
    expect(client.send(message).duplicate).toBe(false);
    expect(client.send({ ...message, data: { ok: false } }).duplicate).toBe(true);
    expect(client.receive("agent:worker-1")).toHaveLength(1);
  });
});

describe("MailboxClient request/reply", () => {
  it("correlates the reply with the request", async () => {
    const { client, peer } = createClients({ visibilityTimeoutMs: 5_000, pollIntervalMs: 5 });
    const request = envelope({
      id: "req-1",
      to: "agent:worker-1",
      thread: "thread-1",
      data: { task: 1 },
    });
    const pending = client.request(request, { timeoutMs: 1_000, pollIntervalMs: 2 });

    const delivered = peer.receive("agent:worker-1");
    expect(delivered).toHaveLength(1);
    const requestEnvelope = defined(delivered[0]).envelope;
    expect(requestEnvelope.correlationId).toBeTypeOf("string");
    expect(requestEnvelope.replyTo).toMatch(/^reply:/);
    expect(requestEnvelope.thread).toBe("thread-1");

    peer.reply(
      requestEnvelope,
      envelope({ id: "rep-1", type: "gethen.task.result", data: { ok: true } }),
    );
    peer.ack("agent:worker-1", [requestEnvelope.id]);

    const reply = await pending;
    expect(reply).toMatchObject({
      id: "rep-1",
      type: "gethen.task.result",
      correlationId: requestEnvelope.correlationId,
      causationId: "req-1",
      to: requestEnvelope.replyTo,
      thread: "thread-1",
    });
    expect(client.receive(defined(requestEnvelope.replyTo))).toEqual([]);
  });

  it("times out with MailboxTimeoutError and honours an envelope deadline", async () => {
    const { client } = createClients();
    const error = await client
      .request(envelope({ id: "req-1", to: "agent:worker-1" }), {
        timeoutMs: 25,
        pollIntervalMs: 2,
      })
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(MailboxTimeoutError);
    expect((error as MailboxTimeoutError).correlationId).toBeTypeOf("string");
    expect((error as MailboxTimeoutError).timeoutMs).toBe(25);

    await expect(
      client.request(
        envelope({
          id: "req-2",
          to: "agent:worker-1",
          deadline: "2020-01-01T00:00:00.000Z",
        }),
        { timeoutMs: 5_000 },
      ),
    ).rejects.toBeInstanceOf(MailboxTimeoutError);
  });

  it("reply routes to replyTo and requires it", () => {
    const { client } = createClients();
    const request = envelope({ id: "req-1", to: "agent:worker-1" });
    expect(() => client.reply(request, envelope({ id: "rep-1" }))).toThrow(/replyTo/);

    const routedRequest: Envelope = {
      ...request,
      replyTo: "reply:r1",
      correlationId: "corr-1",
    };
    expect(
      client.reply(routedRequest, envelope({ id: "rep-1", type: "gethen.task.result" })),
    ).toEqual({ mailbox: "reply:r1", duplicate: false });
    const reply = defined(client.receive("reply:r1")[0]);
    expect(reply.envelope).toMatchObject({
      to: "reply:r1",
      correlationId: "corr-1",
      causationId: "req-1",
    });
  });
});

describe("MailboxClient subscribe", () => {
  it("handles a batch, acks it and advances the durable cursor", async () => {
    const { client, store } = createClients({ dedupeWindowMs: 0 });
    client.send(envelope({ id: "e1", to: "topic:builds" }));
    client.send(envelope({ id: "e2", to: "topic:builds" }));

    const handled: string[] = [];
    const handle = client.subscribe(
      "builds",
      (message) => {
        handled.push(message.id);
      },
      { name: "consumer", autoStart: false },
    );
    await expect(handle.runOnce()).resolves.toBe(2);
    expect(handled).toEqual(["e1", "e2"]);
    expect(handle.cursor()).toBe("e2");
    expect(store.getSubscription("builds", "consumer")).toMatchObject({ cursor: "e2" });
    expect(client.receive("topic:builds")).toEqual([]);
  });

  it("polls automatically when autoStart is on", async () => {
    const { client } = createClients({ pollIntervalMs: 5 });
    client.send(envelope({ id: "e1", to: "topic:builds" }));
    client.send(envelope({ id: "e2", to: "topic:builds" }));

    const handled: string[] = [];
    const handle = client.subscribe(
      "builds",
      (message) => {
        handled.push(message.id);
      },
      { name: "auto", pollIntervalMs: 5 },
    );
    await waitFor(() => handled.length === 2);
    handle.stop();
    expect(handled).toEqual(["e1", "e2"]);
    expect(handle.cursor()).toBe("e2");
    expect(handle).toMatchObject({ topic: "builds", name: "auto", mailbox: "topic:builds" });
  });

  it("nacks failed handlers and dead-letters after maxAttempts", async () => {
    const { client } = createClients({ maxAttempts: 1, retryDelayMs: 0, dedupeWindowMs: 0 });
    client.send(envelope({ id: "bad", to: "topic:builds" }));

    const errors: unknown[] = [];
    const handle = client.subscribe(
      "builds",
      () => {
        throw new Error("boom");
      },
      {
        name: "failing",
        autoStart: false,
        onError: (error) => {
          errors.push(error);
        },
      },
    );
    await expect(handle.runOnce()).resolves.toBe(0);
    expect(errors).toHaveLength(1);
    expect(client.deadLetters("topic:builds").map((item) => item.envelope.id)).toEqual(["bad"]);
  });
});

describe("MailboxClient delivery defaults", () => {
  it("applies visibility, dedupe and retry defaults and passes stats/purge through", () => {
    const { client, advance } = createClients({
      visibilityTimeoutMs: 1_000,
      dedupeWindowMs: 5_000,
      receiveBatchSize: 5,
      maxAttempts: 3,
    });
    client.send(envelope({ id: "e1", to: "agent:a" }));
    client.send(envelope({ id: "e2", to: "agent:a" }));

    const received = client.receive("agent:a");
    expect(received).toHaveLength(2);
    expect(received[0]?.leaseUntil).toBe(plus(AT, 1_000));
    const e1 = defined(received[0]).envelope.id;
    expect(client.renewLeases("agent:a", [e1], 4_000)).toBe(1);

    advance(1_500);
    const redelivered = client.receive("agent:a");
    expect(redelivered.map((message) => message.envelope.id)).toEqual(["e2"]);
    expect(redelivered[0]?.attempts).toBe(2);
    expect(client.ack("agent:a", [e1])).toBe(1);
    expect(client.stats("agent:a")).toMatchObject({
      ready: 0,
      leased: 1,
      done: 1,
      pending: 1,
    });
    expect(client.purge()).toEqual({ purged: 0 });
    expect(client.deadLetters("agent:a")).toEqual([]);
    expect(client.listSubscriptions()).toEqual([]);
  });
});

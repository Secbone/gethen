import {
  type Envelope,
  MemoryMailboxStore,
  type NewEnvelopeInput,
  newEnvelope,
} from "@gethen/mailbox";
import type { TransportMessage, TransportSession, TransportSessionState } from "@gethen/transport";
import { describe, expect, it } from "vitest";
import {
  createDetachedMailboxClientLink,
  createMailboxClientLink,
  createMailboxServerLink,
  MAILBOX_LINK_VERSION,
  type MailboxClientLink,
  type MailboxClientLinkOptions,
  type MailboxServerLink,
  type MailboxServerLinkOptions,
} from "./index";
import {
  createMailboxFrame,
  decodeMailboxFrame,
  type MailboxLinkFrame,
  parseErrorPayload,
  parseHelloResult,
  parsePingResult,
  toTransportMessage,
} from "./protocol";

const MAILBOX = "agent:worker-1";

// ---------------------------------------------------------------------------
// In-memory L1 transport pair
// ---------------------------------------------------------------------------

class FakeTransportSession implements TransportSession {
  readonly id: string;
  state: TransportSessionState = "open";
  closeReason: string | undefined;
  readonly bufferedAmount = 0;
  peer: FakeTransportSession | undefined;

  private readonly messageListeners = new Set<(message: TransportMessage) => void>();
  private readonly stateListeners = new Set<(state: TransportSessionState) => void>();
  private readonly inbox: TransportMessage[] = [];
  private flushing = false;
  private sequence = 0;

  constructor(id: string) {
    this.id = id;
  }

  async send(message: TransportMessage): Promise<void> {
    const peer = this.peer;
    if (this.state !== "open" || peer === undefined) throw new Error("transport session is closed");
    peer.enqueue({ ...message, id: `${peer.id}-in-${++peer.sequence}` });
  }

  onMessage(handler: (message: TransportMessage) => void): () => void {
    this.messageListeners.add(handler);
    return () => {
      this.messageListeners.delete(handler);
    };
  }

  onState(handler: (state: TransportSessionState) => void): () => void {
    this.stateListeners.add(handler);
    return () => {
      this.stateListeners.delete(handler);
    };
  }

  async ping(): Promise<void> {}

  async close(reason = "local_close"): Promise<void> {
    if (this.state === "closed") return;
    this.closeReason = reason;
    this.transition("closed");
    const peer = this.peer;
    this.peer = undefined;
    if (peer !== undefined && peer.state !== "closed") {
      peer.closeReason = reason;
      peer.transition("closed");
      peer.peer = undefined;
    }
  }

  enqueue(message: TransportMessage): void {
    if (this.state !== "open") return;
    this.inbox.push(message);
    void this.flush();
  }

  private async flush(): Promise<void> {
    if (this.flushing) return;
    this.flushing = true;
    try {
      while (this.inbox.length > 0 && this.state === "open") {
        const message = this.inbox.shift();
        if (message === undefined) break;
        for (const listener of [...this.messageListeners]) {
          try {
            listener(message);
          } catch {
            // A throwing listener must not stop the fake transport.
          }
        }
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
      }
    } finally {
      this.flushing = false;
    }
  }

  private transition(state: TransportSessionState): void {
    this.state = state;
    for (const listener of [...this.stateListeners]) {
      try {
        listener(state);
      } catch {
        // ignore
      }
    }
  }
}

function createPair(): [FakeTransportSession, FakeTransportSession] {
  const left = new FakeTransportSession("left");
  const right = new FakeTransportSession("right");
  left.peer = right;
  right.peer = left;
  return [left, right];
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

interface Clock {
  current: Date;
}

interface Harness {
  clock: Clock;
  store: MemoryMailboxStore;
  serverSession: FakeTransportSession;
  clientSession: FakeTransportSession;
  server: MailboxServerLink;
  close(): Promise<void>;
}

const harnesses: Harness[] = [];
const clients: MailboxClientLink[] = [];

interface HarnessOptions {
  clock?: Clock;
  store?: MemoryMailboxStore;
  mailbox?: string;
  workers?: readonly string[];
  pollIntervalMs?: number;
  visibilityTimeoutMs?: number;
  batchSize?: number;
}

function createHarness(options: HarnessOptions = {}): Harness {
  const clock = options.clock ?? { current: new Date("2026-11-17T00:00:00.000Z") };
  const now = (): Date => clock.current;
  const store = options.store ?? new MemoryMailboxStore({ now });
  const [serverSession, clientSession] = createPair();
  const serverOptions: MailboxServerLinkOptions = {
    session: serverSession,
    store,
    now,
    pollIntervalMs: options.pollIntervalMs ?? 5,
    visibilityTimeoutMs: options.visibilityTimeoutMs ?? 100,
    ...(options.batchSize === undefined ? {} : { batchSize: options.batchSize }),
    ...(options.workers === undefined ? {} : { workers: options.workers }),
  };
  const server = createMailboxServerLink(serverOptions);
  const harness: Harness = {
    clock,
    store,
    serverSession,
    clientSession,
    server,
    close: async () => {
      server.close();
      await clientSession.close("harness_close");
    },
  };
  harnesses.push(harness);
  return harness;
}

async function attachClient(
  harness: Harness,
  options: Partial<MailboxClientLinkOptions> = {},
): Promise<MailboxClientLink> {
  const client = await createMailboxClientLink({
    session: harness.clientSession,
    mailbox: options.mailbox ?? MAILBOX,
    receiveTimeoutMs: 200,
    requestTimeoutMs: 1_000,
    ...options,
  });
  clients.push(client);
  return client;
}

function envelope(to: string, overrides: Partial<NewEnvelopeInput> = {}): Envelope {
  return newEnvelope({
    source: "agent:master",
    type: "gethen.task.assign",
    to,
    data: { task: "demo" },
    ...overrides,
  });
}

function enqueue(harness: Harness, mailbox: string, message: Envelope): void {
  harness.store.enqueue(mailbox, message, { now: harness.clock.current.toISOString() });
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition was not met in time");
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
}

async function receiveOne(client: MailboxClientLink, timeoutMs = 1_000): Promise<Envelope> {
  const messages = await client.receive({ timeoutMs });
  expect(messages).toHaveLength(1);
  const message = messages[0];
  if (message === undefined) throw new Error("expected one delivered message");
  return message.envelope;
}

function awaitFrame(
  session: FakeTransportSession,
  predicate: (frame: MailboxLinkFrame) => boolean,
  timeoutMs = 2_000,
): Promise<MailboxLinkFrame> {
  return new Promise<MailboxLinkFrame>((resolve, reject) => {
    let off: (() => void) | undefined;
    const timer = setTimeout(() => {
      off?.();
      reject(new Error("timed out waiting for a mailbox-link frame"));
    }, timeoutMs);
    timer.unref();
    off = session.onMessage((message) => {
      const decoded = decodeMailboxFrame(message.payload);
      if (!decoded.ok || !predicate(decoded.value)) return;
      clearTimeout(timer);
      off?.();
      resolve(decoded.value);
    });
  });
}

async function sendFrame(session: FakeTransportSession, frame: MailboxLinkFrame): Promise<void> {
  await session.send(toTransportMessage(frame, "2026-11-17T00:00:00.000Z"));
}

function parseError(frame: MailboxLinkFrame): { code: string; message: string } {
  const parsed = parseErrorPayload(frame.payload);
  if (!parsed.ok) throw new Error(`unexpected error payload: ${parsed.message}`);
  return parsed.value;
}

// ---------------------------------------------------------------------------
// Tests: delivery lifecycle
// ---------------------------------------------------------------------------

describe("@gethen/mailbox-link delivery", () => {
  it("subscribes and delivers a leased message that the client acks", async () => {
    const harness = createHarness();
    enqueue(harness, MAILBOX, envelope(MAILBOX));
    const client = await attachClient(harness);

    expect(harness.server.subscriptions()).toEqual([MAILBOX]);
    const delivered = await client.receive({ timeoutMs: 1_000 });
    expect(delivered).toHaveLength(1);
    const message = delivered[0];
    if (message === undefined) throw new Error("expected a delivery");
    expect(message.mailbox).toBe(MAILBOX);
    expect(message.envelope.to).toBe(MAILBOX);
    expect(message.attempts).toBe(1);
    expect(Date.parse(message.leaseUntil)).toBeGreaterThan(harness.clock.current.getTime());

    await expect(client.ack([message.envelope.id])).resolves.toBe(1);
    expect(harness.store.stats(MAILBOX).pending).toBe(0);
  });

  it("waits for a delivery that is enqueued after receive() starts", async () => {
    const harness = createHarness();
    const client = await attachClient(harness);
    const pending = client.receive({ timeoutMs: 1_000 });
    const message = envelope(MAILBOX);
    enqueue(harness, MAILBOX, message);

    const [delivered] = await pending;
    expect(delivered?.envelope.id).toBe(message.id);
  });

  it("redelivers an unacked message after its lease expires", async () => {
    const harness = createHarness({ visibilityTimeoutMs: 100 });
    enqueue(harness, MAILBOX, envelope(MAILBOX));
    const client = await attachClient(harness);
    const first = await receiveOne(client);

    harness.clock.current = new Date(harness.clock.current.getTime() + 150);
    const second = await receiveOne(client);

    expect(second.id).toBe(first.id);
    const deliveries = harness.store.stats(MAILBOX);
    expect(deliveries.leased).toBe(1);
  });

  it("requeues a nacked message for the next poll", async () => {
    const harness = createHarness();
    enqueue(harness, MAILBOX, envelope(MAILBOX));
    const client = await attachClient(harness);
    const first = await receiveOne(client);

    await expect(client.nack([first.id], { action: "requeue" })).resolves.toBe(1);

    const second = await receiveOne(client);
    expect(second.id).toBe(first.id);
  });

  it("dead-letters a nacked message instead of redelivering it", async () => {
    const harness = createHarness();
    enqueue(harness, MAILBOX, envelope(MAILBOX));
    const client = await attachClient(harness);
    const first = await receiveOne(client);

    await expect(client.nack([first.id], { action: "dead" })).resolves.toBe(1);

    expect(harness.store.deadLetters(MAILBOX)).toHaveLength(1);
    const empty = await client.receive({ timeoutMs: 100 });
    expect(empty).toEqual([]);
  });

  it("renews a lease so the message is not redelivered", async () => {
    const harness = createHarness({ visibilityTimeoutMs: 100 });
    enqueue(harness, MAILBOX, envelope(MAILBOX));
    const client = await attachClient(harness);
    const first = await receiveOne(client);
    harness.clock.current = new Date(harness.clock.current.getTime() + 60);

    await expect(client.renewLeases([first.id], 300)).resolves.toBe(1);
    harness.clock.current = new Date(harness.clock.current.getTime() + 100);

    const none = await client.receive({ timeoutMs: 100 });
    expect(none).toEqual([]);

    harness.clock.current = new Date(harness.clock.current.getTime() + 300);
    const second = await receiveOne(client);
    expect(second.id).toBe(first.id);
  });
});

// ---------------------------------------------------------------------------
// Tests: routing, request/reply and correlation
// ---------------------------------------------------------------------------

describe("@gethen/mailbox-link routing", () => {
  it("routes send by envelope.to and dedupes repeated ids", async () => {
    const harness = createHarness();
    const client = await attachClient(harness);
    const message = envelope("agent:worker-2");

    await expect(client.send(message)).resolves.toEqual({
      mailbox: "agent:worker-2",
      duplicate: false,
    });
    await expect(client.send(message)).resolves.toEqual({
      mailbox: "agent:worker-2",
      duplicate: true,
    });
    expect(harness.store.stats("agent:worker-2").ready).toBe(1);
  });

  it("round-trips request and reply between two linked workers", async () => {
    const clock: Clock = { current: new Date("2026-11-17T00:00:00.000Z") };
    const store = new MemoryMailboxStore({ now: () => clock.current });
    const harnessA = createHarness({ clock, store });
    const harnessB = createHarness({ clock, store });
    const clientA = await attachClient(harnessA, { mailbox: "agent:worker-a" });
    const clientB = await attachClient(harnessB, { mailbox: "agent:worker-b" });

    const request = envelope("agent:worker-b");
    const pending = clientA.request(request);
    const incoming = await clientB.receive({ timeoutMs: 1_000 });
    expect(incoming).toHaveLength(1);
    const received = incoming[0];
    if (received === undefined) throw new Error("worker-b did not receive the request");
    expect(received.envelope.to).toBe("agent:worker-b");
    expect(received.envelope.replyTo).toBe("agent:worker-a");

    const reply = envelope(received.envelope.replyTo ?? "agent:worker-a", {
      type: "gethen.task.result",
      data: { ok: true },
    });
    await clientB.reply(received.envelope, reply);

    const answer = await pending;
    expect(answer.correlationId).toBe(received.envelope.correlationId);
    expect(answer.data).toEqual({ ok: true });
    await waitFor(() => store.stats("agent:worker-a").pending === 0);
  });

  it("correlates concurrent requests by frame id", async () => {
    const harness = createHarness();
    const client = await attachClient(harness);
    const first = envelope("agent:worker-2");
    const second = envelope("agent:worker-3");

    const [firstResult, secondResult] = await Promise.all([
      client.send(first),
      client.send(second),
    ]);

    expect(firstResult.mailbox).toBe("agent:worker-2");
    expect(secondResult.mailbox).toBe("agent:worker-3");
    expect(harness.store.stats("agent:worker-2").ready).toBe(1);
    expect(harness.store.stats("agent:worker-3").ready).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Tests: protocol errors and access control
// ---------------------------------------------------------------------------

describe("@gethen/mailbox-link protocol", () => {
  it("refreshes hello with heartbeat without resubscribing", async () => {
    const harness = createHarness();
    const client = await attachClient(harness);
    await expect(client.heartbeat()).resolves.toBeUndefined();
    client.close();
    await expect(client.heartbeat()).rejects.toThrow(/closed|attached/);
  });

  it("answers hello and ping with server metadata", async () => {
    const harness = createHarness();

    const helloResponse = awaitFrame(harness.clientSession, (frame) => frame.id === "hello-1");
    await sendFrame(
      harness.clientSession,
      createMailboxFrame("hello", "hello-1", { protocol: MAILBOX_LINK_VERSION, mailbox: MAILBOX }),
    );
    const helloFrame = await helloResponse;
    const hello = parseHelloResult(helloFrame.payload);
    if (!hello.ok) throw new Error(hello.message);
    expect(hello.value.protocol).toBe(MAILBOX_LINK_VERSION);
    expect(hello.value.mailbox).toBe(MAILBOX);

    const pingResponse = awaitFrame(harness.clientSession, (frame) => frame.id === "ping-1");
    await sendFrame(harness.clientSession, createMailboxFrame("ping", "ping-1", {}));
    const pingFrame = await pingResponse;
    const ping = parsePingResult(pingFrame.payload);
    if (!ping.ok) throw new Error(ping.message);
    expect(Date.parse(ping.value.at)).toBe(harness.clock.current.getTime());
  });

  it("returns an error frame for an unknown op", async () => {
    const harness = createHarness();
    const response = awaitFrame(harness.clientSession, (frame) => frame.id === "bad-op");

    await sendFrame(harness.clientSession, createMailboxFrame("deliver", "bad-op", {}));

    const error = parseError(await response);
    expect(error.code).toBe("unknown_op");
  });

  it("returns an error frame for a malformed JSON frame", async () => {
    const harness = createHarness();
    const response = awaitFrame(harness.clientSession, (frame) => frame.op === "error");

    await harness.clientSession.send({
      id: "broken",
      payload: "{not json",
      contentType: "application/json",
      at: harness.clock.current.toISOString(),
    });

    expect(parseError(await response).code).toBe("invalid_frame");
  });

  it("rejects a send with an invalid envelope", async () => {
    const harness = createHarness();
    const response = awaitFrame(harness.clientSession, (frame) => frame.id === "bad-envelope");

    await sendFrame(
      harness.clientSession,
      createMailboxFrame("send", "bad-envelope", { envelope: { id: "x" } }),
    );

    expect(parseError(await response).code).toBe("invalid_envelope");
  });

  it("rejects mailboxes outside the configured workers allowlist", async () => {
    const harness = createHarness({ workers: ["worker-1"] });

    await expect(attachClient(harness, { mailbox: MAILBOX })).resolves.toBeDefined();
    await expect(attachClient(harness, { mailbox: "agent:worker-2" })).rejects.toMatchObject({
      code: "not_allowed",
    });
  });

  it("rejects a send whose envelope has no destination", async () => {
    const harness = createHarness();
    const client = await attachClient(harness);

    await expect(client.send(envelope(""))).rejects.toMatchObject({ code: "invalid_envelope" });
  });
});

// ---------------------------------------------------------------------------
// Tests: reconnect and teardown
// ---------------------------------------------------------------------------

describe("@gethen/mailbox-link lifecycle", () => {
  it("restores the subscription when attached to a new session after a disconnect", async () => {
    const harness = createHarness();
    const client = await createDetachedMailboxClientLink({
      mailbox: MAILBOX,
      receiveTimeoutMs: 200,
      requestTimeoutMs: 1_000,
    });
    clients.push(client);
    await client.attach(harness.clientSession);
    const before = envelope(MAILBOX);
    enqueue(harness, MAILBOX, before);
    expect((await receiveOne(client)).id).toBe(before.id);

    await harness.close();
    await waitFor(() => !client.connected);

    const next = createHarness({ store: harness.store, clock: harness.clock });
    await client.attach(next.clientSession);
    expect(next.server.subscriptions()).toEqual([MAILBOX]);

    const after = envelope(MAILBOX);
    enqueue(next, MAILBOX, after);
    expect((await receiveOne(client)).id).toBe(after.id);
  });

  it("rejects pending receives and requests when the session disconnects", async () => {
    const harness = createHarness();
    const client = await attachClient(harness);
    const receiveFailure = expect(client.receive({ timeoutMs: 2_000 })).rejects.toMatchObject({
      code: "disconnected",
    });
    const requestFailure = expect(client.request(envelope("agent:other"))).rejects.toMatchObject({
      code: "disconnected",
    });

    await harness.close();

    await receiveFailure;
    await requestFailure;
  });

  it("closes cleanly and refuses further operations", async () => {
    const harness = createHarness();
    const states: string[] = [];
    const client = await attachClient(harness, { onState: (state) => states.push(state) });

    client.close();

    expect(client.connected).toBe(false);
    expect(states).toEqual(["connected", "closed"]);
    await expect(client.receive()).rejects.toMatchObject({ code: "closed" });
    await expect(client.send(envelope(MAILBOX))).rejects.toMatchObject({ code: "closed" });
    await expect(client.attach(harness.clientSession)).rejects.toMatchObject({ code: "closed" });
  });

  it("stops polling when the server link closes", async () => {
    const harness = createHarness();
    const client = await attachClient(harness);
    harness.server.close();

    enqueue(harness, MAILBOX, envelope(MAILBOX));
    const empty = await client.receive({ timeoutMs: 100 });
    expect(empty).toEqual([]);
  });

  it("keeps a shared-store mailbox independent per link", async () => {
    const clock: Clock = { current: new Date("2026-11-17T00:00:00.000Z") };
    const store = new MemoryMailboxStore({ now: () => clock.current });
    const harnessA = createHarness({ clock, store });
    const harnessB = createHarness({ clock, store });
    const clientA = await attachClient(harnessA, { mailbox: "agent:a" });
    const clientB = await attachClient(harnessB, { mailbox: "agent:b" });

    enqueue(harnessA, "agent:b", envelope("agent:b"));
    expect(await clientB.receive({ timeoutMs: 1_000 })).toHaveLength(1);
    expect(await clientA.receive({ timeoutMs: 100 })).toEqual([]);
  });
});

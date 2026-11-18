/**
 * Server side of `@gethen/mailbox-link`: serves a {@link MailboxStore} to one
 * remote peer over an L1 {@link TransportSession}.
 *
 * The Master creates one link per worker transport session. The link owns the
 * polling/lease mechanics (subscribe -> poll -> deliver), forwards
 * ack/nack/renew to the store, routes send/reply through the L2 mailbox client,
 * and answers ping/hello. When the session closes it stops polling; messages
 * that were delivered but not acked are redelivered by the store once their
 * lease expires — the link never retransmits on its own.
 */
import {
  type EnqueueResult,
  EnvelopeError,
  MailboxClient,
  type MailboxStore,
  type ReceivedMessage,
} from "@gethen/mailbox";
import type { TransportMessage, TransportSession } from "@gethen/transport";
import {
  createMailboxFrame,
  type DeliverPayload,
  decodeMailboxFrame,
  type HelloPayload,
  MAILBOX_LINK_VERSION,
  MailboxLinkError,
  type MailboxLinkErrorCode,
  type MailboxLinkFrame,
  parseAckPayload,
  parseHelloPayload,
  parseNackPayload,
  parsePingPayload,
  parseRenewPayload,
  parseReplyPayload,
  parseSendPayload,
  parseSubscribePayload,
  type SubscribeResult,
  toTransportMessage,
  type WireDeliveredMessage,
} from "./protocol";

export const DEFAULT_SERVER_BATCH_SIZE = 10;
export const DEFAULT_SERVER_VISIBILITY_TIMEOUT_MS = 30_000;
export const DEFAULT_SERVER_MAX_ATTEMPTS = 5;
export const DEFAULT_SERVER_POLL_INTERVAL_MS = 50;
export const DEFAULT_MAX_CONCURRENT_REQUESTS = 32;

export interface MailboxServerLinkOptions {
  /** The worker's transport session (server end). */
  session: TransportSession;
  /** Master-side store that owns durability and lease state. */
  store: MailboxStore;
  /**
   * Optional allowlist of mailboxes this link may consume. A bare worker id
   * `worker-1` is normalized to `agent:worker-1`; when omitted every mailbox is
   * allowed (single-tenant deployments). Send/reply routing is not restricted.
   */
  workers?: readonly string[];
  batchSize?: number;
  visibilityTimeoutMs?: number;
  maxAttempts?: number;
  pollIntervalMs?: number;
  maxConcurrentRequests?: number;
  now?: () => Date;
  /**
   * Observer for an authenticated Worker's `hello` registration. The host
   * validates that the reported worker id matches the transport identity.
   */
  onHello?: (hello: HelloPayload, session: TransportSession) => void;
  /** Transport/protocol problems; never thrown into the caller's stack. */
  onError?: (error: unknown) => void;
}

export interface MailboxServerLink {
  readonly session: TransportSession;
  /** Mailboxes currently polled for this connection. */
  subscriptions(): string[];
  /** Stop polling and detach from the session (the session itself is not closed). */
  close(): void;
}

interface Subscription {
  mailbox: string;
  name: string;
  batchSize: number;
  visibilityTimeoutMs: number;
  timer: NodeJS.Timeout | undefined;
  polling: boolean;
  stopped: boolean;
}

export function createMailboxServerLink(options: MailboxServerLinkOptions): MailboxServerLink {
  const { session, store } = options;
  const now = options.now ?? (() => new Date());
  const batchSize = positiveInteger(options.batchSize ?? DEFAULT_SERVER_BATCH_SIZE, "batchSize");
  const visibilityTimeoutMs = positiveNumber(
    options.visibilityTimeoutMs ?? DEFAULT_SERVER_VISIBILITY_TIMEOUT_MS,
    "visibilityTimeoutMs",
  );
  const maxAttempts = positiveInteger(
    options.maxAttempts ?? DEFAULT_SERVER_MAX_ATTEMPTS,
    "maxAttempts",
  );
  const pollIntervalMs = positiveNumber(
    options.pollIntervalMs ?? DEFAULT_SERVER_POLL_INTERVAL_MS,
    "pollIntervalMs",
  );
  const maxConcurrentRequests = positiveInteger(
    options.maxConcurrentRequests ?? DEFAULT_MAX_CONCURRENT_REQUESTS,
    "maxConcurrentRequests",
  );
  const allowedMailboxes = normalizeWorkers(options.workers);

  const mailbox = new MailboxClient({
    store,
    now,
    visibilityTimeoutMs,
    maxAttempts,
    receiveBatchSize: batchSize,
    pollIntervalMs,
  });
  const subscriptions = new Map<string, Subscription>();
  const offMessage = session.onMessage((message) => handleTransportMessage(message));
  const offState = session.onState((state) => {
    if (state === "closed") stopSubscriptions();
  });
  let activeRequests = 0;
  let deliverSequence = 0;
  let closed = false;

  function reportError(error: unknown): void {
    if (options.onError === undefined) return;
    try {
      options.onError(error);
    } catch {
      // A throwing hook must not break the link.
    }
  }

  function sendFrame(frame: MailboxLinkFrame): void {
    if (closed || session.state !== "open") return;
    void session.send(toTransportMessage(frame, now().toISOString())).catch((error: unknown) => {
      reportError(error);
    });
  }

  function respond(id: string, payload: unknown): void {
    sendFrame(createMailboxFrame("payload", id, payload));
  }

  function sendError(id: string, code: MailboxLinkErrorCode, message: string): void {
    sendFrame(createMailboxFrame("error", id, { code, message }));
  }

  function denied(
    mailboxName: string,
  ): { code: MailboxLinkErrorCode; message: string } | undefined {
    if (allowedMailboxes === undefined || allowedMailboxes.has(mailboxName)) return undefined;
    return { code: "not_allowed", message: `mailbox "${mailboxName}" is not allowed on this link` };
  }

  function handleTransportMessage(message: TransportMessage): void {
    if (closed) return;
    const decoded = decodeMailboxFrame(message.payload);
    if (!decoded.ok) {
      reportError(new MailboxLinkError(decoded.code, decoded.message));
      sendError(message.id, decoded.code, decoded.message);
      return;
    }
    const frame = decoded.value;
    if (activeRequests >= maxConcurrentRequests) {
      sendError(frame.id, "too_many_requests", "too many concurrent mailbox requests");
      return;
    }
    activeRequests += 1;
    try {
      dispatch(frame);
    } catch (error) {
      reportError(error);
      sendError(frame.id, "internal", messageOf(error));
    } finally {
      activeRequests -= 1;
    }
  }

  function dispatch(frame: MailboxLinkFrame): void {
    switch (frame.op) {
      case "hello": {
        const payload = parseHelloPayload(frame.payload);
        if (!payload.ok) {
          sendError(frame.id, payload.code, payload.message);
          return;
        }
        if (options.onHello !== undefined) {
          try {
            options.onHello(payload.value, session);
          } catch (error) {
            reportError(error);
          }
        }
        respond(frame.id, {
          protocol: MAILBOX_LINK_VERSION,
          serverTime: now().toISOString(),
          ...(payload.value.mailbox === undefined ? {} : { mailbox: payload.value.mailbox }),
        });
        return;
      }
      case "subscribe": {
        const payload = parseSubscribePayload(frame.payload);
        if (!payload.ok) {
          sendError(frame.id, payload.code, payload.message);
          return;
        }
        const gate = denied(payload.value.mailbox);
        if (gate !== undefined) {
          sendError(frame.id, gate.code, gate.message);
          return;
        }
        // The server owns lease policy: a client may ask for a smaller batch
        // or shorter lease, never a larger/longer one than the link allows.
        const subscription = startSubscription(
          payload.value.mailbox,
          payload.value.name,
          Math.min(payload.value.batchSize ?? batchSize, batchSize),
          Math.min(payload.value.visibilityTimeoutMs ?? visibilityTimeoutMs, visibilityTimeoutMs),
        );
        respond(frame.id, subscription);
        return;
      }
      case "ack": {
        const payload = parseAckPayload(frame.payload);
        if (!payload.ok) {
          sendError(frame.id, payload.code, payload.message);
          return;
        }
        const gate = denied(payload.value.mailbox);
        if (gate !== undefined) {
          sendError(frame.id, gate.code, gate.message);
          return;
        }
        respond(frame.id, { acked: mailbox.ack(payload.value.mailbox, payload.value.ids) });
        return;
      }
      case "nack": {
        const payload = parseNackPayload(frame.payload);
        if (!payload.ok) {
          sendError(frame.id, payload.code, payload.message);
          return;
        }
        const gate = denied(payload.value.mailbox);
        if (gate !== undefined) {
          sendError(frame.id, gate.code, gate.message);
          return;
        }
        const nacked = mailbox.nack(payload.value.mailbox, payload.value.ids, {
          action: payload.value.action ?? "requeue",
          ...(payload.value.delayMs === undefined ? {} : { delayMs: payload.value.delayMs }),
        });
        respond(frame.id, { nacked });
        return;
      }
      case "renew": {
        const payload = parseRenewPayload(frame.payload);
        if (!payload.ok) {
          sendError(frame.id, payload.code, payload.message);
          return;
        }
        const gate = denied(payload.value.mailbox);
        if (gate !== undefined) {
          sendError(frame.id, gate.code, gate.message);
          return;
        }
        const renewed = mailbox.renewLeases(
          payload.value.mailbox,
          payload.value.ids,
          payload.value.visibilityTimeoutMs ?? visibilityTimeoutMs,
        );
        respond(frame.id, { renewed });
        return;
      }
      case "send": {
        const payload = parseSendPayload(frame.payload);
        if (!payload.ok) {
          sendError(frame.id, payload.code, payload.message);
          return;
        }
        routeResult(frame.id, () => mailbox.send(payload.value.envelope));
        return;
      }
      case "reply": {
        const payload = parseReplyPayload(frame.payload);
        if (!payload.ok) {
          sendError(frame.id, payload.code, payload.message);
          return;
        }
        routeResult(frame.id, () => mailbox.reply(payload.value.request, payload.value.envelope));
        return;
      }
      case "ping": {
        const payload = parsePingPayload(frame.payload);
        if (!payload.ok) {
          sendError(frame.id, payload.code, payload.message);
          return;
        }
        respond(frame.id, { at: now().toISOString() });
        return;
      }
      case "deliver":
      case "payload":
      case "error":
        sendError(frame.id, "unknown_op", `op "${frame.op}" is server-to-client only`);
        return;
      default:
        sendError(frame.id, "unknown_op", `unknown mailbox link op "${String(frame.op)}"`);
    }
  }

  function routeResult(id: string, route: () => EnqueueResult): void {
    try {
      const result = route();
      respond(id, result);
    } catch (error) {
      if (error instanceof EnvelopeError) {
        sendError(id, "invalid_envelope", error.message);
        return;
      }
      throw error;
    }
  }

  function startSubscription(
    mailboxName: string,
    name: string | undefined,
    subscriptionBatchSize: number,
    subscriptionVisibilityTimeoutMs: number,
  ): SubscribeResult {
    const subscriptionName = name ?? `link-${mailboxName}`;
    const existing = subscriptions.get(mailboxName);
    if (existing !== undefined) {
      existing.name = subscriptionName;
      existing.batchSize = subscriptionBatchSize;
      existing.visibilityTimeoutMs = subscriptionVisibilityTimeoutMs;
    } else {
      const subscription: Subscription = {
        mailbox: mailboxName,
        name: subscriptionName,
        batchSize: subscriptionBatchSize,
        visibilityTimeoutMs: subscriptionVisibilityTimeoutMs,
        timer: undefined,
        polling: false,
        stopped: false,
      };
      subscriptions.set(mailboxName, subscription);
      subscription.timer = setInterval(() => {
        void poll(subscription);
      }, pollIntervalMs);
      subscription.timer.unref();
      void poll(subscription);
    }
    return {
      mailbox: mailboxName,
      subscription: subscriptionName,
      batchSize: subscriptionBatchSize,
      visibilityTimeoutMs: subscriptionVisibilityTimeoutMs,
    };
  }

  async function poll(subscription: Subscription): Promise<void> {
    if (closed || subscription.stopped || subscription.polling) return;
    subscription.polling = true;
    try {
      const messages = mailbox.receive(subscription.mailbox, {
        limit: subscription.batchSize,
        visibilityTimeoutMs: subscription.visibilityTimeoutMs,
      });
      if (messages.length > 0) deliver(subscription.mailbox, messages);
    } catch (error) {
      reportError(error);
    } finally {
      subscription.polling = false;
    }
  }

  function deliver(mailboxName: string, messages: ReceivedMessage[]): void {
    const payload: DeliverPayload = {
      mailbox: mailboxName,
      messages: messages.map(toWireMessage),
    };
    sendFrame(createMailboxFrame("deliver", `deliver-${++deliverSequence}`, payload));
  }

  function stopSubscriptions(): void {
    for (const subscription of subscriptions.values()) {
      subscription.stopped = true;
      if (subscription.timer !== undefined) clearInterval(subscription.timer);
    }
    subscriptions.clear();
  }

  function close(): void {
    if (closed) return;
    closed = true;
    stopSubscriptions();
    offMessage();
    offState();
  }

  return {
    session,
    subscriptions: () => [...subscriptions.keys()],
    close,
  };
}

function toWireMessage(message: ReceivedMessage): WireDeliveredMessage {
  return {
    envelope: message.envelope,
    attempts: message.attempts,
    leaseUntil: message.leaseUntil,
  };
}

function normalizeWorkers(workers: readonly string[] | undefined): Set<string> | undefined {
  if (workers === undefined) return undefined;
  return new Set(workers.map((worker) => (worker.includes(":") ? worker : `agent:${worker}`)));
}

function positiveNumber(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`mailbox-link: ${name} must be a positive finite number`);
  }
  return value;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`mailbox-link: ${name} must be a positive integer`);
  }
  return value;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

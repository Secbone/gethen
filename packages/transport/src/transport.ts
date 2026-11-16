/**
 * L1 transport entry point: the transport-agnostic `Transport` interface plus
 * adapter selection and capability negotiation.
 *
 * `createTransport({ driver: "ws", ... })` returns the ws adapter and the
 * capabilities it negotiated. An unknown driver fails with
 * `unsupported_driver`; adding an adapter (ssh-stdio, unix-socket, nats) means
 * adding one union member and one factory branch here.
 */
import {
  createWsTransport,
  type WsTransportOptions,
  wsTransportCapabilities,
} from "./adapters/ws/client";
import {
  type TransportAuth,
  type TransportEndpoint,
  TransportError,
  type TransportSession,
} from "./session";

/** Adapter kinds L1 can run over. */
export type TransportDriver = "ws";

/** What a transport instance supports; callers negotiate on this, not on `instanceof`. */
export interface TransportCapabilities {
  driver: TransportDriver;
  /** L1 delivers discrete messages (never raw streams). */
  framing: "message";
  /** Binary payloads round-trip as bytes. */
  binary: boolean;
  /** The adapter performs ping/pong liveness checks. */
  heartbeat: boolean;
  /** `send` applies a bounded queue and buffered-amount backpressure. */
  backpressure: boolean;
  /** Stream resume/replay is not an L1 feature; L2 mailboxes provide redelivery. */
  resume: boolean;
}

export interface Transport {
  readonly capabilities: TransportCapabilities;
  connect(endpoint: TransportEndpoint, auth: TransportAuth): Promise<TransportSession>;
}

/** Configuration for `createTransport`; the `driver` selects the adapter. */
export type TransportConfig = { driver: "ws" } & WsTransportOptions;

export function createTransport(config: TransportConfig): Transport {
  switch (config.driver) {
    case "ws":
      return createWsTransport(config);
    default: {
      const driver = (config as { driver?: unknown }).driver;
      throw new TransportError(
        "unsupported_driver",
        `unsupported transport driver "${String(driver)}" (available: ws)`,
      );
    }
  }
}

export { wsTransportCapabilities };

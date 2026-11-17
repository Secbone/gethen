/**
 * `@gethen/agent`: the Worker-side L2 mailbox consumer.
 *
 * It consumes `gethen.task.*` envelopes from `mailbox:<workerId>` and drives
 * them through the harness-agnostic `HarnessExecutor` port (`@gethen/harness`).
 * It knows no transport, no DSH and no legacy frame types: the mailbox
 * client/link is injected, and any harness implementing the port can run
 * behind it.
 */
export * from "./consumer";

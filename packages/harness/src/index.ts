/**
 * `@gethen/harness`: the harness-agnostic executor port.
 *
 * A mailbox consumer starts one `HarnessTask` on a `HarnessExecutor`, then
 * receives `HarnessEvent`s, blocking `HarnessEscalation`s and one final
 * `HarnessReport`. DSH (`@gethen/executor-sdk`) is the first implementation;
 * see README.md for the adapter mapping and the zero-DSH boundary.
 */
export * from "./port";

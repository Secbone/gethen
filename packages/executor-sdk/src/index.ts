/**
 * `@gethen/executor-sdk`: the real Worker execution driver.
 *
 * `createSdkExecutor()` implements the worker-agent `SessionExecutor` port with
 * DSH SDK JSON-RPC sessions (`dsh --profile sdk`): one runtime per workspace,
 * one session per assignment, streamed `session.event`s, and DSH's native
 * `approval/request` / `user-question/ask` channels mapped onto Master
 * escalations. See README.md for the escalation mapping and cancellation
 * semantics.
 */
export * from "./executor";
export * from "./harness-adapter";

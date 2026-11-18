/**
 * `@gethen/master-runtime`: the control-plane assembly shared by the CLI
 * (`gethen serve`) and the DSH plugin. See `./runtime`.
 */

export type { BridgeSettlementMetrics } from "@gethen/bridge-mailbox";
// Host-convenience re-exports: a CLI/plugin that layers fleet scheduling on the
// live worker snapshots can take the fleet/router types from here instead of
// duplicating the assembly's dependency edges.
export type {
  FleetSnapshot,
  Node as FleetNode,
  NodeHealth,
  Runtime as FleetRuntime,
  Slot,
} from "@gethen/fleet";
export { MailboxClient, newEnvelope, SqliteMailboxStore } from "@gethen/mailbox";
export type {
  RouteDecision,
  RoutePlan,
  TaskRequirement,
  WorkerCapabilities,
  WorkerManifest,
} from "@gethen/router";
export * from "./control";
export * from "./fleet";
export * from "./runtime";
export * from "./warnings";

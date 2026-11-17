/**
 * `@gethen/master-runtime`: the control-plane assembly shared by the CLI
 * (`gethen serve`) and the DSH plugin. See `./runtime`.
 */

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
export type { RouteDecision, TaskRequirement } from "@gethen/router";
export * from "./control";
export * from "./runtime";

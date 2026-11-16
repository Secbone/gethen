/**
 * `@gethen/bridge-ws`: the Master-side WebSocket `LaunchPort`.
 *
 * Exposes `createWsLaunchPort`, which returns the `LaunchPort` handed to
 * `MasterService` plus the `handleFrame` sink that a host wires to
 * `WorkerGateway`'s `onFrame`.
 */
export * from "./launch";

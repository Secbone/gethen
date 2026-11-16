# @gethen/bridge-ws

Master-side WebSocket `LaunchPort`: turns `LaunchPort.launch(request)` into
`gethen.worker/v1` dispatch frames over a `WorkerGateway`, then resolves the
launch from the matching `report` (or from a blocking `escalation` it resolves
through the injected resolver).

```ts
const ws = createWsLaunchPort({ gateway, resolveEscalation, selectWorker });
createWorkerGateway({ ..., onFrame: (conn, frame) => ws.handleFrame(conn, frame) });
```

## Worker selection contract

Fleet choice belongs to the **master-runtime** (the fleet scheduler); this
adapter only validates a worker id and dispatches to it. `selectWorker` is the
seam between the two:

- `selectWorker(request: LaunchRequest)` is called once per launch with the full
  request (ticket, assignment, pack digest, prompt) and may be sync or async.
- It returns `{ workerId }`; the id **must** match a currently connected worker.
- `undefined` means "no worker fits": the launch rejects with `WS_NO_WORKER`.
- A throw/rejection, or an id that is not connected, rejects with
  `WS_WORKER_UNAVAILABLE` (the cause is kept in the message, prefixed with the
  assignment id).

Without `selectWorker`, `options.workerId` is used, then the first connection
that completed hello. `selectWorker` always takes precedence over `workerId`.

## Error codes

`WS_NO_WORKER`, `WS_WORKER_UNAVAILABLE`, `WS_DISPATCH_FAILED`,
`WS_LAUNCH_TIMEOUT`, `WS_RESOLUTION_FAILED` — see `WsLaunchErrorCode` in
`src/launch.ts`.

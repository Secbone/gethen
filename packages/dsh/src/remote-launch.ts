import { createRemoteBridge, type RemoteClientLike } from "@gethen/bridge-remote";
import {
  type LaunchPort,
  type LaunchRequest,
  type LaunchResult,
  parseReportText,
  type Report,
} from "@gethen/master";

export interface RemoteLaunchConfig {
  /** DSH Remote host id (deviceId) of the worker machine. */
  hostId: string;
  /** Workspace id on that host, as returned by `listRemoteWorkspaces`. */
  workspaceId: string;
  /** Advertised worker name; defaults to the assignment's worker. */
  worker?: string;
  /** Model advertised in manifests; not forced on the session. */
  model?: string;
  /** Delay between session-state polls. */
  pollMs?: number;
  /** Maximum polls after the first one before the launch gives up waiting. */
  maxPolls?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => Date;
}

const DEFAULT_POLL_MS = 1_000;
const DEFAULT_MAX_POLLS = 120;

/**
 * Compose a `LaunchPort` from the DSH Remote session tools: create a session in
 * the configured workspace, send the assignment prompt, wait until the session
 * stops running, read the transcript and parse the `[GETHEN-REPORT]` marker.
 *
 * Receipts are composed here from transcript evidence; P3 has no per-session
 * receipt tool (see `@gethen/bridge-remote`'s `receipt()` note).
 */
export function createRemoteLaunchPort(
  config: RemoteLaunchConfig,
  client: RemoteClientLike,
): LaunchPort {
  const now = config.now ?? (() => new Date());
  const sleep =
    config.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const bridge = createRemoteBridge({
    client,
    hostId: config.hostId,
    workspaceId: config.workspaceId,
    ...(config.model === undefined ? {} : { model: config.model }),
  });
  const worker = (request: LaunchRequest): string => config.worker ?? request.assignment.worker;

  return {
    async launch(request: LaunchRequest): Promise<LaunchResult> {
      const ref = await bridge.open({
        ticket: request.ticket.id,
        assignment: request.assignment.id,
        worker: worker(request),
        workspace: config.workspaceId,
        prompt: request.prompt,
        pack: request.packDigest,
      });
      const pollMs = config.pollMs ?? DEFAULT_POLL_MS;
      const maxPolls = config.maxPolls ?? DEFAULT_MAX_POLLS;
      let state = await bridge.state(ref.session);
      for (let poll = 0; poll < maxPolls && state === "running"; poll += 1) {
        await sleep(pollMs);
        state = await bridge.state(ref.session);
      }
      const page = await bridge.read(ref.session);
      const transcript = page.events.map((event) => event.text ?? "").join("\n");
      const at = now().toISOString();
      const report: Report = parseReportText(transcript) ?? {
        kind: state === "idle" ? "waiting" : "failed",
        summary:
          state === "idle"
            ? "worker finished without a report marker"
            : `worker did not settle: state ${state}`,
        at,
      };
      return {
        report,
        receipt: {
          assignment: request.assignment.id,
          ticket: request.ticket.id,
          worker: worker(request),
          workspace: config.workspaceId,
          packDigest: request.packDigest,
          at,
          outcome: report.kind === "failed" ? "failed" : "completed",
          summary: report.summary,
          events: page.events.length,
        },
      };
    },
  };
}

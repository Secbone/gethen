/**
 * The structural DSH Remote client surface this adapter consumes.
 *
 * Deliberately structural: the Gethen side never imports DSH, so the real
 * `dshRemoteClient` (or a feature-detect wrapper, or a fake) is injected. See
 * ADR-0008 "One Remote channel, two consumers".
 */

export interface RemoteDevice {
  deviceId: string;
  name: string;
  online: boolean;
  platform?: string;
}

export interface RemoteWorkspace {
  workspaceId: string;
  path: string;
  title: string;
}

/**
 * The subset of the DSH Remote client used by the bridge adapter. Every method
 * accepts the caller's abort signal and forwards it to the transport.
 */
export interface RemoteClientLike {
  /** Machines reachable through the Remote channel. */
  devices(signal?: AbortSignal): Promise<RemoteDevice[]>;
  /** Workspaces one device exposes. */
  listRemoteWorkspaces(deviceId: string, signal?: AbortSignal): Promise<RemoteWorkspace[]>;
  /**
   * Invoke one typed Remote session tool. The bridge uses:
   * `remote_create_session`, `remote_send_message`, `remote_wait_sessions`,
   * `remote_read_session` and `remote_interrupt_session`.
   */
  runSessionTool(operation: string, args: unknown, signal?: AbortSignal): Promise<unknown>;
}

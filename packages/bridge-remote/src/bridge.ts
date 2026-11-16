/**
 * `WorkerBridge` over the DSH Remote session tools.
 *
 * The adapter is a thin translation layer: it knows the Remote client's
 * structural surface ({@link RemoteClientLike}) and the Gethen bridge contract,
 * never DSH internals. Tool-call argument shapes are documented per method so a
 * future DSH version bump has one place to change.
 *
 * P3 scope: `receipt()` cannot be composed from a single tool call yet; the
 * Master builds receipts from {@link WorkerBridge.read} transcripts.
 */

import { randomUUID } from "node:crypto";
import type {
  AssignmentReceipt,
  DispatchRequest,
  TranscriptEvent,
  TranscriptPage,
  WorkerBridge,
  WorkerCapabilities,
  WorkerManifest,
  WorkerSessionRef,
  WorkerState,
} from "@gethen/worker-bridge";
import type { RemoteClientLike } from "./client";

export interface RemoteBridgeOptions {
  /** Injected Remote client (the real `dshRemoteClient` or a fake in tests). */
  client: RemoteClientLike;
  /** Remote host this bridge dispatches to. */
  hostId: string;
  /** Remote workspace the sessions are created in. */
  workspaceId: string;
  /** Advertised model; exposed in every manifest and not forced on sessions. */
  model?: string;
  /**
   * Id generator for Remote operation ids. The returned id must be unique per
   * call; values shorter than 16 characters are padded to the DSH minimum and
   * values longer than 128 characters are rejected.
   */
  ids?: { next(prefix: string): string };
  /** Forwarded to every Remote client call. */
  signal?: AbortSignal;
}

const ADAPTER_ID = "remote";
/** DSH Remote backend discriminator; typed operations are dispatched to it. */
const BACKEND = "harness";
const READ_PAGE_SIZE = 20;
const MIN_OPERATION_ID_LENGTH = 16;
const MAX_OPERATION_ID_LENGTH = 128;

/** Conservative capability defaults until the Remote client advertises more. */
const CAPABILITIES: WorkerCapabilities = Object.freeze({
  promptInjection: "system",
  skills: "none",
  mcp: false,
  permissions: "approval",
  streaming: true,
  steer: true,
  fork: false,
});

const DEFAULT_IDS = { next: (prefix: string): string => `${prefix}-${randomUUID()}` };

/**
 * Build the Remote transport for one DSH host.
 *
 * Tool calls made by the returned bridge:
 *
 * | method      | operation                    | args                                                          |
 * |-------------|------------------------------|---------------------------------------------------------------|
 * | `list`      | `devices` / `listRemoteWorkspaces` | none / `deviceId`                                       |
 * | `open`      | `remote_create_session`      | `{hostId, backend:"harness", workspaceId, operationId}`        |
 * | `open`      | `remote_send_message`        | `{hostId, backend:"harness", workspaceId, sessionId, message, operationId}` |
 * | `send`      | `remote_send_message`        | `{hostId, backend:"harness", workspaceId, sessionId, message, operationId}` |
 * | `state`     | `remote_wait_sessions`       | `{targets:[{hostId, backend:"harness", workspaceId, sessionId}], timeoutMs:0}` |
 * | `read`      | `remote_read_session`        | `{hostId, backend:"harness", workspaceId, sessionId, maxMessages:20, cursor?}` |
 * | `interrupt` | `remote_interrupt_session`   | `{hostId, backend:"harness", workspaceId, sessionId, operationId}` |
 */
export function createRemoteBridge(options: RemoteBridgeOptions): WorkerBridge {
  const { client, hostId, workspaceId, model, signal } = options;
  const ids = options.ids ?? DEFAULT_IDS;

  const nextOperationId = (prefix: string): string => normalizeOperationId(ids.next(prefix));

  const sendMessage = async (session: string, text: string, operationId: string): Promise<void> => {
    await client.runSessionTool(
      "remote_send_message",
      { hostId, backend: BACKEND, workspaceId, sessionId: session, message: text, operationId },
      signal,
    );
  };

  return {
    id: ADAPTER_ID,

    async list(): Promise<WorkerManifest[]> {
      const devices = await client.devices(signal);
      const manifests: WorkerManifest[] = [];
      for (const device of devices) {
        // An offline device cannot be asked for its workspaces; advertise none
        // instead of failing the whole fleet snapshot.
        const workspaces = device.online
          ? await client.listRemoteWorkspaces(device.deviceId, signal)
          : [];
        manifests.push({
          id: device.deviceId,
          machine: device.name,
          runtime: "dsh",
          workspaces: workspaces.map((workspace) => ({ id: workspace.workspaceId })),
          capabilities: { ...CAPABILITIES },
          models: model === undefined ? [] : [model],
          costClass: "mid",
          load: { running: 0, quota: 1 },
          health: {
            state: device.online ? "online" : "offline",
            lastHeartbeatAt: new Date().toISOString(),
          },
        });
      }
      return manifests;
    },

    async open(request: DispatchRequest): Promise<WorkerSessionRef> {
      const createOperationId = nextOperationId("remote");
      const created = await client.runSessionTool(
        "remote_create_session",
        { hostId, backend: BACKEND, workspaceId, operationId: createOperationId },
        signal,
      );
      const session = extractSessionId(created);
      if (session === undefined) {
        throw new Error("SESSION_MISSING: remote_create_session returned no session id");
      }
      await sendMessage(session, request.prompt, nextOperationId("send"));
      return { session, worker: request.worker, workspace: request.workspace };
    },

    async send(
      session: string,
      text: string,
      operationId: string,
    ): Promise<"accepted" | "unknown"> {
      await sendMessage(session, text, operationId);
      return "accepted";
    },

    async state(session: string): Promise<WorkerState> {
      const result = await client.runSessionTool(
        "remote_wait_sessions",
        { targets: [{ hostId, backend: BACKEND, workspaceId, sessionId: session }], timeoutMs: 0 },
        signal,
      );
      return mapSessionState(extractSessionState(result, session));
    },

    async read(
      session: string,
      cursor?: { beforeSeq: number; throughSeq: number },
    ): Promise<TranscriptPage> {
      const result = await client.runSessionTool(
        "remote_read_session",
        {
          hostId,
          backend: BACKEND,
          workspaceId,
          sessionId: session,
          maxMessages: READ_PAGE_SIZE,
          ...(cursor === undefined ? {} : { cursor }),
        },
        signal,
      );
      return toTranscriptPage(result, session);
    },

    async interrupt(session: string): Promise<void> {
      await client.runSessionTool(
        "remote_interrupt_session",
        {
          hostId,
          backend: BACKEND,
          workspaceId,
          sessionId: session,
          operationId: nextOperationId("interrupt"),
        },
        signal,
      );
    },

    /**
     * P3 does not fabricate evidence. A `remote_*` session exposes no receipt,
     * so the caller composes one from {@link WorkerBridge.read} transcript
     * events plus its own assignment bookkeeping.
     */
    async receipt(_session: string): Promise<AssignmentReceipt> {
      throw new Error(
        "RECEIPT_UNAVAILABLE: P3 composes receipts from the remote transcript at the caller",
      );
    },
  };
}

function normalizeOperationId(raw: string): string {
  if (raw.length === 0) {
    throw new Error("OPERATION_ID_INVALID: operation id must not be empty");
  }
  if (raw.length > MAX_OPERATION_ID_LENGTH) {
    throw new Error(
      `OPERATION_ID_INVALID: operation id exceeds ${MAX_OPERATION_ID_LENGTH} characters`,
    );
  }
  // DSH rejects operation ids shorter than 16 characters; keep injected ids
  // usable by padding rather than failing the dispatch.
  return raw.length < MIN_OPERATION_ID_LENGTH ? raw.padEnd(MIN_OPERATION_ID_LENGTH, "0") : raw;
}

/** Accepts `{sessionId}`, `{session:{id}}` and a bare session id string. */
function extractSessionId(result: unknown): string | undefined {
  if (typeof result === "string") return result.length > 0 ? result : undefined;
  if (!isRecord(result)) return undefined;
  const direct = result.sessionId;
  if (typeof direct === "string" && direct.length > 0) return direct;
  const session = result.session;
  if (isRecord(session) && typeof session.id === "string" && session.id.length > 0) {
    return session.id;
  }
  return undefined;
}

/** Only `running`, `idle` and `unknown` are emitted; other tool states degrade. */
function mapSessionState(raw: string | undefined): WorkerState {
  switch (raw) {
    case "running":
    case "idle":
    case "unknown":
      return raw;
    default:
      return "unknown";
  }
}

/** Tolerates `{state}`, `{status}`, `{sessions:[...]}` and a bare string. */
function extractSessionState(result: unknown, session: string): string | undefined {
  if (typeof result === "string") return result;
  if (!isRecord(result)) return undefined;
  const direct = stateOf(result);
  if (direct !== undefined) return direct;
  const sessions = result.sessions;
  if (Array.isArray(sessions)) {
    const match =
      sessions.find((entry) => isRecord(entry) && entry.sessionId === session) ?? sessions[0];
    return isRecord(match) ? stateOf(match) : undefined;
  }
  if (isRecord(sessions)) {
    const entry = sessions[session];
    if (typeof entry === "string") return entry;
    return isRecord(entry) ? stateOf(entry) : undefined;
  }
  const single = result.session;
  return isRecord(single) ? stateOf(single) : undefined;
}

function stateOf(record: Record<string, unknown>): string | undefined {
  if (typeof record.state === "string") return record.state;
  if (typeof record.status === "string") return record.status;
  return undefined;
}

/** Normalizes the Remote transcript page into the bridge's `TranscriptPage`. */
function toTranscriptPage(result: unknown, session: string): TranscriptPage {
  if (!isRecord(result)) {
    throw new Error("TRANSCRIPT_INVALID: remote_read_session returned a non-object");
  }
  const events: TranscriptEvent[] = [];
  if (Array.isArray(result.events)) {
    result.events.forEach((raw, index) => {
      if (isRecord(raw)) events.push(toEvent(raw, index));
    });
  }
  const next = toCursor(result.next);
  return {
    session,
    events,
    ...(next === undefined ? {} : { next }),
    truncated: result.truncated === true,
  };
}

function toEvent(raw: Record<string, unknown>, index: number): TranscriptEvent {
  const seq = typeof raw.seq === "number" ? raw.seq : index + 1;
  const at = typeof raw.at === "string" ? raw.at : "";
  const type = typeof raw.type === "string" ? raw.type : "unknown";
  return typeof raw.text === "string" ? { seq, at, type, text: raw.text } : { seq, at, type };
}

function toCursor(value: unknown): { beforeSeq: number; throughSeq: number } | undefined {
  if (!isRecord(value)) return undefined;
  const { beforeSeq, throughSeq } = value;
  if (typeof beforeSeq !== "number" || typeof throughSeq !== "number") return undefined;
  return { beforeSeq, throughSeq };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

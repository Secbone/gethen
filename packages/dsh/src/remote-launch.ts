import { createRemoteBridge, type RemoteClientLike } from "@gethen/bridge-remote";
import {
  type EscalationKindLike,
  type EscalationOptionLike,
  type EscalationRecord,
  type EscalationResolutionInput,
  type LaunchPort,
  type LaunchRequest,
  type LaunchResult,
  parseReportText,
  RESOLUTION_CLOSE,
  RESOLUTION_OPEN,
  RESOLUTION_PROTO,
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
  /**
   * Answers a Worker escalation. Returning `undefined` parks the escalation for
   * a person: the launch ends with a `waiting` report and does not send a
   * resolution back to the session.
   */
  resolveEscalation?: (record: EscalationRecord) => Promise<EscalationResolutionInput | undefined>;
  /** Observer fired as soon as an escalation is parsed (status "waiting"). */
  onEscalation?: (record: EscalationRecord) => void;
  /** Observer fired once an escalation is resolved or parked, after its update. */
  onResolution?: (record: EscalationRecord) => void;
  /** Maximum escalation round-trips per launch; defaults to 3. */
  maxRounds?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => Date;
}

const DEFAULT_POLL_MS = 1_000;
const DEFAULT_MAX_POLLS = 120;
const DEFAULT_MAX_ROUNDS = 3;

const ESCALATION_OPEN = "[GETHEN-ESCALATION]";
const ESCALATION_CLOSE = "[/GETHEN-ESCALATION]";
const ESCALATION_KINDS: readonly EscalationKindLike[] = [
  "decision",
  "blocked",
  "permission",
  "scope",
  "conflict",
];
const ROUND_LIMIT_SUMMARY = "escalation round limit reached";

/** The subset of the bridge's session state this loop reasons about. */
type SessionState = "running" | "waiting" | "idle" | "ended" | "unknown";

/** Wait until the session stops running, polling at most `maxPolls` times. */
async function waitForIdle(options: {
  session: string;
  state: (session: string) => Promise<SessionState>;
  pollMs: number;
  maxPolls: number;
  sleep: (ms: number) => Promise<void>;
}): Promise<SessionState> {
  const { session, state: readState, pollMs, maxPolls, sleep } = options;
  let state = await readState(session);
  for (let poll = 0; poll < maxPolls && state === "running"; poll += 1) {
    await sleep(pollMs);
    state = await readState(session);
  }
  return state;
}

/**
 * Compose a `LaunchPort` from the DSH Remote session tools: create a session in
 * the configured workspace, send the assignment prompt, wait until the session
 * stops running, read the transcript and parse the Worker's markers.
 *
 * P3b closes the blocking-escalation loop in the same session:
 *
 * 1. wait for idle, read the transcript;
 * 2. if a `[GETHEN-ESCALATION]` block exists, record it, ask the injected
 *    resolver, and either send a `[GETHEN-RESOLUTION]` back and keep waiting
 *    (same session, no relaunch) or park it for a person;
 * 3. otherwise parse the `[GETHEN-REPORT]` block and finish.
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
  const pollMs = config.pollMs ?? DEFAULT_POLL_MS;
  const maxPolls = config.maxPolls ?? DEFAULT_MAX_POLLS;
  const maxRounds = config.maxRounds ?? DEFAULT_MAX_ROUNDS;

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

      const escalations: EscalationRecord[] = [];
      let round = 0;

      for (;;) {
        const state = await waitForIdle({
          session: ref.session,
          state: (session) => bridge.state(session),
          pollMs,
          maxPolls,
          sleep,
        });
        const page = await bridge.read(ref.session);
        const transcript = page.events.map((event) => event.text ?? "").join("\n");
        const at = now().toISOString();

        const escalation = parseEscalationText(transcript);
        if (escalation !== undefined) {
          if (round >= maxRounds) {
            return composeResult(request, worker(request), config.workspaceId, {
              report: { kind: "waiting", summary: ROUND_LIMIT_SUMMARY, at },
              events: page.events.length,
              at,
              escalations,
            });
          }

          const record = toEscalationRecord(escalation, request, ref.session, at);
          escalations.push(record);
          config.onEscalation?.(record);

          const resolution = await config.resolveEscalation?.(record);
          if (resolution === undefined) {
            record.status = "parked";
            config.onResolution?.(record);
            const summary = `waiting for a person: ${record.question}`;
            return composeResult(request, worker(request), config.workspaceId, {
              report: { kind: "waiting", summary, question: record.question, at },
              events: page.events.length,
              at,
              escalations,
            });
          }

          record.status = resolution.kind === "answer" ? "resolved" : "parked";
          record.resolution = {
            kind: resolution.kind,
            text: resolution.text,
            ...(resolution.option === undefined ? {} : { option: resolution.option }),
            at,
          };
          config.onResolution?.(record);
          await bridge.send(
            ref.session,
            buildResolutionBlock(record, resolution, at),
            `resolution-round-${round}`,
          );
          round += 1;
          continue;
        }

        const report: Report = parseReportText(transcript) ?? fallbackReport(state, at);
        return composeResult(request, worker(request), config.workspaceId, {
          report,
          events: page.events.length,
          at,
          escalations,
        });
      }
    },
  };
}

/** The parseable subset of a Worker escalation block. */
interface EscalationBlock {
  id: string;
  ticket?: string;
  assignment?: string;
  session?: string;
  kind: EscalationKindLike;
  question: string;
  options: EscalationOptionLike[];
  defaultOption?: string;
}

/**
 * Parse the first `[GETHEN-ESCALATION] ... [/GETHEN-ESCALATION]` block of a
 * transcript. Returns `undefined` when there is no marker; a block that exists
 * but is malformed throws, so an invented or truncated escalation never gets
 * recorded as a real one.
 */
function parseEscalationText(text: string): EscalationBlock | undefined {
  const open = text.indexOf(ESCALATION_OPEN);
  if (open === -1) return undefined;

  const bodyStart = open + ESCALATION_OPEN.length;
  const close = text.indexOf(ESCALATION_CLOSE, bodyStart);
  if (close === -1) {
    throw new Error(`escalation block is missing its closing marker ${ESCALATION_CLOSE}`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text.slice(bodyStart, close).trim());
  } catch (error) {
    throw new Error(
      `escalation block is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  }
  if (!isRecord(parsed)) throw new Error("escalation block must contain a JSON object");

  const { id, ticket, assignment, session, kind, question, options, defaultOption } = parsed;
  if (typeof id !== "string" || id.length === 0) {
    throw new Error('escalation is missing required field "id"');
  }
  if (typeof kind !== "string" || !isEscalationKind(kind)) {
    throw new Error(`escalation "kind" must be one of: ${ESCALATION_KINDS.join(", ")}`);
  }
  if (typeof question !== "string" || question.length === 0) {
    throw new Error('escalation is missing required field "question"');
  }
  if (!Array.isArray(options)) {
    throw new Error('escalation field "options" must be an array');
  }
  const parsedOptions = options.map((option, index) => {
    if (!isRecord(option) || typeof option.id !== "string" || typeof option.label !== "string") {
      throw new Error(`escalation option ${index} must have string "id" and "label"`);
    }
    return { id: option.id, label: option.label };
  });
  if (defaultOption !== undefined && typeof defaultOption !== "string") {
    throw new Error('escalation field "defaultOption" must be a string when present');
  }

  const block: EscalationBlock = {
    id,
    kind,
    question,
    options: parsedOptions,
  };
  const ticketId = optionalId(ticket, "ticket");
  if (ticketId !== undefined) block.ticket = ticketId;
  const assignmentId = optionalId(assignment, "assignment");
  if (assignmentId !== undefined) block.assignment = assignmentId;
  const sessionId = optionalId(session, "session");
  if (sessionId !== undefined) block.session = sessionId;
  if (defaultOption !== undefined) block.defaultOption = defaultOption;
  return block;
}

/** Turn a parsed block into the record the engine persists and observers see. */
function toEscalationRecord(
  block: EscalationBlock,
  request: LaunchRequest,
  session: string,
  at: string,
): EscalationRecord {
  return {
    id: block.id,
    ticket: block.ticket ?? request.ticket.id,
    assignment: block.assignment ?? request.assignment.id,
    session: block.session ?? session,
    kind: block.kind,
    question: block.question,
    options: block.options,
    ...(block.defaultOption === undefined ? {} : { defaultOption: block.defaultOption }),
    status: "waiting",
    at,
  };
}

/** Render the answer sent back into the same session. */
function buildResolutionBlock(
  record: EscalationRecord,
  resolution: EscalationResolutionInput,
  at: string,
): string {
  const body: Record<string, unknown> = {
    proto: RESOLUTION_PROTO,
    id: `resolution-${record.id}`,
    escalation: record.id,
    kind: resolution.kind,
    text: resolution.text,
    confidence: 1,
    basis: ["inference"],
    at,
  };
  if (resolution.option !== undefined) body.option = resolution.option;
  return [RESOLUTION_OPEN, JSON.stringify(body, null, 2), RESOLUTION_CLOSE].join("\n");
}

/** A Worker that stops without any marker needs a person either way. */
function fallbackReport(state: SessionState, at: string): Report {
  return state === "idle"
    ? { kind: "waiting", summary: "worker finished without a report marker", at }
    : { kind: "failed", summary: `worker did not settle: state ${state}`, at };
}

/** Compose the LaunchResult: receipt from transcript evidence, escalations kept. */
function composeResult(
  request: LaunchRequest,
  worker: string,
  workspace: string,
  outcome: {
    report: Report;
    events: number;
    at: string;
    escalations: EscalationRecord[];
  },
): LaunchResult {
  const result: LaunchResult = {
    report: outcome.report,
    receipt: {
      assignment: request.assignment.id,
      ticket: request.ticket.id,
      worker,
      workspace,
      packDigest: request.packDigest,
      at: outcome.at,
      outcome: outcome.report.kind === "failed" ? "failed" : "completed",
      summary: outcome.report.summary,
      events: outcome.events,
    },
  };
  if (outcome.escalations.length > 0) result.escalations = outcome.escalations;
  return result;
}

function isEscalationKind(value: string): value is EscalationKindLike {
  return (ESCALATION_KINDS as readonly string[]).includes(value);
}

function optionalId(value: unknown, field: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`escalation field "${field}" must be a non-empty string when present`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

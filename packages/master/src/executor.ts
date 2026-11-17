/**
 * Launch effect executor: the only place the control plane calls a Worker.
 *
 * The engine queues a `launch` effect whose payload is a snapshot of the Ticket
 * and its pending Assignment. The handler below:
 *
 * 1. validates that payload;
 * 2. marks the Assignment started in the engine (pure state change);
 * 3. calls `LaunchPort.launch` with the rendered assignment prompt;
 * 4. feeds the resulting Report back into the engine and appends the receipt.
 *
 * A launch failure becomes a `failed` Report (so the Ticket's stage says a
 * person is needed) and is rethrown so the outbox can retry. Nothing here owns
 * transport or persistence; both arrive as ports.
 */
import type { Engine, LaunchEffectPayload } from "./engine";
import type { Assignment, Ticket } from "./model";
import type { OutboxHandler } from "./outbox";
import type { Clock, LaunchPort, LaunchRequest, LaunchResult, ReceiptSink } from "./ports";
import { isLaunchTimeout } from "./ports";
import { buildAssignmentPrompt } from "./prompts";

/** Dependencies of the launch handler. */
export interface LaunchHandlerDeps {
  engine: Engine;
  launch: LaunchPort;
  receipts: ReceiptSink;
  clock?: Clock;
  /**
   * Derives the human-facing objective placed in the assignment envelope. It
   * receives the request as it would be launched with the default objective
   * (the prompt is rebuilt once the real objective is known).
   */
  objective?: (request: LaunchRequest) => string;
}

/** The outbox handler shape the relay expects. */
export type LaunchHandler = (payload: unknown) => Promise<void>;

/** Handlers keyed by effect kind, ready for `OutboxRunner` verbatim. */
export type MasterOutboxHandlers = { launch: OutboxHandler } & Record<string, OutboxHandler>;

const SYSTEM_CLOCK: Clock = { now: () => new Date() };

/**
 * Build the `launch` outbox handler. The returned function is safe to register
 * directly with `OutboxRunner`; a thrown error means "retry this effect".
 */
export function createLaunchHandler(deps: LaunchHandlerDeps): LaunchHandler {
  const clock = deps.clock ?? SYSTEM_CLOCK;
  return async (payload: unknown): Promise<void> => {
    const { ticket, assignment, packDigest } = asLaunchPayload(payload);
    // A person may close the Ticket after the effect was queued. Do not start a
    // Worker for closed work; record the aborted attempt and settle the effect.
    const current = deps.engine.ticket(ticket.id);
    if (current === undefined || current.endedAt !== undefined) {
      deps.receipts.append({
        assignment: assignment.id,
        ticket: ticket.id,
        worker: assignment.worker,
        workspace: assignment.workspace,
        packDigest,
        at: clock.now().toISOString(),
        outcome: "aborted",
        summary:
          current === undefined
            ? "ticket disappeared before the launch effect ran"
            : `ticket closed as ${current.outcome ?? "unknown"} before the launch effect ran`,
        events: 0,
      });
      return;
    }
    deps.engine.assignmentStarted(ticket.id, assignment.id);

    const request = buildLaunchRequest(deps, ticket, assignment, packDigest);
    let result: LaunchResult;
    try {
      result = await deps.launch.launch(request);
    } catch (error) {
      const at = clock.now().toISOString();
      deps.engine.report(ticket.id, { kind: "failed", summary: String(error), at });
      // A silence timeout still leaves the dispatched task running. Retrying the
      // effect would launch a second Worker on the same Assignment, so settle it
      // here; a later result for it supersedes the provisional failure.
      if (isLaunchTimeout(error)) {
        deps.receipts.append({
          assignment: assignment.id,
          ticket: ticket.id,
          worker: assignment.worker,
          workspace: assignment.workspace,
          packDigest,
          at,
          outcome: "failed",
          summary: String(error),
          events: 0,
        });
        return;
      }
      throw error;
    }

    deps.engine.report(ticket.id, result.report);
    deps.receipts.append(result.receipt);
  };
}

/** All handlers this package registers with the outbox relay. */
export function createOutboxHandlers(deps: LaunchHandlerDeps): MasterOutboxHandlers {
  return { launch: createLaunchHandler(deps) };
}

function buildLaunchRequest(
  deps: LaunchHandlerDeps,
  ticket: Ticket,
  assignment: Assignment,
  packDigest: string,
): LaunchRequest {
  const fallback = defaultObjective(deps, ticket);
  const objective = objectiveFor(deps, ticket, assignment, packDigest, fallback);
  const prompt = buildAssignmentPrompt({ ticket, assignment, packDigest, objective });
  return { assignment, ticket, packDigest, prompt };
}

function objectiveFor(
  deps: LaunchHandlerDeps,
  ticket: Ticket,
  assignment: Assignment,
  packDigest: string,
  fallback: string,
): string {
  if (deps.objective === undefined) return fallback;
  const provisionalPrompt = buildAssignmentPrompt({
    ticket,
    assignment,
    packDigest,
    objective: fallback,
  });
  return deps.objective({ assignment, ticket, packDigest, prompt: provisionalPrompt });
}

/** The objective a Ticket implies when the caller supplies none. */
function defaultObjective(deps: LaunchHandlerDeps, ticket: Ticket): string {
  const text = deps.engine.ticketText(ticket.id);
  if (text !== undefined) return text;
  return `Complete the work authorized by ticket ${ticket.id} (source: ${ticket.source}).`;
}

/**
 * Validate the engine's launch effect snapshot. Only the shape this handler
 * depends on is checked; the engine still owns the Ticket/Assignment relation.
 */
function asLaunchPayload(payload: unknown): LaunchEffectPayload {
  if (!isRecord(payload)) {
    throw new Error("launch payload must be an object");
  }
  const { ticket, assignment, packDigest } = payload;
  if (!isRecord(ticket) || typeof ticket.id !== "string" || ticket.id.length === 0) {
    throw new Error('launch payload is missing a ticket with a string "id"');
  }
  if (!isRecord(assignment) || typeof assignment.id !== "string" || assignment.id.length === 0) {
    throw new Error('launch payload is missing an assignment with a string "id"');
  }
  if (typeof packDigest !== "string") {
    throw new Error('launch payload is missing a string "packDigest"');
  }
  return {
    ticket: ticket as unknown as Ticket,
    assignment: assignment as unknown as Assignment,
    packDigest,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

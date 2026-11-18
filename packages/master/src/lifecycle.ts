/**
 * Ticket lifecycle rules: terminal state, acknowledgement and archive policy.
 *
 * Pure functions only. The engine persists whatever these return; the overview
 * and the CLI derive the same answers from stored facts, so "what needs a
 * person" cannot drift between the tool surface and the terminal.
 */
import {
  type Assignment,
  DAY_MS,
  type StageFacts,
  type Ticket,
  type TicketAck,
  type TicketCloseOutcome,
  type TicketOutcome,
  type TicketStage,
  ticketStage,
} from "./model";

/** The three states the operator sees. Closed and archived Tickets are terminal. */
export type TicketLifecycle = "open" | "closed" | "archived";

/** How long ended Tickets stay visible, per outcome. */
export interface ArchivePolicy {
  readonly retainMs: Readonly<Record<TicketOutcome, number>>;
}

/** Default retention: finished work stays visible for a week, abandoned work for a day. */
export const DEFAULT_ARCHIVE_POLICY: ArchivePolicy = Object.freeze({
  retainMs: Object.freeze({
    merged: 30 * DAY_MS,
    completed: 7 * DAY_MS,
    closed: 7 * DAY_MS,
    dropped: DAY_MS,
    stopped: DAY_MS,
  }),
});

/** How long open work may wait for a person before it is closed automatically. */
export interface StalePolicy {
  /** A `ready` report awaiting review becomes `completed`. */
  readonly readyAfterMs: number;
  /** A `failed`/`blocked` report awaiting a person becomes `dropped`. */
  readonly failedAfterMs: number;
}

/** Default stale window: one week for both review and failure parking. */
export const DEFAULT_STALE_POLICY: StalePolicy = Object.freeze({
  readyAfterMs: 7 * DAY_MS,
  failedAfterMs: 7 * DAY_MS,
});

/** The terminal state of a Ticket; archived implies closed. */
export function ticketLifecycle(ticket: Ticket): TicketLifecycle {
  if (ticket.archivedAt !== undefined) return "archived";
  return ticket.endedAt === undefined ? "open" : "closed";
}

/** True while a pending/running Assignment still owns work for this Ticket. */
export function hasLiveAssignment(ticket: Ticket, assignments: readonly Assignment[]): boolean {
  const ids = new Set(ticket.assignments);
  return assignments.some(
    (assignment) =>
      ids.has(assignment.id) && (assignment.state === "pending" || assignment.state === "running"),
  );
}

/** Stored facts the stage table reads. No timers and no human guessing. */
export function factsForTicket(ticket: Ticket, assignments: readonly Assignment[]): StageFacts {
  return {
    liveAssignment: hasLiveAssignment(ticket, assignments),
    // Ticket rows carry no report timestamp, so the persisted view cannot tell
    // whether a person spoke after the latest report. Callers that know better
    // may override this flag.
    personSinceReport: false,
    deliveryFailed: ticket.delivery !== undefined,
  };
}

/** The stage table applied to stored facts. */
export function stageForTicket(ticket: Ticket, assignments: readonly Assignment[]): TicketStage {
  return ticketStage(ticket, factsForTicket(ticket, assignments));
}

/**
 * What the person acknowledged. Any change to the actionable facts changes the
 * fingerprint, which makes the Ticket actionable again.
 */
export function ticketFingerprint(ticket: Ticket, stage: TicketStage): string {
  return [
    stage,
    ticket.latestReport ?? "",
    ticket.attention?.askedAt ?? "",
    ticket.delivery?.at ?? "",
  ].join("|");
}

/** True when a person still has to act on this Ticket. */
export function needsAction(ticket: Ticket, assignments: readonly Assignment[]): boolean {
  if (ticketLifecycle(ticket) !== "open") return false;
  const stage = stageForTicket(ticket, assignments);
  if (!stage.startsWith("person/")) return false;
  return ticket.ack?.fingerprint !== ticketFingerprint(ticket, stage);
}

/** Record that a person saw the current state of an open Ticket. */
export function acknowledgeTicket(
  ticket: Ticket,
  at: string,
  assignments: readonly Assignment[],
): Ticket {
  const stage = stageForTicket(ticket, assignments);
  const ack: TicketAck = {
    at,
    stage,
    fingerprint: ticketFingerprint(ticket, stage),
  };
  return { ...ticket, ack };
}

/** Last stored activity on a Ticket: its own open time or its latest Assignment. */
export function ticketActivityAt(ticket: Ticket, assignments: readonly Assignment[]): string {
  const ids = new Set(ticket.assignments);
  let latest = ticket.openedAt;
  for (const assignment of assignments) {
    if (!ids.has(assignment.id)) continue;
    for (const at of [assignment.startedAt, assignment.endedAt]) {
      if (at === undefined) continue;
      if (Date.parse(at) > Date.parse(latest)) latest = at;
    }
  }
  return latest;
}

/**
 * Open Tickets whose report has waited past the policy without a live
 * Assignment or a pending question. Auto-closing these is what keeps the
 * needs-action list from growing forever; an answered/parked escalation is
 * never closed behind the person's back.
 */
export function staleCandidates(
  tickets: readonly Ticket[],
  assignments: readonly Assignment[],
  now: Date,
  policy: StalePolicy = DEFAULT_STALE_POLICY,
): Array<{ ticket: Ticket; outcome: TicketCloseOutcome }> {
  const due: Array<{ ticket: Ticket; outcome: TicketCloseOutcome }> = [];
  for (const ticket of tickets) {
    if (ticket.endedAt !== undefined || ticket.archivedAt !== undefined) continue;
    if (ticket.attention !== undefined) continue;
    if (hasLiveAssignment(ticket, assignments)) continue;
    const report = ticket.latestReport;
    const retainMs =
      report === "ready"
        ? policy.readyAfterMs
        : report === "failed" || report === "blocked"
          ? policy.failedAfterMs
          : undefined;
    if (retainMs === undefined || !Number.isFinite(retainMs)) continue;
    const activity = Date.parse(ticketActivityAt(ticket, assignments));
    if (!Number.isFinite(activity) || activity + retainMs > now.getTime()) continue;
    due.push({ ticket, outcome: report === "ready" ? "completed" : "dropped" });
  }
  return due;
}

/** Archive a terminal Ticket. */
export function archiveTicket(ticket: Ticket, at: string): Ticket {
  return ticket.archivedAt === undefined ? { ...ticket, archivedAt: at } : ticket;
}

/**
 * Ended Tickets whose outcome retention has elapsed. Archived Tickets are not
 * candidates; the caller stamps `archivedAt` with one shared clock reading.
 */
export function archiveCandidates(
  tickets: readonly Ticket[],
  now: Date,
  policy: ArchivePolicy = DEFAULT_ARCHIVE_POLICY,
): Ticket[] {
  const due: Ticket[] = [];
  for (const ticket of tickets) {
    if (ticket.archivedAt !== undefined || ticket.endedAt === undefined) continue;
    const ended = Date.parse(ticket.endedAt);
    if (Number.isNaN(ended)) continue;
    const outcome = ticket.outcome ?? "closed";
    const retainMs = policy.retainMs[outcome];
    if (!Number.isFinite(retainMs)) continue;
    if (ended + retainMs <= now.getTime()) due.push(ticket);
  }
  return due;
}

/**
 * Read-only control-plane projections: what needs a person, what is open, and
 * what has reached a terminal state. CLI status, the HTTP surface and the DSH
 * tools all consume these shapes instead of re-deriving ticket state.
 */

import { needsAction, stageForTicket, type TicketLifecycle, ticketLifecycle } from "./lifecycle";
import type { Assignment, AssignmentReceipt, EscalationRecord, Ticket, TicketStage } from "./model";

/** Which Tickets an overview lists. The default is what a person must act on. */
export type OverviewFilter = "needs-action" | "open" | "all";

export const OVERVIEW_FILTERS: readonly OverviewFilter[] = ["needs-action", "open", "all"];

export interface TicketSummary {
  id: string;
  source: string;
  stage: TicketStage;
  lifecycle: TicketLifecycle;
  needsAction: boolean;
  openedAt: string;
  assignments: number;
  latestReport?: string;
  outcome?: string;
  question?: string;
  assignmentId?: string;
  worker?: string;
  acknowledgedAt?: string;
  endedAt?: string;
  archivedAt?: string;
}

export interface ControlCounts {
  tickets: number;
  open: number;
  needsAction: number;
  archived: number;
  receipts: number;
  escalations: number;
}

export interface ControlOverview {
  generatedAt: string;
  filter: OverviewFilter;
  counts: ControlCounts;
  tickets: TicketSummary[];
}

export interface ControlStatus {
  counts: ControlCounts;
  recent: TicketSummary[];
}

export interface ProjectionInput {
  tickets: readonly Ticket[];
  assignments: readonly Assignment[];
  receipts: readonly AssignmentReceipt[];
  escalations: readonly EscalationRecord[];
}

/** One Ticket projected with the same stage table the engine uses. */
export function summarizeTicket(ticket: Ticket, assignments: readonly Assignment[]): TicketSummary {
  const ids = new Set(ticket.assignments);
  const latestId = ticket.assignments.at(-1);
  const latest =
    latestId === undefined ? undefined : assignments.find((row) => row.id === latestId);
  const summary: TicketSummary = {
    id: ticket.id,
    source: ticket.source,
    stage: stageForTicket(ticket, assignments),
    lifecycle: ticketLifecycle(ticket),
    needsAction: needsAction(ticket, assignments),
    openedAt: ticket.openedAt,
    assignments: assignments.filter((row) => ids.has(row.id)).length,
  };
  if (ticket.latestReport !== undefined) summary.latestReport = ticket.latestReport;
  if (ticket.outcome !== undefined) summary.outcome = ticket.outcome;
  if (ticket.attention !== undefined) summary.question = ticket.attention.question;
  if (latest !== undefined) {
    summary.assignmentId = latest.id;
    summary.worker = latest.worker;
  }
  if (ticket.ack !== undefined) summary.acknowledgedAt = ticket.ack.at;
  if (ticket.endedAt !== undefined) summary.endedAt = ticket.endedAt;
  if (ticket.archivedAt !== undefined) summary.archivedAt = ticket.archivedAt;
  return summary;
}

export function controlCounts(input: ProjectionInput): ControlCounts {
  const summaries = input.tickets.map((ticket) => summarizeTicket(ticket, input.assignments));
  return {
    tickets: summaries.length,
    open: summaries.filter((ticket) => ticket.lifecycle === "open").length,
    needsAction: summaries.filter((ticket) => ticket.needsAction).length,
    archived: summaries.filter((ticket) => ticket.lifecycle === "archived").length,
    receipts: input.receipts.length,
    escalations: input.escalations.length,
  };
}

/** Project and filter; `needs-action` is the safe default for an operator view. */
export function buildControlOverview(
  input: ProjectionInput,
  options: { now?: Date; filter?: OverviewFilter } = {},
): ControlOverview {
  const filter = options.filter ?? "needs-action";
  const summaries = input.tickets.map((ticket) => summarizeTicket(ticket, input.assignments));
  const tickets = summaries.filter((ticket) => {
    if (filter === "all") return true;
    if (filter === "open") return ticket.lifecycle === "open";
    return ticket.needsAction;
  });
  return {
    generatedAt: (options.now ?? new Date()).toISOString(),
    filter,
    counts: controlCounts(input),
    tickets,
  };
}

/** Counts plus the newest Tickets, in newest-first order. */
export function buildControlStatus(
  input: ProjectionInput,
  options: { recentLimit?: number } = {},
): ControlStatus {
  const limit = Math.max(0, options.recentLimit ?? 10);
  const summaries = input.tickets.map((ticket) => summarizeTicket(ticket, input.assignments));
  return {
    counts: controlCounts(input),
    recent: summaries.slice(-limit).reverse(),
  };
}

/**
 * GitHub as a Source: open Issues and Pull Requests become Triggers.
 *
 * P1 polls one page of the REST issues endpoint, ordered by `updated` ascending,
 * and keeps two read positions in {@link SourceState}:
 *
 * - `github:<repo>:cursor` — the newest `updated_at` seen on the last page; sent
 *   back as `since` so GitHub only returns items updated after it.
 * - `github:<repo>#<number>` — per-thread watermark; an item whose `updated_at`
 *   is not newer than its watermark is a replay and is skipped.
 *
 * No pagination and no webhooks in P1; a Source never calls fetch directly, it
 * goes through {@link HttpPort} so tests can script responses.
 */

import type { Addressed, Trigger } from "../model";
import type { HttpPort, HttpResponse, SourcePort, SourceState } from "../ports";

export interface GitHubSourceOptions {
  /** `owner/name`. */
  repo: string;
  /** Login used to detect `@mentions` and direct assignments. */
  agentLogin: string;
  http: HttpPort;
  state: SourceState;
  token?: string;
  /** When non-empty, only items carrying at least one of these labels are mapped. */
  labels?: string[];
  /** Defaults to `https://api.github.com`; a trailing slash is ignored. */
  baseUrl?: string;
}

const DEFAULT_BASE_URL = "https://api.github.com";
const MAX_TEXT_LENGTH = 8_000;
const ERROR_BODY_FRAGMENT = 200;

/** A non-2xx GitHub response; `status` and a `body` fragment are kept for callers. */
export class GitHubHttpError extends Error {
  readonly status: number;
  readonly body: string;

  constructor(status: number, body: string) {
    const fragment =
      body.length > ERROR_BODY_FRAGMENT ? `${body.slice(0, ERROR_BODY_FRAGMENT)}…` : body;
    super(`GitHub HTTP ${status}: ${fragment}`);
    this.name = "GitHubHttpError";
    this.status = status;
    this.body = fragment;
  }
}

interface GitHubUser {
  login?: string;
  type?: string;
}

interface GitHubLabel {
  name?: string;
}

/** The subset of the REST issue/PR payload this Source maps. */
interface GitHubIssue {
  number?: number;
  title?: string;
  body?: string | null;
  updated_at?: string;
  html_url?: string;
  user?: GitHubUser | null;
  assignee?: GitHubUser | null;
  assignees?: GitHubUser[] | null;
  labels?: GitHubLabel[] | null;
  pull_request?: unknown;
}

interface GitHubIssueWithKey extends GitHubIssue {
  number: number;
  updated_at: string;
  title: string;
}

export class GitHubSource implements SourcePort {
  readonly id = "github";
  readonly label: string;

  private readonly repo: string;
  private readonly agentLogin: string;
  private readonly http: HttpPort;
  private readonly state: SourceState;
  private readonly token?: string;
  private readonly labels: string[];
  private readonly baseUrl: string;

  constructor(options: GitHubSourceOptions) {
    this.repo = options.repo;
    this.agentLogin = options.agentLogin;
    this.http = options.http;
    this.state = options.state;
    this.token = options.token;
    this.labels = options.labels ?? [];
    this.baseUrl = (options.baseUrl ?? DEFAULT_BASE_URL).replace(/\/+$/, "");
    this.label = `GitHub ${options.repo}`;
  }

  async poll(): Promise<Trigger[]> {
    const cursor = this.state.get<string>(this.cursorKey());
    const query = "state=open&sort=updated&direction=asc&per_page=50";
    const since = cursor === undefined ? "" : `&since=${cursor}`;
    const response = await this.http.send({
      url: `${this.baseUrl}/repos/${this.repo}/issues?${query}${since}`,
      method: "GET",
      headers: this.headers(),
    });
    this.assertSuccess(response, "list issues");

    const triggers: Trigger[] = [];
    let newest: string | undefined = cursor;
    for (const item of parseIssues(response.body)) {
      if (!hasKey(item)) continue;
      if (newest === undefined || item.updated_at > newest) newest = item.updated_at;
      // Label scope is applied before the watermark so an item outside the
      // configured labels never counts as read on its thread.
      if (!this.matchesLabels(item)) continue;

      const thread = this.threadKey(item.number);
      const watermark = this.state.get<string>(thread);
      if (watermark !== undefined && item.updated_at <= watermark) continue;

      triggers.push(this.toTrigger(item));
      this.state.set(thread, item.updated_at);
    }

    if (newest !== undefined && newest !== cursor) this.state.set(this.cursorKey(), newest);
    return triggers;
  }

  /**
   * Post a comment on the issue/PR a thread refers to. `threadOrChannel` is the
   * Trigger thread form `github:<repo>#<number>`; other forms are rejected.
   */
  async reply(threadOrChannel: string, text: string): Promise<void> {
    const number = this.numberFromThread(threadOrChannel);
    const response = await this.http.send({
      url: `${this.baseUrl}/repos/${this.repo}/issues/${number}/comments`,
      method: "POST",
      headers: { ...this.headers(), "Content-Type": "application/json" },
      body: JSON.stringify({ body: text }),
    });
    if (response.status !== 200 && response.status !== 201) {
      throw new GitHubHttpError(response.status, response.body);
    }
  }

  private headers(): Record<string, string> {
    const headers: Record<string, string> = {
      Accept: "application/vnd.github+json",
      "User-Agent": "gethen",
    };
    if (this.token !== undefined) headers.Authorization = `Bearer ${this.token}`;
    return headers;
  }

  private assertSuccess(response: HttpResponse, action: string): void {
    if (response.status >= 200 && response.status < 300) return;
    throw new GitHubHttpError(response.status, response.body || action);
  }

  private toTrigger(item: GitHubIssueWithKey): Trigger {
    const user = item.user ?? undefined;
    const actor = user?.login ?? "ghost";
    const text = `${item.title}\n\n${item.body ?? ""}`;
    const addressed: Addressed = text.includes(`@${this.agentLogin}`)
      ? "mention"
      : this.isAssignedToAgent(item)
        ? "direct"
        : "none";
    return {
      id: `github:${this.repo}#${item.number}:${item.updated_at}`,
      source: this.id,
      thread: this.threadKey(item.number),
      actor,
      bot: user?.type === "Bot" || actor.endsWith("[bot]"),
      addressed,
      text: text.length > MAX_TEXT_LENGTH ? text.slice(0, MAX_TEXT_LENGTH) : text,
      at: item.updated_at,
      meta: {
        repo: this.repo,
        number: String(item.number),
        kind: item.pull_request === undefined ? "issue" : "pull_request",
        url: item.html_url ?? `https://github.com/${this.repo}/issues/${item.number}`,
      },
    };
  }

  private isAssignedToAgent(item: GitHubIssue): boolean {
    if (item.assignee?.login === this.agentLogin) return true;
    return (item.assignees ?? []).some((assignee) => assignee?.login === this.agentLogin);
  }

  private matchesLabels(item: GitHubIssue): boolean {
    if (this.labels.length === 0) return true;
    return (item.labels ?? []).some(
      (label) => label.name !== undefined && this.labels.includes(label.name),
    );
  }

  private numberFromThread(thread: string): number {
    const prefix = `github:${this.repo}#`;
    if (!thread.startsWith(prefix)) {
      throw new Error(`Not a ${this.label} thread: ${thread}`);
    }
    const raw = thread.slice(prefix.length);
    if (!/^[1-9]\d*$/.test(raw)) {
      throw new Error(`Not a ${this.label} thread: ${thread}`);
    }
    return Number(raw);
  }

  private threadKey(number: number): string {
    return `github:${this.repo}#${number}`;
  }

  private cursorKey(): string {
    return `github:${this.repo}:cursor`;
  }
}

function parseIssues(body: string): GitHubIssue[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    throw new Error("GitHub issues response was not valid JSON");
  }
  if (!Array.isArray(parsed)) throw new Error("GitHub issues response was not an array");
  return parsed as GitHubIssue[];
}

function hasKey(item: GitHubIssue): item is GitHubIssueWithKey {
  return (
    typeof item.number === "number" &&
    Number.isSafeInteger(item.number) &&
    typeof item.updated_at === "string" &&
    item.updated_at.length > 0 &&
    typeof item.title === "string"
  );
}

import { describe, expect, it } from "vitest";
import type { HttpPort, HttpRequest, HttpResponse, SourceState } from "../ports";
import { GitHubHttpError, GitHubSource } from "./github";

const T1 = "2026-11-16T09:00:00Z";
const T2 = "2026-11-16T09:30:00Z";
const T3 = "2026-11-16T10:00:00Z";

const LIST_URL =
  "https://api.github.com/repos/acme/app/issues?state=open&sort=updated&direction=asc&per_page=50";

class ScriptedHttp implements HttpPort {
  readonly requests: HttpRequest[] = [];
  private readonly responses: Array<HttpResponse | Error>;

  constructor(responses: Array<HttpResponse | Error>) {
    this.responses = [...responses];
  }

  async send(request: HttpRequest): Promise<HttpResponse> {
    this.requests.push(request);
    const next = this.responses.shift();
    if (next === undefined) {
      throw new Error(`unexpected HTTP request: ${request.method ?? "GET"} ${request.url}`);
    }
    if (next instanceof Error) throw next;
    return next;
  }
}

function memoryState(): SourceState {
  const values = new Map<string, unknown>();
  return {
    get: <T>(key: string): T | undefined => values.get(key) as T | undefined,
    set: (key: string, value: unknown): void => {
      values.set(key, value);
    },
  };
}

function ok(body: unknown): HttpResponse {
  return { status: 200, headers: {}, body: JSON.stringify(body) };
}

function status(code: number, body = ""): HttpResponse {
  return { status: code, headers: {}, body };
}

function issue(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    number: 12,
    title: "Bug: flaky login test",
    body: "Please fix @gethen",
    updated_at: T1,
    html_url: "https://github.com/acme/app/issues/12",
    user: { login: "alice", type: "User" },
    assignee: null,
    assignees: [],
    labels: [],
    ...overrides,
  };
}

function setup(
  options: {
    responses?: Array<HttpResponse | Error>;
    state?: SourceState;
    repo?: string;
    agentLogin?: string;
    token?: string;
    labels?: string[];
    baseUrl?: string;
  } = {},
) {
  const http = new ScriptedHttp(options.responses ?? []);
  const state = options.state ?? memoryState();
  const source = new GitHubSource({
    repo: options.repo ?? "acme/app",
    agentLogin: options.agentLogin ?? "gethen",
    http,
    state,
    token: options.token,
    labels: options.labels,
    baseUrl: options.baseUrl,
  });
  return { source, http, state };
}

describe("GitHubSource.poll", () => {
  it("maps open issues and pull requests to triggers", async () => {
    const { source, http, state } = setup({
      responses: [
        ok([
          issue(),
          issue({
            number: 13,
            title: "Bump foo",
            body: null,
            updated_at: T2,
            html_url: "https://github.com/acme/app/pull/13",
            user: { login: "dependabot[bot]", type: "Bot" },
            pull_request: { url: "https://api.github.com/repos/acme/app/pulls/13" },
          }),
        ]),
      ],
    });

    expect(source.id).toBe("github");
    expect(source.label).toBe("GitHub acme/app");

    const triggers = await source.poll();

    expect(http.requests).toEqual([
      {
        url: LIST_URL,
        method: "GET",
        headers: { Accept: "application/vnd.github+json", "User-Agent": "gethen" },
      },
    ]);
    expect(triggers).toEqual([
      {
        id: `github:acme/app#12:${T1}`,
        source: "github",
        thread: "github:acme/app#12",
        actor: "alice",
        bot: false,
        addressed: "mention",
        text: "Bug: flaky login test\n\nPlease fix @gethen",
        at: T1,
        meta: {
          repo: "acme/app",
          number: "12",
          kind: "issue",
          url: "https://github.com/acme/app/issues/12",
        },
      },
      {
        id: `github:acme/app#13:${T2}`,
        source: "github",
        thread: "github:acme/app#13",
        actor: "dependabot[bot]",
        bot: true,
        addressed: "none",
        text: "Bump foo\n\n",
        at: T2,
        meta: {
          repo: "acme/app",
          number: "13",
          kind: "pull_request",
          url: "https://github.com/acme/app/pull/13",
        },
      },
    ]);
    expect(state.get("github:acme/app#12")).toBe(T1);
    expect(state.get("github:acme/app#13")).toBe(T2);
    expect(state.get("github:acme/app:cursor")).toBe(T2);
  });

  it("detects assignment as direct and lets a mention win", async () => {
    const { source } = setup({
      responses: [
        ok([
          issue({
            number: 20,
            title: "Assigned work",
            body: "No mention here",
            assignee: { login: "gethen", type: "User" },
          }),
          issue({
            number: 21,
            title: "Assigned with mention",
            body: "hi @gethen",
            assignees: [{ login: "gethen", type: "User" }],
          }),
        ]),
      ],
    });

    const triggers = await source.poll();

    expect(triggers.map((trigger) => trigger.addressed)).toEqual(["direct", "mention"]);
  });

  it("marks bots by user type or login suffix", async () => {
    const { source } = setup({
      responses: [
        ok([
          issue({ number: 30, user: { login: "coderabbitai", type: "Bot" } }),
          issue({ number: 31, user: { login: "release-bot[bot]", type: "User" } }),
          issue({ number: 32, user: { login: "alice", type: "User" } }),
        ]),
      ],
    });

    const triggers = await source.poll();

    expect(triggers.map((trigger) => trigger.bot)).toEqual([true, true, false]);
  });

  it("skips replayed updates and advances the cursor", async () => {
    const { source, http, state } = setup({
      responses: [ok([issue()]), ok([issue()]), ok([issue({ updated_at: T3 })])],
    });

    expect(await source.poll()).toHaveLength(1);
    expect(state.get("github:acme/app:cursor")).toBe(T1);

    expect(await source.poll()).toHaveLength(0);
    expect(http.requests[1]?.url).toBe(`${LIST_URL}&since=${T1}`);

    const updated = await source.poll();
    expect(updated).toHaveLength(1);
    expect(updated[0]?.id).toBe(`github:acme/app#12:${T3}`);
    expect(updated[0]?.at).toBe(T3);
    expect(http.requests[2]?.url).toBe(`${LIST_URL}&since=${T1}`);
    expect(state.get("github:acme/app#12")).toBe(T3);
  });

  it("leaves the cursor untouched when GitHub returns no items", async () => {
    const { source, state } = setup({ responses: [ok([])] });

    expect(await source.poll()).toEqual([]);
    expect(state.get("github:acme/app:cursor")).toBeUndefined();
  });

  it("only maps items carrying one of the configured labels", async () => {
    const { source, http } = setup({
      responses: [
        ok([
          issue({ number: 40, labels: [{ name: "bug" }] }),
          issue({ number: 41, labels: [{ name: "gethen" }, { name: "bug" }] }),
          issue({ number: 42, labels: [{ name: "docs" }] }),
        ]),
      ],
      labels: ["gethen"],
    });

    const triggers = await source.poll();

    expect(triggers).toHaveLength(1);
    expect(triggers[0]?.thread).toBe("github:acme/app#41");
    // The cursor tracks the whole page, including items outside the label scope.
    expect(http.requests[0]?.url).toBe(LIST_URL);
  });

  it("truncates text but detects mentions in the full body", async () => {
    const { source } = setup({
      responses: [
        ok([
          issue({ number: 50, title: "Long", body: `${"x".repeat(9_000)} @gethen` }),
          issue({ number: 51, title: "Short", body: null }),
        ]),
      ],
    });

    const [long, short] = await source.poll();

    expect(long?.text).toHaveLength(8_000);
    expect(long?.addressed).toBe("mention");
    expect(short?.text).toBe("Short\n\n");
    expect(short?.addressed).toBe("none");
  });

  it("sends the token and trims a trailing slash from the base URL", async () => {
    const { source, http } = setup({
      responses: [ok([])],
      token: "secret",
      baseUrl: "https://ghe.example/api/v3/",
    });

    await source.poll();

    expect(http.requests[0]?.url).toBe(
      "https://ghe.example/api/v3/repos/acme/app/issues?state=open&sort=updated&direction=asc&per_page=50",
    );
    expect(http.requests[0]?.headers).toEqual({
      Accept: "application/vnd.github+json",
      "User-Agent": "gethen",
      Authorization: "Bearer secret",
    });
  });

  it("throws with the status and a body fragment on 403, 429 and other failures", async () => {
    for (const code of [403, 429, 500]) {
      const { source, state } = setup({
        responses: [status(code, '{"message":"API rate limit exceeded for 1.2.3.4"}')],
      });

      const error = await source.poll().catch((thrown: unknown) => thrown);

      expect(error).toBeInstanceOf(GitHubHttpError);
      const httpError = error as GitHubHttpError;
      expect(httpError.status).toBe(code);
      expect(httpError.body).toContain("API rate limit exceeded");
      expect(httpError.message).toContain(String(code));
      expect(state.get("github:acme/app:cursor")).toBeUndefined();
    }
  });

  it("rejects responses that are not a JSON array", async () => {
    const notJson = setup({ responses: [status(200, "not json")] });
    await expect(notJson.source.poll()).rejects.toThrow(/not valid JSON/);

    const notArray = setup({ responses: [ok({ message: "unexpected" })] });
    await expect(notArray.source.poll()).rejects.toThrow(/not an array/);
  });
});

describe("GitHubSource.reply", () => {
  it("posts a comment to the issue a thread names", async () => {
    const { source, http } = setup({
      responses: [status(201, "{}"), status(200, "{}")],
      token: "secret",
    });

    await expect(source.reply("github:acme/app#12", "done @alice")).resolves.toBeUndefined();
    await expect(source.reply("github:acme/app#13", "also done")).resolves.toBeUndefined();

    expect(http.requests[0]).toEqual({
      url: "https://api.github.com/repos/acme/app/issues/12/comments",
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        "User-Agent": "gethen",
        Authorization: "Bearer secret",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ body: "done @alice" }),
    });
    expect(http.requests[1]?.url).toBe("https://api.github.com/repos/acme/app/issues/13/comments");
  });

  it("rejects failed posts and threads that do not belong to the repo", async () => {
    const { source, http } = setup({
      responses: [status(422, '{"message":"Validation Failed"}')],
    });

    const error = await source
      .reply("github:acme/app#12", "nope")
      .catch((thrown: unknown) => thrown);
    expect(error).toBeInstanceOf(GitHubHttpError);
    expect((error as GitHubHttpError).status).toBe(422);

    await expect(source.reply("github:other/repo#1", "nope")).rejects.toThrow(
      /Not a GitHub acme\/app thread/,
    );
    await expect(source.reply("github:acme/app#0", "nope")).rejects.toThrow(
      /Not a GitHub acme\/app thread/,
    );
    expect(http.requests).toHaveLength(1);
  });
});

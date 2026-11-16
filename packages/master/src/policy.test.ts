import { describe, expect, it } from "vitest";

import type {
  Mandate,
  Policy,
  PolicyRule,
  SourcePolicy,
  Ticket,
  Trigger,
  TriggerFacts,
} from "./model";
import type { CreatePolicyInput } from "./policy";
import { canResume, createPolicy, EMPTY_POLICY, gateTrigger, mandateValid } from "./policy";

const AT = "2026-11-16T06:00:00.000Z";
const LATER = "2026-11-16T06:30:00.000Z";

function source(overrides: Partial<SourcePolicy> = {}): SourcePolicy {
  return { allowed: ["alice"], bots: ["dependabot"], ...overrides };
}

function policy(overrides: Partial<CreatePolicyInput> = {}): Policy {
  return createPolicy({
    version: 1,
    acceptMode: "auto",
    rules: [{ id: "bug", name: "Bug fixes", instruction: "Fix reported bugs", enabled: true }],
    sources: { github: source() },
    ...overrides,
  });
}

function trigger(overrides: Partial<Trigger> = {}): Trigger {
  return {
    id: "trg-1",
    source: "github",
    thread: "thr-1",
    actor: "alice",
    bot: false,
    addressed: "mention",
    text: "please fix the bug",
    at: AT,
    ...overrides,
  };
}

function facts(overrides: Partial<TriggerFacts> = {}): TriggerFacts {
  return {
    source: "github",
    sender: "alice",
    addressed: "mention",
    bot: false,
    followup: false,
    scope: "issue",
    ...overrides,
  };
}

function mandate(overrides: Partial<Mandate> = {}): Mandate {
  return {
    id: "mnd-1",
    ticket: "tkt-1",
    policyVersion: 1,
    strength: "explicit",
    scope: "issue",
    basis: { via: "addressed", source: "github", sender: "alice" },
    matchedRules: ["bug"],
    createdAt: AT,
    ...overrides,
  };
}

function ticket(overrides: Partial<Ticket> = {}): Ticket {
  return {
    id: "tkt-1",
    mandate: "mnd-1",
    source: "github",
    threads: ["thr-1"],
    subscriptions: ["thr-1"],
    channels: [],
    inputs: [],
    assignments: [],
    openedAt: AT,
    ...overrides,
  };
}

describe("createPolicy", () => {
  it("defaults missing rules and sources and freezes the snapshot", () => {
    const created = createPolicy({ version: 2, acceptMode: "observe" });

    expect(created).toEqual({ version: 2, acceptMode: "observe", rules: [], sources: {} });
    expect(Object.isFrozen(created)).toBe(true);
  });

  it("copies rules and sources so later input mutation does not leak in", () => {
    const rules: PolicyRule[] = [
      { id: "bug", name: "Bug fixes", instruction: "Fix reported bugs", enabled: true },
    ];
    const sources: Record<string, SourcePolicy> = {
      github: { allowed: ["alice"], bots: [] },
    };

    const created = createPolicy({ version: 1, acceptMode: "auto", rules, sources });
    rules.push({ id: "docs", name: "Docs", instruction: "Write docs", enabled: true });
    sources.github?.allowed.push("mallory");

    expect(created.rules).toHaveLength(1);
    expect(created.sources.github?.allowed).toEqual(["alice"]);
  });

  it("rejects a non-positive version", () => {
    expect(() => createPolicy({ version: 0, acceptMode: "off" })).toThrow("policy.version");
    expect(() => createPolicy({ version: -3, acceptMode: "off" })).toThrow("policy.version");
  });

  it("rejects a non-integer version", () => {
    expect(() => createPolicy({ version: 1.5, acceptMode: "off" })).toThrow("policy.version");
    expect(() => createPolicy({ version: Number.NaN, acceptMode: "off" })).toThrow(
      "policy.version",
    );
  });

  it("rejects an unknown accept mode", () => {
    const invalid = { version: 1, acceptMode: "sometimes" } as unknown as CreatePolicyInput;
    expect(() => createPolicy(invalid)).toThrow("policy.acceptMode");
  });

  it("rejects duplicate rule ids", () => {
    expect(() =>
      createPolicy({
        version: 1,
        acceptMode: "auto",
        rules: [
          { id: "bug", name: "Bug fixes", instruction: "Fix reported bugs", enabled: true },
          { id: "bug", name: "Bug fixes again", instruction: "Fix reported bugs", enabled: false },
        ],
      }),
    ).toThrow(/duplicate: bug/);
  });
});

describe("EMPTY_POLICY", () => {
  it("is a frozen conservative default that admits nothing", () => {
    expect(EMPTY_POLICY).toMatchObject({
      version: 1,
      acceptMode: "off",
      rules: [],
      sources: {},
    });
    expect(Object.isFrozen(EMPTY_POLICY)).toBe(true);
    expect(gateTrigger(trigger(), facts(), EMPTY_POLICY)).toMatchObject({
      admissible: false,
      strength: "not_admitted",
    });
  });
});

describe("gateTrigger", () => {
  it("admits an allowed person who mentions the agent as explicit", () => {
    expect(gateTrigger(trigger(), facts(), policy())).toEqual({
      admissible: true,
      strength: "explicit",
      reasons: [],
    });
  });

  it("admits an allowed person's direct message as explicit", () => {
    expect(gateTrigger(trigger(), facts({ addressed: "direct" }), policy())).toEqual({
      admissible: true,
      strength: "explicit",
      reasons: [],
    });
  });

  it("admits an allowed person's follow-up on a Thread with Tickets as explicit", () => {
    expect(gateTrigger(trigger(), facts({ addressed: "none", followup: true }), policy())).toEqual({
      admissible: true,
      strength: "explicit",
      reasons: [],
    });
  });

  it("denies an allowed person's unaddressed message without ticket context", () => {
    const result = gateTrigger(trigger(), facts({ addressed: "none" }), policy());

    expect(result).toMatchObject({ admissible: false, strength: "not_admitted" });
    expect(result.reasons.map((reason) => reason.code)).toEqual(["not_admitted"]);
  });

  it("denies a person who is not on the allowlist even when they address the agent", () => {
    const result = gateTrigger(trigger(), facts({ sender: "mallory" }), policy());

    expect(result).toMatchObject({ admissible: false, strength: "not_admitted" });
    expect(result.reasons[0]).toMatchObject({ triggerId: "trg-1", values: { sender: "mallory" } });
  });

  it("denies a follow-up from a person who is not on the allowlist", () => {
    const result = gateTrigger(
      trigger(),
      facts({ sender: "mallory", addressed: "none", followup: true }),
      policy(),
    );

    expect(result).toMatchObject({ admissible: false, strength: "not_admitted" });
  });

  it("denies a source that has no Policy entry", () => {
    const result = gateTrigger(trigger(), facts({ source: "unknown" }), policy());

    expect(result).toMatchObject({ admissible: false, strength: "not_admitted" });
  });

  it("admits an allowlisted bot as inferred", () => {
    const result = gateTrigger(
      trigger(),
      facts({ sender: "dependabot", bot: true, addressed: "none" }),
      policy(),
    );

    expect(result).toEqual({ admissible: true, strength: "inferred", reasons: [] });
  });

  it("treats an allowlisted bot's mention as inferred, not explicit", () => {
    const result = gateTrigger(trigger(), facts({ sender: "dependabot", bot: true }), policy());

    expect(result).toEqual({ admissible: true, strength: "inferred", reasons: [] });
  });

  it("admits inferred work in observe mode; auto-accept is a later decision", () => {
    const result = gateTrigger(
      trigger(),
      facts({ sender: "dependabot", bot: true, addressed: "none" }),
      policy({ acceptMode: "observe" }),
    );

    expect(result).toEqual({ admissible: true, strength: "inferred", reasons: [] });
  });

  it("refuses inferred work when acceptMode is off", () => {
    const result = gateTrigger(
      trigger(),
      facts({ sender: "dependabot", bot: true, addressed: "none" }),
      policy({ acceptMode: "off" }),
    );

    expect(result.admissible).toBe(false);
    expect(result.strength).toBe("not_admitted");
    expect(result.reasons[0]?.code).toBe("accept_mode_off");
    expect(result.reasons[0]?.triggerId).toBe("trg-1");
  });

  it("denies a bot that is not on the allowlist", () => {
    const result = gateTrigger(
      trigger(),
      facts({ sender: "evil[bot]", bot: true, addressed: "none" }),
      policy(),
    );

    expect(result).toMatchObject({ admissible: false, strength: "not_admitted" });
    expect(result.reasons.map((reason) => reason.code)).toEqual(["not_admitted"]);
  });
});

describe("mandateValid", () => {
  it("keeps a Mandate valid while the Policy version is unchanged", () => {
    expect(mandateValid(mandate(), policy())).toBe(true);
  });

  it("keeps a Mandate valid when a Policy bump leaves its matched rules enabled", () => {
    expect(mandateValid(mandate(), policy({ version: 2 }))).toBe(true);
  });

  it("invalidates a Mandate whose matched rule was disabled", () => {
    const next = policy({
      version: 2,
      rules: [{ id: "bug", name: "Bug fixes", instruction: "Fix reported bugs", enabled: false }],
    });

    expect(mandateValid(mandate(), next)).toBe(false);
  });

  it("invalidates a Mandate when one of several matched rules was disabled", () => {
    const next = policy({
      version: 2,
      rules: [
        { id: "bug", name: "Bug fixes", instruction: "Fix reported bugs", enabled: true },
        { id: "docs", name: "Docs", instruction: "Write docs", enabled: false },
      ],
    });

    expect(mandateValid(mandate({ matchedRules: ["bug", "docs"] }), next)).toBe(false);
  });

  it("invalidates a Mandate whose matched rule was removed", () => {
    expect(mandateValid(mandate(), policy({ version: 2, rules: [] }))).toBe(false);
  });

  it("invalidates a Mandate whose source left the Policy", () => {
    expect(mandateValid(mandate(), policy({ version: 2, sources: {} }))).toBe(false);
  });

  it("invalidates a withdrawn Mandate even at the same Policy version", () => {
    expect(mandateValid(mandate({ withdrawnAt: LATER }), policy())).toBe(false);
  });

  it("ignores rules when the Mandate matched none", () => {
    expect(mandateValid(mandate({ matchedRules: [] }), policy({ version: 2, rules: [] }))).toBe(
      true,
    );
  });
});

describe("canResume", () => {
  it("resumes an ended Ticket while its Mandate is valid", () => {
    expect(canResume(ticket({ endedAt: LATER, outcome: "completed" }), mandate(), policy())).toBe(
      true,
    );
  });

  it("resumes an open Ticket with a valid Mandate", () => {
    expect(canResume(ticket(), mandate(), policy())).toBe(true);
  });

  it("refuses to resume a stopped Ticket even with a valid Mandate", () => {
    expect(canResume(ticket({ endedAt: LATER, outcome: "stopped" }), mandate(), policy())).toBe(
      false,
    );
  });

  it("refuses to resume when the Mandate was withdrawn", () => {
    expect(
      canResume(
        ticket({ endedAt: LATER, outcome: "completed" }),
        mandate({ withdrawnAt: LATER }),
        policy(),
      ),
    ).toBe(false);
  });

  it("refuses to resume when the Mandate no longer satisfies the Policy", () => {
    const next = policy({
      version: 2,
      rules: [{ id: "bug", name: "Bug fixes", instruction: "Fix reported bugs", enabled: false }],
    });

    expect(canResume(ticket({ endedAt: LATER, outcome: "closed" }), mandate(), next)).toBe(false);
  });
});

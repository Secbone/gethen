/**
 * In-memory repositories for the control-plane engine: deterministic tests and
 * single-process simulations. Values are stored by reference (the Engine saves
 * after each mutation), and the Policy lives in one mutable slot so a test can
 * move its version while an Eval is in flight.
 */
import type {
  Assignment,
  Decision,
  Eval,
  Input,
  Mandate,
  Policy,
  Thread,
  Ticket,
  Trigger,
} from "./model";
import type { PolicyPort, Repo, Repositories } from "./ports";

class MemoryRepo<T extends { id: string }> implements Repo<T> {
  protected readonly rows = new Map<string, T>();

  get(id: string): T | undefined {
    return this.rows.get(id);
  }

  save(value: T): void {
    this.rows.set(value.id, value);
  }

  list(): T[] {
    return [...this.rows.values()];
  }
}

/**
 * A Trigger is unique per (source, id), so two Sources may reuse an id without
 * overwriting each other. `get` still answers by the Trigger's own id.
 */
class TriggerRepo extends MemoryRepo<Trigger> {
  override get(id: string): Trigger | undefined {
    return this.list().find((trigger) => trigger.id === id);
  }

  override save(value: Trigger): void {
    this.rows.set(`${value.source}\u0000${value.id}`, value);
  }
}

const DEFAULT_POLICY: Policy = {
  version: 1,
  acceptMode: "auto",
  rules: [],
  sources: {},
};

export class MemoryRepositories implements Repositories {
  readonly threads: Repo<Thread> = new MemoryRepo<Thread>();
  readonly triggers: Repo<Trigger> = new TriggerRepo();
  readonly evals: Repo<Eval> = new MemoryRepo<Eval>();
  readonly decisions: Repo<Decision> = new MemoryRepo<Decision>();
  readonly tickets: Repo<Ticket> = new MemoryRepo<Ticket>();
  readonly mandates: Repo<Mandate> = new MemoryRepo<Mandate>();
  readonly assignments: Repo<Assignment> = new MemoryRepo<Assignment>();
  readonly inputs: Repo<Input> = new MemoryRepo<Input>();
  readonly policy: PolicyPort;

  private currentPolicy: Policy;

  constructor(policy: Policy = DEFAULT_POLICY) {
    this.currentPolicy = policy;
    this.policy = { current: () => this.currentPolicy };
  }

  /** Replace the Policy snapshot; bump its version to simulate a policy change. */
  setPolicy(policy: Policy): void {
    this.currentPolicy = policy;
  }
}

export function createMemoryRepositories(policy: Policy = DEFAULT_POLICY): MemoryRepositories {
  return new MemoryRepositories(policy);
}

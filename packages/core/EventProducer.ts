import { RawEvent } from "./EventType.js";
import { RunningMigration } from "./Migrator.js";
import { Producer } from "./Producer.js";
import { Topic } from "./Topic.js";
import { TypeSpec } from "./TypeSpec.js";

export class EventProducer<TEvent> implements Producer<TEvent> {
  readonly #spec: TypeSpec;
  readonly #inner: Producer<RawEvent<TEvent>>;
  readonly #topic: Topic<RawEvent<TEvent>>;
  readonly runningMigrations: RunningMigration<any, any>[];

  constructor(
    spec: TypeSpec,
    inner: Producer<RawEvent<TEvent>>,
    topic: Topic<RawEvent<TEvent>>,
    runningMigrations: RunningMigration<any, any>[],
  ) {
    this.#spec = spec;
    this.#inner = inner;
    this.#topic = topic;
    this.runningMigrations = runningMigrations;
  }

  toString() {
    return `EventProducer<${this.runningMigrations.map(
      (m) => m.sourceTopic.name + " -> ",
    )}${this.#topic.name}> ${this.#spec}`;
  }

  async produce(
    event: TEvent,
    key: string | Uint8Array | null = null,
  ): Promise<void> {
    this.#spec.assert(event);
    await this.#inner.produce(
      {
        timestamp: Date.now(),
        message: event,
      },
      key ? Buffer.from(key) : null,
    );
  }

  async [Symbol.asyncDispose]() {
    await Promise.all([
      this.#inner[Symbol.asyncDispose](),
      ...this.runningMigrations.map((m) => m[Symbol.asyncDispose]()),
    ]);
  }
}

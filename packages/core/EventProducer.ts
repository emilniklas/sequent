import { RawEvent } from "./EventType.js";
import { RunningMigration } from "./Migrator.js";
import { Producer } from "./Producer.js";
import { Topic } from "./Topic.js";
import { TypeSpec } from "./TypeSpec.js";

export class EventProducer<TEvent> implements Producer<TEvent> {
  static readonly #ENCODER = new TextEncoder();

  readonly #spec: TypeSpec;
  readonly #inner: Producer<RawEvent<TEvent>>;
  readonly #topic: Topic<RawEvent<TEvent>>;
  readonly runningMigrations: RunningMigration<any, any>[];
  readonly #keyer?: (event: TEvent) => Uint8Array | null;

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

    if (TypeSpec.isRecord(spec) && "id" in spec.spec) {
      let idSpec = spec.spec.id;
      while (TypeSpec.isOptional(idSpec)) {
        idSpec = idSpec.spec;
      }

      if (TypeSpec.isBytes(idSpec)) {
        this.#keyer = (evt) => (evt as { id?: Uint8Array }).id ?? null;
      } else if (TypeSpec.isString(idSpec)) {
        this.#keyer = (evt) => {
          const id = (evt as { id?: string }).id;
          if (id == null) {
            return null;
          }
          return EventProducer.#ENCODER.encode(id);
        };
      } else if (TypeSpec.isNumber(idSpec)) {
        this.#keyer = (evt) => {
          const id = (evt as { id?: number }).id;
          if (id == null) {
            return null;
          }
          const data = new Float64Array(1);
          data[0] = id;
          return new Uint8Array(data);
        };
      }
    }
  }

  toString() {
    return `EventProducer<${this.runningMigrations.map(
      (m) => m.sourceTopic.name + " -> ",
    )}${this.#topic.name}> ${this.#spec}`;
  }

  async produce(
    event: TEvent,
    key?: string | Uint8Array | null,
  ): Promise<void> {
    this.#spec.assert(event);

    await this.#inner.produce(
      {
        timestamp: Date.now(),
        message: event,
      },
      key ? Buffer.from(key) : this.#keyer?.(event) ?? null,
    );
  }

  async [Symbol.asyncDispose]() {
    await Promise.all([
      this.#inner[Symbol.asyncDispose](),
      ...this.runningMigrations.map((m) => m[Symbol.asyncDispose]()),
    ]);
  }
}

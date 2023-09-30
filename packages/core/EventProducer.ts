import { Aggregate } from "./Aggregate.js";
import { EventType, RawEvent } from "./EventType.js";
import { RunningMigration } from "./Migrator.js";
import { Producer } from "./Producer.js";
import { Topic } from "./Topic.js";
import { TypeSpec } from "./TypeSpec.js";

export class EventProducer<TEvent> implements Producer<TEvent> {
  static readonly #ENCODER = new TextEncoder();

  readonly #eventType: EventType<TEvent>;
  readonly #inner: Producer<RawEvent<TEvent>>;
  readonly #topic: Topic<RawEvent<TEvent>>;
  readonly runningMigrations: RunningMigration<any, any>[];
  readonly #keyer?: (event: TEvent) => Uint8Array | null;
  readonly #aggregate?: Aggregate;

  constructor(
    eventType: EventType<TEvent>,
    inner: Producer<RawEvent<TEvent>>,
    topic: Topic<RawEvent<TEvent>>,
    runningMigrations: RunningMigration<any, any>[],
    aggregate?: Aggregate,
  ) {
    this.#eventType = eventType;
    this.#inner = inner;
    this.#topic = topic;
    this.runningMigrations = runningMigrations;
    this.#aggregate = aggregate;

    const spec = eventType.spec;

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
    return `EventProducer<${this.#eventType.name}${
      this.#aggregate == null ? "" : ` (${this.#aggregate.name})`
    }, ${this.runningMigrations.map((m) => m.sourceTopic.name + " -> ")}${
      this.#topic.name
    }> ${this.#eventType.spec}`;
  }

  async produce(
    event: TEvent,
    key?: string | Uint8Array | null,
  ): Promise<void> {
    this.#eventType.spec.assert(event);

    if (this.#aggregate != null && key) {
      throw new Error(
        `Cannot override event key for ${this.#eventType.name} event. ${
          this.#aggregate.name
        } event producers derive their event keys from the "id" field.`,
      );
    }

    const k = key ? Buffer.from(key) : this.#keyer?.(event) ?? null;

    if (this.#aggregate != null && k == null) {
      throw new Error(
        `${
          this.#eventType.name
        } could not derive event key from the "id" field.`,
      );
    }

    await this.#inner.produce(
      {
        timestamp: Date.now(),
        message: event,
      },
      k,
    );
  }

  async [Symbol.asyncDispose]() {
    await Promise.all([
      this.#inner[Symbol.asyncDispose](),
      ...this.runningMigrations.map((m) => m[Symbol.asyncDispose]()),
    ]);
  }
}

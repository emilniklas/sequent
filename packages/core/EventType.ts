import { Consumer, ConsumerGroup } from "./Consumer.js";
import { EventConsumer } from "./EventConsumer.js";
import { EventProducer } from "./EventProducer.js";
import { Logger } from "./Logger.js";
import { Migrator } from "./Migrator.js";
import { Topic } from "./Topic.js";
import { TopicFactory } from "./TopicFactory.js";
import { TypeOf, TypeSpec } from "./TypeSpec.js";

export interface RawEvent<TMessage> {
  readonly timestamp: number;
  readonly message: TMessage;
}

export interface Event<TMessage> {
  readonly timestamp: Date;
  readonly message: TMessage;
}

export namespace EventType {
  export type TypeOf<TEventType> = TEventType extends EventType<infer R>
    ? R
    : never;
}

export class EventType<TEvent> {
  readonly name: string;
  readonly #spec: TypeSpec;
  readonly #migrators: Migrator<any, any>[];
  readonly #nonce: number;

  constructor(
    name: string,
    spec: TypeSpec,
    migrators: Migrator<any, any>[],
    nonce: number,
  ) {
    this.name = name;
    this.#spec = spec;
    this.#migrators = migrators;
    this.#nonce = nonce;
  }

  static new<TSpec extends TypeSpec>(
    name: string,
    spec: TSpec,
    { nonce = 0 }: { nonce?: number } = {},
  ): EventType<TypeOf<TSpec>> {
    return new EventType<TypeOf<TSpec>>(name, spec, [], nonce);
  }

  toString() {
    return `${this.name} ${this.#spec}`;
  }

  async topicName(): Promise<string> {
    const hashDigest = Array.from(
      new Uint8Array(
        await crypto.subtle.digest(
          "SHA-1",
          new TextEncoder().encode(this.toString() + this.#nonce.toString()),
        ),
      ),
      (b) => b.toString(16).padStart(2, "0"),
    ).join("");

    return `${this.name}-${hashDigest}`;
  }

  async topic(topicFactory: TopicFactory): Promise<Topic<RawEvent<TEvent>>> {
    return topicFactory.make<RawEvent<TEvent>>(await this.topicName());
  }

  async producer(
    topicFactory: TopicFactory,
    { logger = Logger.DEFAULT }: { logger?: Logger } = {},
  ): Promise<EventProducer<TEvent>> {
    const migratorsLogger = logger.withContext({ package: "@sequent/core" });

    const runningMigrations = await Promise.all(
      this.#migrators.map((m) => m.run(migratorsLogger, topicFactory)),
    );

    const topic = await this.topic(topicFactory);

    return new EventProducer(
      this.#spec,
      await topic.producer(),
      topic,
      runningMigrations,
    );
  }

  async consumer(
    topicFactory: TopicFactory,
    group: ConsumerGroup,
    {
      onCatchUp = () => {},
      logger = Logger.DEFAULT,
    }: { onCatchUp?: () => void; logger?: Logger } = {},
  ): Promise<Consumer<Event<TEvent>>> {
    const topic = await this.topic(topicFactory);
    return new EventConsumer(
      logger.withContext({ package: "@sequent/core" }),
      await topic.consumer(group),
      onCatchUp,
    );
  }

  addFields<TSpec extends { readonly [field: string]: TypeSpec }>(
    fields: NewFields<TEvent, TSpec>,
    { nonce = 0 }: { nonce?: number } = {},
  ): EventType<Omit<TEvent, keyof TSpec> & TypeOf<TypeSpec.Record<TSpec>>> {
    type NewTEvent = Omit<TEvent, keyof TSpec> & TypeOf<TypeSpec.Record<TSpec>>;

    if (!TypeSpec.isRecord(this.#spec)) {
      throw new Error("Cannot add fields to a non-record type");
    }

    const patchSpec = Object.fromEntries(
      Object.entries(fields).map(
        ([field, { type }]: [
          keyof TSpec,
          NewField<TEvent, TSpec[keyof TSpec]>,
        ]) => [field, type],
      ),
    );

    const newSpec = TypeSpec.Record({ ...this.#spec.spec, ...patchSpec });

    const migrator = new Migrator<TEvent, NewTEvent>({
      source: this,
      destination: () => newType,
      migration: (event) => {
        return {
          ...event,
          ...Object.fromEntries(
            Object.entries(fields).map(
              ([field, { migrate }]: [
                keyof TSpec,
                NewField<TEvent, TSpec[keyof TSpec]>,
              ]) => [field, migrate(event)],
            ),
          ),
        } as NewTEvent;
      },
    });

    const newType: EventType<NewTEvent> = new EventType<NewTEvent>(
      this.name,
      newSpec,
      [...this.#migrators, migrator],
      this.#nonce + nonce,
    );

    return newType;
  }
}

export type NewFields<
  TEvent,
  TSpec extends { readonly [field: string]: TypeSpec },
> = {
  readonly [TField in keyof TSpec]: NewField<TEvent, TSpec[TField]>;
};

export interface NewField<TEvent, TSpec extends TypeSpec> {
  type: TSpec;
  migrate: (event: TEvent) => TypeOf<TSpec>;
}

import { Aggregate } from "./Aggregate.js";
import { Consumer, ConsumerGroup } from "./Consumer.js";
import { CatchUpOptions, EventConsumer } from "./EventConsumer.js";
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
  readonly #aggregate?: Aggregate;

  constructor(
    name: string,
    spec: TypeSpec,
    migrators: Migrator<any, any>[],
    nonce: number,
    aggregate?: Aggregate,
  ) {
    this.name = name;
    this.#spec = spec;
    this.#migrators = migrators;
    this.#nonce = nonce;
    this.#aggregate = aggregate;
  }

  static new<TSpec extends TypeSpec>(
    name: string,
    spec: TSpec,
    { nonce = 0 }: { nonce?: number } = {},
  ): EventType<TypeOf<TSpec>> {
    return new EventType<TypeOf<TSpec>>(name, spec, [], nonce);
  }

  get spec(): TypeSpec {
    return this.#spec;
  }

  withinAggregate(aggregate: Aggregate) {
    return new EventType<TEvent>(
      this.name,
      this.spec,
      this.#migrators,
      this.#nonce,
      aggregate,
    );
  }

  toString() {
    return `${this.name}${
      this.#aggregate == null ? "" : ` (${this.#aggregate.name})`
    } ${this.#spec}`;
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

    return [this.#aggregate?.name, this.name, hashDigest]
      .filter(Boolean)
      .join("-");
  }

  async topic(topicFactory: TopicFactory): Promise<Topic<RawEvent<TEvent>>> {
    return topicFactory.make<RawEvent<TEvent>>(await this.topicName());
  }

  async producer(
    topicFactory: TopicFactory,
    {
      logger = Logger.DEFAULT,
      catchUpOptions,
    }: {
      logger?: Logger;
      catchUpOptions?: Partial<CatchUpOptions>;
    } = {},
  ): Promise<EventProducer<TEvent>> {
    const migratorsLogger = logger.withContext({ package: "@sequent/core" });

    const runningMigrations = await Promise.all(
      this.#migrators.map((m) =>
        m.run(topicFactory, {
          logger: migratorsLogger,
          catchUpOptions,
        }),
      ),
    );

    const topic = await this.topic(topicFactory);

    return new EventProducer(
      this,
      await topic.producer(),
      topic,
      runningMigrations,
      this.#aggregate,
    );
  }

  async consumer(
    topicFactory: TopicFactory,
    group: ConsumerGroup,
    {
      onCatchUp = () => {},
      logger = Logger.DEFAULT,
      catchUpOptions,
    }: {
      onCatchUp?: () => void;
      logger?: Logger;
      catchUpOptions?: Partial<CatchUpOptions>;
    } = {},
  ): Promise<Consumer<Event<TEvent>>> {
    const topic = await this.topic(topicFactory);
    return new EventConsumer(await topic.consumer(group), {
      onCatchUp,
      logger: logger.withContext({ package: "@sequent/core" }),
      catchUpOptions,
    });
  }

  map<TSpec extends TypeSpec>(
    spec: TSpec,
    migration: (event: TEvent) => TypeOf<TSpec>,
    opts?: { nonce?: number },
  ): EventType<TypeOf<TSpec>> {
    return this.flatMap(spec, (event) => [migration(event)], opts);
  }

  flatMap<TSpec extends TypeSpec>(
    spec: TSpec,
    migration: (event: TEvent) => Iterable<TypeOf<TSpec>>,
    { nonce = 0 }: { nonce?: number } = {},
  ): EventType<TypeOf<TSpec>> {
    const migrator = new Migrator<TEvent, TypeOf<TSpec>>({
      source: this,
      destination: () => newType,
      migration,
    });
    const newType: EventType<TypeOf<TSpec>> = new EventType<TypeOf<TSpec>>(
      this.name,
      spec,
      [...this.#migrators, migrator],
      this.#nonce + nonce,
    );
    return newType;
  }

  filter(
    f: (event: TEvent) => boolean,
    opts?: { nonce?: number },
  ): EventType<TEvent>;
  filter<TSpec extends TypeSpec>(
    spec: TSpec,
    f: (event: TEvent) => event is TypeOf<TSpec>,
    opts?: { nonce?: number },
  ): EventType<TypeOf<TSpec>>;
  filter<TSpec extends TypeSpec>(
    specOrF: TSpec | ((event: TEvent) => boolean),
    fOrOpts?: ((event: TEvent) => event is TypeOf<TSpec>) | { nonce?: number },
    maybeOpts?: { nonce?: number },
  ): EventType<TypeOf<TSpec>> {
    let spec;
    let f: (event: TEvent) => boolean;
    let opts;
    if (typeof specOrF === "function") {
      spec = this.spec;
      f = specOrF;
      opts = fOrOpts as { nonce?: number } | undefined;
    } else {
      spec = specOrF;
      f = fOrOpts as (event: TEvent) => event is TypeOf<TSpec>;
      opts = maybeOpts as { nonce?: number } | undefined;
    }

    let nonce = opts?.nonce ?? 0;

    // If we're filtering without asserting a new spec,
    // we have to change the nonce so that we're sure
    // a new topic will be created for the filtered
    // events.
    if (this.spec.toString() === spec.toString()) {
      nonce++;
    }

    return this.flatMap(
      spec,
      (event) => {
        if (f(event)) {
          return [event];
        } else {
          return [];
        }
      },
      { nonce },
    );
  }

  removeFields<const TFieldNames extends readonly (keyof TEvent)[]>(
    fields: TFieldNames,
    opts?: { nonce?: number },
  ): EventType<Omit<TEvent, TFieldNames[number]>> {
    if (!TypeSpec.isRecord(this.#spec)) {
      throw new Error("Cannot remove fields from a non-record type");
    }

    const newSpecFields = {
      ...this.#spec.spec,
    } as Record<keyof TEvent, TypeSpec>;
    for (const field of fields) {
      delete newSpecFields[field];
    }

    const newSpec = TypeSpec.Record(newSpecFields);

    return this.map<TypeSpec>(
      newSpec,
      (event) => {
        const newEvent = { ...event };
        for (const field of fields) {
          delete newEvent[field];
        }
        return newEvent;
      },
      opts,
    );
  }

  addOptionalFields<TSpec extends { readonly [field: string]: TypeSpec }>(
    spec: TSpec,
    opts?: { nonce?: number },
  ): EventType<
    Omit<TEvent, keyof TSpec> &
      TypeOf<
        TypeSpec.Record<{
          readonly [P in keyof TSpec]: TypeSpec.Optional<TSpec[P]>;
        }>
      >
  > {
    if (!TypeSpec.isRecord(this.#spec)) {
      throw new Error("Cannot add fields to a non-record type");
    }

    const patchSpec = Object.fromEntries(
      Object.entries(spec).map(([field, type]) => [
        field,
        TypeSpec.Optional(type),
      ]),
    ) as {
      readonly [P in keyof TSpec]: TypeSpec.Optional<TSpec[P]>;
    };

    const newSpec = TypeSpec.Record({ ...this.#spec.spec, ...patchSpec });

    const newOptionalFields: (keyof TSpec)[] = Object.keys(spec);
    return this.map<TypeSpec>(
      newSpec,
      (event) => {
        const newEvent: any = { ...event };
        // Delete any existing value with the same name
        // to prevent type inconsistencies. If the user wants
        // to turn a field optional, they should use the
        // `turnFieldsOptional` operator instead.
        for (const field of newOptionalFields) {
          delete newEvent[field];
        }
        return newEvent;
      },
      opts,
    );
  }

  turnFieldsOptional<const TFieldNames extends readonly (keyof TEvent)[]>(
    fields: TFieldNames,
    opts?: { nonce?: number },
  ): EventType<
    Omit<TEvent, TFieldNames[number]> & {
      readonly [P in TFieldNames[number]]?: TEvent[P] | null | undefined;
    }
  > {
    if (!TypeSpec.isRecord(this.#spec)) {
      throw new Error("Cannot change fields on a non-record type");
    }

    const newSpec = TypeSpec.Record(
      Object.fromEntries(
        Object.entries(this.#spec.spec).map(([field, type]) => [
          field,
          fields.includes(field as keyof TEvent)
            ? TypeSpec.Optional(type)
            : type,
        ]),
      ),
    );

    return this.map<TypeSpec>(newSpec, (event) => event, opts);
  }

  addFields<TSpec extends { readonly [field: string]: TypeSpec }>(
    fields: NewFields<TEvent, TSpec>,
    opts?: { nonce?: number },
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
    ) as unknown as TSpec;

    const newSpec = TypeSpec.Record({ ...this.#spec.spec, ...patchSpec });

    return this.map<TypeSpec>(
      newSpec,
      (event) =>
        ({
          ...event,
          ...Object.fromEntries(
            Object.entries(fields).map(
              ([field, { migrate }]: [
                keyof TSpec,
                NewField<TEvent, TSpec[keyof TSpec]>,
              ]) => [field, migrate(event)],
            ),
          ),
        }) as NewTEvent,
      opts,
    );
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

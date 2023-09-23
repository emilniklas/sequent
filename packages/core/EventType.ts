import { Consumer, ConsumerGroup, Envelope } from "./Consumer.js";
import { Logger } from "./Logger.js";
import { Migrator, RunningMigration } from "./Migrator.js";
import { Producer } from "./Producer.js";
import { Topic } from "./Topic.js";
import { TopicFactory } from "./TopicFactory.js";
import { TypeOf, TypeSpec } from "./TypeSpec.js";

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

  async produce(event: TEvent) {
    this.#spec.assert(event);
    await this.#inner.produce({
      timestamp: Date.now(),
      message: event,
    });
  }

  async [Symbol.asyncDispose]() {
    await Promise.all([
      this.#inner[Symbol.asyncDispose](),
      ...this.runningMigrations.map((m) => m[Symbol.asyncDispose]()),
    ]);
  }
}

export class EventConsumer<TEvent> implements Consumer<Event<TEvent>> {
  static readonly #CATCH_UP_DELAY = 5000;
  static readonly #PROGRESS_LOG_FREQUENCY = 3000;
  static readonly #NUMBER_FORMAT = new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 1,
  });

  readonly #logger: Logger;
  readonly #inner: Consumer<RawEvent<TEvent>>;
  readonly #onCatchUp: () => void;

  #caughtUp = false;
  #catchUpDelayTimer?: ReturnType<typeof setTimeout>;
  #logProcessInterval?: ReturnType<typeof setInterval>;

  #progressCounter = 0;

  constructor(
    logger: Logger,
    inner: Consumer<RawEvent<TEvent>>,
    onCatchUp: () => void,
  ) {
    this.#logger = logger;
    this.#inner = inner;
    this.#onCatchUp = onCatchUp;
  }

  #rescheduleCatchUpDelay() {
    clearTimeout(this.#catchUpDelayTimer);
    if (!this.#caughtUp) {
      this.#catchUpDelayTimer = setTimeout(() => {
        this.#logger.debug("Caught up due to halted consumer");
        this.#catchUp();
      }, EventConsumer.#CATCH_UP_DELAY);
    }
  }

  #logProgress() {
    if (this.#caughtUp) {
      clearInterval(this.#logProcessInterval);
      return;
    }

    this.#logger.debug("Still catching up...", {
      throughput: `${EventConsumer.#NUMBER_FORMAT.format(
        this.#progressCounter / (EventConsumer.#PROGRESS_LOG_FREQUENCY / 1000),
      )} events/s`,
    });

    this.#progressCounter = 0;
  }

  #catchUp() {
    clearTimeout(this.#catchUpDelayTimer);
    if (!this.#caughtUp) {
      this.#caughtUp = true;
      this.#onCatchUp();
    }
  }

  async consume(opts?: {
    signal?: AbortSignal;
  }): Promise<Envelope<Event<TEvent>> | undefined> {
    if (this.#logProcessInterval == null && !this.#caughtUp) {
      this.#logProcessInterval = setInterval(
        this.#logProgress.bind(this),
        EventConsumer.#PROGRESS_LOG_FREQUENCY,
      );
    }

    this.#rescheduleCatchUpDelay();

    const onAbort = this.#catchUp.bind(this);
    opts?.signal?.addEventListener("abort", onAbort);
    const envelope = await this.#inner.consume(opts);
    opts?.signal?.removeEventListener("abort", onAbort);

    if (envelope == null) {
      return undefined;
    }

    this.#progressCounter++;

    if (
      Date.now() - envelope.event.timestamp <=
      EventConsumer.#CATCH_UP_DELAY
    ) {
      this.#logger.debug("Caught up due to recent event");
      this.#catchUp();
    }

    return envelope.map((e) => ({
      timestamp: new Date(e.timestamp),
      message: e.message,
    }));
  }

  async [Symbol.asyncDispose]() {
    await this.#inner[Symbol.asyncDispose]();
  }
}

export interface RawEvent<TMessage> {
  readonly timestamp: number;
  readonly message: TMessage;
}

export interface Event<TMessage> {
  readonly timestamp: Date;
  readonly message: TMessage;
}

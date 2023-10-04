import {
  Codec,
  Consumer,
  ConsumerGroup,
  Deserializer,
  Envelope,
  Logger,
  Producer,
  Serializer,
  Topic,
  TopicFactory,
  V8Codec,
} from "@sequent/core";
import { KeyStore } from "./KeyStore.js";

export interface EncryptedEvent<TId> {
  readonly ids: TId[];
  readonly event: Uint8Array;
}

export type IdExtractor<TId, TEvent> = (event: TEvent) => TId[];

export class PersonalTopicFactory<TId> implements TopicFactory {
  readonly #inner: TopicFactory;
  readonly #store: KeyStore<TId>;
  readonly #codec: Codec<any>;
  readonly #idExtractor: IdExtractor<TId, unknown>;
  readonly #logger: Logger;

  constructor(opts: {
    topicFactory: TopicFactory;
    keyStore: KeyStore<TId>;
    idExtractor: IdExtractor<TId, unknown>;
    codec?: Codec<any>;
    logger?: Logger;
  }) {
    this.#inner = opts.topicFactory;
    this.#store = opts.keyStore;
    this.#idExtractor = opts.idExtractor;
    this.#codec = opts.codec ?? new V8Codec();
    this.#logger = opts.logger ?? Logger.DEFAULT;
  }

  async make<TEvent>(name: string): Promise<Topic<TEvent>> {
    const topic = await this.#inner.make<EncryptedEvent<TId>>(name);
    return new PersonalTopic<TId, TEvent>({
      topic,
      keyStore: this.#store,
      codec: this.#codec,
      idExtractor: this.#idExtractor,
      logger: this.#logger,
    });
  }
}

export class PersonalTopic<TId, TEvent> implements Topic<TEvent> {
  readonly #inner: Topic<EncryptedEvent<TId>>;
  readonly #store: KeyStore<TId>;
  readonly #codec: Codec<TEvent>;
  readonly #idExtractor: IdExtractor<TId, TEvent>;
  readonly #logger: Logger;

  constructor(opts: {
    topic: Topic<EncryptedEvent<TId>>;
    keyStore: KeyStore<TId>;
    codec: Codec<TEvent>;
    idExtractor: IdExtractor<TId, TEvent>;
    logger: Logger;
  }) {
    this.#inner = opts.topic;
    this.#store = opts.keyStore;
    this.#codec = opts.codec;
    this.#idExtractor = opts.idExtractor;
    this.#logger = opts.logger;
  }

  get name() {
    return this.#inner.name;
  }

  async consumer(group: ConsumerGroup): Promise<Consumer<TEvent>> {
    const consumer = await this.#inner.consumer(group);
    return new PersonalConsumer<TId, TEvent>({
      consumer,
      keyStore: this.#store,
      deserializer: this.#codec,
      logger: this.#logger,
    });
  }

  async producer(): Promise<Producer<TEvent>> {
    const producer = await this.#inner.producer();
    return new PersonalProducer<TId, TEvent>({
      producer,
      keyStore: this.#store,
      serializer: this.#codec,
      idExtractor: this.#idExtractor,
      logger: this.#logger,
    });
  }
}

export class PersonalConsumer<TId, TEvent> implements Consumer<TEvent> {
  readonly #inner: Consumer<EncryptedEvent<TId>>;
  readonly #store: KeyStore<TId>;
  readonly #deserializer: Deserializer<TEvent>;
  readonly #logger: Logger;

  constructor(opts: {
    consumer: Consumer<EncryptedEvent<TId>>;
    keyStore: KeyStore<TId>;
    deserializer: Deserializer<TEvent>;
    logger: Logger;
  }) {
    this.#inner = opts.consumer;
    this.#store = opts.keyStore;
    this.#deserializer = opts.deserializer;
    this.#logger = opts.logger;
  }

  async consume(
    opts?: { signal?: AbortSignal | undefined } | undefined,
  ): Promise<Envelope<TEvent> | undefined> {
    const envelope = await this.#inner.consume(opts);

    if (envelope == null) {
      return undefined;
    }

    const pkeys = await Promise.all(
      envelope.event.ids.map((id) => this.#store.getPrivate(id)),
    );

    const missingKeyIndices = pkeys.flatMap((k, i) => (k == null ? [i] : []));

    if (missingKeyIndices.length > 0) {
      this.#logger.info("Skipped consuming event, private key(s) gone", {
        ids: missingKeyIndices.map((i) => envelope.event.ids[i]),
      });
      await envelope[Symbol.asyncDispose]();
      return this.consume(opts);
    }

    return envelope.map(({ event }) => {
      for (let i = 0; i < pkeys.length; i++) {
        event = pkeys[i]!.decrypt(event);
      }

      return this.#deserializer.deserialize(event);
    });
  }

  async [Symbol.asyncDispose]() {
    await this.#inner[Symbol.asyncDispose]();
  }
}

export class PersonalProducer<TId, TEvent> {
  readonly #inner: Producer<EncryptedEvent<TId>>;
  readonly #store: KeyStore<TId>;
  readonly #serializer: Serializer<TEvent>;
  readonly #idExtractor: IdExtractor<TId, TEvent>;
  readonly #logger: Logger;

  constructor(opts: {
    producer: Producer<EncryptedEvent<TId>>;
    keyStore: KeyStore<TId>;
    serializer: Serializer<TEvent>;
    idExtractor: IdExtractor<TId, TEvent>;
    logger: Logger;
  }) {
    this.#inner = opts.producer;
    this.#store = opts.keyStore;
    this.#serializer = opts.serializer;
    this.#idExtractor = opts.idExtractor;
    this.#logger = opts.logger;
  }

  async produce(event: TEvent, eventKey: Uint8Array | null) {
    const ids = Array.from(this.#idExtractor(event));

    const keys = await Promise.all(ids.map((id) => this.#store.getPublic(id)));

    const missingKeyIndices = keys.flatMap((k, i) => (k == null ? [i] : []));

    if (missingKeyIndices.length > 0) {
      this.#logger.info("Skipped producing event, public key(s) gone", {
        ids: missingKeyIndices.map((i) => ids[i]),
      });
      return;
    }

    let serializedEvent = this.#serializer.serialize(event);

    for (let i = ids.length - 1; i >= 0; i--) {
      serializedEvent = keys[i]!.encrypt(serializedEvent);
    }

    await this.#inner.produce(
      {
        ids,
        event: serializedEvent,
      },
      eventKey,
    );
  }

  async [Symbol.asyncDispose]() {
    await this.#inner[Symbol.asyncDispose]();
  }
}

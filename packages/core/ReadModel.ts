import { Casing } from "./Casing.js";
import { Consumer, ConsumerGroup, Envelope } from "./Consumer.js";
import { CatchUpOptions } from "./EventConsumer.js";
import { EventType, Event } from "./EventType.js";
import { Logger } from "./Logger.js";
import { TopicFactory } from "./TopicFactory.js";

export interface ReadModelClientFactory<TClient> {
  readonly namingConvention: Casing;
  make(namespace: string): Promise<TClient>;
  onCatchUp?(client: TClient): void | Promise<void>;
}

export type Ingestor<TEvent, TClient> = (
  event: Event<TEvent>,
  client: TClient,
  key: Uint8Array | null,
) => void | Promise<void>;

interface RegisteredIngestor<TEvent, TClient> {
  readonly eventType: EventType<TEvent>;
  readonly ingestor: Ingestor<TEvent, TClient>;
  readonly nonce: number;
}

export type Initializer<TClient> = (client: TClient) => void | Promise<void>;

interface RegisteredInitializer<TClient> {
  readonly initializer: Initializer<TClient>;
  readonly nonce: number;
}

export class ReadModel<TClient extends object> {
  readonly #name: string;
  readonly #ingestors: RegisteredIngestor<any, TClient>[];
  readonly #initializers: RegisteredInitializer<TClient>[];

  private constructor(
    name: string,
    ingestors: RegisteredIngestor<any, TClient>[],
    initializers: RegisteredInitializer<TClient>[],
  ) {
    this.#name = name;
    this.#ingestors = ingestors;
    this.#initializers = initializers;
  }

  static new<TClient extends object>(name: string): ReadModel<TClient> {
    return new ReadModel(name, [], []);
  }

  onInit(
    initializer: Initializer<TClient>,
    { nonce = 0 }: { nonce?: number } = {},
  ) {
    return new ReadModel(this.#name, this.#ingestors, [
      ...this.#initializers,
      { initializer, nonce },
    ]);
  }

  on<TEvent>(
    eventType: EventType<TEvent>,
    ingestor: Ingestor<TEvent, TClient>,
    { nonce = 0 }: { nonce?: number } = {},
  ): ReadModel<TClient> {
    return new ReadModel(
      this.#name,
      [...this.#ingestors, { eventType, ingestor, nonce }],
      this.#initializers,
    );
  }

  async start(
    topicFactory: TopicFactory,
    clientFactory: ReadModelClientFactory<TClient>,
    {
      signal,
      logger = Logger.DEFAULT,
      catchUpOptions,
    }: {
      signal?: AbortSignal;
      logger?: Logger;
      catchUpOptions?: Partial<CatchUpOptions>;
    } = {},
  ): Promise<TClient> {
    const data = this.#initializers
      .map(
        ({ initializer, nonce }) => initializer.toString() + nonce.toString(),
      )
      .concat(
        this.#ingestors.map(
          ({ eventType, nonce, ingestor }) =>
            eventType.toString() + nonce.toString() + ingestor.toString(),
        ),
      )
      .join();

    const digest = Array.from(
      new Uint8Array(
        await crypto.subtle.digest("SHA-1", new TextEncoder().encode(data)),
      ),
      (b) => b.toString(16).padStart(2, "0"),
    ).join("");

    const namespace =
      clientFactory.namingConvention.convert(this.#name) +
      clientFactory.namingConvention.suffixSeparator +
      digest;

    const client = await clientFactory.make(namespace);

    const stack = new AsyncDisposableStack();
    signal?.addEventListener("abort", () => stack.disposeAsync());

    if (Symbol.dispose in client || Symbol.asyncDispose in client) {
      stack.use(client as Disposable | AsyncDisposable);
    }

    for (const initializer of this.#initializers) {
      await initializer.initializer(client);
    }

    const topicNames = await Promise.all(
      this.#ingestors.map((i) => i.eventType.topicName()),
    );

    const catchUpPromises: Promise<void>[] = [];

    const ingestionLogger = logger.withContext({
      package: "@sequent/core",
      eventTypes: this.#ingestors.map((i) => i.eventType.name),
      topics: topicNames,
      readModel: this.#name,
      namespace,
    });

    const ingestor = new MultiConsumerIngestor(
      await Promise.all(
        this.#ingestors.map(async ({ eventType, ingestor }) => {
          const topicName = await eventType.topicName();

          const group = ConsumerGroup.join(`${namespace}-${topicName}`);

          let onCatchUp!: () => void;
          catchUpPromises.push(
            new Promise<void>((r) => {
              onCatchUp = r;
            }),
          );

          const consumer = await eventType.consumer(topicFactory, group, {
            onCatchUp,
            logger: ingestionLogger,
            catchUpOptions,
          });

          return new ConsumerIngestor(
            consumer,
            ingestor,
            client,
            catchUpOptions?.catchUpDelayMs,
            signal,
          );
        }),
      ),
    );

    ingestionLogger.info("Ingesting events");

    (async () => {
      while (!stack.disposed) {
        await ingestor.next();
      }
    })();

    await Promise.all(catchUpPromises);

    await clientFactory.onCatchUp?.(client);

    ingestionLogger.info("Ingestor caught up");

    return client;
  }
}

class MultiConsumerIngestor<TClient> {
  readonly #consumerIngestors: ConsumerIngestor<any, TClient>[];

  constructor(consumerIngestors: ConsumerIngestor<any, TClient>[]) {
    this.#consumerIngestors = consumerIngestors;
  }

  async next(): Promise<void> {
    const peekedDates = await Promise.all(
      this.#consumerIngestors.map((ci) => ci.peek()),
    );

    let earliestDate: Date | undefined;
    let earliestIndex: number | undefined;

    for (let i = 0; i < peekedDates.length; i++) {
      const date = peekedDates[i];

      if (date == null) {
        continue;
      }

      if (earliestDate == null || earliestDate > date) {
        earliestDate = date;
        earliestIndex = i;
      }
    }

    if (earliestIndex == null) {
      await Promise.race(this.#consumerIngestors.map((ci) => ci.ready()));

      return this.next();
    }

    await this.#consumerIngestors[earliestIndex].take();
  }
}

class Prefetch<TEvent> {
  readonly #consumer: Consumer<Event<TEvent>>;
  readonly #timeoutMs: number = 300;
  readonly #signal?: AbortSignal;

  constructor(
    consumer: Consumer<Event<TEvent>>,
    catchUpDelayMs?: number,
    signal?: AbortSignal,
  ) {
    this.#consumer = consumer;
    this.#signal = signal;
    if (catchUpDelayMs != null) {
      this.#timeoutMs = catchUpDelayMs * 0.7;
    }

    this.#startPrefetch();
  }

  #prefetching!: Promise<void>;
  #prefetchingWithTimeout!: Promise<void>;
  #prefetched?: Envelope<Event<TEvent>>;

  #startPrefetch() {
    if (this.#signal?.aborted) {
      return;
    }

    this.#prefetching = this.#consumer
      .consume({ signal: this.#signal })
      .then((envelope) => {
        this.#prefetched = envelope;
      });

    this.#prefetchingWithTimeout = Promise.race([
      this.#prefetching,
      new Promise<void>((r) => setTimeout(r, this.#timeoutMs)),
    ]);
  }

  async ready() {
    await this.#prefetching;
  }

  /**
   * This method "peeks" at the prefetched message. It waits for
   * the prefetch to conclude (if it hasn't already) before doing
   * so. However, if the prefetch is taking more than `#timeoutMs`,
   * then `peek()` will start returning `undefined` until a new
   * prefetch message comes in.
   */
  async peek(): Promise<Date | undefined> {
    await this.#prefetchingWithTimeout;
    return this.#prefetched?.event.timestamp;
  }

  /**
   * This method waits until the prefetch is definitely done
   * (or cancelled) if it isn't already and returns the
   * prefetched envelope, removing it from the state and
   * starts prefetching another.
   */
  async take(): Promise<Envelope<Event<TEvent>> | undefined> {
    await this.#prefetching;
    const envelope = this.#prefetched;
    this.#prefetched = undefined;
    this.#startPrefetch();
    return envelope;
  }
}

class ConsumerIngestor<TEvent, TClient> {
  readonly #prefetch: Prefetch<TEvent>;
  readonly #ingestor: Ingestor<TEvent, TClient>;
  readonly #client: TClient;

  constructor(
    consumer: Consumer<Event<TEvent>>,
    ingestor: Ingestor<TEvent, TClient>,
    client: TClient,
    catchUpDelayMs?: number,
    signal?: AbortSignal,
  ) {
    this.#prefetch = new Prefetch(consumer, catchUpDelayMs, signal);
    this.#ingestor = ingestor;
    this.#client = client;
  }

  async peek(): Promise<Date | undefined> {
    return this.#prefetch.peek();
  }

  async ready() {
    await this.#prefetch.ready();
  }

  async take(): Promise<void> {
    const envelope = await this.#prefetch.take();
    if (envelope) {
      try {
        await this.#ingestor(envelope.event, this.#client, envelope.key);
      } catch (e) {
        await envelope.nack();
        throw e;
      } finally {
        await envelope[Symbol.asyncDispose]();
      }
    }
  }
}

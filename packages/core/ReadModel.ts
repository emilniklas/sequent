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
      clientFactory.namingConvention
        .convert(`${this.#name}_HASH`)
        .slice(0, -3) + digest;

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

          return new ConsumerIngestor(consumer, ingestor, client);
        }),
      ),
      catchUpOptions,
    );

    ingestionLogger.info("Ingesting events");

    (async () => {
      while (!stack.disposed) {
        await ingestor.next(signal);
      }
    })();

    await Promise.all(catchUpPromises);

    await clientFactory.onCatchUp?.(client);

    ingestionLogger.info("Ingestor caught up");

    return client;
  }
}

class MultiConsumerIngestor<TClient> {
  static readonly #DEFAULT_TIMEOUT_MS = 300;

  readonly #consumerIngestors: ConsumerIngestor<any, TClient>[];
  readonly #timeoutMs: number;

  constructor(
    consumerIngestors: ConsumerIngestor<any, TClient>[],
    catchUpOptions?: Partial<CatchUpOptions>,
  ) {
    this.#consumerIngestors = consumerIngestors;
    this.#timeoutMs = MultiConsumerIngestor.#DEFAULT_TIMEOUT_MS;

    if (catchUpOptions?.catchUpDelayMs != null) {
      this.#timeoutMs = catchUpOptions.catchUpDelayMs * 0.7;
    }
  }

  async next(signal?: AbortSignal): Promise<void> {
    const peekedDates = await Promise.all(
      this.#consumerIngestors.map((ci) => ci.peek(this.#timeoutMs, signal)),
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
      await Promise.race(
        this.#consumerIngestors.map((ci) => ci.peek(Infinity, signal)),
      );

      return this.next(signal);
    }

    await this.#consumerIngestors[earliestIndex].take(signal);
  }
}

class ConsumerIngestor<TEvent, TClient> {
  readonly #consumer: Consumer<Event<TEvent>>;
  readonly #ingestor: Ingestor<TEvent, TClient>;
  readonly #client: TClient;
  #prefetched?:
    | Promise<Envelope<Event<TEvent>> | undefined>
    | Envelope<Event<TEvent>>;

  constructor(
    consumer: Consumer<Event<TEvent>>,
    ingestor: Ingestor<TEvent, TClient>,
    client: TClient,
  ) {
    this.#consumer = consumer;
    this.#ingestor = ingestor;
    this.#client = client;
  }

  #prefetch(
    signal?: AbortSignal,
  ): Promise<Envelope<Event<TEvent>> | undefined> {
    if (!this.#prefetched) {
      this.#prefetched = this.#consumer
        .consume({ signal })
        .then((envelope) => {
          this.#prefetched = envelope;
          return envelope;
        })
        .catch((e) => {
          this.#prefetched = undefined;
          throw e;
        });
    }

    return Promise.resolve(this.#prefetched);
  }

  peek(timeoutMs: number, signal?: AbortSignal): Promise<Date | undefined> {
    return Promise.race([
      this.#prefetch(signal).then((e) => e?.event.timestamp),
      new Promise<undefined>((r) => {
        if (isFinite(timeoutMs)) {
          setTimeout(r, timeoutMs);
        }
      }),
    ]);
  }

  async take(signal?: AbortSignal): Promise<void> {
    const envelope = await this.#prefetch(signal);
    this.#prefetched = undefined;
    if (envelope) {
      try {
        await this.#ingestor(envelope.event, this.#client);
      } catch (e) {
        await envelope.nack();
        throw e;
      } finally {
        await envelope[Symbol.asyncDispose]();
      }
    }
  }
}

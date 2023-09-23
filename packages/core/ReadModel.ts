import { Casing } from "./Casing.js";
import { ConsumerGroup } from "./Consumer.js";
import { EventType, Event } from "./EventType.js";
import { Logger } from "./Logger.js";
import { TopicFactory } from "./TopicFactory.js";

export interface ReadModelClientFactory<TClient> {
  readonly namingConvention: Casing;
  make(namespace: string): Promise<TClient>;
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

export class ReadModel<TModel, TClient extends object> {
  readonly #name: string;
  readonly #ingestors: RegisteredIngestor<any, TClient>[];

  private constructor(
    name: string,
    ingestors: RegisteredIngestor<any, TClient>[],
  ) {
    this.#name = name;
    this.#ingestors = ingestors;
  }

  static new<TModel, TClient extends object>(
    name: string,
  ): ReadModel<TModel, TClient> {
    return new ReadModel(name, []);
  }

  on<TEvent>(
    eventType: EventType<TEvent>,
    ingestor: Ingestor<TEvent, TClient>,
    { nonce = 0 }: { nonce?: number } = {},
  ): ReadModel<TModel, TClient> {
    return new ReadModel(this.#name, [
      ...this.#ingestors,
      { eventType, ingestor, nonce },
    ]);
  }

  async start(
    topicFactory: TopicFactory,
    clientFactory: ReadModelClientFactory<TClient>,
    {
      signal,
      logger = Logger.DEFAULT,
    }: { signal?: AbortSignal; logger?: Logger } = {},
  ): Promise<TClient> {
    const data = this.#ingestors
      .map(
        ({ eventType, nonce, ingestor }) =>
          eventType.toString() + nonce.toString() + ingestor.toString(),
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

    await Promise.all(
      this.#ingestors.map(async ({ eventType, ingestor }) => {
        const topicName = await eventType.topicName();

        const group = ConsumerGroup.join(`${namespace}-${topicName}`);

        const ingestionLogger = logger.withContext({
          package: "@sequent/core",
          eventType: eventType.name,
          topic: topicName,
          readModel: this.#name,
          namespace,
        });

        await new Promise<void>(async (onCatchUp) => {
          const consumer = await eventType.consumer(topicFactory, group, {
            onCatchUp,
            logger: ingestionLogger,
          });

          ingestionLogger.info("Ingesting events");

          while (!stack.disposed) {
            const envelope = await consumer.consume({
              signal,
            });
            if (envelope == null) {
              continue;
            }
            await ingestor(envelope.event, client);
            await envelope[Symbol.asyncDispose]();
          }
        });

        ingestionLogger.info("Ingestor caught up");
      }),
    );

    if (Symbol.dispose in client || Symbol.asyncDispose in client) {
      stack.use(client as Disposable | AsyncDisposable);
    }

    return client;
  }
}

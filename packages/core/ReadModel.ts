import { Casing } from "./Casing.js";
import { ConsumerGroup } from "./Consumer.js";
import { EventType, Event } from "./EventType.js";
import { TopicFactory } from "./TopicFactory.js";

export interface ReadModelClientFactory<TClient> {
  readonly namingConvention: Casing;
  make(namespace: string): TClient;
}

export type Ingestor<TEvent, TClient> = (
  event: Event<TEvent>,
  client: TClient
) => void | Promise<void>;

interface RegisteredIngestor<TEvent, TClient> {
  readonly eventType: EventType<TEvent>;
  readonly ingestor: Ingestor<TEvent, TClient>;
  readonly nonce: number;
}

export class ReadModel<TModel, TClient> {
  readonly #name: string;
  readonly #ingestors: RegisteredIngestor<any, TClient>[];

  private constructor(
    name: string,
    ingestors: RegisteredIngestor<any, TClient>[]
  ) {
    this.#name = name;
    this.#ingestors = ingestors;
  }

  static new<TModel, TClient>(name: string): ReadModel<TModel, TClient> {
    return new ReadModel(name, []);
  }

  on<TEvent>(
    eventType: EventType<TEvent>,
    ingestor: Ingestor<TEvent, TClient>,
    { nonce = 0 }: { nonce?: number } = {}
  ): ReadModel<TModel, TClient> {
    return new ReadModel(this.#name, [
      ...this.#ingestors,
      { eventType, ingestor, nonce },
    ]);
  }

  async start(
    topicFactory: TopicFactory,
    clientFactory: ReadModelClientFactory<TClient>
  ): Promise<TClient> {
    const data = this.#ingestors
      .map(
        ({ eventType, nonce, ingestor }) =>
          eventType.toString() + nonce.toString() + ingestor.toString()
      )
      .join();

    const digest = Array.from(
      new Uint8Array(
        await crypto.subtle.digest("SHA-1", new TextEncoder().encode(data))
      ),
      (b) => b.toString(16).padStart(2, "0")
    ).join("");

    const namespace =
      clientFactory.namingConvention
        .convert(`${this.#name}_HASH`)
        .slice(0, -3) + digest;

    const client = await clientFactory.make(namespace);

    await Promise.all(
      this.#ingestors.map(async ({ eventType, ingestor }) => {
        const topic = await topicFactory.make<Event<any>>(
          await eventType.topicName()
        );
        const consumer = topic.consumer(ConsumerGroup.join(namespace));
        (async () => {
          while (true) {
            const envelope = await consumer.consume();
            await ingestor(envelope.event, client);
            await envelope[Symbol.asyncDispose]();
          }
        })();
      })
    );

    return client;
  }
}

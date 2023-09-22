import * as kafka from "kafkajs";
import {
  Consumer,
  ConsumerGroup,
  Deserializer,
  Envelope,
  StartFrom,
} from "@sequent/core";

export class KafkaConsumer<TEvent> implements Consumer<TEvent> {
  readonly #deserializer: Deserializer<TEvent>;
  readonly #consumer: kafka.Consumer;
  readonly #buffer: kafka.EachMessagePayload[] = [];
  readonly #resumers: (() => void)[] = [];
  #closed = false;

  constructor(
    deserializer: Deserializer<TEvent>,
    consumer: kafka.Consumer,
    topicName: string,
    group: ConsumerGroup
  ) {
    this.#deserializer = deserializer;
    this.#consumer = consumer;

    consumer.subscribe({
      topics: [topicName],
      fromBeginning: group.startFrom === StartFrom.Beginning,
    });

    consumer.run({
      autoCommit: false,
      eachMessage: async (m) => {
        this.#buffer.push(m);
        this.#resumers.shift()?.();
      },
    });
  }

  static async new<TEvent>(
    deserializer: Deserializer<TEvent>,
    client: kafka.Kafka,
    topicName: string,
    group: ConsumerGroup
  ): Promise<KafkaConsumer<TEvent>> {
    const consumer = client.consumer({
      groupId: group.name,
      allowAutoTopicCreation: false,
    });

    await consumer.connect();

    return new KafkaConsumer(deserializer, consumer, topicName, group);
  }

  async consume({ signal }: { signal?: AbortSignal } = {}): Promise<
    Envelope<TEvent> | undefined
  > {
    if (this.#closed || signal?.aborted) {
      return undefined;
    }
    while (this.#buffer.length === 0 && !this.#closed) {
      await new Promise<void>((resume) => {
        this.#resumers.push(resume);
        signal?.addEventListener("abort", () => resume());
      });
    }
    const element = this.#buffer.shift();
    if (element == null) {
      return undefined;
    }
    const { message, topic, partition } = element;

    if (message.value == null) {
      return this.consume();
    }

    const event = this.#deserializer.deserialize(Buffer.from(message.value));

    return new Envelope(event, {
      ack: async () => {
        await this.#consumer.commitOffsets([
          {
            topic,
            partition,
            offset: String(Number(message.offset) + 1),
          },
        ]);
      },
      nack: async () => {},
    });
  }

  async [Symbol.asyncDispose]() {
    this.#closed = true;
    while (this.#resumers.length > 0) {
      this.#resumers.shift()!();
    }
    await this.#consumer.disconnect();
  }
}

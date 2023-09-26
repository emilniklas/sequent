import * as kafka from "kafkajs";
import { Producer, Serializer } from "@sequent/core";

export class KafkaProducer<TEvent> implements Producer<TEvent> {
  readonly #serializer: Serializer<TEvent>;
  readonly #producer: kafka.Producer;
  readonly #topicName: string;

  constructor(
    serializer: Serializer<TEvent>,
    producer: kafka.Producer,
    topicName: string,
  ) {
    this.#serializer = serializer;
    this.#producer = producer;
    this.#topicName = topicName;
  }

  static async new<TEvent>(
    serializer: Serializer<TEvent>,
    client: kafka.Kafka,
    topicName: string,
  ): Promise<KafkaProducer<TEvent>> {
    const producer = client.producer({ allowAutoTopicCreation: false });
    await producer.connect();

    return new KafkaProducer(serializer, producer, topicName);
  }

  async produce(event: TEvent, key: Uint8Array | null) {
    await this.#producer.send({
      topic: this.#topicName,
      messages: [
        {
          value: Buffer.from(this.#serializer.serialize(event)),
          key: key ? Buffer.from(key) : null,
        },
      ],
    });
  }

  async [Symbol.asyncDispose]() {
    await this.#producer.disconnect();
  }
}

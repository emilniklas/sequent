import * as kafka from "kafkajs";
import {
  Codec,
  Consumer,
  ConsumerGroup,
  Producer,
  Topic,
} from "@sequent/core";
import { KafkaConsumer } from "./KafkaConsumer.js";
import { KafkaProducer } from "./KafkaProducer.js";

export class KafkaTopic<TEvent> implements Topic<TEvent>, AsyncDisposable {
  readonly #client: kafka.Kafka;
  readonly #codec: Codec<TEvent>;
  readonly name: string;
  readonly #disposableStack = new AsyncDisposableStack();

  constructor(client: kafka.Kafka, codec: Codec<TEvent>, name: string) {
    this.#client = client;
    this.#codec = codec;
    this.name = name;
  }

  async producer(): Promise<Producer<TEvent>> {
    return this.#disposableStack.use(
      await KafkaProducer.new(this.#codec, this.#client, this.name)
    );
  }

  async consumer(group: ConsumerGroup): Promise<Consumer<TEvent>> {
    return this.#disposableStack.use(
      await KafkaConsumer.new(this.#codec, this.#client, this.name, group)
    );
  }

  async [Symbol.asyncDispose]() {
    await this.#disposableStack.disposeAsync();
  }
}

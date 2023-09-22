import * as kafka from "kafkajs";
import { Codec, JSONCodec, Topic, TopicFactory } from "@sequent/core";
import { KafkaTopic } from "./KafkaTopic.js";

export class KafkaTopicFactory implements TopicFactory, AsyncDisposable {
  readonly #codec: Codec<any>;
  readonly #client: kafka.Kafka;
  readonly #disposableStack = new AsyncDisposableStack();
  #admin?: kafka.Admin;

  constructor(client: kafka.Kafka, opts: { codec?: Codec<any> } = {}) {
    this.#client = client;
    this.#codec = opts.codec ?? new JSONCodec();
  }

  async make<TEvent>(name: string): Promise<Topic<TEvent>> {
    if (this.#admin == null) {
      const admin = this.#client.admin();
      this.#admin = admin;
      this.#disposableStack.defer(async () => {
        await admin.disconnect();
      });
    }

    await this.#admin.createTopics({
      topics: [
        {
          topic: name,
          numPartitions: 1,
          replicationFactor: 1,
        },
      ],
    });

    return this.#disposableStack.use(
      new KafkaTopic<TEvent>(this.#client, this.#codec, name),
    );
  }

  async [Symbol.asyncDispose]() {
    await this.#disposableStack.disposeAsync();
  }
}

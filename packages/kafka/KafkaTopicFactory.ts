import * as kafka from "kafkajs";
import { Codec, JSONCodec, Topic, TopicFactory } from "@sequent/core";
import { KafkaTopic } from "./KafkaTopic.js";

export class KafkaTopicFactory implements TopicFactory, AsyncDisposable {
  readonly #codec: Codec<any>;
  readonly #client: kafka.Kafka;
  readonly #disposableStack = new AsyncDisposableStack();
  readonly #topicConfig: Record<string, string>;
  readonly #assertedTopics = new Map<string, Promise<boolean> | null>();
  #admin?: kafka.Admin;

  constructor(
    client: kafka.Kafka,
    opts: { codec?: Codec<any>; topicConfig?: Record<string, string> } = {},
  ) {
    this.#client = client;
    this.#codec = opts.codec ?? new JSONCodec();
    this.#topicConfig = {
      // Infinite retention by default
      "retention.bytes": "-1",
      "retention.ms": "-1",
      ...opts.topicConfig,
    };
  }

  async make<TEvent>(name: string): Promise<Topic<TEvent>> {
    if (this.#admin == null) {
      const admin = this.#client.admin();
      this.#admin = admin;
      this.#disposableStack.defer(async () => {
        await admin.disconnect();
      });
    }

    if (!this.#assertedTopics.has(name)) {
      this.#assertedTopics.set(
        name,
        this.#admin.createTopics({
          topics: [
            {
              topic: name,
              configEntries: Object.entries(this.#topicConfig).map(
                ([name, value]) => ({ name, value }),
              ),
            },
          ],
        }),
      );
    }
    await this.#assertedTopics.get(name);

    return this.#disposableStack.use(
      new KafkaTopic<TEvent>(this.#client, this.#codec, name),
    );
  }

  async [Symbol.asyncDispose]() {
    await this.#disposableStack.disposeAsync();
  }
}

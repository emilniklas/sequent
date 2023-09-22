import { Producer, Serializer } from "@sequent/core";
import { FileTopic } from "./FileTopic.js";

export class FileProducer<TEvent> implements Producer<TEvent> {
  readonly #topic: FileTopic<TEvent>;
  readonly #serializer: Serializer<TEvent>;

  constructor(topic: FileTopic<TEvent>, serializer: Serializer<TEvent>) {
    this.#topic = topic;
    this.#serializer = serializer;
  }

  async produce(event: TEvent) {
    const data = this.#serializer.serialize(event);
    const prefix = new BigUint64Array([BigInt(data.byteLength)]);
    const dataWithPrefix = new Uint8Array(8 + data.byteLength);
    dataWithPrefix.set(new Uint8Array(prefix.buffer), 0);
    dataWithPrefix.set(data, prefix.byteLength);

    await this.#topic.append(dataWithPrefix);
  }

  async [Symbol.asyncDispose]() {}
}

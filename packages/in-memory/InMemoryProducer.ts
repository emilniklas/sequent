import { Producer } from "@sequent/core";
import { InMemoryTopic } from "./InMemoryTopic.js";

export class InMemoryProducer<TEvent> implements Producer<TEvent> {
  readonly #topic: InMemoryTopic<TEvent>;

  constructor(topic: InMemoryTopic<TEvent>) {
    this.#topic = topic;
  }

  async produce(event: TEvent) {
    this.#topic.push(event);
  }

  async [Symbol.asyncDispose]() {}
}

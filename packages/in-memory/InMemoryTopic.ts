import {
  ConsumerGroup,
  StartFrom,
  Topic,
} from "@sequent/core";
import { InMemoryProducer } from "./InMemoryProducer.js";
import { InMemoryConsumer } from "./InMemoryConsumer.js";

export class InMemoryTopic<TEvent> implements Topic<TEvent> {
  readonly name: string;
  readonly #events: TEvent[] = [];
  readonly #listeners: Record<number, ((event: TEvent) => void)[] | undefined> =
    {};

  readonly #consumers = new Map<string, InMemoryConsumer<TEvent>>();

  constructor(name: string) {
    this.name = name;
  }

  producer() {
    return new InMemoryProducer<TEvent>(this);
  }

  consumer(group: ConsumerGroup) {
    let consumer = this.#consumers.get(group.name);
    if (consumer == null) {
      let startOffset: number;
      switch (group.startFrom) {
        case StartFrom.Beginning:
          startOffset = 0;
          break;
        case StartFrom.End:
          startOffset = this.#events.length;
          break;
      }
      this.#consumers.set(
        group.name,
        (consumer = new InMemoryConsumer(this, startOffset))
      );
    }

    return consumer;
  }

  get(offset: number): Promise<TEvent> {
    if (offset < this.#events.length) {
      return Promise.resolve(this.#events[offset]);
    }

    return new Promise<TEvent>((res) => {
      (this.#listeners[offset] ??= []).push(res);
    });
  }

  push(event: TEvent) {
    const index = this.#events.length;
    this.#events.push(event);
    this.#listeners[index]?.forEach((l) => l(event));
    delete this.#listeners[index];
  }
}

import { Consumer, Envelope } from "@sequent/core";
import { InMemoryTopic } from "./InMemoryTopic.js";
import { InMemoryMutex } from "./InMemoryMutex.js";

export class InMemoryConsumer<TEvent> implements Consumer<TEvent> {
  readonly #topic: InMemoryTopic<TEvent>;
  readonly #offset: InMemoryMutex<number>;

  constructor(topic: InMemoryTopic<TEvent>, startOffset: number) {
    this.#topic = topic;
    this.#offset = new InMemoryMutex(startOffset);
  }

  async goTo(offset: number) {
    const guard = await this.#offset.get();
    guard.state = offset;
    await guard[Symbol.asyncDispose]();
  }

  async consume(): Promise<Envelope<TEvent>> {
    const offset = await this.#offset.get();

    const event = await this.#topic.get(offset.state);

    return new Envelope(event, {
      ack: async () => {
        offset.state++;
        await offset[Symbol.asyncDispose]();
      },
      nack: async () => {},
    });
  }

  async [Symbol.asyncDispose]() {}
}

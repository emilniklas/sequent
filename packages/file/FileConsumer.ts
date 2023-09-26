import { Consumer, Deserializer, Envelope } from "@sequent/core";
import { FileTopic } from "./FileTopic.js";
import { FileMutex, FileMutexGuard } from "./FileMutex.js";

export class FileConsumer<TEvent> implements Consumer<TEvent> {
  readonly #topic: FileTopic<TEvent>;
  readonly #deserializer: Deserializer<TEvent>;
  readonly #offset: FileMutex<number>;

  constructor(
    topic: FileTopic<TEvent>,
    deserializer: Deserializer<TEvent>,
    offset: FileMutex<number>,
  ) {
    this.#topic = topic;
    this.#deserializer = deserializer;
    this.#offset = offset;
  }

  async #pollRead(
    guard: FileMutexGuard<number>,
    offset: number,
    length: number,
  ): Promise<[Uint8Array, FileMutexGuard<number>]> {
    let data = await this.#topic.read(guard.state + offset, length);
    while (data == null) {
      // Is at end, poll
      await guard[Symbol.asyncDispose]();
      await new Promise((r) => setTimeout(r, 100));
      guard = await this.#offset.get();
      data = await this.#topic.read(guard.state + offset, length);
    }

    return [data, guard];
  }

  async consume(): Promise<Envelope<TEvent>> {
    let guard = await this.#offset.get();
    let data: Uint8Array;
    [data, guard] = await this.#pollRead(guard, 0, 8);

    const length = Number(new BigUint64Array(data.buffer).at(0)!);
    [data, guard] = await this.#pollRead(guard, 8, length);

    const event = this.#deserializer.deserialize(data);
    return new Envelope(event, null, {
      ack: async () => {
        guard.state += 8 + length;
        await guard[Symbol.asyncDispose]();
      },
      nack: async () => {
        await guard[Symbol.asyncDispose]();
      },
    });
  }

  async [Symbol.asyncDispose]() {}
}

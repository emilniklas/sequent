import {
  Codec,
  Consumer,
  ConsumerGroup,
  JSONCodec,
  Producer,
  StartFrom,
  Topic,
} from "@sequent/core";
import { FileConsumer } from "./FileConsumer.js";
import { FileProducer } from "./FileProducer.js";
import { open } from "node:fs/promises";
import { writeSync, readSync, fstatSync } from "node:fs";
import * as path from "node:path";
import { FileMutex } from "./FileMutex.js";

export class FileTopic<TEvent> implements Topic<TEvent> {
  readonly name: string;
  readonly #directory: string;
  readonly #handle: number;
  readonly #codec: Codec<TEvent>;

  constructor(
    name: string,
    directory: string,
    handle: number,
    codec: Codec<TEvent>
  ) {
    this.name = name;
    this.#directory = directory;
    this.#handle = handle;
    this.#codec = codec;
  }

  static async create<TEvent>(
    directory: string,
    name: string,
    codec: Codec<TEvent>
  ): Promise<FileTopic<TEvent>> {
    let handle = await open(path.join(directory, name + ".log"), "a+");
    if (typeof handle !== "number") {
      handle = (handle as { fd: number }).fd;
    }
    return new FileTopic(name, directory, handle, codec);
  }

  async append(bytes: Uint8Array) {
    writeSync(this.#handle, bytes, 0, bytes.byteLength);
  }

  async read(offset: number, length: number): Promise<Uint8Array | undefined> {
    const data = new Uint8Array(length);
    const readn = readSync(this.#handle, data, 0, length, offset);
    if (readn === 0) {
      return undefined;
    }
    if (readn !== length) {
      throw new Error(
        `Read length mismatch: expected to read ${length}, read ${readn}`
      );
    }
    return data;
  }

  async producer(): Promise<Producer<TEvent>> {
    return new FileProducer<TEvent>(this, this.#codec);
  }

  async consumer(group: ConsumerGroup): Promise<Consumer<TEvent>> {
    let initialOffset: number;
    switch (group.startFrom) {
      case StartFrom.Beginning:
        initialOffset = 0;
        break;
      case StartFrom.End:
        initialOffset = fstatSync(this.#handle).size;
        break;
    }
    const offsetFile = path.join(
      this.#directory,
      this.name + "__" + group.name + ".offset"
    );
    const offsetMutex = new FileMutex<number>(
      new JSONCodec<number>(),
      offsetFile,
      initialOffset
    );
    return new FileConsumer<TEvent>(this, this.#codec, offsetMutex);
  }
}

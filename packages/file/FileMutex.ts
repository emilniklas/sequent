import {
  Codec,
  Mutex,
  MutexGuard,
  Serializer,
} from "@sequent/core";
import { lock as lockCb, unlock as unlockCb, Options as LockOptions } from "lockfile";
import { readFile, writeFile } from "node:fs/promises";
import { promisify } from "node:util";

const lock = promisify(lockCb) as (path: string, options: LockOptions) => Promise<void>;
const unlock = promisify(unlockCb);

export class FileMutex<T> implements Mutex<T> {
  readonly #codec: Codec<T>;
  readonly #filePath: string;
  readonly #initialState: T;

  constructor(codec: Codec<T>, filePath: string, initialState: T) {
    this.#codec = codec;
    this.#filePath = filePath;
    this.#initialState = initialState;
  }

  async get(): Promise<FileMutexGuard<T>> {
    await lock(this.#filePath + ".lock", { wait: 5000 });
    const state = await readFile(this.#filePath)
      .then((b) => this.#codec.deserialize(b))
      .catch(() => this.#initialState);
    return new FileMutexGuard(this.#filePath, this.#codec, state);
  }
}

export class FileMutexGuard<T> implements MutexGuard<T> {
  readonly #filePath: string;
  readonly #serializer: Serializer<T>;

  state: T;

  constructor(filePath: string, serializer: Serializer<T>, state: T) {
    this.#filePath = filePath;
    this.#serializer = serializer;
    this.state = state;
  }

  async [Symbol.asyncDispose]() {
    await writeFile(this.#filePath, this.#serializer.serialize(this.state));
    await unlock(this.#filePath + ".lock");
  }
}

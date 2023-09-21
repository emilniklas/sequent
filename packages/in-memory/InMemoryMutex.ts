import { Mutex } from "@sequent/core";

export class InMemoryMutex<T> implements Mutex<T> {
  #state: Promise<T>;

  constructor(initialState: T) {
    this.#state = Promise.resolve(initialState);
  }

  async get(): Promise<InMemoryMutexGuard<T>> {
    const oldPromise = this.#state;
    let release!: (newState: T) => void;
    this.#state = new Promise<T>((r) => {
      release = r;
    });
    return new InMemoryMutexGuard<T>(await oldPromise, release);
  }
}

export class InMemoryMutexGuard<T> implements InMemoryMutexGuard<T> {
  state: T;
  readonly #release: (newState: T) => void;

  constructor(state: T, release: (newState: T) => void) {
    this.state = state;
    this.#release = release;
  }

  async [Symbol.asyncDispose]() {
    this.#release(this.state);
  }
}

export interface Consumer<TEvent> extends AsyncDisposable {
  consume(opts?: {
    signal?: AbortSignal;
  }): Promise<Envelope<TEvent> | undefined>;
}

export class Envelope<TEvent> implements AsyncDisposable {
  readonly #event: TEvent;
  readonly #key: Uint8Array | null;
  readonly #acker: Acker;
  #nacked = false;

  constructor(event: TEvent, key: Uint8Array | null, acker: Acker) {
    this.#event = event;
    this.#key = key;
    this.#acker = acker;
  }

  get event(): TEvent {
    return this.#event;
  }

  get key(): Uint8Array | null {
    return this.#key;
  }

  async nack() {
    this.#nacked = true;
    await this.#acker.nack();
  }

  async [Symbol.asyncDispose]() {
    if (!this.#nacked) {
      await this.#acker.ack();
    }
  }

  map<TEvent2>(f: (event: TEvent) => TEvent2): Envelope<TEvent2> {
    return new Envelope(f(this.#event), this.#key, this.#acker);
  }
}

export interface Acker {
  ack(): Promise<void>;
  nack(): Promise<void>;
}

export interface ConsumerGroup {
  readonly name: string;
  readonly startFrom: StartFrom;
}

export namespace ConsumerGroup {
  export function anonymous(startFrom = StartFrom.Beginning): ConsumerGroup {
    return {
      name: crypto.randomUUID(),
      startFrom,
    };
  }

  export function join(
    name: string,
    startFrom = StartFrom.Beginning,
  ): ConsumerGroup {
    return {
      name,
      startFrom,
    };
  }
}

export enum StartFrom {
  Beginning = "BEGINNING",
  End = "END",
}

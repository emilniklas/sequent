export interface Consumer<TEvent> extends AsyncDisposable {
  consume(opts?: {
    signal?: AbortSignal;
  }): Promise<Envelope<TEvent> | undefined>;
}

export class Envelope<TEvent> implements AsyncDisposable {
  readonly #event: TEvent;
  readonly #acker: Acker;
  #nacked = false;

  constructor(event: TEvent, acker: Acker) {
    this.#event = event;
    this.#acker = acker;
  }

  get event(): TEvent {
    return this.#event;
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
    return new Envelope(f(this.#event), this.#acker);
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

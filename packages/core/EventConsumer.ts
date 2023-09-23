import { Consumer, Envelope } from "./Consumer.js";
import { Event, RawEvent } from "./EventType.js";
import { Logger } from "./Logger.js";

export interface CatchUpOptions {
  readonly progressLogFrequencyMs: number;
  readonly catchUpDelayMs: number;
}

export class EventConsumer<TEvent> implements Consumer<Event<TEvent>> {
  static readonly #DEFAULT_CATCH_UP_DELAY = 1000;
  static readonly #DEFAULT_PROGRESS_LOG_FREQUENCY = 3000;
  static readonly #NUMBER_FORMAT = new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 1,
  });

  readonly #inner: Consumer<RawEvent<TEvent>>;
  readonly #logger: Logger;
  readonly #onCatchUp: () => void;
  readonly #catchUpOptions: CatchUpOptions;

  #caughtUp = false;
  #catchUpDelayTimer?: ReturnType<typeof setTimeout>;
  #logProcessInterval?: ReturnType<typeof setInterval>;

  #progressCounter = 0;

  constructor(
    inner: Consumer<RawEvent<TEvent>>,
    opts: {
      logger: Logger;
      onCatchUp: () => void;
      catchUpOptions?: Partial<CatchUpOptions>;
    },
  ) {
    this.#inner = inner;
    this.#logger = opts.logger;
    this.#onCatchUp = opts.onCatchUp;
    this.#catchUpOptions = {
      progressLogFrequencyMs:
        opts.catchUpOptions?.progressLogFrequencyMs ??
        EventConsumer.#DEFAULT_PROGRESS_LOG_FREQUENCY,
      catchUpDelayMs:
        opts.catchUpOptions?.catchUpDelayMs ??
        EventConsumer.#DEFAULT_CATCH_UP_DELAY,
    };
  }

  #rescheduleCatchUpDelay() {
    clearTimeout(this.#catchUpDelayTimer);
    if (!this.#caughtUp) {
      this.#catchUpDelayTimer = setTimeout(() => {
        this.#logger.debug("Caught up due to halted consumer");
        this.#catchUp();
      }, this.#catchUpOptions.catchUpDelayMs);
    }
  }

  #logProgress() {
    if (this.#caughtUp) {
      clearInterval(this.#logProcessInterval);
      return;
    }

    this.#logger.debug("Still catching up...", {
      throughput: `${EventConsumer.#NUMBER_FORMAT.format(
        this.#progressCounter /
          (this.#catchUpOptions.progressLogFrequencyMs / 1000),
      )} events/s`,
    });

    this.#progressCounter = 0;
  }

  #catchUp() {
    clearTimeout(this.#catchUpDelayTimer);
    if (!this.#caughtUp) {
      this.#caughtUp = true;
      this.#onCatchUp();
    }
  }

  async consume(opts?: {
    signal?: AbortSignal;
  }): Promise<Envelope<Event<TEvent>> | undefined> {
    if (this.#logProcessInterval == null && !this.#caughtUp) {
      this.#logProcessInterval = setInterval(
        this.#logProgress.bind(this),
        this.#catchUpOptions.progressLogFrequencyMs,
      );
    }

    this.#rescheduleCatchUpDelay();

    const onAbort = this.#catchUp.bind(this);
    opts?.signal?.addEventListener("abort", onAbort);
    const envelope = await this.#inner.consume(opts);
    opts?.signal?.removeEventListener("abort", onAbort);

    if (envelope == null) {
      return undefined;
    }

    this.#progressCounter++;

    if (
      Date.now() - envelope.event.timestamp <=
      this.#catchUpOptions.catchUpDelayMs
    ) {
      this.#logger.debug("Caught up due to recent event");
      this.#catchUp();
    }

    return envelope.map((e) => ({
      timestamp: new Date(e.timestamp),
      message: e.message,
    }));
  }

  async [Symbol.asyncDispose]() {
    await this.#inner[Symbol.asyncDispose]();
  }
}

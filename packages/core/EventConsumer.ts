import { Consumer, Envelope } from "./Consumer.js";
import { Event, RawEvent } from "./EventType.js";
import { Logger } from "./Logger.js";

export class EventConsumer<TEvent> implements Consumer<Event<TEvent>> {
  static readonly #CATCH_UP_DELAY = 5000;
  static readonly #PROGRESS_LOG_FREQUENCY = 3000;
  static readonly #NUMBER_FORMAT = new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 1,
  });

  readonly #logger: Logger;
  readonly #inner: Consumer<RawEvent<TEvent>>;
  readonly #onCatchUp: () => void;

  #caughtUp = false;
  #catchUpDelayTimer?: ReturnType<typeof setTimeout>;
  #logProcessInterval?: ReturnType<typeof setInterval>;

  #progressCounter = 0;

  constructor(
    logger: Logger,
    inner: Consumer<RawEvent<TEvent>>,
    onCatchUp: () => void,
  ) {
    this.#logger = logger;
    this.#inner = inner;
    this.#onCatchUp = onCatchUp;
  }

  #rescheduleCatchUpDelay() {
    clearTimeout(this.#catchUpDelayTimer);
    if (!this.#caughtUp) {
      this.#catchUpDelayTimer = setTimeout(() => {
        this.#logger.debug("Caught up due to halted consumer");
        this.#catchUp();
      }, EventConsumer.#CATCH_UP_DELAY);
    }
  }

  #logProgress() {
    if (this.#caughtUp) {
      clearInterval(this.#logProcessInterval);
      return;
    }

    this.#logger.debug("Still catching up...", {
      throughput: `${EventConsumer.#NUMBER_FORMAT.format(
        this.#progressCounter / (EventConsumer.#PROGRESS_LOG_FREQUENCY / 1000),
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
        EventConsumer.#PROGRESS_LOG_FREQUENCY,
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
      EventConsumer.#CATCH_UP_DELAY
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

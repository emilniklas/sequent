import { ConsumerGroup, StartFrom } from "./Consumer.js";
import { EventType } from "./EventType.js";
import { TopicFactory } from "./TopicFactory.js";

export class Migrator<TSourceEvent, TDestinationEvent> {
  readonly #source: EventType<TSourceEvent>;
  readonly #destination: () => EventType<TDestinationEvent>;
  readonly #migration: (source: TSourceEvent) => TDestinationEvent;

  #running?: Promise<void>;

  constructor(opts: {
    source: EventType<TSourceEvent>;
    destination: () => EventType<TDestinationEvent>;
    migration: (source: TSourceEvent) => TDestinationEvent;
  }) {
    this.#source = opts.source;
    this.#destination = opts.destination;
    this.#migration = opts.migration;
  }

  run(topicFactory: TopicFactory, signal?: AbortSignal): Promise<void> {
    if (this.#running) {
      return this.#running;
    }

    return (this.#running = Promise.resolve().then(async () => {
      const sourceTopic = await this.#source.topic(topicFactory);
      const destinationTopic = await this.#destination().topic(topicFactory);

      const stack = new AsyncDisposableStack();
      const sourceConsumer = stack.use(
        await sourceTopic.consumer(
          ConsumerGroup.join(
            `${sourceTopic.name}-${destinationTopic.name}`,
            StartFrom.Beginning,
          ),
        ),
      );

      const destinationProducer = stack.use(await destinationTopic.producer());

      return new Promise<void>(async (resolve) => {
        let caughtUp = false;
        const CATCH_UP_DELAY = 5000;

        const onCatchUp = () => {
          if (!caughtUp) {
            caughtUp = true;
            resolve();
          }
        };

        let catchUpDelayTimer: ReturnType<typeof setTimeout> | undefined;
        const rescheduleCatchUpDelay = () => {
          clearTimeout(catchUpDelayTimer);
          if (!caughtUp) {
            catchUpDelayTimer = setTimeout(onCatchUp, CATCH_UP_DELAY);
          }
        };

        signal?.addEventListener("abort", async () => {
          await stack.disposeAsync();
          clearTimeout(catchUpDelayTimer);
          resolve();
        });

        rescheduleCatchUpDelay();

        while (!signal?.aborted) {
          const envelope = await sourceConsumer.consume({ signal });
          if (envelope == null) {
            continue;
          }
          try {
            rescheduleCatchUpDelay();

            if (
              Date.now() - envelope.event.timestamp.getTime() <=
              CATCH_UP_DELAY
            ) {
              onCatchUp();
            }

            const newMessage = this.#migration(envelope.event.message);
            await destinationProducer.produce({
              timestamp: envelope.event.timestamp,
              message: newMessage,
            });
          } catch (e) {
            await envelope.nack();
            throw e;
          } finally {
            await envelope[Symbol.asyncDispose]();
          }
        }
      });
    }));
  }
}

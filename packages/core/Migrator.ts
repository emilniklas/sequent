import { ConsumerGroup, StartFrom } from "./Consumer.js";
import { CatchUpOptions, EventConsumer } from "./EventConsumer.js";
import { EventType, RawEvent } from "./EventType.js";
import { Logger } from "./Logger.js";
import { Topic } from "./Topic.js";
import { TopicFactory } from "./TopicFactory.js";

export interface RunningMigration<TSourceEvent, TDestinationEvent>
  extends AsyncDisposable {
  readonly sourceEventType: EventType<TSourceEvent>;
  readonly sourceTopic: Topic<RawEvent<TSourceEvent>>;
  readonly destinationEventType: EventType<TDestinationEvent>;
  readonly destinationTopic: Topic<RawEvent<TDestinationEvent>>;
}

export class Migrator<TSourceEvent, TDestinationEvent> {
  readonly #source: EventType<TSourceEvent>;
  readonly #destination: () => EventType<TDestinationEvent>;
  readonly #migration: (source: TSourceEvent) => Iterable<TDestinationEvent>;

  #running?: Promise<RunningMigration<TSourceEvent, TDestinationEvent>>;

  constructor(opts: {
    source: EventType<TSourceEvent>;
    destination: () => EventType<TDestinationEvent>;
    migration: (source: TSourceEvent) => Iterable<TDestinationEvent>;
  }) {
    this.#source = opts.source;
    this.#destination = opts.destination;
    this.#migration = opts.migration;
  }

  run(
    topicFactory: TopicFactory,
    {
      logger,
      catchUpOptions,
    }: {
      logger: Logger;
      catchUpOptions?: Partial<CatchUpOptions>;
    },
  ): Promise<RunningMigration<TSourceEvent, TDestinationEvent>> {
    if (this.#running) {
      return this.#running;
    }

    return (this.#running = Promise.resolve().then(async () => {
      const controller = new AbortController();

      const sourceTopic = await this.#source.topic(topicFactory);
      const destinationTopic = await this.#destination().topic(topicFactory);

      const migrationLogger = logger.withContext({
        sourceTopic: sourceTopic.name,
        destinationTopic: destinationTopic.name,
      });

      const stack = new AsyncDisposableStack();
      const sourceConsumerRaw = stack.use(
        await sourceTopic.consumer(
          ConsumerGroup.join(
            `${sourceTopic.name}-${destinationTopic.name}`,
            StartFrom.Beginning,
          ),
        ),
      );

      await new Promise<void>(async (onCatchUp) => {
        const sourceConsumer = new EventConsumer(sourceConsumerRaw, {
          logger: migrationLogger,
          onCatchUp,
          catchUpOptions,
        });
        const destinationProducer = stack.use(
          await destinationTopic.producer(),
        );

        migrationLogger.info("Migrating topic");

        while (!controller.signal.aborted) {
          const envelope = await sourceConsumer.consume({
            signal: controller.signal,
          });
          if (envelope == null) {
            continue;
          }
          try {
            const newMessages = this.#migration(envelope.event.message);
            for (const newMessage of newMessages) {
              await destinationProducer.produce(
                {
                  timestamp: envelope.event.timestamp.getTime(),
                  message: newMessage,
                },
                envelope.key,
              );
            }
          } catch (e) {
            await envelope.nack();
            throw e;
          } finally {
            await envelope[Symbol.asyncDispose]();
          }
        }
      });

      migrationLogger.info("Migrator caught up");

      return {
        sourceEventType: this.#source,
        sourceTopic,
        destinationEventType: this.#destination(),
        destinationTopic,
        async [Symbol.asyncDispose]() {
          controller.abort();
          await stack.disposeAsync();
        },
      };
    }));
  }
}

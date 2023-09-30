import { CatchUpOptions } from "./EventConsumer.js";
import { EventProducer } from "./EventProducer.js";
import { EventType } from "./EventType.js";
import { Logger } from "./Logger.js";
import { ReadModel, ReadModelClientFactory } from "./ReadModel.js";
import { TopicFactory } from "./TopicFactory.js";
import { TypeSpec } from "./TypeSpec.js";

export class Aggregate {
  readonly #name: string;
  readonly #topicFactory: TopicFactory;

  constructor(name: string, topicFactory: TopicFactory) {
    this.#name = name;
    this.#topicFactory = topicFactory;
  }

  get name() {
    return this.#name;
  }

  assertValidEventType(eventType: EventType<any>) {
    this.#assertValidEventTypeSpec(eventType.spec);
  }

  #assertValidEventTypeSpec(spec: TypeSpec) {
    if (!TypeSpec.isRecord(spec) || !("id" in spec.spec)) {
      throw new Error(
        "Aggregate read models must only ingest from events with an `id` field",
      );
    }
  }

  useEventType<TEvent extends { id: string | number | Uint8Array }>(
    type: EventType<TEvent>,
  ): Promise<EventProducer<TEvent>> {
    return type.withinAggregate(this).producer(this.#topicFactory);
  }

  useClientFactory<TClients extends object>(
    clientFactory: ReadModelClientFactory<TClients>,
  ) {
    return new ReadModelAggregate<TClients>(
      this,
      this.#topicFactory,
      clientFactory,
    );
  }
}

export class ReadModelAggregate<TClients extends object> {
  readonly #aggregate: Aggregate;
  readonly #topicFactory: TopicFactory;
  readonly #clientFactory: ReadModelClientFactory<TClients>;

  constructor(
    aggregate: Aggregate,
    topicFactory: TopicFactory,
    clientFactory: ReadModelClientFactory<TClients>,
  ) {
    this.#aggregate = aggregate;
    this.#topicFactory = topicFactory;
    this.#clientFactory = clientFactory;
  }

  useReadModel<TModel extends ReadModel<any>>(
    readModel: TModel,
    opts: {
      signal?: AbortSignal;
      logger?: Logger;
      catchUpOptions?: CatchUpOptions;
    } = {},
  ): ReturnType<TModel["start"]> {
    return readModel.start(this.#topicFactory, this.#clientFactory, {
      ...opts,
      aggregate: this.#aggregate,
    }) as ReturnType<TModel["start"]>;
  }
}

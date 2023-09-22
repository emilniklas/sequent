import { Consumer, ConsumerGroup } from "./Consumer.js";
import { Migrator } from "./Migrator.js";
import { Producer } from "./Producer.js";
import { Topic } from "./Topic.js";
import { TopicFactory } from "./TopicFactory.js";

export namespace EventType {
  export type TypeOf<TEventType> = TEventType extends EventType<infer R>
    ? R
    : never;
}

export class EventType<TEvent> {
  readonly #name: string;
  readonly #spec: TypeSpec;
  readonly #migrators: Migrator<any, any>[];
  readonly #nonce: number;

  constructor(
    name: string,
    spec: TypeSpec,
    migrators: Migrator<any, any>[],
    nonce: number,
  ) {
    this.#name = name;
    this.#spec = spec;
    this.#migrators = migrators;
    this.#nonce = nonce;
  }

  static new<TSpec extends TypeSpec>(
    name: string,
    spec: TSpec,
    { nonce = 0 }: { nonce?: number } = {},
  ): EventType<TypeOf<TSpec>> {
    return new EventType<TypeOf<TSpec>>(name, spec, [], nonce);
  }

  toString() {
    return `${this.#name} ${this.#spec}`;
  }

  async topicName(): Promise<string> {
    const hashDigest = Array.from(
      new Uint8Array(
        await crypto.subtle.digest(
          "SHA-1",
          new TextEncoder().encode(this.toString() + this.#nonce.toString()),
        ),
      ),
      (b) => b.toString(16).padStart(2, "0"),
    ).join("");

    return `${this.#name}-${hashDigest}`;
  }

  async topic(topicFactory: TopicFactory): Promise<Topic<Event<TEvent>>> {
    return topicFactory.make<Event<TEvent>>(await this.topicName());
  }

  async producer(topicFactory: TopicFactory): Promise<Producer<TEvent>> {
    const stack = new AsyncDisposableStack();
    const controller = new AbortController();
    stack.defer(() => controller.abort());

    await Promise.all(
      this.#migrators.map((m) => m.run(topicFactory, controller.signal)),
    );
    const topic = await this.topic(topicFactory);
    return new EventProducer(stack.use(await topic.producer()), stack);
  }

  async consumer(
    topicFactory: TopicFactory,
    group: ConsumerGroup,
  ): Promise<Consumer<Event<TEvent>>> {
    const topic = await this.topic(topicFactory);
    return topic.consumer(group);
  }

  addFields<TSpec extends { readonly [field: string]: TypeSpec }>(
    fields: NewFields<TEvent, TSpec>,
    { nonce = 0 }: { nonce?: number } = {},
  ): EventType<Omit<TEvent, keyof TSpec> & TypeOf<TypeSpec.Record<TSpec>>> {
    type NewTEvent = Omit<TEvent, keyof TSpec> & TypeOf<TypeSpec.Record<TSpec>>;

    if (!TypeSpec.isRecord(this.#spec)) {
      throw new Error("Cannot add fields to a non-record type");
    }

    const patchSpec = Object.fromEntries(
      Object.entries(fields).map(
        ([field, { type }]: [
          keyof TSpec,
          NewField<TEvent, TSpec[keyof TSpec]>,
        ]) => [field, type],
      ),
    );

    const newSpec = TypeSpec.Record({ ...this.#spec.spec, ...patchSpec });

    const migrator = new Migrator<TEvent, NewTEvent>({
      source: this,
      destination: () => newType,
      migration: (event) => {
        return {
          ...event,
          ...Object.fromEntries(
            Object.entries(fields).map(
              ([field, { migrate }]: [
                keyof TSpec,
                NewField<TEvent, TSpec[keyof TSpec]>,
              ]) => [field, migrate(event)],
            ),
          ),
        } as NewTEvent;
      },
    });

    const newType: EventType<NewTEvent> = new EventType<NewTEvent>(
      this.#name,
      newSpec,
      [...this.#migrators, migrator],
      this.#nonce + nonce,
    );

    return newType;
  }
}

export type NewFields<
  TEvent,
  TSpec extends { readonly [field: string]: TypeSpec },
> = {
  readonly [TField in keyof TSpec]: NewField<TEvent, TSpec[TField]>;
};

export interface NewField<TEvent, TSpec extends TypeSpec> {
  type: TSpec;
  migrate: (event: TEvent) => TypeOf<TSpec>;
}

class EventProducer<TEvent> implements Producer<TEvent> {
  readonly #inner: Producer<Event<TEvent>>;
  readonly #disposable: AsyncDisposable;

  constructor(inner: Producer<Event<TEvent>>, disposable: AsyncDisposable) {
    this.#inner = inner;
    this.#disposable = disposable;
  }

  async produce(event: TEvent) {
    await this.#inner.produce({
      timestamp: new Date(),
      message: event,
    });
  }

  async [Symbol.asyncDispose]() {
    await this.#disposable[Symbol.asyncDispose]();
  }
}

export type Event<TMessage> = {
  readonly timestamp: Date;
  readonly message: TMessage;
};

export type TypeSpec =
  | TypeSpec.Primitive
  | TypeSpec.Array<TypeSpec>
  | TypeSpec.Record<Record<string, TypeSpec>>;

const TYPE_OF = Symbol("TYPE_OF");
type TYPE_OF = typeof TYPE_OF;

export type TypeOf<TSpec extends TypeSpec> = ReturnType<
  NonNullable<TSpec[TYPE_OF]>
>;

export namespace TypeSpec {
  const STRING = Symbol("TypeSpec.String");
  export interface String {
    readonly type: typeof STRING;
    readonly [TYPE_OF]?: () => string;
    toString(): string;
  }
  export const String: String = {
    type: STRING,
    toString: () => "String",
  };

  const NUMBER = Symbol("TypeSpec.Number");
  export interface Number {
    readonly type: typeof NUMBER;
    readonly [TYPE_OF]?: () => number;
    toString(): string;
  }
  export const Number: Number = {
    type: NUMBER,
    toString: () => "Number",
  };

  export type Primitive = String | Number;

  const ARRAY = Symbol("TypeSpec.Array");
  export interface Array<TSpec extends TypeSpec> {
    readonly type: typeof ARRAY;
    readonly spec: TSpec;
    // @ts-expect-error
    readonly [TYPE_OF]?: () => TypeOf<TSpec>[];
    toString(): string;
  }
  export function Array<TSpec extends TypeSpec>(spec: TSpec): Array<TSpec> {
    return {
      type: ARRAY,
      spec,
      toString: () => spec.toString() + "[]",
    };
  }

  const RECORD = Symbol("TypeSpec.Record");
  export interface Record<
    TSpec extends { readonly [field: string]: TypeSpec },
  > {
    readonly type: typeof RECORD;
    readonly spec: TSpec;
    readonly [TYPE_OF]?: () => {
      readonly [P in keyof TSpec]: TypeOf<TSpec[P]>;
    };
    toString(): string;
  }
  export function Record<
    const TSpec extends { readonly [field: string]: TypeSpec },
  >(spec: TSpec): Record<TSpec> {
    const indent = (s: string) =>
      s
        .split("\n")
        .map((l) => "  " + l)
        .join("\n");

    return {
      type: RECORD,
      spec,
      toString: () =>
        "{\n" +
        indent(
          Object.entries(spec)
            .map(([name, spec]) => `${name}: ${spec.toString()}`)
            .join("\n"),
        ) +
        "\n}",
    };
  }

  export function isRecord(
    spec: TypeSpec,
  ): spec is Record<{ readonly [field: string]: TypeSpec }> {
    return spec.type === RECORD;
  }
}

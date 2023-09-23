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
    return new EventProducer(
      stack.use(await topic.producer()),
      this.#spec,
      stack,
    );
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
  readonly #spec: TypeSpec;
  readonly #disposable: AsyncDisposable;

  constructor(
    inner: Producer<Event<TEvent>>,
    spec: TypeSpec,
    disposable: AsyncDisposable,
  ) {
    this.#inner = inner;
    this.#spec = spec;
    this.#disposable = disposable;
  }

  async produce(event: TEvent) {
    this.#spec.assert(event);
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
  export class AssertionError extends Error {
    toString(subject?: string) {
      const formatAed = (aed: AssertionErrorDescription): string => {
        let description = aed.description;
        if (aed.causes) {
          description +=
            "\n" +
            aed.causes
              .map(formatAed)
              .map(
                (f) =>
                  "\n" +
                  f
                    .split("\n")
                    .map((l) => "  " + l)
                    .join("\n"),
              )
              .join();
        }
        return description;
      };

      return formatAed(this.toJSON(subject));
    }

    toJSON(subject?: string): AssertionErrorDescription {
      let description = "";
      if (subject) {
        description += subject + " ";
      }
      description += this.message;

      if (this.cause == null) {
        return { description };
      }

      const rawCauses: unknown[] = !global.Array.isArray(this.cause)
        ? [this.cause]
        : this.cause;

      return {
        description,
        causes: rawCauses.map((rc) => {
          if (rc instanceof AssertionError) {
            return rc.toJSON();
          }
          return { description: "unknown cause", cause: rc };
        }),
      };
    }
  }

  export interface AssertionErrorDescription {
    description: string;
    causes?: AssertionErrorDescription[];
  }

  const STRING = Symbol("TypeSpec.String");
  export interface String {
    readonly type: typeof STRING;
    readonly [TYPE_OF]?: () => string;
    toString(): string;
    assert(value: unknown): asserts value is string;
  }
  export const String: String = {
    type: STRING,
    toString: () => "String",
    assert(value) {
      if (typeof value !== "string") {
        throw new AssertionError("is not a string");
      }
    },
  };

  const NUMBER = Symbol("TypeSpec.Number");
  export interface Number {
    readonly type: typeof NUMBER;
    readonly [TYPE_OF]?: () => number;
    toString(): string;
    assert(value: unknown): asserts value is number;
  }
  export const Number: Number = {
    type: NUMBER,
    toString: () => "Number",
    assert(value) {
      if (typeof value !== "number") {
        throw new AssertionError("is not a number");
      }
    },
  };

  export type Primitive = String | Number;

  const ARRAY = Symbol("TypeSpec.Array");
  export interface Array<TSpec extends TypeSpec> {
    readonly type: typeof ARRAY;
    readonly spec: TSpec;
    // @ts-expect-error
    readonly [TYPE_OF]?: () => TypeOf<TSpec>[];
    toString(): string;
    assert(value: unknown): asserts value is TypeOf<TSpec>[];
  }
  export function Array<TSpec extends TypeSpec>(spec: TSpec): Array<TSpec> {
    return {
      type: ARRAY,
      spec,
      toString: () => spec.toString() + "[]",
      assert(value) {
        if (!global.Array.isArray(value)) {
          throw new AssertionError("is not an array");
        }

        const elementAssertions: AssertionError[] = [];
        for (let i = 0; i < value.length; i++) {
          try {
            spec.assert(value[i]);
          } catch (e) {
            elementAssertions.push(
              new AssertionError(`has invalid element at index ${i}`, {
                cause: e,
              }),
            );
          }
        }

        switch (elementAssertions.length) {
          case 0:
            return;

          case 1:
            throw elementAssertions[0];

          default:
            throw new AssertionError("has multiple invalid elements", {
              cause: elementAssertions,
            });
        }
      },
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
    assert(value: unknown): asserts value is {
      readonly [P in keyof TSpec]: TypeOf<TSpec[P]>;
    };
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
      assert(value) {
        if (value == null || typeof value !== "object") {
          throw new AssertionError("is not an object");
        }

        if (![null, Object.prototype].includes(Object.getPrototypeOf(value))) {
          throw new AssertionError("has a complex prototype chain");
        }

        const keys = Reflect.ownKeys(value);
        const requiredKeys = new Set(Object.keys(spec));

        const fieldAssertions: AssertionError[] = [];
        for (const key of keys) {
          try {
            if (typeof key === "symbol") {
              throw new AssertionError("is a symbol key");
            }

            if (!requiredKeys.has(key)) {
              throw new AssertionError("is not a defined key");
            }
            requiredKeys.delete(key);

            spec[key].assert((value as any)[key]);
          } catch (e) {
            fieldAssertions.push(
              new AssertionError(`has invalid "${key.toString()}" field`, {
                cause: e,
              }),
            );
          }
        }

        for (const missingKey of requiredKeys) {
          fieldAssertions.push(
            new AssertionError(`is missing required "${missingKey}" field`),
          );
        }

        switch (fieldAssertions.length) {
          case 0:
            return;

          case 1:
            throw fieldAssertions[0];

          default:
            throw new AssertionError("has multiple issues", {
              cause: fieldAssertions,
            });
        }
      },
    };
  }

  export function isRecord(
    spec: TypeSpec,
  ): spec is Record<{ readonly [field: string]: TypeSpec }> {
    return spec.type === RECORD;
  }
}

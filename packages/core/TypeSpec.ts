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

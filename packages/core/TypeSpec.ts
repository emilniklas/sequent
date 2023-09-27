export type TypeSpec =
  | TypeSpec.Bytes
  | TypeSpec.String
  | TypeSpec.Number
  | TypeSpec.Boolean
  | TypeSpec.Array<TypeSpec>
  | TypeSpec.Record<Record<string, TypeSpec>>
  | TypeSpec.Optional<TypeSpec>
  | TypeSpec.Union<TypeSpec[]>;

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

  interface TypeSpecOperators {
    or<TSelf extends TypeSpec, TSpec extends TypeSpec>(
      this: TSelf,
      spec: TSpec,
    ): Union<[TSelf, TSpec]>;
  }

  class BaseTypeSpec implements TypeSpecOperators {
    or<TSelf extends TypeSpec, TSpec extends TypeSpec>(
      this: TSelf,
      spec: TSpec,
    ): Union<[TSelf, TSpec]> {
      return new UnionSpec([this, spec]);
    }
  }

  const STRING = Symbol("TypeSpec.String");
  export interface String extends TypeSpecOperators {
    readonly type: typeof STRING;
    readonly [TYPE_OF]?: () => string;
    toString(): string;
    assert(value: unknown): asserts value is string;
  }
  export const String: String = new (class StringSpec extends BaseTypeSpec {
    readonly type: typeof STRING = STRING;
    toString() {
      return "String";
    }
    assert(value: unknown) {
      if (typeof value !== "string") {
        throw new AssertionError("is not a string");
      }
    }
  })();

  const BYTES = Symbol("TypeSpec.Bytes");
  export interface Bytes extends TypeSpecOperators {
    readonly type: typeof BYTES;
    readonly [TYPE_OF]?: () => Uint8Array;
    toString(): string;
    assert(value: unknown): asserts value is Uint8Array;
  }
  export const Bytes: Bytes = new (class StringSpec extends BaseTypeSpec {
    readonly type: typeof BYTES = BYTES;
    toString() {
      return "Bytes";
    }
    assert(value: unknown) {
      if (
        !value ||
        typeof value !== "object" ||
        !(value instanceof Uint8Array)
      ) {
        throw new AssertionError("is not a Uint8Array");
      }
    }
  })();

  const NUMBER = Symbol("TypeSpec.Number");
  export interface Number extends TypeSpecOperators {
    readonly type: typeof NUMBER;
    readonly [TYPE_OF]?: () => number;
    toString(): string;
    assert(value: unknown): asserts value is number;
  }
  export const Number: Number = new (class NumberSpec extends BaseTypeSpec {
    readonly type: typeof NUMBER = NUMBER;
    toString() {
      return "Number";
    }
    assert(value: unknown) {
      if (typeof value !== "number") {
        throw new AssertionError("is not a number");
      }
    }
  })();

  const BOOLEAN = Symbol("TypeSpec.Boolean");
  export interface Boolean extends TypeSpecOperators {
    readonly type: typeof BOOLEAN;
    readonly [TYPE_OF]?: () => boolean;
    toString(): string;
    assert(value: unknown): asserts value is boolean;
  }
  export const Boolean: Boolean = new (class BooleanSpec extends BaseTypeSpec {
    readonly type: typeof BOOLEAN = BOOLEAN;
    toString() {
      return "Boolean";
    }
    assert(value: unknown) {
      if (typeof value !== "boolean") {
        throw new AssertionError("is not a boolean");
      }
    }
  })();

  const ARRAY = Symbol("TypeSpec.Array");
  export interface Array<TSpec extends TypeSpec> extends TypeSpecOperators {
    readonly type: typeof ARRAY;
    readonly spec: TSpec;
    readonly [TYPE_OF]?: () => TypeOf<TSpec>[];
    toString(): string;
    assert(value: unknown): asserts value is TypeOf<TSpec>[];
  }
  class ArraySpec<TSpec extends TypeSpec>
    extends BaseTypeSpec
    implements Array<TSpec>
  {
    readonly spec: TSpec;
    readonly type: typeof ARRAY = ARRAY;

    constructor(spec: TSpec) {
      super();
      this.spec = spec;
    }

    toString() {
      return this.spec.toString() + "[]";
    }

    assert(value: unknown) {
      if (!global.Array.isArray(value)) {
        throw new AssertionError("is not an array");
      }

      const elementAssertions: AssertionError[] = [];
      for (let i = 0; i < value.length; i++) {
        try {
          this.spec.assert(value[i]);
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
    }
  }
  export function Array<TSpec extends TypeSpec>(spec: TSpec): Array<TSpec> {
    return new ArraySpec(spec);
  }

  const OPTIONAL = Symbol("TypeSpec.Optional");
  export interface Optional<TSpec extends TypeSpec> extends TypeSpecOperators {
    readonly type: typeof OPTIONAL;
    readonly spec: TSpec;
    readonly [TYPE_OF]?: () => TypeOf<TSpec> | null | undefined;
    toString(): string;
    assert(value: unknown): asserts value is TypeOf<TSpec> | null | undefined;
  }
  class OptionalSpec<TSpec extends TypeSpec>
    extends BaseTypeSpec
    implements Optional<TSpec>
  {
    readonly type: typeof OPTIONAL = OPTIONAL;
    readonly spec: TSpec;

    constructor(spec: TSpec) {
      super();
      this.spec = spec;
    }

    toString() {
      return this.spec.toString() + "?";
    }

    assert(value: unknown) {
      if (value == null) {
        return;
      }
      this.spec.assert(value);
    }
  }
  export function Optional<TSpec extends TypeSpec>(
    spec: TSpec,
  ): Optional<TSpec> {
    return new OptionalSpec(spec);
  }

  const UNION = Symbol("TypeSpec.Union");
  export interface Union<TSpec extends TypeSpec[]> extends TypeSpecOperators {
    readonly type: typeof UNION;
    readonly spec: TSpec;
    readonly [TYPE_OF]?: () => TypeOfUnion<TSpec>;
    toString(): string;
    assert(value: unknown): asserts value is TypeOfUnion<TSpec>;
  }
  type TypeOfUnion<TSpec extends TypeSpec[]> = {
    [I in keyof TSpec]: TypeOf<TSpec[I]>;
  }[number];

  class UnionSpec<TSpec extends TypeSpec[]>
    extends BaseTypeSpec
    implements Union<TSpec>
  {
    readonly type: typeof UNION = UNION;
    readonly spec: TSpec;

    constructor(spec: TSpec) {
      super();
      this.spec = spec;

      // @ts-expect-error We're circumventing the type system a little bit here,
      // as an optimization.
      this.or = this.#or.bind(this);
    }

    toString() {
      return this.spec.map((s) => s.toString()).join(" | ");
    }

    assert(value: unknown) {
      const variantAssertions: AssertionError[] = [];
      for (const variant of this.spec) {
        try {
          variant.assert(value);
          return;
        } catch (e) {
          variantAssertions.push(e as AssertionError);
        }
      }
      throw new AssertionError("is neither variant", {
        cause: variantAssertions,
      });
    }

    #or<TOtherSpec extends TypeSpec>(
      spec: TOtherSpec,
    ): Union<[...TSpec, TOtherSpec]> {
      return new UnionSpec([...this.spec, spec]);
    }
  }

  const RECORD = Symbol("TypeSpec.Record");
  export interface Record<TSpec extends { readonly [field: string]: TypeSpec }>
    extends TypeSpecOperators {
    readonly type: typeof RECORD;
    readonly spec: TSpec;
    readonly [TYPE_OF]?: () => TypeOfRecord<TSpec>;
    toString(): string;
    assert(value: unknown): asserts value is TypeOfRecord<TSpec>;
  }
  type TypeOfRecord<TSpec extends { readonly [field: string]: TypeSpec }> = {
    readonly [P in keyof TSpec as TSpec[P] extends Optional<any>
      ? never
      : P]: TypeOf<TSpec[P]>;
  } & {
    readonly [P in keyof TSpec as TSpec[P] extends Optional<any>
      ? P
      : never]?: TypeOf<TSpec[P]>;
  };
  class RecordSpec<TSpec extends { readonly [field: string]: TypeSpec }>
    extends BaseTypeSpec
    implements Record<TSpec>
  {
    readonly type: typeof RECORD = RECORD;
    readonly spec: TSpec;

    constructor(spec: TSpec) {
      super();
      this.spec = spec;
    }

    #indent(s: string) {
      return s
        .split("\n")
        .map((l) => "  " + l)
        .join("\n");
    }

    toString(): string {
      return (
        "{\n" +
        this.#indent(
          Object.entries(this.spec)
            .map(([name, spec]) => `${name}: ${spec.toString()}`)
            .join("\n"),
        ) +
        "\n}"
      );
    }

    assert(value: unknown) {
      if (value == null || typeof value !== "object") {
        throw new AssertionError("is not an object");
      }

      if (![null, Object.prototype].includes(Object.getPrototypeOf(value))) {
        throw new AssertionError("has a complex prototype chain");
      }

      const keys = Reflect.ownKeys(value);
      const requiredKeys = new Set(Object.keys(this.spec));
      const optionalKeys = new Set();
      for (const key of global.Array.from(requiredKeys)) {
        if (isOptional(this.spec[key])) {
          requiredKeys.delete(key);
          optionalKeys.add(key);
        }
      }

      const fieldAssertions: AssertionError[] = [];
      for (const key of keys) {
        try {
          if (typeof key === "symbol") {
            throw new AssertionError("is a symbol key");
          }

          if (!optionalKeys.has(key)) {
            if (!requiredKeys.has(key)) {
              throw new AssertionError("is not a defined key");
            }
            requiredKeys.delete(key);
          }

          this.spec[key].assert((value as any)[key]);
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
    }
  }
  export function Record<
    const TSpec extends { readonly [field: string]: TypeSpec },
  >(spec: TSpec): Record<TSpec> {
    return new RecordSpec(spec);
  }

  export function isRecord(
    spec: TypeSpec,
  ): spec is Record<{ readonly [field: string]: TypeSpec }> {
    return spec.type === RECORD;
  }

  export function isOptional(spec: TypeSpec): spec is Optional<TypeSpec> {
    return spec.type === OPTIONAL;
  }
}

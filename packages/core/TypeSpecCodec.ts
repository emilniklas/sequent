import { Codec, Deserializer, Serializer } from "./Codec.js";
import { TypeOf, TypeSpec } from "./TypeSpec.js";

export class TypeSpecCodec<TSpec extends TypeSpec>
  implements Codec<TypeOf<TSpec>>
{
  readonly #serializer: TypeSpecSerializer<TSpec>;
  readonly #deserializer: TypeSpecDeserializer<TSpec>;

  constructor({ spec, codec }: { spec: TSpec; codec: Codec<TypeOf<TSpec>> }) {
    this.#serializer = new TypeSpecSerializer<TSpec>({
      spec,
      serializer: codec,
    });
    this.#deserializer = new TypeSpecDeserializer<TSpec>({
      spec,
      deserializer: codec,
    });
  }

  serialize(element: TypeOf<TSpec>): Uint8Array {
    return this.#serializer.serialize(element);
  }

  deserialize(element: Uint8Array): TypeOf<TSpec> {
    return this.#deserializer.deserialize(element);
  }
}

export class TypeSpecSerializer<TSpec extends TypeSpec>
  implements Serializer<TypeOf<TSpec>>
{
  readonly #spec: TSpec;
  readonly #serializer: Serializer<TypeOf<TSpec>>;

  constructor(opts: { spec: TSpec; serializer: Serializer<TypeOf<TSpec>> }) {
    this.#spec = opts.spec;
    this.#serializer = opts.serializer;
  }

  serialize(element: TypeOf<TSpec>): Uint8Array {
    this.#spec.assert(element);
    return this.#serializer.serialize(element);
  }
}

export class TypeSpecDeserializer<TSpec extends TypeSpec>
  implements Deserializer<TypeOf<TSpec>>
{
  readonly #spec: TSpec;
  readonly #deserializer: Deserializer<TypeOf<TSpec>>;

  constructor(opts: {
    spec: TSpec;
    deserializer: Deserializer<TypeOf<TSpec>>;
  }) {
    this.#spec = opts.spec;
    this.#deserializer = opts.deserializer;
  }

  deserialize(element: Uint8Array): TypeOf<TSpec> {
    const model = this.#deserializer.deserialize(element);
    this.#spec.assert(model);
    return model;
  }
}

import { Codec, Deserializer, Serializer } from "./Codec.js";

type Reviver = (this: object, key: string, value: unknown) => unknown;
type Replacer = Reviver | (number | string)[] | null;

export class JSONCodec<TElement> implements Codec<TElement> {
  readonly #serializer: JSONSerializer<TElement>;
  readonly #deserializer: JSONDeserializer<TElement>;

  constructor(
    opts: {
      replacer?: Replacer;
      reviver?: Reviver;
      space?: string | number;
    } = {},
  ) {
    this.#serializer = new JSONSerializer(opts.replacer, opts.space);
    this.#deserializer = new JSONDeserializer(opts.reviver);
  }

  serialize(element: TElement): Uint8Array {
    return this.#serializer.serialize(element);
  }

  deserialize(element: Uint8Array): TElement {
    return this.#deserializer.deserialize(element);
  }
}

export class JSONSerializer<TElement> implements Serializer<TElement> {
  static readonly #ENCODER = new TextEncoder();

  readonly #replacer?: Replacer;
  readonly #space?: string | number;

  constructor(replacer?: Replacer, space?: string | number) {
    this.#replacer = replacer;
    this.#space = space;
  }

  serialize(element: TElement): Uint8Array {
    return JSONSerializer.#ENCODER.encode(
      JSON.stringify(
        element,
        // @ts-expect-error
        this.#replacer,
        this.#space,
      ),
    );
  }
}

export class JSONDeserializer<TElement> implements Deserializer<TElement> {
  static readonly #DECODER = new TextDecoder();

  readonly #reviver?: Reviver;

  constructor(reviver?: Reviver) {
    this.#reviver = reviver;
  }

  deserialize(element: Uint8Array): TElement {
    return JSON.parse(JSONDeserializer.#DECODER.decode(element), this.#reviver);
  }
}

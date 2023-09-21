import { Codec, Deserializer, Serializer } from "./Codec.js";

export class JSONCodec<TElement> implements Codec<TElement> {
  readonly #serializer: JSONSerializer<TElement>;
  readonly #deserializer: JSONDeserializer<TElement>;

  constructor(
    opts: {
      replacer?: Parameters<typeof JSON.stringify>[1];
      reviver?: Parameters<typeof JSON.parse>[1];
      space?: Parameters<typeof JSON.stringify>[2];
    } = {}
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

  readonly #replacer?: Parameters<typeof JSON.stringify>[1];
  readonly #space?: Parameters<typeof JSON.stringify>[2];

  constructor(
    replacer?: Parameters<typeof JSON.stringify>[1],
    space?: Parameters<typeof JSON.stringify>[2]
  ) {
    this.#replacer = replacer;
    this.#space = space;
  }

  serialize(element: TElement): Uint8Array {
    return JSONSerializer.#ENCODER.encode(
      JSON.stringify(element, this.#replacer, this.#space)
    );
  }
}

export class JSONDeserializer<TElement> implements Deserializer<TElement> {
  static readonly #DECODER = new TextDecoder();

  readonly #reviver?: Parameters<typeof JSON.parse>[1];

  constructor(reviver?: Parameters<typeof JSON.parse>[1]) {
    this.#reviver = reviver;
  }

  deserialize(element: Uint8Array): TElement {
    return JSON.parse(JSONDeserializer.#DECODER.decode(element), this.#reviver);
  }
}

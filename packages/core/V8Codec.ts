import { Codec, Deserializer, Serializer } from "./Codec.js";
// @ts-expect-error
import * as v8 from "node:v8";

export class V8Codec<TElement> implements Codec<TElement> {
  readonly #serializer = new V8Serializer<TElement>();
  readonly #deserializer = new V8Deserializer<TElement>();

  serialize(element: TElement): Uint8Array {
    return this.#serializer.serialize(element);
  }

  deserialize(element: Uint8Array): TElement {
    return this.#deserializer.deserialize(element);
  }
}

export class V8Serializer<TElement> implements Serializer<TElement> {
  serialize(element: TElement): Uint8Array {
    return v8.serialize(element);
  }
}

export class V8Deserializer<TElement> implements Deserializer<TElement> {
  deserialize(element: Uint8Array): TElement {
    return v8.deserialize(element);
  }
}

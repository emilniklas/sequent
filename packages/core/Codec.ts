export interface Codec<TElement>
  extends Serializer<TElement>,
    Deserializer<TElement> {}

export interface Serializer<TElement> {
  serialize(element: TElement): Uint8Array;
}

export interface Deserializer<TElement> {
  deserialize(element: Uint8Array): TElement;
}

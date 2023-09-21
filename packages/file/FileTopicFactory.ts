import { Codec, Topic, TopicFactory } from "@sequent/core";
import { FileTopic } from "./FileTopic.js";

export class FileTopicFactory implements TopicFactory {
  readonly #directory: string;
  readonly #codec: Codec<any>;

  constructor(directory: string, codec: Codec<any>) {
    this.#directory = directory;
    this.#codec = codec;
  }

  async make<TEvent>(name: string): Promise<Topic<TEvent>> {
    return FileTopic.create<TEvent>(this.#directory, name, this.#codec);
  }
}

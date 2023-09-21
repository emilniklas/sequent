import { TopicFactory } from "@sequent/core";
import { InMemoryTopic } from "./InMemoryTopic.js";

export class InMemoryTopicFactory implements TopicFactory {
  readonly #cache: Record<string, InMemoryTopic<any>> = {};

  async make<TEvent>(name: string) {
    return (this.#cache[name] ??= new InMemoryTopic<TEvent>(name));
  }
}

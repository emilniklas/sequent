import { Topic } from "./Topic.js";

export interface TopicFactory {
  make<TEvent>(name: string): Promise<Topic<TEvent>>;
}

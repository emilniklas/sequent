import { Consumer, ConsumerGroup } from "./Consumer.js";
import { Producer } from "./Producer.js";

export interface Topic<TEvent> {
  readonly name: string;
  producer(): Producer<TEvent>;
  consumer(group: ConsumerGroup): Consumer<TEvent>;
}

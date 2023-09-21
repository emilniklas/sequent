export interface Producer<TEvent> {
  produce(event: TEvent): Promise<void>;
}

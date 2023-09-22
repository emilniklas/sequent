export interface Producer<TEvent> extends AsyncDisposable {
  produce(event: TEvent): Promise<void>;
}

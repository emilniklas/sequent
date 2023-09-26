export interface Producer<TEvent> extends AsyncDisposable {
  produce(event: TEvent, key: Uint8Array | null): Promise<void>;
}

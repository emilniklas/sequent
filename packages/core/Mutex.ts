export interface Mutex<T> {
  get(): Promise<MutexGuard<T>>;
}

export interface MutexGuard<T> extends AsyncDisposable {
  state: T;
}

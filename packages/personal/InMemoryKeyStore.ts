import { KeyStore } from "./KeyStore.js";
import { PrivateKey } from "./PrivateKey.js";
import { PublicKey } from "./PublicKey.js";

export class InMemoryKeyStore<TId> implements KeyStore<TId> {
  readonly #store = new Map<TId, PrivateKey>();

  async create(id: TId): Promise<PrivateKey> {
    const pkey = await PrivateKey.generate();
    this.#store.set(id, pkey);
    return pkey;
  }

  async getPrivate(id: TId): Promise<PrivateKey | undefined> {
    return this.#store.get(id);
  }

  async getPublic(id: TId): Promise<PublicKey | undefined> {
    return this.#store.get(id)?.publicKey;
  }

  async delete(id: TId): Promise<void> {
    this.#store.delete(id);
  }
}

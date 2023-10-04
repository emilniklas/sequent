import { PrivateKey } from "./PrivateKey.js";
import { PublicKey } from "./PublicKey.js";

export interface KeyStore<TId> {
  create(id: TId): Promise<PrivateKey>;
  getPrivate(id: TId): Promise<PrivateKey | undefined>;
  getPublic(id: TId): Promise<PublicKey | undefined>;
  delete(id: TId): Promise<void>;
}

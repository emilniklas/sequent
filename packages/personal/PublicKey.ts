import {
  KeyObject,
  createCipheriv,
  publicEncrypt,
  randomBytes,
  scryptSync,
} from "node:crypto";

export class PublicKey {
  readonly #key: KeyObject;

  constructor(key: KeyObject) {
    this.#key = key;
  }

  encrypt(data: Uint8Array): Uint8Array {
    const encryptionKey = randomBytes(64);

    const password = encryptionKey.subarray(0, 32);
    const salt = encryptionKey.subarray(32, 48);
    const iv = encryptionKey.subarray(48, 64);

    const key = scryptSync(password, salt, 32);

    const cipher = createCipheriv("aes-256-cbc", key, iv);

    return Buffer.concat([
      publicEncrypt(this.#key, encryptionKey),
      cipher.update(data),
      cipher.final(),
    ]);
  }
}

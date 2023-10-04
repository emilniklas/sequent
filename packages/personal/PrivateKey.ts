import {
  createDecipheriv,
  generateKeyPair as generateKeyPairCb,
  KeyObject,
  privateDecrypt,
  scryptSync,
} from "node:crypto";
import { promisify } from "node:util";
import { PublicKey } from "./PublicKey.js";

const generateKeyPair = promisify(generateKeyPairCb);

export class PrivateKey {
  readonly #privateKey: KeyObject;
  readonly #publicKey: KeyObject;

  constructor(privateKey: KeyObject, publicKey: KeyObject) {
    this.#privateKey = privateKey;
    this.#publicKey = publicKey;
  }

  get publicKey() {
    return new PublicKey(this.#publicKey);
  }

  static async generate(): Promise<PrivateKey> {
    const { publicKey, privateKey } = await generateKeyPair("rsa", {
      modulusLength: 4096,
    });
    return new PrivateKey(privateKey, publicKey);
  }

  decrypt(data: Uint8Array): Uint8Array {
    const decryptedEncryptionKey = privateDecrypt(
      this.#privateKey,
      data.subarray(0, 512),
    );
    const password = decryptedEncryptionKey.subarray(0, 32);
    const salt = decryptedEncryptionKey.subarray(32, 48);
    const iv = decryptedEncryptionKey.subarray(48, 64);
    data = data.subarray(512);
    const key = scryptSync(password, salt, 32);

    const decipher = createDecipheriv("aes-256-cbc", key, iv);

    return Buffer.concat([decipher.update(data), decipher.final()]);
  }
}

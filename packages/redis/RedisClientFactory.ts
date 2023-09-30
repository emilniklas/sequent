import {
  Casing,
  Codec,
  JSONCodec,
  ReadModelClientFactory,
} from "@sequent/core";
import { Redis, RedisKey } from "ioredis";

export class RedisClientFactory
  implements ReadModelClientFactory<RedisClient<any>>
{
  readonly #client: Redis;
  readonly #codec: Codec<any>;
  readonly #method: RedisNamespacingMethod;
  readonly namingConvention = Casing.PascalCase;

  constructor(
    client: Redis,
    opts: { codec?: Codec<any>; method?: RedisNamespacingMethod } = {},
  ) {
    this.#client = client;
    this.#codec = opts.codec ?? new JSONCodec();
    this.#method = opts.method ?? RedisNamespacingMethod.Prefix;
  }

  async make<TModel>(namespace: string[]): Promise<RedisClient<TModel>> {
    switch (this.#method) {
      case RedisNamespacingMethod.Prefix:
        return new PrefixNamespacedRedisClient({
          client: this.#client,
          codec: this.#codec,
          namespace: namespace.join(":"),
        });

      case RedisNamespacingMethod.Hash:
        return new HashNamespacedRedisClient({
          client: this.#client,
          codec: this.#codec,
          namespace: namespace.join(":"),
        });
    }
  }
}

export interface RedisClient<TModel> {
  readonly namespace: string;
  get(key: RedisKey): Promise<TModel | undefined>;
  set(key: RedisKey, value: TModel): Promise<void>;
  exists(key: RedisKey): Promise<boolean>;
  delete(key: RedisKey): Promise<RedisDeleteResult>;
}

export enum RedisDeleteResult {
  NoOp = "NOOP",
  Deleted = "DELETED",
}

export enum RedisNamespacingMethod {
  Prefix = "PREFIX",
  Hash = "HASH",
}

export class PrefixNamespacedRedisClient<TModel>
  implements RedisClient<TModel>
{
  readonly #client: Redis;
  readonly #codec: Codec<TModel>;
  readonly namespace: string;

  constructor(opts: {
    client: Redis;
    codec: Codec<TModel>;
    namespace: string;
  }) {
    this.#client = opts.client;
    this.#codec = opts.codec;
    this.namespace = opts.namespace;
  }

  #key(key: RedisKey): Buffer {
    return Buffer.concat([Buffer.from(this.namespace + ":"), Buffer.from(key)]);
  }

  async get(key: RedisKey): Promise<TModel | undefined> {
    const value = await this.#client.getBuffer(this.#key(key));
    if (value == null) {
      return undefined;
    }

    return this.#codec.deserialize(value);
  }

  async exists(key: RedisKey): Promise<boolean> {
    return (await this.#client.exists(this.#key(key))) === 1;
  }

  async set(key: RedisKey, value: TModel) {
    await this.#client.set(
      this.#key(key),
      Buffer.from(this.#codec.serialize(value)),
    );
  }

  async delete(key: RedisKey): Promise<RedisDeleteResult> {
    const res = await this.#client.del(this.#key(key));
    if (res === 1) {
      return RedisDeleteResult.Deleted;
    }
    return RedisDeleteResult.NoOp;
  }
}

export class HashNamespacedRedisClient<TModel> implements RedisClient<TModel> {
  readonly #client: Redis;
  readonly #codec: Codec<TModel>;
  readonly namespace: string;

  constructor(opts: {
    client: Redis;
    codec: Codec<TModel>;
    namespace: string;
  }) {
    this.#client = opts.client;
    this.#codec = opts.codec;
    this.namespace = opts.namespace;
  }

  async get(key: RedisKey): Promise<TModel | undefined> {
    const value = await this.#client.hgetBuffer(this.namespace, key);
    if (value == null) {
      return undefined;
    }

    return this.#codec.deserialize(value);
  }

  async exists(key: RedisKey): Promise<boolean> {
    return (await this.#client.hexists(this.namespace, key)) === 1;
  }

  async set(key: RedisKey, value: TModel) {
    await this.#client.hset(
      this.namespace,
      key,
      Buffer.from(this.#codec.serialize(value)),
    );
  }

  async delete(key: RedisKey): Promise<RedisDeleteResult> {
    const res = await this.#client.hdel(this.namespace, key);
    if (res === 1) {
      return RedisDeleteResult.Deleted;
    }
    return RedisDeleteResult.NoOp;
  }
}

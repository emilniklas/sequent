import { Casing, Codec, ReadModelClientFactory } from "@sequent/core";
import { mkdirSync } from "node:fs";
import { writeFile, readdir, readFile } from "node:fs/promises";
import * as path from "node:path";

export interface FileDatabaseEntry<TModel> {
  readonly id: string;
  readonly model: TModel;
}

export class FileDatabase<TModel> {
  readonly #directory: string;
  readonly #codec: Codec<TModel>;

  constructor(directory: string, codec: Codec<TModel>) {
    this.#directory = directory;
    this.#codec = codec;
  }

  async insert(id: string, model: TModel) {
    await writeFile(
      path.join(this.#directory, id),
      this.#codec.serialize(model),
    );
  }

  async add(model: TModel) {
    await this.insert(crypto.randomUUID(), model);
  }

  async *all(): AsyncIterableIterator<FileDatabaseEntry<TModel>> {
    const files = await readdir(this.#directory);
    for (const filename of files) {
      const entry = await this.find(filename);
      if (entry) {
        yield entry;
      }
    }
  }

  find(id: string): Promise<FileDatabaseEntry<TModel> | undefined> {
    return this.#read(path.join(this.#directory, id));
  }

  async #read(
    filename: string,
  ): Promise<FileDatabaseEntry<TModel> | undefined> {
    try {
      const model = this.#codec.deserialize(await readFile(filename));
      return { model, id: path.basename(filename) };
    } catch {
      return undefined;
    }
  }
}

export class FileDatabaseClientFactory
  implements ReadModelClientFactory<FileDatabase<any>>
{
  readonly namingConvention = Casing.kebabcase;
  readonly #directory: string;
  readonly #codec: Codec<any>;

  constructor(directory: string, codec: Codec<any>) {
    this.#directory = directory;
    this.#codec = codec;
  }

  async make<TModel>(namespace: string[]): Promise<FileDatabase<TModel>> {
    const dirpath = path.join(this.#directory, ...namespace);
    mkdirSync(dirpath, { recursive: true });
    return new FileDatabase(dirpath, this.#codec);
  }
}

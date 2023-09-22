import { Casing, ReadModelClientFactory } from "@sequent/core";
import { Collection, CollectionOptions, Db, Document } from "mongodb";

export class CollectionFactory
  implements ReadModelClientFactory<Collection<any>>
{
  readonly #db: Db;
  readonly #options?: CollectionOptions;
  readonly namingConvention = Casing.camelCase;

  constructor(db: Db, options?: CollectionOptions) {
    this.#db = db;
    this.#options = options;
  }

  async make<TModel extends Document>(
    namespace: string
  ): Promise<Collection<TModel>> {
    return this.#db.collection<TModel>(namespace, this.#options);
  }
}

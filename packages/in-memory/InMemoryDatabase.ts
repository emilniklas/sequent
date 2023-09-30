import { Casing, ReadModelClientFactory } from "@sequent/core";

export interface InMemoryDatabaseEntry<TModel> {
  readonly id: string;
  readonly model: TModel;
}

export class InMemoryDatabase<TModel> {
  readonly namespace: string;
  readonly #modelsById = new Map<string, InMemoryDatabaseEntry<TModel>>();

  constructor(namespace: string) {
    this.namespace = namespace;
  }

  insert(id: string, model: TModel) {
    this.#modelsById.set(id, { id, model });
  }

  add(model: TModel) {
    this.insert(crypto.randomUUID(), model);
  }

  all(): Iterable<InMemoryDatabaseEntry<TModel>> {
    return this.#modelsById.values();
  }

  find(id: string): InMemoryDatabaseEntry<TModel> | undefined {
    return this.#modelsById.get(id);
  }
}

export class InMemoryDatabaseReadModelClientFactory
  implements ReadModelClientFactory<InMemoryDatabase<any>>
{
  readonly namingConvention = Casing.camelCase;
  readonly #cache: Record<string, InMemoryDatabase<any>> = {};

  async make<TModel>(namespace: string[]): Promise<InMemoryDatabase<TModel>> {
    const name = namespace.join("/");
    return (this.#cache[name] ??= new InMemoryDatabase(name));
  }
}

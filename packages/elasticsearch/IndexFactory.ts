import { Casing, ReadModelClientFactory } from "@sequent/core";
import { Client } from "@elastic/elasticsearch";
import * as T from "@elastic/elasticsearch/lib/api/types.js";

export class IndexFactory
  implements ReadModelClientFactory<ElasticSearchIndex<any>>
{
  readonly #client: Client;
  readonly namingConvention = Casing.kebabcase;

  constructor(client: Client) {
    this.#client = client;
  }

  async make<TModel>(namespace: string): Promise<ElasticSearchIndex<TModel>> {
    return new ElasticSearchIndex(this.#client, namespace);
  }

  async onCatchUp(client: ElasticSearchIndex<any>) {
    await client.refresh();
  }
}

export class ElasticSearchIndex<TModel> {
  readonly #client: Client;
  readonly #indexName: string;

  constructor(client: Client, indexName: string) {
    this.#client = client;
    this.#indexName = indexName;
  }

  refresh(params?: Omit<T.IndicesRefreshRequest, "index">) {
    return this.#client.indices.refresh({ ...params, index: this.#indexName });
  }

  index(params: Omit<T.IndexRequest<TModel>, "index">) {
    return this.#client.index<TModel>({
      ...params,
      index: this.#indexName,
    });
  }

  search<TAggregations = Record<T.AggregateName, T.AggregationsAggregate>>(
    params: Omit<T.SearchRequest, "index">,
  ) {
    return this.#client.search<TModel, TAggregations>({
      ...params,
      index: this.#indexName,
    });
  }
}

import "@sequent/core";
import { describe, it } from "node:test";
import assert from "node:assert";
import { setImmediate } from "node:timers/promises";
import { InMemoryTopicFactory } from "./InMemoryTopicFactory.js";
import {
  InMemoryDatabase,
  InMemoryDatabaseReadModelClientFactory,
} from "./InMemoryDatabase.js";
import { ConsumerGroup, EventType, ReadModel, TypeSpec } from "@sequent/core";

describe("@sequent/in-memory", () => {
  it("implements topic factory", async () => {
    const factory = new InMemoryTopicFactory();

    const topic = await factory.make<number>("test");
    assert.equal(topic.name, "test");

    const producer = await topic.producer();
    const consumer = await topic.consumer(ConsumerGroup.anonymous());

    await producer.produce(4);

    let sum = 0;
    (async () => {
      while (true) {
        const envelope = await consumer.consume();
        sum += envelope.event;
        envelope[Symbol.asyncDispose]();
      }
    })();

    await producer.produce(6);

    await setImmediate();

    assert.equal(sum, 10);
  });

  it("works with the full model", async () => {
    const topicFactory = new InMemoryTopicFactory();
    const clientFactory = new InMemoryDatabaseReadModelClientFactory();

    const EntityCreated = EventType.new(
      "EntityCreated",
      TypeSpec.Record({
        name: TypeSpec.String,
      }),
    );

    interface InMemoryEntity {
      name: string;
    }

    const rootInMemoryEntity =
      ReadModel.new<InMemoryDatabase<InMemoryEntity>>("InMemoryEntity");

    const producer = await EntityCreated.producer(topicFactory);

    await producer.produce({ name: "First" });
    await producer.produce({ name: "Second" });

    {
      const InMemoryEntity = rootInMemoryEntity.on(
        EntityCreated,
        (event, db) => {
          db.add({ name: event.message.name });
        },
      );
      const client = await InMemoryEntity.start(topicFactory, clientFactory);

      await setImmediate();

      assert.deepEqual(
        Array.from(client.all(), (e) => e.model),
        [{ name: "First" }, { name: "Second" }],
      );
    }

    await producer.produce({ name: "Third" });

    {
      const InMemoryEntity = rootInMemoryEntity.on(
        EntityCreated,
        (event, db) => {
          db.add({ name: event.message.name + "!" });
        },
      );
      const client = await InMemoryEntity.start(topicFactory, clientFactory);

      await setImmediate();

      assert.deepEqual(
        Array.from(client.all(), (e) => e.model),
        [{ name: "First!" }, { name: "Second!" }, { name: "Third!" }],
      );
    }
  });

  it("works with migrations", async () => {
    const topicFactory = new InMemoryTopicFactory();

    const Event1 = EventType.new(
      "Event",
      TypeSpec.Record({
        a: TypeSpec.String,
      }),
    );

    {
      const producer = await Event1.producer(topicFactory);

      await producer.produce({
        a: "example1",
      });
    }

    const Event2 = Event1.addFields({
      b: {
        type: TypeSpec.Number,
        migrate(event) {
          return event.a.length;
        },
      },
    });

    {
      const producer = await Event2.producer(topicFactory);

      await producer.produce({
        a: "example2",
        b: 1000,
      });
    }

    const consumer = await Event2.consumer(
      topicFactory,
      ConsumerGroup.anonymous(),
    );

    const events: EventType.TypeOf<typeof Event2>[] = [];
    for (let i = 0; i < 2; i++) {
      const envelope = (await consumer.consume())!;
      events.push(envelope.event.message);
      await envelope[Symbol.asyncDispose]();
    }

    assert.deepEqual(events, [
      { a: "example1", b: "example2".length },
      { a: "example2", b: 1000 },
    ]);
  });
});

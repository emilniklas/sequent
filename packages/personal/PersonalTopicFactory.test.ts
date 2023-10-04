import { describe, it } from "node:test";
import { PersonalTopicFactory } from "./PersonalTopicFactory.js";
import { InMemoryTopicFactory } from "@sequent/in-memory";
import { InMemoryKeyStore } from "./InMemoryKeyStore.js";
import { ConsumerGroup } from "@sequent/core";
import assert from "node:assert";

describe("PersonalTopicFactory", async () => {
  it("handles encryption of event messages", async () => {
    const keyStore = new InMemoryKeyStore<string>();

    interface UserEmailChanged {
      readonly id: string;
      readonly email: string;
    }

    const topicFactory = new PersonalTopicFactory<string>({
      topicFactory: new InMemoryTopicFactory(),
      keyStore,
      idExtractor: (e) => [(e as UserEmailChanged).id],
    });

    const userEmailChangedTopic =
      await topicFactory.make<UserEmailChanged>("UserEmailChanged");

    const producer = await userEmailChangedTopic.producer();
    const consumer = await userEmailChangedTopic.consumer(
      ConsumerGroup.anonymous(),
    );

    const userId = "1";

    const event: UserEmailChanged = {
      id: userId,
      email: "jane@doe.com",
    };

    await producer.produce(event, null);

    await keyStore.create(userId);

    // Should succeed since the key has been created.
    await producer.produce(event, null);

    const envelope = await consumer.consume();
    assert(envelope != null);

    assert.deepEqual(envelope.event, event);
  });

  it("supports dependency on multiple personal entities", async () => {
    const keyStore = new InMemoryKeyStore<string>();

    interface UsersConnected {
      readonly user1Id: string;
      readonly user2Id: string;
      readonly personalMessage: string;
    }

    const topicFactory = new PersonalTopicFactory<string>({
      topicFactory: new InMemoryTopicFactory(),
      keyStore,
      idExtractor: (e) => [
        (e as UsersConnected).user1Id,
        (e as UsersConnected).user2Id,
      ],
    });

    const usersConnectedTopic =
      await topicFactory.make<UsersConnected>("UsersConnected");

    const producer = await usersConnectedTopic.producer();
    const consumer = await usersConnectedTopic.consumer(
      ConsumerGroup.anonymous(),
    );

    const janeId = "jane";
    const johnId = "john";
    const joeId = "joe";

    await keyStore.create(janeId);
    await keyStore.create(johnId);
    await keyStore.create(joeId);

    await producer.produce(
      {
        user1Id: janeId,
        user2Id: johnId,
        // Very personal!
        personalMessage:
          "I want to connect with you, and my home address is XYZ",
      },
      null,
    );
    await producer.produce(
      {
        user1Id: johnId,
        user2Id: joeId,
        personalMessage: "I want you to be part of my professional network",
      },
      null,
    );
    const lastEvent: UsersConnected = {
      user1Id: joeId,
      user2Id: janeId,
      personalMessage: "Hey do you know John?",
    };
    await producer.produce(lastEvent, null);

    // As John is deleted from the system, both messages of which he is part
    // becomes unreadable, because they are dependent on his private key.
    await keyStore.delete(johnId);

    const envelope = await consumer.consume();
    assert.deepEqual(envelope?.event, lastEvent);
  });
});

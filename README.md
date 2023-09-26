# Sequent

Sequent is a framework for event sourcing applications. It aims to simplify the
workflows usually involved with managing an application in this paradigm over time.

## Architecture

Traditionally, applications are written in a style where external requests penetrate
through layers of the application, with most or all valid requests has a common
database as its target. Whether it is a read or write operation, the system is
built around a persistent data model, usually highly impacted by the initial choice
of database.

```
               Router     Business     Data
                 │           │          │
 Request ───────►├──────────►├─────────►│ ┌────┐
                 │           │          │ │ DB │
Response ◄───────┤◄──────────┤◄─────────┤ └────┘
                 │           │          │

```

As requirements change, database migration patterns allow developers to change the
original data model. Retaining consistency in the data is a difficult and brittle
process, frequently resulting in problems, like:

* **Unoptimized Data Access:** As it is more cumbersome to change the underlying
  data model, it's tempting to settle for poorly optimized database querys and
  subsequent data transformation/filtering in the application layer.
* **Database Migration Race Conditions:** As a database migration runs, the
  outgoing version of the software is potentially inconsistent with the data model.
  Starting the incoming version before the migration has been applied makes _it_
  inconsistent with the data model instead. A rolling update in production may
  therefore malfunction due to either version not seeing its expected database
  schema.
* **Multi-Stage Rollout:** To prevent issues, smaller changes may be applied in
  consecutive deployments, where intermediate versions might include duplicate
  implementations. While it can be effective, this strategy is difficult if
  release iterations cannot be delivered quickly. And if something goes wrong, it's
  not unlikely that the database is in an even more tricky state to manage.

### Enter Event Sourcing and CQRS

As an alternative to the architecture reviewed above, Event Sourcing is based on
the notion that the current state of the database can be _materialized_ from the
full history of operations that could have changed it.

If we persist all of the side effects produced by the system, we can not only
recover the database state from it, but we can freely change _how_ that state is
derived, and also _what_ data model is used. Furthermore, we can derive multiple
data models (conventionally called "read models") from the same events.

```
                Router     Business     Data
                  │           │          │
        ─────────►├──────────►├─────────►│ ┌───────────┐      ┌─────────┐
  Query           │           │          │ │Read Models├┐◄────┤Ingestion│
        ◄─────────┤◄──────────┤◄─────────┤ └┬──────────┘│     └─────────┘
                  │           │          │  └───────────┘          ▲
------------------│-----------│----------│                         │
                  │           │          │ ┌─────────────┐         │
Command ─────────►├──────────►├─────────►│ │ Event Model ├─────────┘
                  │           │          │ └─────────────┘
                  │           │          │
```

This is an example of Command Query Responsibility Segregation – or CQRS for short –
and is considered a good method for building scalable architecture.

## Usage

Read models and events can be designed independently of each other. In fact, it is
recommended that you do _not_ attempt to make canonical models for entities in the
system, e.g. if we're making a todo list, then we might want to model a `Todo`.

Instead, you should be thinking about modeling this way:

* **Events are canonical, and database agnostic.** So, in the todo system, we can
  imagine events like `TodoRegistered` and `TodoCompleted`.
* **Read models are explicitly tied to a database implementation.** If we're using
  a MongoDB to store a representation of the todos, that representation should be
  tied to MongoDB. You may choose to call it a `MongoDBTodo` just to be explicit
  about it.

Here's how modeling looks with Sequent:

```typescript
import { EventType, TypeSpec } from "@sequent/core";

const TodoRegistered = EventType.new(
  "TodoRegistered",
  TypeSpec.Record({
    id: TypeSpec.String,
    title: TypeSpec.String,
  }),
);

const TodoCompleted = EventType.new(
  "TodoCompleted",
  TypeSpec.Record({
    id: TypeSpec.String,
  }),
);
```

Notice how the above declarations doesn't define a `Todo` type with a `completed`
property. This is not the responsibility of the event model, but of a read model.

```typescript
import { ReadModel } from "@sequent/core";
import { Collection } from "mongodb";

interface MongoDBTodo {
  id: string;
  title: string;
  completed: boolean;
}

const MongoDBTodo = ReadModel.new<Collection<MongoDBTodo>>("Todo");
```

Notice how we're using the native `Collection` type from the `mongodb` package.
This just so happens to be how MongoDB is used with Sequent, but it's trivial
to add whatever database you want.

At this point, we have designed the _shape_ of the `MongoDBTodo`, but not where
the data comes from. The read model can be used like this, but it will always be
empty, since we haven't defined how events are translated into state. To do that,
we use the fluent API exposed by the `ReadModel`.

```typescript
// ...

const MongoDBTodo = ReadModel.new<Collection<MongoDBTodo>>("Todo")
  .on(TodoRegistered, async (event, col) => {
    await col.insertOne({
      id: event.message.id,
      title: event.message.title,
      completed: false,
    });
  })
  .on(TodoCompleted, async (event, col) => {
    await col.updateOne(
      { id: event.message.id },
      { $set: { completed: true } },
    );
  });
```

The `col` passed in to the ingestor functions have the type
`Collection<MongoDBTodo>` as declared in the call to `ReadModel.new`. This tells
Sequent that this read model ingests events into this type and nothing else.

Finally, we need to choose our log database implementation, and bootstrap the
application. Let's quickly review two interfaces declared by the Sequent core
package:

* `TopicFactory` – implementations of this interface know how to provision
  persistent event log topics, which store all the events emitted by the system.
  Implementations include `InMemoryTopicFactory`, `FileTopicFactory`, and
  `KafkaTopicFactory`.
* `ReadModelClientFactory<TClient>` – implementations know how to provision
  individual read model storage spaces (e.g. tables), as well as produce data
  access objects of the `TClient`.

To bootstrap our application, we choose the `KafkaTopicFactory` as the
`TopicFactory` implementation, and the MongoDB specific `CollectionFactory`,
as an implementation of `ReadModelClientFactory` that produces `Collection`s.

First, we need to initialize the individual clients to the two different databases.

```typescript
import { Kafka } from "kafkajs";
import { MongoClient } from "mongodb";

const kafka = new Kafka({
  brokers: ["127.0.0.1:9092"],
});

const mongo = new MongoClient(
  "mongodb://127.0.0.1:27017",
);
```

Next, we wrap them in the Sequent object that implement the interfaces
reviewed above.

```typescript
import { KafkaTopicFactory } from "@sequent/kafka";
import { CollectionFactory } from "@sequent/mongodb";

const topicFactory = new KafkaTopicFactory(kafka);
const clientFactory = new CollectionFactory(mongo.db());
```

Finally, we turn `EventType`s into `Producer`s that can emit events, and
`ReadModel`s are used to acquire the data access clients. We can do this in
parallel.

```typescript
const [
  todoRegisteredProducer,
  todoCompletedProducer,
  todosCollection,
] = await Promise.all([
  TodoRegistered.producer(topicFactory),
  TodoCompleted.producer(topicFactory),
  MongoDBTodo.start(topicFactory, clientFactory),
]);
```

Now, we can create new todos like this:

```typescript
await todoRegisteredProducer.produce({
  id: crypto.randomUUID(),
  title: "Get Milk",
});
```

As soon as this message is sent, the `TodoRegistered` ingestor for the
`MongoDBTodo` read model will immediately ingest the event, creating the todo
in the database. We can then query it using the collection acquired above:

```typescript
const todo = await todosCollection.findOne({ title: "Get Milk" });
```

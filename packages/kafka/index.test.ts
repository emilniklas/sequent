import "@sequent/core";
import * as kafka from "kafkajs";
import { after, before, describe, it } from "node:test";
import assert from "node:assert";
import { ChildProcess, spawn } from "node:child_process";
import { KafkaTopicFactory } from "./KafkaTopicFactory.js";
import { ConsumerGroup } from "@sequent/core";
import getPort from "get-port";
import { constants } from "node:os";

describe("@sequent/kafka", () => {
  let subprocess!: ChildProcess;
  let onExit!: Promise<unknown>;
  let factory!: KafkaTopicFactory;
  before(async () => {
    const port = await getPort({
      port: [9092, ...Array.from(new Array(30), (_, i) => 9094 + i)],
    });
    onExit = new Promise<unknown>((onExit) => {
      subprocess = spawn("docker", [
        "run",
        "--rm",
        `-p${port}:${port}`,
        "redpandadata/redpanda",
        "redpanda",
        "start",
        "--mode",
        "dev-container",
        "--overprovisioned",
        "--default-log-level=error",
        "--kafka-addr",
        `0.0.0.0:${port}`,
      ]).once("exit", onExit);
    });
    factory = new KafkaTopicFactory(
      new kafka.Kafka({
        brokers: [`127.0.0.1:${port}`],
        logLevel: kafka.logLevel.NOTHING,
      }),
    );
  });

  after(async () => {
    await factory[Symbol.asyncDispose]();
    subprocess.kill(constants.signals.SIGTERM);
    await onExit;
  });

  it("implements topic factory", async () => {
    const topic = await factory.make<number>("test");
    assert.equal(topic.name, "test");

    const producer = await topic.producer();
    const consumer = await topic.consumer(ConsumerGroup.anonymous());

    await producer.produce(4, null);

    let sum = 0;
    const consuming = (async () => {
      while (true) {
        const envelope = (await consumer.consume())!;
        sum += envelope.event;
        envelope[Symbol.asyncDispose]();

        if (sum === 10) {
          return;
        }
      }
    })();

    await producer.produce(6, null);

    await consuming;

    assert.equal(sum, 10);
  });
});

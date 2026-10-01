const { Kafka } = require("kafkajs");
const { ORDER_CREATED_TOPIC } = require("../../../../shared/kafka/topics");
const { getBrokers } = require("../../../../shared/kafka/config");
const {
  INVENTORY_CONSUMER_GROUP,
  getPartitionsConsumedConcurrently,
  shouldReadFromBeginning,
} = require("../../../../shared/kafka/consumer-groups");
const {
  getConsumerOptions,
  registerRebalanceLogging,
} = require("../../../../shared/kafka/rebalancing");

const kafka = new Kafka({
  clientId: "inventory-service",
  brokers: getBrokers(),
});

const consumer = kafka.consumer(
  getConsumerOptions(
    INVENTORY_CONSUMER_GROUP,
    process.env.KAFKA_INVENTORY_GROUP_INSTANCE_ID,
  ),
);

async function startConsumer() {

  await consumer.connect();

  console.log(`Kafka consumer connected as ${INVENTORY_CONSUMER_GROUP}`);

  registerRebalanceLogging(consumer, "Inventory");

  await consumer.subscribe({
    topic: ORDER_CREATED_TOPIC,
    // This only applies when the consumer group has no committed offset.
    fromBeginning: shouldReadFromBeginning(),
  });

  consumer.on(consumer.events.COMMIT_OFFSETS, (event) => {
    console.log("Kafka offsets committed", event.payload);
  });

  await consumer.run({
    // Commit only after inventory processing succeeds. A failure leaves the
    // offset uncommitted, so Kafka redelivers the event to this group.
    autoCommit: false,
    eachBatchAutoResolve: false,
    partitionsConsumedConcurrently: getPartitionsConsumedConcurrently(),

    eachBatch: async ({
      batch,
      heartbeat,
      isRunning,
      isStale,
      resolveOffset,
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) {
          break;
        }

        const order = JSON.parse(message.value.toString());

        console.log(
          `Received ${batch.topic} event with key ${message.key?.toString() ?? "<none>"} from partition ${batch.partition} at offset ${message.offset} in group ${INVENTORY_CONSUMER_GROUP}`,
        );

        console.log(order);

        // Update inventory here. Throw if the update fails: its offset will
        // not be committed and Kafka will deliver it again.

        resolveOffset(message.offset);
        await consumer.commitOffsets([
          {
            topic: batch.topic,
            partition: batch.partition,
            // Kafka commits the next offset to read, not the one just read.
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);

        await heartbeat();
      }
    },

  });

  return async function stopConsumer() {
    await consumer.stop();
    await consumer.disconnect();
  };
}

module.exports = {
  startConsumer,
};

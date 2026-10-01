const { Kafka } = require("kafkajs");
const { ORDER_CREATED_TOPIC } = require("../../../../shared/kafka/topics");
const { getBrokers } = require("../../../../shared/kafka/config");
const {
  getConsumerOptions,
  registerRebalanceLogging,
} = require("../../../../shared/kafka/rebalancing");

const ANALYTICS_CONSUMER_GROUP =
  process.env.KAFKA_ANALYTICS_CONSUMER_GROUP || "analytics-service-group";

const kafka = new Kafka({
  clientId: "analytics-service",
  brokers: getBrokers(),
});

const consumer = kafka.consumer(
  getConsumerOptions(
    ANALYTICS_CONSUMER_GROUP,
    process.env.KAFKA_ANALYTICS_GROUP_INSTANCE_ID,
  ),
);

async function startConsumer() {
  await consumer.connect();

  console.log(`Kafka analytics consumer connected as ${ANALYTICS_CONSUMER_GROUP}`);

  registerRebalanceLogging(consumer, "Analytics");

  await consumer.subscribe({
    topic: ORDER_CREATED_TOPIC,
    fromBeginning: process.env.KAFKA_ANALYTICS_READ_FROM_BEGINNING === "true",
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());

      console.log("Analytics observed order.created", {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        order,
      });

      // Record analytics metrics here. This consumer group has independent
      // offsets and does not affect inventory-service processing.
    },
  });

  return async function stopConsumer() {
    await consumer.stop();
    await consumer.disconnect();
  };
}

module.exports = { startConsumer };

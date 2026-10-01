const { Kafka, ConfigResourceTypes } = require("kafkajs");
const {
  ensureTopics,
  ORDER_CREATED_TOPIC,
  ORDER_LATEST_BY_PRODUCT_TOPIC,
} = require("../../../../shared/kafka/topics");
const { getBrokers } = require("../../../../shared/kafka/config");
const {
  getOrderCreatedMessageKey,
} = require("../../../../shared/kafka/message-keys");

const kafka = new Kafka({
  clientId: "order-service",
  brokers: getBrokers(),
});

const producer = kafka.producer();

async function connectProducer() {
  await ensureTopics(kafka, ConfigResourceTypes);
  await producer.connect();

  console.log("Kafka producer connected");
}

async function publishOrderCreated(order) {
  const key = getOrderCreatedMessageKey(order);
  const message = {
    // Kafka hashes this stable key to select a partition. Every event for one
    // product therefore stays ordered within that partition.
    key,
    value: JSON.stringify(order),
  };

  await producer.sendBatch({
    topicMessages: [
      { topic: ORDER_CREATED_TOPIC, messages: [message] },
      // Consumers that need only the latest order per product can rebuild from
      // this compacted projection instead of replaying the full event stream.
      { topic: ORDER_LATEST_BY_PRODUCT_TOPIC, messages: [message] },
    ],
  });

  console.log("Order event and compacted latest-order projection published");
}

module.exports = {
  connectProducer,
  publishOrderCreated,
};

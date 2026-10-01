const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function connectProducer() {
  await producer.connect();

  console.log("Kafka producer connected");
}

async function publishOrderCreated(order) {
  await producer.send({
    topic: "order.created",
    messages: [
      {
        key: order.id.toString(),
        value: JSON.stringify(order),
      },
    ],
  });

  console.log("order.created event published");
}

module.exports = {
  connectProducer,
  publishOrderCreated,
};
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "inventory-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "inventory-service-group",
});

async function startConsumer() {

  await consumer.connect();

  console.log("Kafka consumer connected");

  await consumer.subscribe({
    topic: "order.created",
    fromBeginning: false,
  });

  await consumer.run({

    eachMessage: async ({ topic, partition, message }) => {

      const order = JSON.parse(message.value.toString());

      console.log("Received order.created event");

      console.log(order);

      // Update inventory here
    },

  });
}

module.exports = {
  startConsumer,
};
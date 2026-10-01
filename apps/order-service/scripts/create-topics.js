const { Kafka, ConfigResourceTypes } = require("kafkajs");
const {
  ensureTopics,
} = require("../../../shared/kafka/topics");
const { getBrokers } = require("../../../shared/kafka/config");

async function main() {
  const kafka = new Kafka({
    clientId: "order-service-topic-admin",
    brokers: getBrokers(),
  });

  const definitions = await ensureTopics(kafka, ConfigResourceTypes);

  definitions.forEach(({ topic, numPartitions, replicationFactor, configEntries }) => {
    const retentionMs = configEntries.find(
      ({ name }) => name === "retention.ms",
    ).value;

    console.log(
      `Topic ${topic} is ready with at least ${numPartitions} partition(s), replication factor ${replicationFactor}, and ${retentionMs}ms retention.`,
    );
  });
}

main().catch((error) => {
  console.error("Unable to prepare Kafka topics", error);
  process.exitCode = 1;
});

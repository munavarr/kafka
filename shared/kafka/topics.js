const ORDER_CREATED_TOPIC = "order.created";
const ORDER_LATEST_BY_PRODUCT_TOPIC = "order.latest-by-product";
const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_SEGMENT_MS = 24 * 60 * 60 * 1000;
const DEFAULT_SEGMENT_BYTES = 128 * 1024 * 1024;

function positiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);

  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function getRetentionMs() {
  if (process.env.KAFKA_ORDER_CREATED_RETENTION_MS === undefined) {
    return DEFAULT_RETENTION_MS;
  }

  const parsed = Number(process.env.KAFKA_ORDER_CREATED_RETENTION_MS);

  // Kafka uses -1 to retain records indefinitely.
  return Number.isSafeInteger(parsed) && (parsed === -1 || parsed >= 0)
    ? parsed
    : DEFAULT_RETENTION_MS;
}

function getSegmentMs() {
  return positiveInteger(
    process.env.KAFKA_ORDER_CREATED_SEGMENT_MS,
    DEFAULT_SEGMENT_MS,
  );
}

function getSegmentBytes() {
  return positiveInteger(
    process.env.KAFKA_ORDER_CREATED_SEGMENT_BYTES,
    DEFAULT_SEGMENT_BYTES,
  );
}

function getTopicDefinitions() {
  const numPartitions = positiveInteger(
    process.env.KAFKA_ORDER_CREATED_PARTITIONS,
    3,
  );
  const replicationFactor = positiveInteger(
    process.env.KAFKA_REPLICATION_FACTOR,
    3,
  );

  return [
    {
      topic: ORDER_CREATED_TOPIC,
      numPartitions,
      replicationFactor,
      configEntries: [
        { name: "cleanup.policy", value: "delete" },
        { name: "retention.ms", value: String(getRetentionMs()) },
        { name: "segment.ms", value: String(getSegmentMs()) },
        { name: "segment.bytes", value: String(getSegmentBytes()) },
      ],
    },
    {
      // A derived, current-state projection. Compaction keeps only the most
      // recent record for each product key after log cleaning.
      topic: ORDER_LATEST_BY_PRODUCT_TOPIC,
      numPartitions,
      replicationFactor,
      configEntries: [
        { name: "cleanup.policy", value: "compact" },
        { name: "min.cleanable.dirty.ratio", value: "0.01" },
        { name: "segment.ms", value: String(getSegmentMs()) },
        { name: "segment.bytes", value: String(getSegmentBytes()) },
      ],
    },
  ];
}

/**
 * Creates missing topics and increases partition counts when requested.
 * Kafka permits increasing partitions, but never decreasing them.
 */
async function ensureTopics(kafka, configResourceTypes) {
  const definitions = getTopicDefinitions();
  const admin = kafka.admin();

  await admin.connect();

  try {
    await admin.createTopics({
      topics: definitions,
      waitForLeaders: true,
    });

    const metadata = await admin.fetchTopicMetadata({
      topics: definitions.map(({ topic }) => topic),
    });

    const topicPartitions = definitions.flatMap(({ topic, numPartitions }) => {
      const currentPartitions = metadata.topics.find(
        (metadataTopic) => metadataTopic.name === topic,
      )?.partitions.length ?? 0;

      return numPartitions > currentPartitions
        ? [{ topic, count: numPartitions }]
        : [];
    });

    if (topicPartitions.length > 0) {
      await admin.createPartitions({ topicPartitions });
    }

    await admin.alterConfigs({
      resources: definitions.map(({ topic, configEntries }) => ({
        type: configResourceTypes.TOPIC,
        name: topic,
        configEntries,
      })),
    });

    return definitions;
  } finally {
    await admin.disconnect();
  }
}

module.exports = {
  ORDER_CREATED_TOPIC,
  ORDER_LATEST_BY_PRODUCT_TOPIC,
  DEFAULT_RETENTION_MS,
  DEFAULT_SEGMENT_MS,
  DEFAULT_SEGMENT_BYTES,
  ensureTopics,
  getRetentionMs,
  getSegmentMs,
  getSegmentBytes,
  getTopicDefinitions,
};

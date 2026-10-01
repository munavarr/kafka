const DEFAULT_BROKERS = [
  "localhost:9092",
  "localhost:9093",
  "localhost:9094",
];

function getBrokers() {
  if (!process.env.KAFKA_BROKERS) {
    return DEFAULT_BROKERS;
  }

  return process.env.KAFKA_BROKERS.split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
}

module.exports = { DEFAULT_BROKERS, getBrokers };

const INVENTORY_CONSUMER_GROUP =
  process.env.KAFKA_INVENTORY_CONSUMER_GROUP || "inventory-service-group";

function getPartitionsConsumedConcurrently() {
  const configured = Number.parseInt(
    process.env.KAFKA_PARTITIONS_CONSUMED_CONCURRENTLY,
    10,
  );

  return Number.isInteger(configured) && configured > 0 ? configured : 1;
}

function shouldReadFromBeginning() {
  return process.env.KAFKA_READ_FROM_BEGINNING === "true";
}

module.exports = {
  INVENTORY_CONSUMER_GROUP,
  getPartitionsConsumedConcurrently,
  shouldReadFromBeginning,
};

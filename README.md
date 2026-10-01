# Kafka topics and partitions

The order flow uses one explicit topic:

| Topic | Partitions | Message key | Consumer group |
| --- | ---: | --- | --- |
| `order.created` | 3 by default | `product:<productId>` | `inventory-service-group` |

The order service creates the key as `product:<productId>`. Kafka hashes this
stable key to choose a partition, which keeps events for the same product on
the same partition (and therefore in order) while different products can be
processed in parallel. The order endpoint rejects messages without a
`productId`, so `order.created` messages are never published without this key.

## Retention

`order.created` uses Kafka's `delete` cleanup policy and retains records for
seven days by default (`604800000` ms). The topic setup applies this setting to
new and existing topics. Change the retention window when creating or
reconciling topics:

```bash
KAFKA_ORDER_CREATED_RETENTION_MS=259200000 npm run kafka:topics
```

The example retains events for three days. Set the value to `-1` only when you
intend to retain the topic indefinitely and have planned for the required disk
capacity.

## Log segments

Kafka appends records to log segments. `order.created` rolls to a new segment
every 24 hours or at 128 MiB, whichever happens first. This closes segments
regularly so the seven-day retention policy can remove expired data promptly.

Override either limit when reconciling the topic:

```bash
KAFKA_ORDER_CREATED_SEGMENT_MS=3600000 \
KAFKA_ORDER_CREATED_SEGMENT_BYTES=67108864 \
npm run kafka:topics
```

The example rolls hourly or at 64 MiB. Smaller segments improve retention
precision and recovery parallelism, but increase metadata and file overhead.

## Compaction

`order.created` remains a delete-retained event stream: compacting it would
lose earlier orders for the same product. The order service also publishes each
event to `order.latest-by-product`, a compacted projection with the same
`product:<productId>` key. After Kafka log cleaning, this topic retains the
latest order record for each product key, making it suitable for rebuilding a
latest-order view without replaying every historical event.

The compacted topic rolls on the same time/size limits and uses a low
`min.cleanable.dirty.ratio` (`0.01`) so it becomes eligible for cleaning soon
after segments accumulate newer records. Compaction runs asynchronously, so
consumers can temporarily observe older records before Kafka finishes cleaning.

The Docker configuration starts a three-broker KRaft cluster. Every partition
has three replicas, and Kafka requires two in-sync replicas before accepting a
write. This keeps the topic available if one broker fails.

Start Kafka, then create the topic:

```bash
docker compose up -d
cd apps/order-service
npm run kafka:topics
```

Topic creation also runs when order-service starts. Override the defaults with:

```bash
KAFKA_ORDER_CREATED_PARTITIONS=6 KAFKA_REPLICATION_FACTOR=3 npm run kafka:topics
```

Partition counts can only increase. Replication factor is set when a topic is
created; changing it for an existing topic requires Kafka partition reassignment.
Services default to all three local broker endpoints (`9092`, `9093`, and
`9094`), or you can override them with `KAFKA_BROKERS`.

## Consumer groups

Inventory uses the `inventory-service-group` consumer group by default. Run
multiple inventory-service instances with that same group ID to divide the
three partitions between them; Kafka gives a partition to only one member of a
group at a time. Additional instances beyond the partition count remain idle.

```bash
KAFKA_INVENTORY_CONSUMER_GROUP=inventory-service-group node src/server.js
```

Use a different group ID only for an independent consumer that must receive the
entire `order.created` stream, such as analytics. Kafka stores offsets per
group, so a restarted inventory worker resumes from its group's last committed
offset. `KAFKA_PARTITIONS_CONSUMED_CONCURRENTLY` controls how many assigned
partitions one inventory process handles simultaneously (default: `1`).

The project includes two independent consumers:

| Service | Consumer group | Effect |
| --- | --- | --- |
| inventory-service | `inventory-service-group` | Updates inventory once per group |
| analytics-service | `analytics-service-group` | Observes every order for analytics |

Install the new service once, then start it in a separate terminal. Its group
has its own offsets, so consuming analytics events never advances inventory's
offsets.

```bash
cd apps/analytics-service
npm install
npm start
```

## Consumer rebalancing

Kafka automatically rebalances a group when a consumer starts, stops, or loses
its session. During a rebalance, partitions move between the remaining group
members; a partition is assigned to only one member at a time. Both consumer
services now log when rebalancing begins and the resulting assignments, making
partition ownership visible in their logs.

Each service stops its consumer on `SIGINT` and `SIGTERM`, so Kafka can reassign
its partitions immediately during a deployment. For stable, named replicas,
set a unique static member ID per replica to reduce rebalances caused by brief
restarts:

```bash
KAFKA_INVENTORY_GROUP_INSTANCE_ID=inventory-1 npm start
```

Never reuse a static member ID for two live replicas. Analytics supports the
same pattern through `KAFKA_ANALYTICS_GROUP_INSTANCE_ID`.

## Offsets and delivery guarantees

Inventory disables KafkaJS auto-commit and commits an offset only after the
event's inventory update has succeeded. Kafka stores the *next* offset to read
for each topic partition and consumer group, so after processing offset `42`,
the service commits `43`. If processing fails or the service stops before that
commit, the event is delivered again when the group resumes. Inventory updates
should therefore be idempotent.

For a new group with no stored offsets, set `KAFKA_READ_FROM_BEGINNING=true` to
replay all retained events. Existing committed offsets always take precedence;
use a new group ID when you deliberately need a complete replay.

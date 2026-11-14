# CS4371 Group Project 1 — Person 1

This repository contains the Hacker News producer and the Kafka-to-Kibana path for named-entity totals. It includes 1,000 unique real Hacker News messages collected on November 13, 2026, in `outputs/collected_messages.jsonl`. No classifier or entity extraction application is included here.

## 1. Start Kafka and verify both listeners

From the project root, with Docker Desktop running, use Python 3.11 and Docker Compose v2.35 or newer. Allocate at least 6 GB of memory to Docker Desktop for the full stack. The images are pinned to Apache Kafka 4.1.2 (single-node KRaft) and Elasticsearch, Logstash and Kibana 8.19.24. All four base images were tested on native ARM64 on Apple Silicon. Elasticsearch runs without authentication for this local project; the published ports bind to loopback.

```sh
cp -n .env.example .env
set -a
. ./.env
set +a

docker compose config -q
docker compose up -d kafka kafka-init
docker compose logs kafka-init
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$KAFKA_DOCKER_BOOTSTRAP" --describe
```

Wait for both topics to appear if the topic initialization container is still running. Spark or Python on the host uses `127.0.0.1:9092` by default. Containers on the Compose network use `kafka:29092`. Host and Docker clients must use their respective listeners because Kafka returns different broker addresses to each. If you change the host port in `.env`, restart Kafka and update the Spark bootstrap server to match.

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install 'confluent-kafka==2.12.2'

.venv/bin/python - <<'PY'
import os
from confluent_kafka import Producer

default = f"{os.environ['KAFKA_ADVERTISED_HOST']}:{os.environ['KAFKA_HOST_PORT']}"
producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", default)})
metadata = producer.list_topics(timeout=10)
for name in (os.environ["KAFKA_TOPIC1"], os.environ["KAFKA_TOPIC2"]):
    assert name in metadata.topics and not metadata.topics[name].error, name
print("Host Kafka listener can see both topics.")
PY
```

## 2. Collect and publish real Hacker News messages

The producer reads the [official Hacker News API](https://github.com/HackerNews/API). It scans the latest range of item IDs on startup, then polls for newer IDs, revisiting a small overlap for items that were not yet visible. It accepts story titles and any available story body, plus comment text with HTML removed. It skips deleted or dead items, other item types, and empty messages. Each Kafka record has the HN ID as its key and uses the exact shared value schema:

```json
{"id":"12345","text":"Apple announces new AI products","source":"hackernews"}
```

To collect from scratch while keeping the included corpus intact, first create a separate topic. The following commands use `topic1-fresh` so they can also run on the existing broker:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$KAFKA_DOCKER_BOOTSTRAP" --create --if-not-exists \
  --topic topic1-fresh --partitions 1 --replication-factor 1
.venv/bin/python -u part1/producer.py --topic topic1-fresh \
  --output outputs/fresh_messages.jsonl --limit 1000
wc -l outputs/fresh_messages.jsonl
```

Run without `--limit` to continue polling until Ctrl-C:

```sh
.venv/bin/python -u part1/producer.py
```

On the original broker, the default output path resumes from the 1,000 saved IDs. The file is appended only after Kafka acknowledges delivery, and a file lock prevents two local producers from using the same output at once. Keep a single producer and its output paired with the same topic. The file and Kafka are separate writes: a hard crash after a Kafka acknowledgment but before the file is saved can cause that record to be sent again on restart. `--limit` is the total number of unique rows in the output, including any rows already there. A fresh broker does not automatically receive records from a preexisting JSONL file.

To seed a **fresh, empty** `topic1` with the included corpus instead of recollecting, run this once, before starting the continuous producer. It requires `jq` and preserves both the keys and the shared schema:

```sh
jq -r '.id + "\t" + tojson' "$COLLECTED_MESSAGES_PATH" |
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_DOCKER_BOOTSTRAP" --topic "$KAFKA_TOPIC1" \
    --property parse.key=true
```

Inspect the topic without changing any streaming application's offsets:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server "$KAFKA_DOCKER_BOOTSTRAP" --topic "$KAFKA_TOPIC1"

docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_DOCKER_BOOTSTRAP" --topic "$KAFKA_TOPIC1" \
  --from-beginning --max-messages 3 --timeout-ms 5000 --property print.key=true
```

See `part1/producer.py --help` (run with `.venv/bin/python`) and `.env.example` for connection, output, poll interval, timeout, and backfill settings.

## 3. Start Elasticsearch, Logstash and Kibana

```sh
docker compose up -d --build
docker compose ps -a
docker compose logs elasticsearch-init kibana-init
docker compose logs --tail 30 logstash

curl -fsS "http://127.0.0.1:$ELASTICSEARCH_HOST_PORT/_cluster/health"
curl -fsS "http://127.0.0.1:$KIBANA_HOST_PORT/api/status" | jq '.status.overall'
```

Wait for the init containers to exit with code 0 and for Logstash to report that its pipeline started. Compose builds Logstash with the pipeline config copied in, so Docker Desktop does not need access to the macOS Documents folder at runtime. Run `docker compose up -d --build logstash` after editing `part1/logstash.conf`.

Logstash reads the shared cumulative message format from `topic2`:

```json
{"entity":"Apple","count":15}
```

It validates the entity and integer count, then indexes the latest count under the exact entity string as the Elasticsearch document ID. One Kafka partition and one ordered Logstash worker preserve input order. A later total replaces the earlier total; counts are not added together. The initialized index maps `entity` as a keyword and `count` as a long.

## 4. Test the entity path without touching live data

With the stack running and `jq` installed, run these commands from the same shell. They use a separate topic, index, and consumer group. Use a new suffix if you repeat the test.

```sh
smoke_id=$(date +%s)
smoke_topic="cs4371-topic2-smoke-$smoke_id"
smoke_index="cs4371-entities-smoke-$smoke_id"
smoke_container="cs4371-logstash-smoke-$smoke_id"

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$KAFKA_DOCKER_BOOTSTRAP" --create --topic "$smoke_topic" \
  --partitions 1 --replication-factor 1
docker compose run --rm --no-deps -e ENTITY_INDEX="$smoke_index" elasticsearch-init
docker compose run -d --no-deps --name "$smoke_container" \
  -e KAFKA_TOPIC2="$smoke_topic" -e ENTITY_INDEX="$smoke_index" \
  -e LOGSTASH_KAFKA_GROUP="$smoke_container" \
  logstash logstash -f /usr/share/logstash/pipeline/entities.conf --path.data /tmp/smoke

printf '%s\n' '{"entity":"Test Apple","count":3}' '{"entity":"Test Apple","count":15}' \
  '{"entity":"Test NASA","count":7}' '{"entity":"Test Invalid","count":"999"}' |
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_DOCKER_BOOTSTRAP" --topic "$smoke_topic"
```

Once the temporary Logstash pipeline has started, this check should print `true`. The two Apple updates produce one document with count 15, and the invalid record is dropped.

```sh
docker logs --tail 20 "$smoke_container"
curl -fsS "http://127.0.0.1:$ELASTICSEARCH_HOST_PORT/$smoke_index/_search?size=10&sort=count:desc" |
  jq -e '[.hits.hits[] | [._id, ._source.count]] == [["Test Apple",15],["Test NASA",7]]'

docker rm -f "$smoke_container"
curl -fsS -X DELETE "http://127.0.0.1:$ELASTICSEARCH_HOST_PORT/$smoke_index"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$KAFKA_DOCKER_BOOTSTRAP" --delete --topic "$smoke_topic"
```

## 5. Open the dashboard and capture real results

```sh
open "http://127.0.0.1:$KIBANA_HOST_PORT/app/dashboards#/view/cs4371-entities"
```

The dashboard is installed by `kibana-init`. The panel reads the configured entity index, sorts current documents by count descending and entity ascending, and shows up to 10 bars. The chart always reads the latest cumulative snapshot, so the global time picker does not filter it. If you change the entity index, rerun `docker compose up -d --force-recreate kibana-init` to update the saved dashboard.

Once Person 2's application is available, connect it to the existing broker, read `topic1`, and write its real cumulative totals to `topic2`. If it should process the existing corpus, start it from the earliest offsets with a fresh Spark checkpoint. Confirm real totals are present:

```sh
curl -fsS "http://127.0.0.1:$ELASTICSEARCH_HOST_PORT/$ELASTICSEARCH_ENTITY_INDEX/_search?size=10&sort=count:desc,entity:asc" |
  jq '[.hits.hits[]._source | {entity, count}]'
```

When the first real bars appear, capture the dashboard with macOS Shift-Command-4. Refresh the dashboard and capture again after about 1 minute and 5 minutes of active processing. Save images under `outputs/entity_charts/` with timestamped names, for example `entities-YYYYMMDD-HHMMSS.png`. Keep the dashboard title and chart visible in each capture. The live index is currently empty because Person 2's stream is not available, so no real-result screenshots are included.

Use `docker compose down` to stop the stack while keeping Kafka and Elasticsearch data in their named volumes.

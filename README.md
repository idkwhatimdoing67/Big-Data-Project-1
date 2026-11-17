# CS4371 Group Project 1 — Person 1

This guide covers the Hacker News producer and the Kafka-to-Kibana path for named-entity totals. The included `outputs/collected_messages.jsonl` has 1,010 real Hacker News records with distinct IDs and 1,008 distinct cleaned texts. Person 3's classifier deduplicates by text and uses those 1,008 texts. See [README3.md](README3.md) for model training and classification.

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

On the original broker, the producer resumes from the IDs in the output file. It appends only after Kafka acknowledges delivery, and a file lock prevents two local producers from using the same output at once. Keep a single producer and its output paired with the same topic. A hard crash between the Kafka acknowledgment and the file write can cause that record to be sent again on restart. `--limit` counts distinct cleaned texts, including texts already in the file. The producer preserves distinct Hacker News IDs even when their texts are identical. A fresh broker does not automatically receive records from a preexisting JSONL file.

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

### Verify the corpus with the batch classifier

The Kafka reader in `part2/batch_classify.py` uses the Spark 3.5.3 Kafka connector. The commands below use the matching PySpark version and Java 11 on macOS. To test Kafka on a fresh broker, seed the empty topic with the included corpus as shown above.

```sh
.venv/bin/python -m pip install 'pyspark==3.5.3' 'pandas==2.2.3'
export JAVA_HOME="$(/usr/libexec/java_home -v 11)"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS='--master local[2] --driver-memory 3g --conf spark.sql.shuffle.partitions=8 pyspark-shell'

.venv/bin/python part2/train_model.py \
  --input_data part2/train.csv --model_path .venv/ag_news_model
.venv/bin/python part2/batch_classify.py \
  --input_file "$COLLECTED_MESSAGES_PATH" --model_path .venv/ag_news_model \
  --output_file .venv/batch-file/predictions.csv
```

For the Kafka run, use Maven Central to resolve the Kafka connector. Keep the Ivy settings path free of spaces.

```sh
ivy_dir=$(mktemp -d /tmp/cs4371-ivy.XXXXXX)
cat > "$ivy_dir/ivysettings.xml" <<'XML'
<ivysettings>
  <settings defaultResolver="central"/>
  <resolvers>
    <ibiblio name="central" root="https://repo.maven.apache.org/maven2/" m2compatible="true"/>
  </resolvers>
</ivysettings>
XML
export PYSPARK_SUBMIT_ARGS="${PYSPARK_SUBMIT_ARGS%pyspark-shell} --conf spark.jars.ivySettings=$ivy_dir/ivysettings.xml --conf spark.jars.ivy=$ivy_dir pyspark-shell"
.venv/bin/python part2/batch_classify.py \
  --source kafka --bootstrap_servers "${KAFKA_BOOTSTRAP_SERVERS:-${KAFKA_ADVERTISED_HOST}:${KAFKA_HOST_PORT}}" \
  --topic "$KAFKA_TOPIC1" --model_path .venv/ag_news_model \
  --output_file .venv/batch-kafka/predictions.csv
```

Each run should report 1,008 clean unique messages when using only the included corpus. It also creates a 50-message sample with a blank `human_label` column for manual evaluation.

The submission also includes a [stream prediction export](outputs/part2/stream_predictions.csv) with one row per available Kafka record, including distinct IDs with identical text. To regenerate it after the commands above, add the Kafka connector to Spark's launch options:

```sh
export PYSPARK_SUBMIT_ARGS="${PYSPARK_SUBMIT_ARGS%pyspark-shell} --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell"
.venv/bin/python -m scripts.export_stream_predictions \
  --model-path .venv/ag_news_model --output outputs/part2/stream_predictions.csv
```

To score the completed 50-message sample, run the evaluation script:

```sh
.venv/bin/python part2/evaluate_manual.py --csv_path outputs/part2/manual_50_sample.csv
```

## 3. Start Elasticsearch, Logstash and Kibana

```sh
docker compose up -d --build
docker compose ps -a
docker compose logs elasticsearch-init kibana-init
docker compose logs --tail 30 logstash

curl -fsS "http://127.0.0.1:$ELASTICSEARCH_HOST_PORT/_cluster/health"
curl -fsS "http://127.0.0.1:$KIBANA_HOST_PORT/api/status" | jq '.status.overall'
```

Wait for the init containers to exit with code 0 and for Logstash to report that its pipeline started. Compose builds Logstash with the pipeline config copied in, so Docker Desktop does not need access to the macOS Documents folder at runtime. Run `docker compose up -d --build logstash` after editing `part1/logstash.conf`. Elasticsearch uses a 512 MB heap for the local dataset. Set `ELASTICSEARCH_HEAP_SIZE` in `.env` if you give the stack more memory.

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

Set up Spark and the Kafka connector using the commands above. The combined app uses the same `JAVA_HOME`, `PYSPARK_PYTHON`, `SPARK_LOCAL_IP`, and `PYSPARK_SUBMIT_ARGS` as the stream prediction export. Install the English spaCy model, then run both streaming queries in one Spark application with the saved MLlib model:

```sh
.venv/bin/python -m pip install 'spacy==3.8.7' 'typer-slim==0.15.4' 'click==8.1.8' \
  'https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl'
.venv/bin/python -u -m part1.entity_stream \
  --model-path .venv/ag_news_model \
  --checkpoint checkpoints/entity_stream \
  --class-checkpoint checkpoints/class_stream
```

Both queries read `topic1`. The entity query publishes cumulative totals to `topic2` for Kibana, and the classifier prints a console table of cumulative counts by predicted class after each batch. Keep the producer running in another terminal to see the counts update as messages arrive. The entity-only launcher still works when `--model-path` is omitted.

With fresh checkpoints, both queries start at the earliest available offsets. Give each query its own checkpoint and reuse both directories on restart so Spark retains the cumulative counts and saved offsets. Use one entity stream per output topic. For captures, `--trigger '30 seconds' --max-offsets-per-trigger 125` spreads the saved corpus across multiple batches. To repeat the capture run from scratch, use a fresh checkpoint with a separate Kafka output topic and Elasticsearch index so previous totals stay out of the new chart.

Confirm that real totals are present:

```sh
curl -fsS "http://127.0.0.1:$ELASTICSEARCH_HOST_PORT/$ELASTICSEARCH_ENTITY_INDEX/_search?size=10&sort=count:desc,entity:asc" |
  jq '[.hits.hits[]._source | {entity, count}]'
```

Refresh Kibana at several elapsed times and capture the chart. The included submission captures were taken during a one-hour run at approximately 15, 30, 45, and 60 minutes. Open the captured charts at [15 minutes](outputs/entity_charts/timed-20261116-235143-15min.png), [30 minutes](outputs/entity_charts/timed-20261116-235143-30min.png), [45 minutes](outputs/entity_charts/timed-20261116-235143-45min.png), and [60 minutes](outputs/entity_charts/timed-20261116-235143-60min.png).

### Capture screenshots automatically on macOS

With Docker running, Google Chrome installed, and the Spark environment above loaded in your terminal, run:

```sh
.venv/bin/python -m pip install 'playwright==1.56.0'
caffeinate -i bash scripts/run_timed_entity_capture.sh
```

The script resumes the entity checkpoint, starts a live Hacker News feed, and saves actual Kibana PNG screenshots at 15, 30, 45, and 60 minutes. The filenames in `outputs/entity_charts/` contain the run timestamp and the interval. Keep the Mac powered on with its lid open and leave Docker running. Chrome runs in the background at each interval. Run only one timed capture at a time to avoid sharing the Spark checkpoint between processes.

Use `docker compose down` to stop the stack while keeping Kafka and Elasticsearch data in their named volumes.

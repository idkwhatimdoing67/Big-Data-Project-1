"""Run named-entity counts and optional live classification in one Spark app."""

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, struct, to_json, trim, udf
from pyspark.sql.types import ArrayType, StringType

# Exclude quantities, dates, times, and other numerical expressions.
NAME_TYPES = {
    "PERSON", "NORP", "FAC", "ORG", "GPE", "LOC", "PRODUCT",
    "EVENT", "WORK_OF_ART", "LAW", "LANGUAGE",
}
_nlp = None


def entity_mentions(text):
    global _nlp
    if not text:
        return []
    if _nlp is None:
        import spacy

        _nlp = spacy.load("en_core_web_sm", enable=["tok2vec", "ner"])
    names = []
    for entity in _nlp(text).ents:
        if entity.label_ in NAME_TYPES:
            name = " ".join(entity.text.split())
            if name and len(name.encode("utf-8")) <= 512:
                names.append(name)
    return names


def start_class_query(messages, model, checkpoint, trigger):
    from part2.batch_classify import add_class_names

    counts = (
        add_class_names(model.transform(messages))
        .groupBy("predicted_class").count()
        .orderBy("predicted_class")
    )
    return (
        counts.writeStream.format("console")
        .queryName("real_time_classification_counts")
        .option("checkpointLocation", str(checkpoint))
        .option("truncate", "false")
        .outputMode("complete")
        .trigger(processingTime=trigger)
        .start()
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    host = os.getenv("KAFKA_ADVERTISED_HOST", "127.0.0.1")
    port = os.getenv("KAFKA_HOST_PORT", "9092")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", f"{host}:{port}"))
    parser.add_argument("--input-topic", default=os.getenv("KAFKA_TOPIC1", "topic1"))
    parser.add_argument("--output-topic", default=os.getenv("KAFKA_TOPIC2", "topic2"))
    parser.add_argument("--checkpoint", type=Path, default=Path("checkpoints/entity_stream"))
    parser.add_argument("--model-path", type=Path, help="Saved Spark MLlib model for the live classifier.")
    parser.add_argument("--class-checkpoint", type=Path, default=Path("checkpoints/class_stream"))
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--max-offsets-per-trigger", type=int)
    args = parser.parse_args()
    if args.input_topic == args.output_topic:
        parser.error("The input and output Kafka topics must differ.")
    if args.max_offsets_per_trigger is not None and args.max_offsets_per_trigger < 1:
        parser.error("--max-offsets-per-trigger must be positive.")
    if args.model_path is not None:
        if not args.model_path.is_dir():
            parser.error(f"Saved model directory does not exist: {args.model_path}")
        entity_checkpoint = args.checkpoint.resolve()
        class_checkpoint = args.class_checkpoint.resolve()
        if (entity_checkpoint == class_checkpoint
                or entity_checkpoint in class_checkpoint.parents
                or class_checkpoint in entity_checkpoint.parents):
            parser.error("Use separate checkpoint directories for entities and classification.")

    # Check early so a missing model gives a driver error before polling Kafka.
    import spacy

    if not spacy.util.is_package("en_core_web_sm"):
        parser.error("Install the en_core_web_sm model as described in the README.")
    spark = SparkSession.builder.appName("HackerNewsStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    queries = []
    try:
        model = PipelineModel.load(str(args.model_path)) if args.model_path else None
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.input_topic)
            .option("startingOffsets", "earliest")
        )
        if args.max_offsets_per_trigger is not None:
            source = source.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)

        extract_names = udf(entity_mentions, ArrayType(StringType()))
        messages = (
            source.load()
            .select(from_json(col("value").cast("string"), "id STRING, text STRING, source STRING").alias("message"))
            .select("message.*")
            .where(col("id").isNotNull() & col("text").isNotNull() & (trim(col("text")) != ""))
        )
        counts = (
            messages.select(explode(extract_names(col("text"))).alias("entity"))
            .groupBy("entity").count()
        )
        records = counts.select(
            col("entity").alias("key"),
            to_json(struct("entity", "count")).alias("value"),
        )
        query = (
            records.writeStream.format("kafka")
            .queryName("hacker_news_entity_counts")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", args.output_topic)
            .option("checkpointLocation", str(args.checkpoint))
            .outputMode("complete")
            .trigger(processingTime=args.trigger)
            .start()
        )
        queries.append(query)
        print(f"Entity query started at {datetime.now(timezone.utc).isoformat()}", flush=True)
        if model is not None:
            class_query = start_class_query(messages, model, args.class_checkpoint, args.trigger)
            queries.append(class_query)
            print("Classification query started; cumulative class counts appear below.", flush=True)
        last_batch = -1
        while not spark.streams.awaitAnyTermination(2):
            progress = query.lastProgress
            if progress and progress["numInputRows"] and progress["batchId"] != last_batch:
                last_batch = progress["batchId"]
                states = progress["stateOperators"]
                entities = states[0]["numRowsTotal"] if states else 0
                print(f"Batch {last_batch}: {progress['numInputRows']} messages, {entities} entities", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            for running_query in reversed(queries):
                if running_query.isActive:
                    running_query.stop()
        finally:
            spark.stop()


if __name__ == "__main__":
    main()

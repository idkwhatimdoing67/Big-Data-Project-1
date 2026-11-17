"""Export a prediction for each Hacker News record available in Kafka."""

import argparse
import csv
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_max, col, from_json, trim

from part2.batch_classify import add_class_names


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    default_broker = f"{os.getenv('KAFKA_ADVERTISED_HOST', '127.0.0.1')}:{os.getenv('KAFKA_HOST_PORT', '9092')}"
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", default_broker))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC1", "topic1"))
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("ExportStreamPredictions").getOrCreate()
    query = None
    try:
        model = PipelineModel.load(str(args.model_path))
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 250)
            .load()
        )
        messages = (
            source.select(
                "partition", "offset",
                from_json(col("value").cast("string"), "id STRING, text STRING, source STRING").alias("message"),
            )
            .select("partition", "offset", "message.*")
            .filter(col("id").isNotNull() & col("text").isNotNull() & (trim(col("text")) != ""))
        )
        predictions = (
            add_class_names(model.transform(messages))
            .withColumn("top_probability", array_max(vector_to_array(col("probability"))))
            .select(
                "partition", "offset", "id", "text", "source",
                "predicted_class", "top_probability",
                col("probability").cast("string").alias("probabilities"),
            )
        )
        # Use a fresh checkpoint so the export includes the available Kafka history.
        # Publish the CSV only after the Spark query and the CSV write both succeed.
        with TemporaryDirectory(prefix=".stream-export-", dir=args.output.parent) as scratch_dir:
            scratch = Path(scratch_dir)
            parquet = str(scratch / "predictions")
            query = (
                predictions.writeStream.format("parquet")
                .option("path", parquet)
                .option("checkpointLocation", str(scratch / "checkpoint"))
                .trigger(availableNow=True)
                .start()
            )
            query.awaitTermination()
            result = spark.read.parquet(parquet).orderBy("partition", "offset")
            temporary_csv = scratch / "predictions.csv"
            with temporary_csv.open("w", encoding="utf-8", newline="") as output:
                writer = csv.writer(output)
                writer.writerow(result.columns)
                count = 0
                for row in result.toLocalIterator():
                    writer.writerow(row)
                    count += 1
            if not count:
                raise RuntimeError(f"No valid records were available on Kafka topic {args.topic!r}")
            temporary_csv.replace(args.output)
            print(f"Exported {count} Kafka predictions to {args.output}")
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()

import argparse
import os
import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_max, col, from_json, lit, rand, trim, when
from pyspark.sql.types import StringType, StructField, StructType


def add_class_names(df):
    return df.withColumn(
        "predicted_class",
        when(col("prediction") == 0.0, "World")
        .when(col("prediction") == 1.0, "Sports")
        .when(col("prediction") == 2.0, "Business")
        .when(col("prediction") == 3.0, "Sci/Tech")
        .otherwise("Unknown"),
    )


def main():
    parser = argparse.ArgumentParser(description="Batch Classification")
    parser.add_argument("--input_file", default="outputs/collected_messages.jsonl", help="Path to collected_messages.jsonl")
    parser.add_argument("--model_path", required=True, help="Path to saved model")
    parser.add_argument("--output_file", required=True, help="Path to save predictions.csv")
    parser.add_argument("--source", choices=["file", "kafka"], default="file", help="Read from saved file or Kafka topic1")
    parser.add_argument("--bootstrap_servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="topic1", help="Kafka topic name")
    args, _ = parser.parse_known_args()

    builder = SparkSession.builder.appName("Batch_Classification")
    if args.source == "kafka":
        builder = builder.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading model and data...")
    model = PipelineModel.load(args.model_path)

    if args.source == "kafka":
        schema = StructType([StructField("id", StringType(), True), StructField("text", StringType(), True)])
        raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        df = (
            raw.selectExpr("CAST(value AS STRING) AS json")
            .select(from_json(col("json"), schema).alias("m"))
            .select(col("m.id").alias("id"), col("m.text").alias("text"))
        )
    else:
        df = spark.read.json(args.input_file)

    if "id" not in df.columns:
        df = df.withColumn("id", lit(""))
    if "text" not in df.columns:
        df = df.withColumn("text", lit(""))

    # Slide 21: Drop empty text AND duplicate messages, then report remaining count
    df = (
        df.filter(col("text").isNotNull() & (trim(col("text")) != ""))
        .dropDuplicates(["text"])
    )

    clean_count = df.count()
    print(f"\n=== Clean Unique Messages Remaining: {clean_count} ===")
    if clean_count < 1000:
        print("WARNING: Count is under 1,000 messages. Append more messages to collected_messages.jsonl before final run.")

    print("Running batch predictions...")
    predictions = add_class_names(model.transform(df))

    output_df = (
        predictions.withColumn("prob_array", vector_to_array(col("probability")))
        .withColumn("top_probability", array_max(col("prob_array")))
        .select(
            col("id"),
            col("text"),
            col("prediction"),
            col("predicted_class"),
            col("top_probability"),
            col("probability").cast("string").alias("probabilities"),
        )
    )

    out_dir = os.path.dirname(os.path.abspath(args.output_file))
    os.makedirs(out_dir, exist_ok=True)

    # Save single clean CSV via Pandas so it doesn't create a Spark part-0000 folder
    output_df.toPandas().to_csv(args.output_file, index=False)
    print(f"Predictions saved to {args.output_file}")

    # Report and save total count per class (Slide 22)
    print("\n--- Predicted Count per Class ---")
    class_counts = output_df.groupBy("prediction", "predicted_class").count().orderBy("prediction")
    class_counts.show()
    class_counts.toPandas().to_csv(os.path.join(out_dir, "batch_class_counts.csv"), index=False)

    # Extract, display, and save 5 examples per class (Slide 22)
    print("--- 5 Examples per Class ---")
    examples_all = []
    for class_id, class_name in [(0.0, "World"), (1.0, "Sports"), (2.0, "Business"), (3.0, "Sci/Tech")]:
        print(f"\nClass {int(class_id)} ({class_name}) Examples:")
        subset = output_df.filter(col("prediction") == class_id).limit(5)
        for i, row in enumerate(subset.collect()):
            print(f"  {i+1}: [{row['top_probability']:.2f}] {row['text']}")
        examples_all.append(subset.toPandas())

    if examples_all:
        pd.concat(examples_all, ignore_index=True).to_csv(
            os.path.join(out_dir, "five_examples_per_class.csv"), index=False
        )

    # Export 50 random messages for manual labeling (Slide 22)
    manual_50 = (
        output_df.orderBy(rand(seed=42))
        .limit(50)
        .withColumn("human_label", lit(""))
        .select("id", "text", "predicted_class", "top_probability", "human_label")
    )
    manual_path = os.path.join(out_dir, "manual_50_sample.csv")
    manual_50.toPandas().to_csv(manual_path, index=False)
    print(f"\nSaved 50 random messages for manual labeling to {manual_path}")

    spark.stop()


if __name__ == "__main__":
    main()

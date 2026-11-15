import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel

def main():
    parser = argparse.ArgumentParser(description="Batch Classification")
    parser.add_argument("--input_file", required=True, help="Path to collected_messages.jsonl")
    parser.add_argument("--model_path", required=True, help="Path to saved model")
    parser.add_argument("--output_file", required=True, help="Path to save predictions.csv")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Batch_Classification") \
        .getOrCreate()

    print("Loading model and data...")
    model = PipelineModel.load(args.model_path)
    
    # Read UTF-8 JSON lines
    df = spark.read.json(args.input_file)

    # FIX: Ensure the text column exists and filter out nulls/empties
    if "text" not in df.columns:
        from pyspark.sql.functions import lit
        df = df.withColumn("text", lit(""))
    df = df.filter(col("text").isNotNull() & (col("text") != ""))

    print("Running batch predictions...")
    predictions = model.transform(df)

    # Save outputs to CSV (id, text, prediction, probabilities)
    output_df = predictions.select(
        col("id"), 
        col("text"), 
        col("prediction"), 
        col("probability").cast("string")  # Convert Vector to String for CSV
    )
    output_df.write.csv(args.output_file, header=True, mode="overwrite")
    print(f"Predictions saved to {args.output_file}")

    # Extract and display 5 examples per class
    print("--- 5 Examples per Class ---")
    for class_id in range(4):
        print(f"\nClass {class_id} Examples:")
        examples = predictions.filter(col("prediction") == class_id).select("text").limit(5).collect()
        for i, row in enumerate(examples):
            print(f"  {i+1}: {row['text']}")

    spark.stop()

if __name__ == "__main__":
    main()
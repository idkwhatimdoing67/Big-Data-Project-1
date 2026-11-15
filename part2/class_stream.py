import os
from pyspark.sql.functions import col, count
from pyspark.ml import PipelineModel

def process_classification_stream(spark, input_df, model_path, checkpoint_dir, output_csv_path):
    """
    Called by Person 2's run_streams.py.
    Takes a streaming DataFrame connected to topic1, applies the classification model, 
    and outputs running class counts.
    """
    
    # Load the same saved PipelineModel used for batch processing
    model = PipelineModel.load(model_path)
    
    # Transform the streaming DataFrame
    predictions = model.transform(input_df)
    
    # Calculate running counts by class
    class_counts = predictions.groupBy("prediction").agg(count("*").alias("running_count"))

    # We use a foreachBatch function to write out the updated counts to CSV safely 
    # since standard CSV streaming sinks do not support 'update' or 'complete' modes.
    def write_counts_to_csv(batch_df, batch_id):
        # Convert to Pandas for a clean, single-file CSV overwrite
        pandas_df = batch_df.toPandas()
        pandas_df.to_csv(output_csv_path, index=False)
        print(f"Batch {batch_id} class counts written to {output_csv_path}")

    # Configure the streaming query
    query = class_counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_counts_to_csv) \
        .option("checkpointLocation", checkpoint_dir) \
        .queryName("real_time_classification_counts") \
        .start()

    return query

if __name__ == "__main__":
    # Optional: Standalone testing wrapper for Person 3 
    # (Person 2 will import process_classification_stream directly)
    import argparse
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description="Test Classification Stream")
    parser.add_argument("--model_path", required=True, help="Path to saved model")
    parser.add_argument("--checkpoint_dir", required=True, help="Path to classification checkpoint")
    parser.add_argument("--output_csv", required=True, help="Path to save class_counts.csv")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Test_Class_Stream").getOrCreate()
    
    # Dummy socket stream just for local testing before handing off to Person 2
    test_stream = spark.readStream.format("rate").load()
    test_stream = test_stream.withColumn("text", col("value").cast("string"))

    q = process_classification_stream(spark, test_stream, args.model_path, args.checkpoint_dir, args.output_csv)
    q.awaitTermination()
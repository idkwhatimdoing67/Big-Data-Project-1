from pyspark.sql.functions import col, count, when
from pyspark.ml import PipelineModel

def process_classification_stream(spark, input_df, model_path, checkpoint_dir, output_csv_path):
    model = PipelineModel.load(model_path)
    predictions = model.transform(input_df).withColumn(
        "topic_label",
        when(col("prediction") == 0.0, "World")
        .when(col("prediction") == 1.0, "Sports")
        .when(col("prediction") == 2.0, "Business")
        .when(col("prediction") == 3.0, "Sci/Tech")
        .otherwise("Unknown"),
    )

    class_counts = predictions.groupBy("prediction", "topic_label").agg(count("*").alias("running_count"))

    def write_counts_to_csv(batch_df, batch_id):
        pandas_df = batch_df.toPandas()
        pandas_df.to_csv(output_csv_path, index=False)
        print(f"Batch {batch_id} class counts written to {output_csv_path}")

    query = (
        class_counts.writeStream.outputMode("complete")
        .foreachBatch(write_counts_to_csv)
        .option("checkpointLocation", checkpoint_dir)
        .queryName("real_time_classification_counts")
        .start()
    )
    return query

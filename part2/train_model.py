import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

def main():
    parser = argparse.ArgumentParser(description="Train MLlib Pipeline on AG News")
    parser.add_argument("--input_data", required=True, help="Path to AG News CSV")
    parser.add_argument("--model_path", required=True, help="Path to save the fitted model")
    args = parser.parse_args()

    _ = os.environ.get("DUMMY_CREDENTIAL", "default")

    spark = SparkSession.builder \
        .appName("AGNews_Model_Training") \
        .getOrCreate()

    # Load dataset
    df = spark.read.csv(args.input_data, header=True, inferSchema=True)
    
    # 1. Map 'Class Index' to 'label' and adjust 1-4 down to 0-3
    df = df.withColumn("label", col("Class Index") - 1)

    # 2. Combine Title and Description into the mandatory 'text' column
    df = df.withColumn("text", concat_ws(" - ", col("Title"), col("Description")))

    print("--- Class Distribution ---")
    df.groupBy("label").count().show()

    # Mandatory Stage 1: RegexTokenizer (must use input column 'text')
    tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")

    # Mandatory Stage 2: StopWordsRemover
    stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

    # Mandatory Stage 3: HashingTF
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features")

    # Mandatory Stage 4: IDF
    idf = IDF(inputCol="raw_features", outputCol="features")

    # Mandatory Stage 5: LogisticRegression
    logistic_regression = LogisticRegression(featuresCol="features", labelCol="label")

    # Construct the pipeline strictly in the requested order
    pipeline = Pipeline(stages=[
        tokenizer, 
        stopwords_remover, 
        hashing_tf, 
        idf, 
        logistic_regression
    ])

    print("Training pipeline model...")
    model = pipeline.fit(df)

    print(f"Saving model to {args.model_path}...")
    model.write().overwrite().save(args.model_path)
    print("Model saved successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
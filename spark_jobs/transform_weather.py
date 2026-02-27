import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")

INPUT_BUCKET = "weather-raw"
OUTPUT_BUCKET = "weather-processed"
INPUT_PATH = f"s3a://{INPUT_BUCKET}/berlin/"
OUTPUT_PATH = f"s3a://{OUTPUT_BUCKET}/berlin/"

def create_spark_session():
    return (
        SparkSession.builder
        .appName("WeatherTransform")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def transform():
    spark = create_spark_session()

    df = spark.read.option("multiline", "true").json(INPUT_PATH)

    df.write.mode("overwrite").parquet(OUTPUT_PATH)

    print(f"Written Parquet to {OUTPUT_PATH}")
    spark.stop()

if __name__ == "__main__":
    transform()

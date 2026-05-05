import os

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType


TABLE_PATH = os.getenv("DELTA_TABLE_PATH", "s3a://streamkernel-delta/enriched-tickets")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

TABLE_SCHEMA = StructType(
    [
        StructField("ticketId", StringType(), True),
        StructField("description", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("embedding", ArrayType(FloatType(), False), True),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("streamkernel-delta-prepare")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    empty_df = spark.createDataFrame([], TABLE_SCHEMA)
    empty_df.write.format("delta").mode("overwrite").save(TABLE_PATH)

    row_count = spark.read.format("delta").load(TABLE_PATH).count()
    print(f"Prepared Delta rows: {row_count}")
    spark.stop()


if __name__ == "__main__":
    main()

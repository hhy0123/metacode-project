import sys
import os
import json
import boto3
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_timestamp
from pyspark.sql.types import StructType

# Glue 환경이 아닐 때도 로컬 테스트 가능하게
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket', 'source'])
    s3_bucket = args['s3_bucket']
    source = args['source']
except:
    s3_bucket = os.getenv("S3_BUCKET", "propberg-lakehouse-hhy")
    source = sys.argv[1] if len(sys.argv) > 1 else "molit"

spark = SparkSession.builder \
    .appName(f"propberg-bronze-{source}") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{s3_bucket}/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

today = datetime.utcnow().strftime("%Y-%m-%d")

SOURCE_MAP = {
    "molit": {
        "s3_path": f"s3://{s3_bucket}/raw/molit/{today}/",
        "table": "glue_catalog.propberg_bronze.raw_transactions",
    },
    "rone": {
        "s3_path": f"s3://{s3_bucket}/raw/rone/{today}/",
        "table": "glue_catalog.propberg_bronze.raw_price_index",
    },
    "kosis": {
        "s3_path": f"s3://{s3_bucket}/raw/kosis/{today}/",
        "table": "glue_catalog.propberg_bronze.raw_population",
    },
}

config = SOURCE_MAP[source]

print(f"[bronze_job] {source} 시작 — {config['s3_path']}")

df = spark.read.json(config["s3_path"])
df = df.withColumn("ingested_date", lit(today))

count = df.count()
df.writeTo(config["table"]).append()

print(f"[bronze_job] {source} 완료 — {count}건 → {config['table']}")
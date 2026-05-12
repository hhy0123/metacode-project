import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket', 'operation', 'tables'])
    s3_bucket = args['s3_bucket']
    operation = args['operation']
    tables = args['tables'].split(',')
    older_than_days = int(args.get('older_than_days', 7))
except:
    s3_bucket = os.getenv("S3_BUCKET", "propberg-lakehouse-hhy")
    operation = sys.argv[1] if len(sys.argv) > 1 else "compaction"
    tables = [
        "propberg_bronze.raw_transactions",
        "propberg_silver.transactions_enriched",
        "propberg_gold.daily_trade_summary",
    ]
    older_than_days = 7

spark = SparkSession.builder \
    .appName(f"propberg-mgmt-{operation}") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{s3_bucket}/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

older_than_ts = (datetime.utcnow() - timedelta(days=older_than_days)).strftime("%Y-%m-%d %H:%M:%S")

for table in tables:
    full_table = f"glue_catalog.{table}"
    try:
        if operation == "compaction":
            spark.sql(f"""
                CALL glue_catalog.system.rewrite_data_files(
                    table => '{full_table}',
                    options => map(
                        'target-file-size-bytes', '134217728',
                        'min-file-size-bytes', '67108864'
                    )
                )
            """)
            print(f"[mgmt] compaction 완료: {full_table}")

        elif operation == "expire_snapshots":
            spark.sql(f"""
                CALL glue_catalog.system.expire_snapshots(
                    table => '{full_table}',
                    older_than => TIMESTAMP '{older_than_ts}',
                    retain_last => 3
                )
            """)
            print(f"[mgmt] expire_snapshots 완료: {full_table}")

        elif operation == "remove_orphans":
            spark.sql(f"""
                CALL glue_catalog.system.remove_orphan_files(
                    table => '{full_table}',
                    older_than => TIMESTAMP '{older_than_ts}'
                )
            """)
            print(f"[mgmt] remove_orphans 완료: {full_table}")

        elif operation == "rewrite_manifests":
            spark.sql(f"""
                CALL glue_catalog.system.rewrite_manifests(
                    table => '{full_table}'
                )
            """)
            print(f"[mgmt] rewrite_manifests 완료: {full_table}")

    except Exception as e:
        print(f"[mgmt] {operation} 실패 ({full_table}): {e}")

print(f"[mgmt] {operation} 전체 완료")
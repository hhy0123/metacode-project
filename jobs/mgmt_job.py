"""Iceberg 테이블 매니지먼트 — Compaction / Expire Snapshots / Remove Orphan Files.

매일 새벽 03:00 KST (Airflow `propberg_mgmt` DAG) 자동 실행.
스트리밍 인제스천으로 발생하는 소파일 누적 / 스냅샷 폭증을 자동으로 제어한다.

- Bronze: 1분 트리거로 인한 소파일 다량 발생 → 우선순위 가장 높음
- Silver: MERGE INTO 시 신규 파일 생성 → 두 번째 우선순위
- Gold: CTAS / overwrite 위주 → 세 번째
"""
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_bucket", "operation"])
    s3_bucket = args["s3_bucket"].strip()
    operation = args["operation"].strip()
    older_than_days = int(os.getenv("OLDER_THAN_DAYS", "7"))
except Exception:
    s3_bucket = os.getenv("S3_BUCKET", "propberg-lakehouse-hhy").strip()
    operation = (sys.argv[1] if len(sys.argv) > 1 else "compaction").strip()
    older_than_days = int(os.getenv("OLDER_THAN_DAYS", "7"))

# Bronze 우선 — 스트리밍 마이크로배치로 소파일이 가장 빠르게 누적되는 레이어
TABLES = [
    "propberg_bronze.raw_transactions",
    "propberg_bronze.raw_price_index",
    "propberg_bronze.raw_population",
    "propberg_silver.transactions_enriched",
    "propberg_silver.price_index_enriched",
    "propberg_silver.population_enriched",
    "propberg_gold.daily_trade_summary",
    "propberg_gold.monthly_price_trend",
    "propberg_gold.anomaly_transactions",
]

spark = (
    SparkSession.builder.appName(f"propberg-mgmt-{operation}")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{s3_bucket}/")
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .getOrCreate()
)

older_than_ts = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).strftime("%Y-%m-%d %H:%M:%S")

success = 0
failure = 0
for table in TABLES:
    full_table = f"glue_catalog.{table}"
    try:
        if operation == "compaction":
            spark.sql(
                f"""
                CALL glue_catalog.system.rewrite_data_files(
                    table => '{full_table}',
                    options => map(
                        'target-file-size-bytes', '134217728',
                        'min-file-size-bytes', '67108864',
                        'max-concurrent-file-group-rewrites', '4'
                    )
                )
                """
            )
            print(f"[mgmt] compaction OK: {full_table}")

        elif operation == "expire_snapshots":
            spark.sql(
                f"""
                CALL glue_catalog.system.expire_snapshots(
                    table => '{full_table}',
                    older_than => TIMESTAMP '{older_than_ts}',
                    retain_last => 3
                )
                """
            )
            print(f"[mgmt] expire_snapshots OK: {full_table}")

        elif operation == "remove_orphans":
            spark.sql(
                f"""
                CALL glue_catalog.system.remove_orphan_files(
                    table => '{full_table}',
                    older_than => TIMESTAMP '{older_than_ts}'
                )
                """
            )
            print(f"[mgmt] remove_orphans OK: {full_table}")

        elif operation == "rewrite_manifests":
            spark.sql(
                f"CALL glue_catalog.system.rewrite_manifests(table => '{full_table}')"
            )
            print(f"[mgmt] rewrite_manifests OK: {full_table}")

        else:
            raise ValueError(f"unknown operation: {operation}")

        success += 1
    except Exception as exc:
        failure += 1
        print(f"[mgmt] {operation} FAIL ({full_table}): {exc}")

print(f"[mgmt] {operation} 완료 — success={success} failure={failure}")
# 발표 환경에서는 빈 테이블이나 스냅샷 부족으로 일부 실패해도 Job 자체는 성공으로 처리.
# 운영 환경에선 failure > 0일 때 알람을 띄우도록 별도 모니터링 권장.

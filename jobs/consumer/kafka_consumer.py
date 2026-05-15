"""propberg Bronze 스트리밍 인제스천 (Kafka → Iceberg).

- 토픽별로 독립적인 Spark Structured Streaming 쿼리를 시작합니다.
- foreachBatch 로 Iceberg `propberg_bronze.*` 테이블에 직접 append.
- 토픽별 별도 checkpoint 경로 → exactly-once 보장 (Kafka offset + Iceberg snapshot).
- trigger 1분: tickberg와 동일한 마이크로배치 주기.
- Kafka partition/offset 컬럼을 보존해 리플레이/감사 가능.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
S3_BUCKET = os.getenv("S3_BUCKET", "propberg-lakehouse-hhy")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
TRIGGER_INTERVAL = os.getenv("STREAMING_TRIGGER_INTERVAL", "60 seconds")

TOPIC_CONFIG = {
    "molit-transactions": {
        "table": "glue_catalog.propberg_bronze.raw_transactions",
        "checkpoint": f"s3a://{S3_BUCKET}/checkpoints/bronze_molit/",
    },
    "rone-price-index": {
        "table": "glue_catalog.propberg_bronze.raw_price_index",
        "checkpoint": f"s3a://{S3_BUCKET}/checkpoints/bronze_rone/",
    },
    "kosis-population": {
        "table": "glue_catalog.propberg_bronze.raw_population",
        "checkpoint": f"s3a://{S3_BUCKET}/checkpoints/bronze_kosis/",
    },
}

# 토픽별 schema (Bronze는 STRING으로 받지만 타입 안정성을 위해 명시)
SCHEMAS: dict[str, StructType] = {
    "molit-transactions": StructType([
        StructField("sigungu_code", StringType()),
        StructField("year_month", StringType()),
        StructField("aptSeq", StringType()),
        StructField("aptNm", StringType()),
        StructField("dealAmount", StringType()),
        StructField("excluUseAr", StringType()),
        StructField("floor", StringType()),
        StructField("buildYear", StringType()),
        StructField("dealYear", StringType()),
        StructField("dealMonth", StringType()),
        StructField("dealDay", StringType()),
        StructField("umdNm", StringType()),
        StructField("roadNm", StringType()),
        StructField("slerGbn", StringType()),
        StructField("buyerGbn", StringType()),
        StructField("cdealType", StringType()),
        StructField("cdealDay", StringType()),
        StructField("ingested_at", StringType()),
    ]),
    "rone-price-index": StructType([
        StructField("STATBL_ID", StringType()),
        StructField("DTACYCLE_CD", StringType()),
        StructField("WRTTIME_IDTFR_ID", StringType()),
        StructField("CLS_ID", StringType()),
        StructField("CLS_NM", StringType()),
        StructField("CLS_FULLNM", StringType()),
        StructField("ITM_NM", StringType()),
        StructField("DTA_VAL", StringType()),
        StructField("UI_NM", StringType()),
        StructField("year_month", StringType()),
        StructField("ingested_at", StringType()),
    ]),
    "kosis-population": StructType([
        StructField("stat_name", StringType()),
        StructField("ORG_ID", StringType()),
        StructField("TBL_ID", StringType()),
        StructField("ITM_ID", StringType()),
        StructField("ITM_NM", StringType()),
        StructField("C1_NM", StringType()),
        StructField("UNIT_NM", StringType()),
        StructField("PRD_SE", StringType()),
        StructField("PRD_DE", StringType()),
        StructField("DT", StringType()),
        StructField("ingested_at", StringType()),
    ]),
}

# Bronze 테이블 컬럼 매핑 (한국어/대문자 → 표준 스네이크)
COLUMN_MAPPING: dict[str, dict[str, str]] = {
    "molit-transactions": {
        "aptNm": "apt_name",
        "dealAmount": "deal_amount",
        "excluUseAr": "area",
        "buildYear": "build_year",
        "dealYear": "deal_year",
        "dealMonth": "deal_month",
        "dealDay": "deal_day",
        "umdNm": "dong",
        "roadNm": "road_name",
        "slerGbn": "seller_gbn",
        "buyerGbn": "buyer_gbn",
        "cdealType": "cdeal_type",
        "cdealDay": "cdeal_day",
    },
    "rone-price-index": {
        "STATBL_ID": "statbl_id",
        "DTACYCLE_CD": "dtacycle_cd",
        "WRTTIME_IDTFR_ID": "wrttime_idtfr_id",
        "CLS_ID": "cls_id",
        "CLS_NM": "cls_nm",
        "CLS_FULLNM": "cls_fullnm",
        "ITM_NM": "itm_nm",
        "DTA_VAL": "dta_val",
        "UI_NM": "ui_nm",
    },
    "kosis-population": {
        "ORG_ID": "orgid",
        "TBL_ID": "tblid",
        "ITM_ID": "itm_id",
        "ITM_NM": "itm_nm",
        "C1_NM": "c1_nm",
        "UNIT_NM": "unit_nm",
        "PRD_SE": "prd_se",
        "PRD_DE": "prd_de",
        "DT": "dt",
    },
}

BRONZE_COLUMNS: dict[str, list[str]] = {
    "molit-transactions": [
        "deal_id", "sigungu_code", "year_month", "apt_name", "deal_amount",
        "area", "floor", "build_year", "deal_year", "deal_month", "deal_day",
        "dong", "road_name", "seller_gbn", "buyer_gbn", "cdeal_type",
        "cdeal_day", "raw_json", "ingested_at", "ingested_date",
        "kafka_topic", "kafka_partition", "kafka_offset",
    ],
    "rone-price-index": [
        "statbl_id", "dtacycle_cd", "wrttime_idtfr_id", "cls_id", "cls_nm",
        "cls_fullnm", "itm_nm", "dta_val", "ui_nm", "year_month",
        "ingested_at", "ingested_date",
        "kafka_topic", "kafka_partition", "kafka_offset",
    ],
    "kosis-population": [
        "stat_name", "orgid", "tblid", "itm_id", "itm_nm", "c1_nm",
        "unit_nm", "prd_se", "prd_de", "dt", "ingested_at", "ingested_date",
        "kafka_topic", "kafka_partition", "kafka_offset",
    ],
}


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("propberg-bronze-streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
            "org.apache.iceberg:iceberg-aws-bundle:1.4.2,"
            "software.amazon.awssdk:bundle:2.20.18",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{S3_BUCKET}/")
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.glue_catalog.client.region", AWS_REGION)
        .config("spark.sql.session.timeZone", "Asia/Seoul")
    )
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        builder = (
            builder.config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def make_batch_writer(topic: str, table: str):
    """foreachBatch 핸들러를 topic별로 생성 (late binding 회피)."""
    mapping = COLUMN_MAPPING[topic]
    columns = BRONZE_COLUMNS[topic]

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            print(f"[bronze-stream] topic={topic} batch={batch_id} empty")
            return

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        renamed = batch_df
        for src, dst in mapping.items():
            if src in renamed.columns:
                renamed = renamed.withColumnRenamed(src, dst)

        renamed = renamed.withColumn("ingested_date", F.lit(today))
        renamed = renamed.withColumn("kafka_topic", F.lit(topic))

        # molit-transactions: 국토부 API는 거래 고유 ID를 안 줘서 합성 키 생성
        if topic == "molit-transactions" and "aptSeq" in renamed.columns:
            renamed = renamed.withColumn(
                "deal_id",
                F.concat_ws(
                    "-",
                    F.col("aptSeq"),
                    F.col("deal_year"),
                    F.col("deal_month"),
                    F.col("deal_day"),
                    F.col("floor"),
                    F.col("deal_amount"),
                ),
            )

        if "raw_json" not in renamed.columns and topic == "molit-transactions":
            renamed = renamed.withColumn("raw_json", F.lit(None).cast("string"))

        for col_name in columns:
            if col_name not in renamed.columns:
                renamed = renamed.withColumn(col_name, F.lit(None).cast("string"))

        renamed = renamed.select(*columns)
        renamed.writeTo(table).append()

        count = renamed.count()
        print(f"[bronze-stream] topic={topic} batch={batch_id} rows={count} → {table}")

    return write_batch


def start_topic_stream(spark: SparkSession, topic: str, config: dict):
    schema = SCHEMAS[topic]

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "50000")
        .load()
    )

    parsed = (
        raw.select(
            F.col("partition").cast("int").alias("kafka_partition"),
            F.col("offset").cast("long").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), schema).alias("j"),
        )
        .select("kafka_partition", "kafka_offset", "j.*")
    )

    query = (
        parsed.writeStream.foreachBatch(make_batch_writer(topic, config["table"]))
        .option("checkpointLocation", config["checkpoint"])
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(f"bronze_{topic}")
        .start()
    )
    print(f"[bronze-stream] started topic={topic} → {config['table']} (trigger={TRIGGER_INTERVAL})")
    return query


def main() -> None:
    spark = build_spark()
    print(
        f"[bronze-stream] Kafka={KAFKA_BOOTSTRAP} S3={S3_BUCKET} "
        f"trigger={TRIGGER_INTERVAL} topics={list(TOPIC_CONFIG.keys())}"
    )
    for topic, cfg in TOPIC_CONFIG.items():
        start_topic_stream(spark, topic, cfg)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

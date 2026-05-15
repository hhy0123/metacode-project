"""propberg Silver Job — Bronze → Silver MERGE INTO.

Bronze는 스트리밍 consumer가 영어 컬럼명으로 이미 정규화해서 적재했음.
여기서는 타입 변환 + 시도 분류 + derived 컬럼만 만든 후 Iceberg MERGE INTO 수행.
"""
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    count,
    lit,
    regexp_replace,
    round as spark_round,
    to_timestamp,
    trim,
    unix_timestamp,
    when,
    window,
)
from pyspark.sql.functions import avg as spark_avg
from pyspark.sql.functions import max as spark_max

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "s3_bucket"])
    s3_bucket = args["s3_bucket"]
except Exception:
    s3_bucket = os.getenv("S3_BUCKET", "propberg-lakehouse-hhy")

spark = (
    SparkSession.builder.appName("propberg-silver")
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

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# 시도 코드 매핑 (sigungu_code 앞 2자리)
SIDO_MAP = {
    "11": "서울", "26": "부산", "27": "대구", "28": "인천",
    "29": "광주", "30": "대전", "31": "울산", "36": "세종",
    "41": "경기", "42": "강원", "43": "충북", "44": "충남",
    "45": "전북", "46": "전남", "47": "경북", "48": "경남", "50": "제주",
}


def _sido_expr(sigungu_code_col):
    expr = when(lit(False), "기타")
    for code, name in SIDO_MAP.items():
        expr = expr.when(sigungu_code_col.substr(1, 2) == code, name)
    return expr.otherwise("기타")


def process_transactions() -> None:
    print("[silver_job] transactions_enriched 처리 시작")

    df = spark.table("glue_catalog.propberg_bronze.raw_transactions")

    df_clean = (
        df.withColumn(
            "deal_amount_clean",
            regexp_replace(col("deal_amount"), ",", "").cast("bigint"),
        )
        .withColumn("area_clean", col("area").cast("double"))
        .withColumn("floor_clean", col("floor").cast("int"))
        .withColumn("build_year_clean", col("build_year").cast("int"))
        .withColumn("deal_year_clean", col("deal_year").cast("int"))
        .withColumn("deal_month_clean", col("deal_month").cast("int"))
        .withColumn("deal_day_clean", col("deal_day").cast("int"))
        .withColumn("sido", _sido_expr(col("sigungu_code")))
        .withColumn(
            "price_per_sqm",
            spark_round(col("deal_amount_clean") / col("area_clean"), 2),
        )
        .withColumn("building_age", lit(2026) - col("build_year_clean"))
        .withColumn(
            "deal_date",
            concat(
                col("deal_year_clean").cast("string"),
                lit("-"),
                col("deal_month_clean").cast("string"),
                lit("-"),
                col("deal_day_clean").cast("string"),
            ),
        )
        .withColumn("updated_at", lit(today))
        .select(
            col("deal_id"),
            col("sigungu_code"),
            col("sido"),
            # 시군구는 sigungu_code(법정동 코드 5자리) 자체를 dimension으로 사용.
            # 국토부 API XML에 시군구 한글명을 직접 주지 않아 일관성을 위해 코드 사용.
            # 한글명이 필요하면 별도 sigungu_code → name 매핑 테이블로 lookup하면 된다.
            col("sigungu_code").alias("sigungu"),
            trim(col("dong")).alias("dong"),
            trim(col("apt_name")).alias("apt_name"),
            col("deal_amount_clean").alias("deal_amount"),
            col("price_per_sqm"),
            col("area_clean").alias("area"),
            col("floor_clean").alias("floor"),
            col("build_year_clean").alias("build_year"),
            col("building_age"),
            col("deal_date"),
            col("deal_year_clean").alias("deal_year"),
            col("deal_month_clean").alias("deal_month"),
            trim(col("road_name")).alias("road_name"),
            col("seller_gbn"),
            col("buyer_gbn"),
            col("updated_at"),
            col("ingested_date"),
        )
        .where(col("deal_id").isNotNull())
    )

    df_clean.createOrReplaceTempView("transactions_staged")

    spark.sql(
        """
        MERGE INTO glue_catalog.propberg_silver.transactions_enriched AS target
        USING transactions_staged AS source
        ON target.deal_id = source.deal_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print("[silver_job] transactions_enriched 완료")


def process_price_index() -> None:
    print("[silver_job] price_index_enriched 처리 시작")

    df = spark.table("glue_catalog.propberg_bronze.raw_price_index")

    df_clean = (
        df.withColumn("price_index", col("dta_val").cast("double"))
        .withColumn("updated_at", lit(today))
        .select(
            col("wrttime_idtfr_id").alias("year_month"),
            col("cls_id").alias("region_code"),
            col("cls_nm").alias("region_name"),
            col("cls_fullnm").alias("region_full"),
            col("price_index"),
            col("updated_at"),
            col("ingested_date"),
        )
        .where(col("year_month").isNotNull())
    )

    df_clean.createOrReplaceTempView("price_index_staged")

    spark.sql(
        """
        MERGE INTO glue_catalog.propberg_silver.price_index_enriched AS target
        USING price_index_staged AS source
        ON target.year_month = source.year_month
        AND target.region_code = source.region_code
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print("[silver_job] price_index_enriched 완료")


def process_population() -> None:
    print("[silver_job] population_enriched 처리 시작")

    df = spark.table("glue_catalog.propberg_bronze.raw_population")

    df_clean = (
        df.withColumn("updated_at", lit(today))
        .select(
            col("c1_nm").alias("region_name"),
            col("prd_de").alias("prd_de"),
            lit(None).cast("bigint").alias("population"),
            col("dt").cast("bigint").alias("household"),
            col("updated_at"),
            col("ingested_date"),
        )
        .where(col("region_name").isNotNull() & col("prd_de").isNotNull())
    )

    df_clean.createOrReplaceTempView("population_staged")

    spark.sql(
        """
        MERGE INTO glue_catalog.propberg_silver.population_enriched AS target
        USING population_staged AS source
        ON target.region_name = source.region_name
        AND target.prd_de = source.prd_de
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print("[silver_job] population_enriched 완료")


def process_streaming_health() -> None:
    """Bronze의 ingested_at + dealDay를 기반으로 스트리밍 윈도우별 처리 통계를 derive해
    propberg_gold.streaming_health 테이블에 적재.

    record_count   : 1시간 윈도우당 적재 row 수
    max_kafka_offset : 윈도우 안 최대 Kafka offset (리플레이 기준점)
    avg_lag_seconds : 거래 발생일(dealDay) → ingested_at 까지의 평균 지연(초)
                       — 부동산 거래 신고에서 실제 등록까지의 end-to-end 신선도
    """
    print("[silver_job] streaming_health 처리 시작")

    rows = []
    for topic, table in (
        ("molit-transactions", "glue_catalog.propberg_bronze.raw_transactions"),
        ("rone-price-index", "glue_catalog.propberg_bronze.raw_price_index"),
        ("kosis-population", "glue_catalog.propberg_bronze.raw_population"),
    ):
        df = spark.table(table)
        # producer는 ISO 8601 형식("YYYY-MM-DDTHH:mm:ss.ffffff")으로 ingested_at 발행.
        # to_timestamp의 default format은 'yyyy-MM-dd HH:mm:ss'라 T 구분자를 못 인식한다.
        # cast("timestamp")는 Spark 3.x에서 ISO 8601을 robust하게 처리한다.
        df = df.withColumn("ingested_ts", col("ingested_at").cast("timestamp"))
        # molit만 dealDay가 있어 lag 측정 가능. 그 외 토픽은 0으로.
        if topic == "molit-transactions":
            df = df.withColumn(
                "deal_ts",
                to_timestamp(
                    concat(
                        col("deal_year"), lit("-"),
                        col("deal_month"), lit("-"),
                        col("deal_day"),
                    ),
                    "yyyy-M-d",
                ),
            )
            df = df.withColumn(
                "lag_seconds",
                (unix_timestamp("ingested_ts") - unix_timestamp("deal_ts")).cast("double"),
            )
        else:
            df = df.withColumn("lag_seconds", lit(0.0).cast("double"))

        windowed = (
            df.where(col("ingested_ts").isNotNull())
            .groupBy(window(col("ingested_ts"), "1 hour"))
            .agg(
                count("*").alias("record_count"),
                spark_round(spark_avg(col("lag_seconds")), 2).alias("avg_lag_seconds"),
                spark_max(col("kafka_offset")).alias("max_kafka_offset"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                lit(topic).alias("topic"),
                col("record_count"),
                col("avg_lag_seconds"),
                col("max_kafka_offset").cast("bigint"),
                lit(today).alias("updated_at"),
            )
        )
        rows.append(windowed)

    if not rows:
        print("[silver_job] streaming_health skip — no data")
        return

    health = rows[0]
    for r in rows[1:]:
        health = health.unionByName(r)

    health.createOrReplaceTempView("streaming_health_staged")

    spark.sql(
        """
        MERGE INTO glue_catalog.propberg_gold.streaming_health AS target
        USING streaming_health_staged AS source
        ON target.window_start = source.window_start
        AND target.topic = source.topic
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print("[silver_job] streaming_health 완료")


if __name__ == "__main__":
    process_transactions()
    process_price_index()
    process_population()
    process_streaming_health()
    print("[silver_job] 전체 완료")

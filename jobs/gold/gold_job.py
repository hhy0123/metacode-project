import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, count, max as spark_max, min as spark_min,
    round as spark_round, lag, when, abs as spark_abs
)
from pyspark.sql.window import Window

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket'])
    s3_bucket = args['s3_bucket']
except:
    s3_bucket = os.getenv("S3_BUCKET", "propberg-lakehouse-hhy")

spark = SparkSession.builder \
    .appName("propberg-gold") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{s3_bucket}/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

today = datetime.utcnow().strftime("%Y-%m-%d")

def build_daily_trade_summary():
    print("[gold_job] daily_trade_summary 처리 시작")

    df = spark.table("glue_catalog.propberg_silver.transactions_enriched")

    summary = df.groupBy("deal_date", "sigungu_code", "sido", "sigungu") \
        .agg(
            count("deal_id").alias("trade_count"),
            spark_round(avg("deal_amount"), 0).alias("avg_deal_amount"),
            spark_round(avg("price_per_sqm"), 0).alias("avg_price_per_sqm"),
            spark_max("deal_amount").alias("max_deal_amount"),
            spark_min("deal_amount").alias("min_deal_amount"),
        ) \
        .withColumn("updated_at", lit(today))

    summary.createOrReplaceTempView("daily_trade_staged")

    spark.sql("""
        MERGE INTO glue_catalog.propberg_gold.daily_trade_summary AS target
        USING daily_trade_staged AS source
        ON target.trade_date = source.deal_date
        AND target.sigungu_code = source.sigungu_code
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[gold_job] daily_trade_summary 완료 — {summary.count()}건")

def build_monthly_price_trend():
    print("[gold_job] monthly_price_trend 처리 시작")

    df_tx = spark.table("glue_catalog.propberg_silver.transactions_enriched")
    df_idx = spark.table("glue_catalog.propberg_silver.price_index_enriched")

    monthly = df_tx.groupBy("year_month", "sigungu_code", "sido", "sigungu") \
        .agg(
            spark_round(avg("deal_amount"), 0).alias("avg_deal_amount"),
            spark_round(avg("price_per_sqm"), 0).alias("avg_price_per_sqm"),
            count("deal_id").alias("trade_count"),
        )

    # 가격지수 조인 (전국 지수)
    national_idx = df_idx.filter(col("region_name") == "전국") \
        .select(
            col("year_month").alias("idx_year_month"),
            col("price_index")
        )

    monthly_with_idx = monthly.join(
        national_idx,
        monthly["year_month"] == national_idx["idx_year_month"],
        "left"
    ).drop("idx_year_month")

    # 전월 대비 변동률
    window = Window.partitionBy("sigungu_code").orderBy("year_month")
    monthly_final = monthly_with_idx \
        .withColumn("prev_avg", lag("avg_deal_amount", 1).over(window)) \
        .withColumn("mom_change_rate",
            when(col("prev_avg").isNotNull(),
                spark_round((col("avg_deal_amount") - col("prev_avg")) / col("prev_avg") * 100, 2)
            ).otherwise(lit(None))) \
        .drop("prev_avg") \
        .withColumn("updated_at", lit(today))

    monthly_final.createOrReplaceTempView("monthly_trend_staged")

    spark.sql("""
        MERGE INTO glue_catalog.propberg_gold.monthly_price_trend AS target
        USING monthly_trend_staged AS source
        ON target.year_month = source.year_month
        AND target.sigungu_code = source.sigungu_code
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[gold_job] monthly_price_trend 완료 — {monthly_final.count()}건")

def build_anomaly_transactions():
    print("[gold_job] anomaly_transactions 처리 시작")

    df = spark.table("glue_catalog.propberg_silver.transactions_enriched")

    # 시군구별 평균 대비 30% 이상 이탈 거래 탐지
    avg_by_sigungu = df.groupBy("sigungu_code", "year_month") \
        .agg(avg("deal_amount").alias("avg_amount"))

    anomaly = df.join(avg_by_sigungu, ["sigungu_code", "year_month"]) \
        .withColumn("deviation_rate",
            spark_round((col("deal_amount") - col("avg_amount")) / col("avg_amount") * 100, 2)) \
        .withColumn("anomaly_type",
            when(col("deviation_rate") > 30, "급등")
            .when(col("deviation_rate") < -30, "급락")
            .otherwise(None)) \
        .filter(col("anomaly_type").isNotNull()) \
        .select(
            "deal_id", "sigungu_code", "sido", "sigungu",
            "apt_name", "deal_amount", "avg_amount",
            "deviation_rate", "anomaly_type", "deal_date"
        ) \
        .withColumn("updated_at", lit(today))

    anomaly.createOrReplaceTempView("anomaly_staged")

    spark.sql("""
        MERGE INTO glue_catalog.propberg_gold.anomaly_transactions AS target
        USING anomaly_staged AS source
        ON target.deal_id = source.deal_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[gold_job] anomaly_transactions 완료 — {anomaly.count()}건")

if __name__ == "__main__":
    build_daily_trade_summary()
    build_monthly_price_trend()
    build_anomaly_transactions()
    print("[gold_job] 전체 완료")
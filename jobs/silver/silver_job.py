import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, round as spark_round,
    regexp_replace, trim, year, month
)

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket'])
    s3_bucket = args['s3_bucket']
except:
    s3_bucket = os.getenv("S3_BUCKET", "propberg-lakehouse-hhy")

spark = SparkSession.builder \
    .appName("propberg-silver") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{s3_bucket}/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

today = datetime.utcnow().strftime("%Y-%m-%d")

# 시군구 코드 → 시도/시군구 매핑
SIGUNGU_MAP = {
    "11": "서울", "26": "부산", "27": "대구", "28": "인천",
    "29": "광주", "30": "대전", "31": "울산", "36": "세종",
    "41": "경기", "42": "강원", "43": "충북", "44": "충남",
    "45": "전북", "46": "전남", "47": "경북", "48": "경남", "50": "제주"
}

def process_transactions():
    print("[silver_job] transactions_enriched 처리 시작")

    df = spark.table("glue_catalog.propberg_bronze.raw_transactions")

    # 정제 및 변환
    df_clean = df \
        .withColumn("deal_amount_clean",
            regexp_replace(col("거래금액"), ",", "").cast("bigint")) \
        .withColumn("area_clean", col("전용면적").cast("double")) \
        .withColumn("floor_clean", col("층").cast("int")) \
        .withColumn("build_year_clean", col("건축년도").cast("int")) \
        .withColumn("deal_year_clean", col("년").cast("int")) \
        .withColumn("deal_month_clean", col("월").cast("int")) \
        .withColumn("deal_day_clean", col("일").cast("int")) \
        .withColumn("sido",
            when(col("sigungu_code").substr(1, 2) == "11", "서울")
            .when(col("sigungu_code").substr(1, 2) == "26", "부산")
            .when(col("sigungu_code").substr(1, 2) == "27", "대구")
            .when(col("sigungu_code").substr(1, 2) == "28", "인천")
            .when(col("sigungu_code").substr(1, 2) == "29", "광주")
            .when(col("sigungu_code").substr(1, 2) == "30", "대전")
            .when(col("sigungu_code").substr(1, 2) == "31", "울산")
            .when(col("sigungu_code").substr(1, 2) == "36", "세종")
            .when(col("sigungu_code").substr(1, 2) == "41", "경기")
            .otherwise("기타")) \
        .withColumn("price_per_sqm",
            spark_round(col("deal_amount_clean") / col("area_clean"), 2)) \
        .withColumn("building_age",
            lit(2026) - col("build_year_clean")) \
        .withColumn("deal_date",
            col("deal_year_clean").cast("string")
            .concat(lit("-"))
            .concat(col("deal_month_clean").cast("string"))
            .concat(lit("-"))
            .concat(col("deal_day_clean").cast("string"))) \
        .withColumn("updated_at", lit(today)) \
        .select(
            col("거래일련번호").alias("deal_id"),
            col("sigungu_code"),
            col("sido"),
            trim(col("시군구")).alias("sigungu"),
            trim(col("법정동")).alias("dong"),
            trim(col("아파트")).alias("apt_name"),
            col("deal_amount_clean").alias("deal_amount"),
            col("price_per_sqm"),
            col("area_clean").alias("area"),
            col("floor_clean").alias("floor"),
            col("build_year_clean").alias("build_year"),
            col("building_age"),
            col("deal_date"),
            col("deal_year_clean").alias("deal_year"),
            col("deal_month_clean").alias("deal_month"),
            trim(col("도로명")).alias("road_name"),
            col("매도자").alias("seller_gbn"),
            col("매수자").alias("buyer_gbn"),
            col("updated_at"),
            col("ingested_date"),
        )

    df_clean.createOrReplaceTempView("transactions_staged")

    spark.sql("""
        MERGE INTO glue_catalog.propberg_silver.transactions_enriched AS target
        USING transactions_staged AS source
        ON target.deal_id = source.deal_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[silver_job] transactions_enriched 완료 — {df_clean.count()}건")

def process_price_index():
    print("[silver_job] price_index_enriched 처리 시작")

    df = spark.table("glue_catalog.propberg_bronze.raw_price_index")

    df_clean = df \
        .withColumn("price_index", col("DTA_VAL").cast("double")) \
        .withColumn("updated_at", lit(today)) \
        .select(
            col("WRTTIME_IDTFR_ID").alias("year_month"),
            col("CLS_ID").alias("region_code"),
            col("CLS_NM").alias("region_name"),
            col("CLS_FULLNM").alias("region_full"),
            col("price_index"),
            col("updated_at"),
            col("ingested_date"),
        )

    df_clean.createOrReplaceTempView("price_index_staged")

    spark.sql("""
        MERGE INTO glue_catalog.propberg_silver.price_index_enriched AS target
        USING price_index_staged AS source
        ON target.year_month = source.year_month
        AND target.region_code = source.region_code
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[silver_job] price_index_enriched 완료 — {df_clean.count()}건")

def process_population():
    print("[silver_job] population_enriched 처리 시작")

    df = spark.table("glue_catalog.propberg_bronze.raw_population")

    df_clean = df \
        .withColumn("updated_at", lit(today)) \
        .select(
            col("C1_NM").alias("region_name"),
            col("PRD_DE").alias("prd_de"),
            lit(None).cast("bigint").alias("population"),
            col("DT").cast("bigint").alias("household"),
            col("updated_at"),
            col("ingested_date"),
        )

    df_clean.createOrReplaceTempView("population_staged")

    spark.sql("""
        MERGE INTO glue_catalog.propberg_silver.population_enriched AS target
        USING population_staged AS source
        ON target.region_name = source.region_name
        AND target.prd_de = source.prd_de
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[silver_job] population_enriched 완료 — {df_clean.count()}건")

if __name__ == "__main__":
    process_transactions()
    process_price_index()
    process_population()
    print("[silver_job] 전체 완료")
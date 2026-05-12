-- Gold: 지역별 일별 거래 요약
CREATE TABLE IF NOT EXISTS propberg_gold.daily_trade_summary (
    trade_date          STRING,
    sigungu_code        STRING,
    sido                STRING,
    sigungu             STRING,
    trade_count         BIGINT,
    avg_deal_amount     DOUBLE,
    avg_price_per_sqm   DOUBLE,
    max_deal_amount     BIGINT,
    min_deal_amount     BIGINT,
    updated_at          STRING
)
LOCATION 's3://propberg-lakehouse-hhy/gold/daily_trade_summary/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);

-- Gold: 지역별 월별 가격 트렌드
CREATE TABLE IF NOT EXISTS propberg_gold.monthly_price_trend (
    year_month          STRING,
    sigungu_code        STRING,
    sido                STRING,
    sigungu             STRING,
    avg_deal_amount     DOUBLE,
    avg_price_per_sqm   DOUBLE,
    trade_count         BIGINT,
    price_index         DOUBLE,
    mom_change_rate     DOUBLE,
    updated_at          STRING
)
LOCATION 's3://propberg-lakehouse-hhy/gold/monthly_price_trend/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);

-- Gold: 이상 거래 탐지 (운영 가시성)
CREATE TABLE IF NOT EXISTS propberg_gold.anomaly_transactions (
    deal_id             STRING,
    sigungu_code        STRING,
    sido                STRING,
    sigungu             STRING,
    apt_name            STRING,
    deal_amount         BIGINT,
    avg_amount          DOUBLE,
    deviation_rate      DOUBLE,
    anomaly_type        STRING,
    deal_date           STRING,
    updated_at          STRING
)
LOCATION 's3://propberg-lakehouse-hhy/gold/anomaly_transactions/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
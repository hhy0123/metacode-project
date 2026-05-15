-- Gold: 지역별 일별 거래 요약 (대시보드 사전 집계)
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
PARTITIONED BY (sido)
LOCATION 's3://propberg-lakehouse-hhy/gold/daily_trade_summary/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy',
    'vacuum_max_snapshot_age_seconds'='604800',
    'vacuum_min_snapshots_to_keep'='3'
);

-- Gold: 지역별 월별 가격 트렌드 (R-ONE 지수와 조인)
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
PARTITIONED BY (sido)
LOCATION 's3://propberg-lakehouse-hhy/gold/monthly_price_trend/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy',
    'vacuum_max_snapshot_age_seconds'='604800',
    'vacuum_min_snapshots_to_keep'='3'
);

-- Gold: 이상 거래 탐지 (±30% 이탈, 운영 가시성)
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
PARTITIONED BY (sido, anomaly_type)
LOCATION 's3://propberg-lakehouse-hhy/gold/anomaly_transactions/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy',
    'vacuum_max_snapshot_age_seconds'='604800',
    'vacuum_min_snapshots_to_keep'='3'
);

-- Gold: 스트리밍 인제스천 헬스 (운영 가시성)
CREATE TABLE IF NOT EXISTS propberg_gold.streaming_health (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    topic               STRING,
    record_count        BIGINT,
    avg_lag_seconds     DOUBLE,
    max_kafka_offset    BIGINT,
    updated_at          STRING
)
LOCATION 's3://propberg-lakehouse-hhy/gold/streaming_health/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy',
    'vacuum_max_snapshot_age_seconds'='604800',
    'vacuum_min_snapshots_to_keep'='3'
);

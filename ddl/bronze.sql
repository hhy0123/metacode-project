-- Bronze: 국토부 실거래가 raw 테이블
CREATE TABLE IF NOT EXISTS propberg_bronze.raw_transactions (
    deal_id         STRING,
    sigungu_code    STRING,
    year_month      STRING,
    apt_name        STRING,
    deal_amount     STRING,
    area            STRING,
    floor           STRING,
    build_year      STRING,
    deal_year       STRING,
    deal_month      STRING,
    deal_day        STRING,
    dong            STRING,
    road_name       STRING,
    seller_gbn      STRING,
    buyer_gbn       STRING,
    cdeal_type      STRING,
    cdeal_day       STRING,
    raw_json        STRING,
    ingested_at     STRING,
    ingested_date   STRING
)
LOCATION 's3://propberg-lakehouse-hhy/bronze/raw_transactions/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);

-- Bronze: R-ONE 가격지수 raw 테이블
CREATE TABLE IF NOT EXISTS propberg_bronze.raw_price_index (
    statbl_id       STRING,
    dtacycle_cd     STRING,
    wrttime_idtfr_id STRING,
    cls_nm          STRING,
    cls_fullnm      STRING,
    itm_nm          STRING,
    dta_val         STRING,
    ui_nm           STRING,
    year_month      STRING,
    ingested_at     STRING,
    ingested_date   STRING
)
LOCATION 's3://propberg-lakehouse-hhy/bronze/raw_price_index/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);

-- Bronze: KOSIS 인구 raw 테이블
CREATE TABLE IF NOT EXISTS propberg_bronze.raw_population (
    stat_name       STRING,
    orgid           STRING,
    tblid           STRING,
    itm_id          STRING,
    itm_nm          STRING,
    unit_nm         STRING,
    prd_se          STRING,
    prd_de          STRING,
    dt              STRING,
    ingested_at     STRING,
    ingested_date   STRING
)
LOCATION 's3://propberg-lakehouse-hhy/bronze/raw_population/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
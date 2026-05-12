-- Silver: 실거래가 enriched 테이블
CREATE TABLE IF NOT EXISTS propberg_silver.transactions_enriched (
    deal_id             STRING,
    sigungu_code        STRING,
    sido                STRING,
    sigungu             STRING,
    dong                STRING,
    apt_name            STRING,
    deal_amount         BIGINT,
    price_per_sqm       DOUBLE,
    area                DOUBLE,
    floor               INT,
    build_year          INT,
    building_age        INT,
    deal_date           STRING,
    deal_year           INT,
    deal_month          INT,
    road_name           STRING,
    seller_gbn          STRING,
    buyer_gbn           STRING,
    updated_at          STRING,
    ingested_date       STRING
)
LOCATION 's3://propberg-lakehouse-hhy/silver/transactions_enriched/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);

-- Silver: 가격지수 enriched 테이블
CREATE TABLE IF NOT EXISTS propberg_silver.price_index_enriched (
    year_month          STRING,
    region_code         STRING,
    region_name         STRING,
    region_full         STRING,
    price_index         DOUBLE,
    updated_at          STRING,
    ingested_date       STRING
)
LOCATION 's3://propberg-lakehouse-hhy/silver/price_index_enriched/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);

-- Silver: 인구 enriched 테이블
CREATE TABLE IF NOT EXISTS propberg_silver.population_enriched (
    region_name         STRING,
    prd_de              STRING,
    population          BIGINT,
    household           BIGINT,
    updated_at          STRING,
    ingested_date       STRING
)
LOCATION 's3://propberg-lakehouse-hhy/silver/population_enriched/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);
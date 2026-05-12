# propberg
> **한국 부동산 실거래가 데이터 레이크하우스**
> Apache Iceberg · Kafka · Medallion Architecture · AWS S3 / Glue / Athena · Superset · Grafana

국토교통부 실거래가 API · 한국부동산원 R-ONE API · 통계청 KOSIS API 3소스를
Bronze / Silver / Gold 메달리온 구조로 Iceberg Lakehouse에 적재하고,
지역별 가격 트렌드 분석과 이상 거래 탐지로 운영 가시성을 제공하는 데이터 플랫폼.

**메타코드 DE 부트캠프 8회차 최종 프로젝트**

---

## Stack

- **Storage**: S3 + Apache Iceberg (Silver/Gold), Parquet (Bronze)
- **Catalog**: AWS Glue Data Catalog
- **Compute**: AWS Glue (Spark Batch)
- **Streaming**: Kafka (로컬 Docker) — Topic: `molit-transactions` · `rone-price-index` · `kosis-population`
- **Query**: Athena v3
- **Orchestration**: Airflow (로컬 Docker)
- **BI**: Apache Superset (로컬 Docker)
- **Monitoring**: Prometheus + Grafana (로컬 Docker)

> 개발 단계 AWS 비용 최소화를 위해 Kafka · Airflow · Superset · Grafana는 로컬 Docker로 운영합니다.
> 프로덕션 전환 시 AWS MSK · MWAA · QuickSight로 교체 가능한 구조입니다.

---

## Data Sources

- **국토교통부 실거래가 상세 API** — 아파트 매매 실거래가 (전국 주요 시군구, 2024~2025)
- **한국부동산원 R-ONE API** (A_2024_00045) — 아파트 월별 매매가격지수
- **통계청 KOSIS API** — 지역별 가구 통계 (증강용)

자세한 아키텍처와 설계 결정은 `docs/architecture.md` 참고.

---

## Quick Start

1. `cp .env.example .env` 후 `MOLIT_API_KEY` / `RONE_API_KEY` / `KOSIS_API_KEY` / AWS 값 채움
2. Docker 스택 기동: `docker compose -f infra/docker/docker-compose.yml up -d`
3. 데이터 수집:
   ```bash
   python jobs/producer/molit_producer.py
   python jobs/producer/rone_producer.py
   python jobs/producer/kosis_producer.py
   ```
4. AWS Glue Job 실행 (순서대로): Bronze → Silver → Gold
5. Airflow: http://localhost:8082 (admin/admin)
6. Superset: http://localhost:8088 (admin/admin)
7. Grafana: http://localhost:3000 (admin/admin)

---

## Medallion Schema

### Bronze — Raw Ingestion (Parquet · append only)
```
raw_transactions   — 국토부 실거래가 원본 · 30,217건
raw_price_index    — 부동산원 가격지수 원본 · 5,520건
raw_population     — 통계청 가구 통계 원본 · 293건
```
> Bronze는 원본 보존 목적으로 모든 컬럼을 STRING으로 수신하며 수정하지 않습니다.

### Silver — Cleansed & Enriched (Iceberg · MERGE INTO)
```
transactions_enriched  — 실거래가 정제 + 시도 분류 + 평단가 계산 · 476,272건
price_index_enriched   — 가격지수 정제 + 지역 계층 매핑
population_enriched    — 가구 통계 정제
```

### Gold — Aggregated & Serving (Iceberg · Athena)
```
daily_trade_summary    — 지역별 일별 거래 요약 · 180,888건
monthly_price_trend    — 월별 가격 트렌드 + 가격지수 조인 · 20,396건
anomaly_transactions   — 이상 거래 탐지 (±30% 이탈) · 238,080건
```

---

## Why Iceberg

| 상황 | 이유 |
|---|---|
| 실거래가 소급 수정 (국토부가 자주 함) | Row-level UPSERT → `MERGE INTO` |
| 계약 해제 반영 | Row-level DELETE |
| 수십 년치 거래 이력 | Partition Pruning + Compaction |
| 정책 변경 전후 비교 | Time Travel |

---

## 100× Scale 병목 지점

| 레이어 | 병목 | 대응 |
|---|---|---|
| Bronze | 소파일 폭발 (현재 2,592개) | Compaction 자동화 · target 128MB |
| Silver | MERGE INTO 전체 파티션 스캔 | 시도코드 기준 파티셔닝 |
| Gold | Athena 스캔 비용 증가 | CTAS 사전 집계 + Partition Pruning |
| Kafka | Consumer lag 증가 | 파티션 수 증설 · Consumer Group 분리 |

---

## Iceberg Management Automation

매일 새벽 03:00 KST Airflow `propberg_mgmt` DAG 자동 실행

```
Compaction (소파일 병합 · target 128MB)
  → Expire Snapshots (7일 이상 스냅샷 제거 · 최근 3개 보존)
    → Remove Orphan Files (3일 이상 미사용 파일 삭제)
```

> Bronze는 Parquet으로 적재되므로 Iceberg 매니지먼트 대상에서 제외됩니다.

---

## Project Structure

```
propberg/
├── infra/
│   ├── docker/
│   │   ├── docker-compose.yml
│   │   └── prometheus.yml
│   └── scripts/
│       ├── aws_initial_setup.sh
│       └── run_ddl.sh
├── dags/
│   ├── pipeline_dag.py              # 수집 → Bronze → Silver → Gold (매일 06:00)
│   └── mgmt_dag.py                  # Compaction · Expire · Orphan (매일 03:00)
├── jobs/
│   ├── producer/
│   │   ├── molit_producer.py
│   │   ├── rone_producer.py
│   │   └── kosis_producer.py
│   ├── bronze/bronze_job.py
│   ├── silver/silver_job.py
│   ├── gold/gold_job.py
│   └── mgmt_job.py
├── ddl/
│   ├── bronze.sql
│   ├── silver.sql
│   └── gold.sql
├── monitoring/
│   └── grafana_dashboard.json
├── docs/
│   └── architecture.md
└── .env.example
```

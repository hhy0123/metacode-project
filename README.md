# propberg
> **한국 부동산 실거래가 실시간 데이터 레이크하우스**
> Apache Iceberg · Kafka · Spark Structured Streaming · Medallion · AWS S3 / Glue / Athena · Superset · Grafana

국토교통부 실거래가 · 한국부동산원 R-ONE · 통계청 KOSIS 3소스를
Kafka → Spark Structured Streaming → **Iceberg Bronze (1분 trigger)** → Silver/Gold(배치) 메달리온 구조로 적재하고,
지역별 가격 트렌드 분석과 이상 거래 탐지로 운영 가시성을 제공하는 데이터 플랫폼.

**메타코드 DE 부트캠프 8회차 최종 프로젝트**

---

## Stack

- **Storage**: S3 + Apache Iceberg (Bronze / Silver / Gold 전 레이어)
- **Catalog**: AWS Glue Data Catalog
- **Streaming Ingestion**: Kafka (로컬 Docker) + Spark Structured Streaming (로컬 Docker, 1분 trigger)
- **Batch Compute**: AWS Glue (Spark Batch) — Silver/Gold/매니지먼트
- **Query**: Athena v3
- **Orchestration**: Airflow (로컬 Docker)
- **BI**: Apache Superset (로컬 Docker)
- **Monitoring**: Prometheus + Grafana + kafka-exporter (로컬 Docker)

> 개발 단계 AWS 비용 최소화 — Bronze 스트리밍이 로컬 Spark에서 동작해 S3 적재만 AWS 사용.
> 프로덕션 전환 시 로컬 컴포넌트를 AWS MSK · MWAA · EMR/EKS Spark · QuickSight로 교체 가능.

---

## Data Sources

- **국토교통부 실거래가 상세 API** — 아파트 매매 실거래가 (전국 주요 시군구)
- **한국부동산원 R-ONE API** (A_2024_00045) — 아파트 월별 매매가격지수
- **통계청 KOSIS API** — 지역별 가구 통계

자세한 아키텍처와 설계 결정은 [docs/architecture.md](docs/architecture.md), 운영 가시성은 [docs/observability.md](docs/observability.md) 참고.

---

## Quick Start

```bash
# 1. 환경변수 설정
cp .env.example .env
# MOLIT_API_KEY / RONE_API_KEY / KOSIS_API_KEY / AWS 키 / S3_BUCKET 입력

# 2. AWS 초기 셋업 (S3 버킷 + Glue DB)
bash infra/scripts/aws_initial_setup.sh

# 3. DDL 적용 (Bronze/Silver/Gold Iceberg 테이블 생성)
bash infra/scripts/run_ddl.sh

# 4. 로컬 Docker 스택 기동
docker compose -f infra/docker/docker-compose.yml up -d --build

# 5. 접속
#   - Airflow:    http://localhost:8082 (admin/admin)
#   - Superset:   http://localhost:8088 (admin/admin)
#   - Grafana:    http://localhost:3000 (admin/admin)
#   - Spark UI:   http://localhost:4040 (스트리밍 consumer)
#   - Prometheus: http://localhost:9090
```

기동되면 자동 시작:
- 3개 Producer 데몬 (molit 5분 / rone 1시간 / kosis 6시간 폴링)
- Spark Streaming Consumer (Kafka 3 topic → Bronze Iceberg, 1분 trigger)
- Airflow DAG 2개 (`propberg_pipeline` 매일 06:00, `propberg_mgmt` 매일 03:00)

---

## 데이터 흐름

```
[국토부 / R-ONE / KOSIS API]
        │
        ▼ (Producer 데몬)
   Kafka 3 topics (로컬)
        │
        ▼ (Spark Structured Streaming, 1분 trigger, 토픽별 query)
   Bronze Iceberg (propberg_bronze.raw_*)
        │
        ▼ (Glue Silver Job, 매일 06:00 KST, MERGE INTO)
   Silver Iceberg (propberg_silver.*_enriched)
        │
        ▼ (Glue Gold Job, 매일 06:00 KST, CTAS)
   Gold Iceberg (propberg_gold.*)
        │
        ├─▶ Athena ─▶ Superset (대시보드)
        └─▶ Athena ─▶ Grafana  (운영 가시성)
```

---

## Medallion Schema

### Bronze — 원본 보존 (Iceberg, 스트리밍 append)
```
raw_transactions   — 국토부 실거래가 원본 + kafka 메타
raw_price_index    — 부동산원 가격지수 원본 + kafka 메타
raw_population     — 통계청 가구 통계 원본 + kafka 메타
```
> 모든 비즈니스 컬럼 STRING. `kafka_topic`/`kafka_partition`/`kafka_offset` 보존 → 리플레이/감사 가능.
> `ingested_date` 파티셔닝. 1분 trigger micro-batch.

### Silver — Cleansed & Enriched (Iceberg, MERGE INTO 배치)
```
transactions_enriched  — 정제 + 시도 분류 + 평단가 + 건물연령
price_index_enriched   — 가격지수 정제 + 지역 계층 매핑
population_enriched    — 가구 통계 정제
```
> `sido`/`deal_year` 파티셔닝으로 MERGE INTO 전체 스캔 회피.

### Gold — Aggregated & Serving (Iceberg, 배치)
```
daily_trade_summary    — 지역별 일별 거래 요약
monthly_price_trend    — 월별 가격 트렌드 + 가격지수 조인
anomaly_transactions   — 이상 거래 탐지 (±30% 이탈)
streaming_health       — 스트리밍 윈도우별 처리량/lag
```

---

## Why Iceberg

| 상황 | 이유 |
|---|---|
| 실거래가 소급 수정 (국토부가 자주 함) | Row-level UPSERT → `MERGE INTO` |
| 계약 해제 반영 | Row-level DELETE |
| 스트리밍 마이크로배치 소파일 | `rewrite_data_files` 자동 compaction |
| 정책 변경 전후 비교 | Time Travel |
| 외부 API 응답 스키마 변경 추적 | Schema evolution |

Bronze까지 Iceberg로 통일 → 세 레이어 매니지먼트 도구 공유, 일관된 카탈로그.

---

## Iceberg Management 우선순위

매일 새벽 03:00 KST Airflow `propberg_mgmt` DAG 자동 실행 — 우선순위:

```
1. rewrite_manifests    매니페스트 정리
   ↓
2. compaction           Bronze 우선 (1분 trigger × 3 topic × 1,440분 = 일 4,320 마이크로배치)
   ↓                    → Silver (MERGE 생성 파일) → Gold (배치 overwrite)
3. expire_snapshots     7일 이상 스냅샷 제거, 최근 3개 보존
   ↓
4. remove_orphan_files  3일 이상 미사용 파일 삭제
```

Bronze가 1순위인 이유: 1분 trigger 스트리밍이 소파일을 가장 빠르게 만든다.

---

## 100× / 1000× Scale 병목

| 레이어 | 100x 병목 | 1000x 한계 |
|---|---|---|
| Kafka | partition 6→24, consumer group 분리 | 3 broker 클러스터 (MSK 전환) |
| Spark Streaming | local[2] | EMR/EKS Spark Operator (동적 executor) |
| Bronze 적재 | 1분 trigger 유지 | 30초 trigger + maxOffsetsPerTrigger 1M |
| Silver MERGE | sido/deal_year 파티셔닝 + Z-order | **Flink Iceberg sink로 micro-batch MERGE 전환** ← 가장 먼저 깨짐 |
| Gold Athena | CTAS + Partition Pruning | StarRocks materialized view |
| 매니지먼트 | 일 1회 | 시간당 1회 (Bronze만) |

자세한 비용 추정과 시나리오는 [docs/architecture.md](docs/architecture.md) 참고.

---

## 운영 가시성

**4-Tier 구조** — T1 Grafana(실시간 인프라) / T2 Airflow UI(파이프라인) / T3 Superset(비즈니스) / T4 Athena 콘솔(디버깅)

| SLO | 목표 |
|---|---|
| Bronze 신선도 | molit < 30분 · rone < 3시간 · kosis < 12시간 (각 producer polling 주기 × 6) |
| Kafka consumer lag p95 | < 1,000 |
| Bronze micro-batch 처리 시간 p95 | < 30초 (Spark UI Structured Streaming 탭) |
| Silver/Gold 배치 완료 | 매일 07:30 KST 이전 |
| 매니지먼트 (Compaction/Expire/Orphan) | 매일 03:00 KST 1회 성공 |

Grafana 대시보드 (`monitoring/grafana/dashboards/propberg_streaming.json`)가 컨테이너 기동 시 자동 import. 알람 정책과 런북은 [docs/observability.md](docs/observability.md) 참고.

---

## 개발 환경

```bash
pip install -e ".[dev]"
pre-commit install
pytest tests/ -v
ruff check jobs/ tests/
```

CI는 `.github/workflows/ci.yml` — ruff + pytest + DAG/DDL 검증.

---

## Project Structure

```
propberg/
├── infra/
│   ├── docker/
│   │   ├── docker-compose.yml          # producers + streaming-consumer + kafka-exporter 포함
│   │   ├── Dockerfile.producer
│   │   ├── Dockerfile.streaming
│   │   └── prometheus.yml
│   └── scripts/
│       ├── aws_initial_setup.sh
│       └── run_ddl.sh
├── dags/
│   ├── pipeline_dag.py                 # bronze_freshness_check → Silver → Gold (매일 06:00 KST)
│   └── mgmt_dag.py                     # rewrite_manifests → compaction → expire → orphan (매일 03:00)
├── jobs/
│   ├── producer/
│   │   ├── _runtime.py                 # 데몬 루프 + graceful shutdown
│   │   ├── molit_producer.py
│   │   ├── rone_producer.py
│   │   └── kosis_producer.py
│   ├── consumer/
│   │   └── kafka_consumer.py           # 토픽별 query 분리, 1분 trigger, Iceberg 적재
│   ├── silver/silver_job.py
│   ├── gold/gold_job.py
│   └── mgmt_job.py                     # Bronze 포함 모든 Iceberg 테이블
├── ddl/
│   ├── bronze.sql                      # Iceberg + ingested_date 파티셔닝 + kafka 메타
│   ├── silver.sql                      # Iceberg + sido/deal_year 파티셔닝
│   └── gold.sql                        # Iceberg + sido 파티셔닝 + streaming_health
├── monitoring/
│   └── grafana/
│       ├── provisioning/               # 데이터소스 + 대시보드 provisioning
│       └── dashboards/
│           └── propberg_streaming.json
├── dashboard/
│   └── superset/                       # IaC: Superset 대시보드 JSON 보관
├── tests/
│   ├── test_bronze_schema.py
│   ├── test_ddl_consistency.py
│   └── test_dag_smoke.py
├── docs/
│   ├── architecture.md                 # 100x/1000x 스케일 시나리오 포함
│   └── observability.md                # SLI/SLO/알람/런북
├── .github/workflows/ci.yml
├── pyproject.toml
├── .pre-commit-config.yaml
├── .env.example
└── README.md
```

---

## 평가 기준 매칭

| 평가 기준 | propberg에서 달성한 방법 |
|---|---|
| **운영 가시성** | 4-tier (Grafana / Airflow / Superset / Athena) + kafka-exporter 메트릭 + Spark UI Structured Streaming + SLO 표 + 런북 3개 + `streaming_health` 테이블 |
| **스케일 사고력** | 100x를 4-dimension (Throughput / Batch / Storage / Concurrency)으로 분해 + 부하 capacity 검증표 + 1000x 시나리오 + 비용 추정 |
| **Iceberg 필요성** | 3가지 가치 ↔ 시연 위치 매핑 (MERGE: 실거래가 정정, DELETE: 계약 해제, Time Travel: 정책 전후 비교) |
| **협업·지속 가능성** | Decision Log 14개 + pyproject + ruff + pre-commit + pytest 24개 invariant + GitHub Actions CI |

| 구현 필수 | 위치 |
|---|---|
| Iceberg 테이블 활용 | Bronze/Silver/Gold 전 레이어 |
| 메달리온 아키텍처 | 3계층 분리, 각 레이어 책임 명확 |
| Iceberg 테이블 매니지먼트 | `propberg_mgmt` DAG: rewrite_manifests → compaction → expire → orphan |
| 대시보드 조회 | Superset (BI) + Grafana (운영) |

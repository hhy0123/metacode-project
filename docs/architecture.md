# propberg 아키텍처 설계

## 전체 파이프라인

```
[국토부 API]  ──┐
[부동산원 API] ─┼──▶ Kafka (3 Topics) ──▶ Spark Structured Streaming ──▶ Bronze (Iceberg)
[통계청 API]  ──┘    (로컬 Docker)         (1분 trigger, Iceberg append)        │
                                                                                │
                                                            ┌───────────────────┘
                                                            ▼
                                              Airflow propberg_pipeline (매일 06:00 KST)
                                                            │
                                                  ┌─────────┴─────────┐
                                                  ▼                   ▼
                                           Silver (Iceberg)     Gold (Iceberg)
                                           MERGE INTO           CTAS / overwrite
                                                  │                   │
                                                  └─────────┬─────────┘
                                                            ▼
                                                       Athena v3
                                                            │
                                             ┌──────────────┴──────────────┐
                                             ▼                             ▼
                                    Superset 대시보드               Grafana 모니터링

       Airflow propberg_mgmt (매일 03:00 KST) — rewrite_manifests → compaction → expire → orphan
```

### Airflow ↔ Glue Job 연동 흐름

Airflow 자체는 Spark/Iceberg를 직접 실행하지 않는다. **AWS Glue Console에 미리 등록된 Job**을 `aws glue start-job-run`으로 트리거할 뿐이다. Glue Job 측이 실제 Spark + Iceberg 처리를 수행하고, 결과만 Airflow가 성공/실패로 받는다.

```
Airflow DAG (로컬 Docker)                                AWS Glue (us-east-1)
─────────────────────────                                ─────────────────────
propberg_pipeline                                        ┌─ propberg-silver-job
  └─ bronze_freshness_check  ─ S3 head ──┐               │     · 스크립트: jobs/silver/silver_job.py
  └─ silver_trigger ─ aws glue start ──▶─┼─────────────▶ │     · MERGE INTO Silver
  └─ gold_trigger   ─ aws glue start ──▶─┘               │
                                                         ├─ propberg-gold-job
propberg_mgmt                                            │     · 스크립트: jobs/gold/gold_job.py
  └─ rewrite_manifests ─ aws glue start ─┐               │     · CTAS/MERGE Gold
  └─ compaction        ─ aws glue start ─┤               │
  └─ expire_snapshots  ─ aws glue start ─┼─────────────▶ └─ propberg-mgmt-job
  └─ remove_orphans    ─ aws glue start ─┘                     · 스크립트: jobs/mgmt_job.py
                                                               · --operation 파라미터로 4단계 분기
```

### Glue Job 사전 등록 요건

AWS Glue Console에서 다음 3개 Job을 등록해 두어야 Airflow DAG가 트리거할 수 있다.

| Glue Job 이름 | 스크립트 (이 레포 경로) | Trigger 시점 | Job parameter |
|---|---|---|---|
| `propberg-silver-job` | `jobs/silver/silver_job.py` | 매일 06:00 KST (`propberg_pipeline`) | `--s3_bucket=propberg-lakehouse-hhy` |
| `propberg-gold-job` | `jobs/gold/gold_job.py` | Silver 완료 후 (`propberg_pipeline`) | `--s3_bucket=propberg-lakehouse-hhy` |
| `propberg-mgmt-job` | `jobs/mgmt_job.py` | 매일 03:00 KST (`propberg_mgmt`, 4번 호출, 매번 `--operation` 다름) | `--s3_bucket=propberg-lakehouse-hhy --operation=<compaction\|expire_snapshots\|remove_orphans\|rewrite_manifests>` |

Glue Job 등록 시:
- Type: **Spark 4.0 / G.1X / Worker 2**
- Job parameters에 `--datalake-formats=iceberg` 추가 (Iceberg 활성화)
- 스크립트가 변경되면 (`silver_job.py` 등) Glue Console에서 다시 붙여넣어야 반영됨

### Glue Database 사전 생성

DDL을 적용하기 전에 Glue Catalog에 3개 Database를 만들어야 한다 (AWS Glue Console → Databases → Add database):

| Database | 용도 | S3 위치 prefix |
|---|---|---|
| `propberg_bronze` | 스트리밍 원본 적재 | `s3://propberg-lakehouse-hhy/bronze/` |
| `propberg_silver` | MERGE INTO 정제 결과 | `s3://propberg-lakehouse-hhy/silver/` |
| `propberg_gold` | 사전 집계 + 이상 거래 + streaming_health | `s3://propberg-lakehouse-hhy/gold/` |

DDL 파일 (`ddl/bronze.sql`, `silver.sql`, `gold.sql`)을 Athena에서 실행하면 각 Database 안에 Iceberg 테이블이 생성된다.

---

## 인제스천 모드

| 레이어 | 모드 | 주기 | 비용 |
|---|---|---|---|
| Producer | 데몬 (Docker) | molit 5분, rone 1시간, kosis 6시간 | 로컬, 0 |
| Bronze | **스트리밍 (Spark Structured Streaming)** | **1분 trigger** | S3 PUT 한정 |
| Silver | 배치 (AWS Glue) | 매일 06:00 KST | Glue DPU-hour |
| Gold | 배치 (AWS Glue) | 매일 06:00 KST (Silver 후) | Glue DPU-hour |
| 매니지먼트 | 배치 (AWS Glue) | 매일 03:00 KST | Glue DPU-hour |

Bronze만 실시간이고 Silver/Gold는 배치인 이유:
- 대시보드 응답성은 Gold 사전 집계에서 나옴 — Gold가 실시간일 필요 없음
- Silver는 MERGE INTO 비용이 큼 — 매분 돌리면 Glue 비용 폭증
- 부동산 실거래는 신고 시점이 거래 후 30일까지 가능 — "분 단위 신선도"가 불필요
- 그러나 Bronze의 데이터 손실은 영원히 복구 불가 → Kafka → Iceberg 실시간 적재가 의미 있음

---

## "Streaming" 의미와 한계 — 정직한 설명

이 프로젝트의 외부 데이터 소스(국토부 실거래가, R-ONE 가격지수, KOSIS)는 모두 **webhook/CDC 같은 push 채널을 제공하지 않음**. 따라서 진짜 의미의 streaming source는 존재하지 않는다. Producer가 할 수 있는 건 polling뿐.

순진하게 매 폴링마다 fetch한 row를 그대로 publish하면 **같은 거래가 매번 반복 publish** 되어 Bronze에 중복 누적 — Silver MERGE INTO가 dedupe해도 Kafka/Bronze 단에서 의미 없는 트래픽이 흐른다. 사실상 "정기 backfill + push"가 되어 Streaming이 아니다.

본 프로젝트는 이 한계를 다음 방법으로 보완해 **producer 측 dedupe로 새 row만 Kafka에 publish** 하도록 만들었다:

| 토픽 | dedupe key |
|---|---|
| molit-transactions | `sigungu_code + aptSeq + dealYear + dealMonth + dealDay + dealAmount + floor` |
| rone-price-index | `year_month + CLS_ID + DTA_VAL` |
| kosis-population | `stat_name + PRD_DE + C1 + DT` |

- 본 row 키는 `SeenSet`(메모리 set + JSON 파일 영속화, `/var/lib/propberg/state/` Docker volume)에 보관
- 컨테이너 재시작 후에도 같은 row를 또 publish하지 않음
- 메모리 폭증 방지: LRU 200,000개 한도

이로써 polling 기반이지만 **Kafka에는 새로 발생한 거래만 흐르고**, Bronze에도 새 row만 적재된다 — streaming consumer + 1분 trigger의 의미가 살아난다.

### 진짜 streaming이 되려면 (장래 작업)

- Bid/Ask 단위로 push해주는 CDC 소스 도입 (e.g. 카프카 connect for 시세 API)
- 또는 외부 API를 직접 운영하는 시·구청 시스템과 webhook 계약
- 100× 스케일에서는 위 둘 중 하나가 사실상 필수

---

## 레이어별 역할

### Bronze — 원본 보존 (실시간)
- Kafka 토픽 3개를 토픽별 독립 Streaming Query로 구독
- 1분 trigger micro-batch → Iceberg `propberg_bronze.raw_*`에 append
- `ingested_date`로 파티셔닝 → 시간 범위 쿼리 가속
- `kafka_partition`, `kafka_offset` 컬럼 보존 → 리플레이/감사 가능
- 모든 비즈니스 컬럼은 STRING — 외부 API 스키마 변경에 유연
- 토픽별 별도 checkpoint 경로 → exactly-once 보장

### Silver — 정제 및 보강 (배치 MERGE)
- Bronze에서 읽어 타입 변환, 파생 컬럼 계산, 시도 분류
- `MERGE INTO`로 소급 수정 반영 (Iceberg 핵심 활용)
- 평단가, 건물연령 등 derived 컬럼 추가
- `sido`, `deal_year` 기준 파티셔닝 → MERGE 전체 스캔 회피

### Gold — 집계 및 서빙 (배치 CTAS/overwrite)
- 대시보드용 사전 집계
- `daily_trade_summary`: 일별 거래 요약
- `monthly_price_trend`: 월별 트렌드 + R-ONE 지수 조인
- `anomaly_transactions`: 지역/월 평균 ±30% 이탈 탐지
- `streaming_health`: 스트리밍 윈도우별 처리량 / lag 추적

---

## 설계 결정

### 왜 Iceberg인가
국토부 실거래가는 신고 후 수정이 빈번하다. 일반 Parquet + Glue 구조에선 수정된 데이터를 반영하려면 전체 파티션을 재작성해야 한다. Iceberg의 `MERGE INTO`로 변경 행만 업데이트할 수 있어 비용이 훨씬 낮다.

또 정책 변경(예: 취득세 변경) 전후 비교 시 Time Travel로 특정 시점 데이터를 조회할 수 있다 — 감사(Audit) 용도.

스트리밍 micro-batch에서 발생하는 소파일 누적은 Iceberg `rewrite_data_files`로 자동 병합한다.

### 왜 Bronze까지 Iceberg인가
- 스트리밍이 만드는 소파일을 Iceberg 매니지먼트로 통합 관리 가능
- Bronze에 Time Travel 적용 가능 — 외부 API 응답 변경 추적
- 메타데이터 일관성: 세 레이어가 모두 같은 카탈로그 / 매니지먼트 도구 사용

### 왜 하이브리드 아키텍처인가
Kafka, Airflow, Superset, Grafana, Spark Streaming은 로컬 Docker. S3/Glue/Athena만 AWS. 개발 단계 AWS 비용 최소화 + 실제 운영 환경과 동일한 데이터 흐름. 프로덕션 전환 시 로컬 컴포넌트를 AWS MSK, MWAA, QuickSight로 교체하면 된다.

### Kafka를 쓰는 이유
3개 데이터 소스의 수집 주기가 모두 다름. Topic을 소스별로 분리(`molit-transactions`, `rone-price-index`, `kosis-population`)해 Producer가 독립 동작. 100x 스케일 시 파티션 증설만으로 처리량 확장.

---

## Iceberg 정당화 — 3가지 가치 ↔ propberg 시연 위치

"왜 Parquet + Glue로는 안 되고 Iceberg가 필요한가" 에 대한 도메인 기반 답.

| Iceberg 가치 | 시연 위치 | 부동산 도메인 시나리오 |
|---|---|---|
| ① **MERGE INTO** (atomic upsert) | `propberg_silver.transactions_enriched` | 국토부가 신고 후 거래금액 정정 시 Bronze 중복 → Silver MERGE가 `deal_id` 기준 자동 dedupe. Parquet+Glue로는 전체 파티션 재작성 필요. |
| ② **Row-level DELETE** | `propberg_silver.transactions_enriched` | 매수자 변심으로 **계약 해제** 시 해당 deal_id row 삭제. Parquet+Glue는 row-level delete 불가, 파티션 전체 rewrite 필요. |
| ③ **Time Travel** (audit) | `propberg_silver.*` + Bronze | 정책 변경 (예: 취득세 인상) **전후 시점 데이터 비교** — `TIMESTAMP AS OF '2026-04-01 00:00'`로 한 줄 조회. 정책 영향 분석. |

**"Parquet + Glue로는 안 되는가"**:
1. atomic upsert 불가 (MERGE 없음)
2. HIVE overwrite는 mid-write race → partial read 가능성
3. snapshot 기반 time-travel 불가

---

## Decision Log — 핵심 결정과 트레이드오프

| # | 결정 항목 | 후보 | 선택 | 근거 |
|---|---|---|---|---|
| D1 | Bronze 포맷 | Parquet / **Iceberg** | **Iceberg** | 소급 수정 반영(MERGE) + Time Travel + 매니지먼트 도구 일관성. 단점인 snapshot 오버헤드는 mgmt DAG로 흡수 |
| D2 | Bronze→Silver 처리 모드 | streaming / **batch (일 1회)** | **batch** | MERGE INTO 비용. 매분 MERGE = Glue DPU 폭증. 부동산 신고는 30일 내 가능 = "분 단위 신선도" 불필요 |
| D3 | Streaming trigger | 10초 / **1분** / 5분 | **1분** | 소파일 vs 신선도 균형. 1분 trigger = 일 4,320 micro-batch, compaction 1회로 흡수 가능 |
| D4 | Producer streaming 흉내 | polling-only / **polling + dedupe** | **polling + dedupe** | 외부 API webhook 없음. 매 폴링 같은 row publish하면 Bronze 중복 → SeenSet 영속화로 새 row만 publish |
| D5 | Kafka 토픽 설계 | 단일 / **소스별 분리** / 종목별 | **소스별 분리** (`molit`/`rone`/`kosis`) | 수집 주기 다름(5분/1시간/6시간). 독립 backpressure + 100x 시 파티션 분리 확장 |
| D6 | Silver 파티셔닝 | 없음 / `sido` / **`sido + deal_year`** | **`sido + deal_year`** | MERGE INTO 전체 스캔 회피. 부동산 거래 = 시도×연도 카디널리티 적절 (16시도 × 5년 = 80 partition) |
| D7 | Gold 집계 단위 | hourly / **daily + monthly** | **daily + monthly** | 부동산은 분 단위 거래 발생 안 함. 일/월이 자연스러운 BI 그라뉼래리티 |
| D8 | 이상 거래 threshold | ±10% / **±30%** / ±50% | **±30%** | 부동산 가격 변동성 분석상 ±30%가 의미있는 이상치 기준 (시장 평균 변동 5~10% 범위) |
| D9 | mgmt 자동화 우선순위 | Silver 먼저 / **Bronze 먼저** | **Bronze 먼저** | 1분 trigger 스트리밍이 소파일을 가장 빨리 만드는 레이어 |
| D10 | 매니지먼트 schedule | 시간당 / **일 1회 (03:00 KST)** / 주 1회 | **일 1회** | 일 4,320 micro-batch = 일 1회 compaction이 비용 최적. snapshot retention 7일 |
| D11 | sigungu dimension | 한글명 / **5자리 코드** | **5자리 코드** | API 응답에 일관된 시군구 한글명 없음. 코드는 unique + 안정적, 한글명은 별도 매핑 테이블에서 lookup |
| D12 | dedupe state | 메모리만 / **파일(volume) + 메모리** | **파일 + 메모리** | 컨테이너 재시작 시 중복 publish 방지. LRU 200K 한도로 메모리 폭증 방지 |
| D13 | 타임존 처리 | UTC / **KST** | **KST** | 한국 부동산 = 한국 시각 기준. Airflow schedule + `ingested_date` 파티션 + dealDay 모두 KST 통일. UTC 혼재 = 자정 부근 row 전날 파티션으로 들어가 prune 깨짐 |
| D14 | 발표 한계 인정 | 강한 streaming 주장 / **솔직한 polling+dedupe** | **솔직 명시** | 외부 API가 webhook 미제공 → 진짜 streaming source 아님. docs에 한계 명시 + 어떻게 우회했는지 설계 |

---

## 부하 capacity 검증 — Phase 1 vs 100x

| 컴포넌트 | 이론 한계 (단일 노드) | 현재 부하 | 활용률 | 100x 부하 | 한계 도달? |
|---|---|---|---|---|---|
| molit Producer | ~120 req/min (API limit) | 24 req/5min | **5%** | 2,400 req/min | **초과** → 멀티 API key 또는 가입 종목 분할 |
| Kafka broker | ~50K msg/sec | ~20 msg/sec | **0.04%** | 2K msg/sec | 4% → 단일 broker 유지 가능 |
| Kafka topic partition (key=sigungu) | ~10K msg/sec | 거의 0 | ~0% | 670 msg/sec | 7% → 파티션 6→12 |
| Spark Streaming (local[2]) | ~50K rows/min sink | 5,800/5min = 1,160/min | **2.3%** | 116K/min | **초과** → EMR Serverless 또는 executor 4→8 |
| Silver MERGE INTO (Glue G.1X) | ~50K rows/min | 일 30K = 1회 처리 | 여유 | 일 3M = 1회 처리 | **초과** → 종목별 partition 병렬 또는 micro-batch MERGE |
| Gold CTAS | ~수십만 rows/min | 일 1회 | 여유 | 일 1회 | 여유 |
| Athena scan | 5GB / query (workgroup limit) | <1GB | 여유 | 50GB | **초과** → workgroup limit 상향 + Gold 사전 집계 강제 |

**핵심 결론**:
- 현재 모든 컴포넌트가 한계 대비 **여유 (10% 이하)**
- 100x에서 **가장 먼저 깨지는 곳 = Silver MERGE INTO + Spark Streaming**
- Kafka는 100x에서도 여유 → broker 추가는 안전성(RF=3) 위해서만
- API rate limit이 실질적 첫 병목 — propberg 도메인 특성

---

## 데이터 규모 추정

| 레이어 | 일 row 수 (현재) | 일 row 수 (100x) | 압축 후 크기/일 | 연간 (250일) |
|---|---|---|---|---|
| Bronze raw_transactions | ~30K | ~3M | 5–10 MB | 1.5–2.5 GB |
| Bronze raw_price_index | ~5K | ~500K | 1–2 MB | 250–500 MB |
| Bronze raw_population | ~300 | ~30K | < 1 MB | ~50 MB |
| Silver transactions_enriched | ~30K | ~3M | 8–15 MB (derived 컬럼 포함) | 2–4 GB |
| Gold daily_trade_summary | ~3K (지역×일) | ~10K | < 1 MB | ~100 MB |
| Gold monthly_price_trend | ~500 (지역×월) | ~5K | < 1 MB | ~10 MB |
| Gold anomaly_transactions | 변동 (~1K) | ~100K | < 1 MB | ~50 MB |

**핵심**: 스토리지 비용이 깨지는 게 아니라 **컴퓨트 (Glue DPU + Athena scan)** 가 먼저 깨진다.

---

## 100× 스케일 대응 — 4 dimension 분해

현재 일 30,000건 수준. 100배(일 3M)로 확장 시 깨지는 지점을 4가지 dimension으로 분해.

### Dimension 1: Throughput (인입 처리량)

| 깨짐 | 100x 증상 | 해결 |
|---|---|---|
| 외부 API rate limit | 1분당 호출 한도 초과 | 멀티 API key + 시군구 분할 polling |
| Kafka single broker | 손실 가능 (RF=1) | MSK 또는 broker 3대 + RF=3 |
| Spark Streaming `local[2]` | executor 부족 | EMR Serverless 또는 EKS Spark Operator (auto-scaling) |

### Dimension 2: Batch Window (배치 완료 시간)

| 깨짐 | 100x 증상 | 해결 |
|---|---|---|
| Bronze→Silver MERGE 1회 | 일 1회 MERGE = 3M row, Glue Job timeout 가능 | 시도별 partition 병렬 / 또는 micro-batch MERGE (Spark Structured Streaming foreachBatch) |
| Silver→Gold CTAS | 동일 | Gold cascade (daily → weekly → monthly 사전 집계 단계화) |
| Compaction 일 1회 (03:00) | 3시간 초과 가능 | 시간당 compaction (Bronze만) + day partition 분할 + rewrite_manifests 빈도 ↑ |

### Dimension 3: Storage / Compaction

| 깨짐 | 100x 증상 | 해결 |
|---|---|---|
| Bronze 소파일 폭발 | 일 4,320 micro-batch × 3 topic | compaction target 128MB → 256MB, min-file-size 강화 |
| Snapshot 누적 | 일 4,320 snapshot/topic | expire_snapshots 7일 → 3일 retention, 시간당 expire |
| S3 비용 | 누적 GB 증가 | Lifecycle: Bronze 90일 → Glacier IR |

### Dimension 4: Concurrency / Query (대시보드 부하)

| 깨짐 | 100x 증상 | 해결 |
|---|---|---|
| Athena 5GB / query | 대시보드 쿼리 초과 | Gold 사전 집계 강제 + Partition Pruning 필수 + result reuse |
| Superset 동시 사용자 | 일 N건 대시보드 refresh | Athena workgroup tier 분리 + Materialized View (StarRocks/Trino) |
| Glue Catalog request | 메타 요청 폭증 | Athena query 캐싱 + Glue Catalog 유료 plan ($1/M req) |

## 1000× 스케일 대응

일 30M 거래는 한국 부동산 시장 규모에서는 비현실적이지만, "글로벌 부동산 + 모든 자산 거래"로 확장한다고 가정할 때:

| 영역 | 100x 구조의 한계 | 1000x 대응 |
|---|---|---|
| Kafka | 단일 broker, 24 partitions | **3 broker 클러스터**, 토픽당 96 partitions, MSK 전환 |
| Spark Streaming | local[2] 실행 | **EMR / EKS Spark Operator**, 동적 executor 스케일 |
| Bronze 적재 빈도 | 1분 trigger | **30초 trigger** + `maxOffsetsPerTrigger` 1M |
| Iceberg compaction | 일 1회 03:00 KST | **시간당 1회** (Bronze만), target 256MB |
| Silver MERGE | 일 1회 배치 | **시간당 micro-batch MERGE** (Flink Iceberg sink 검토) |
| Gold 집계 | Athena CTAS | **Materialized View** (StarRocks 또는 Trino MV) |
| 대시보드 | Superset → Athena | **Superset → Trino → Iceberg**, Gold는 StarRocks 동기화 |
| 카탈로그 | Glue Data Catalog | Glue Catalog는 1000x도 견딤 (테이블 한도 백만+) |
| 모니터링 | Prometheus 단일 | Prometheus 페더레이션 + Thanos 장기 저장 |

핵심 통찰: **1000x에서 가장 먼저 깨지는 건 Kafka가 아니라 Silver MERGE INTO**. 시간당 백만 건 MERGE는 Glue DPU로 못 따라간다 → Flink/Spark Structured Streaming의 Iceberg sink로 micro-batch MERGE 전환 필요.

비용 추정 (월):

| 항목 | 현재 | 100x | 1000x |
|---|---|---|---|
| S3 저장 | $1 | $50 | $500 |
| S3 PUT | $0.05 | $5 | $50 |
| Glue DPU | $20 | $200 | $2,000 (또는 EMR/EKS로 절반) |
| Athena 스캔 | $5 | $30 | $200 (또는 Trino + EMR) |
| 합계 | **~$25** | **~$285** | **~$2,750** |

---

## Iceberg Management 자동화

소파일 누적과 스냅샷 증가는 쿼리 성능 저하의 주요 원인. Airflow `propberg_mgmt` DAG가 매일 새벽 03:00 KST에 아래 순서로 자동 실행:

1. **rewrite_manifests** — 매니페스트 정리 (스캔 가속)
2. **compaction** (`rewrite_data_files`) — 소파일을 128MB 단위로 병합
3. **expire_snapshots** — 7일 이상 스냅샷 제거 (최근 3개 보존)
4. **remove_orphan_files** — 3일 이상 미사용 파일 삭제

**우선순위 (조건.txt 평가포인트)**: Bronze가 가장 먼저 — 스트리밍 1분 trigger로 소파일이 가장 빠르게 누적되는 레이어이기 때문. 그 다음 Silver (MERGE 시 새 파일 생성), 마지막 Gold (overwrite 위주라 빈도 낮아도 됨).

---

## 운영 가시성

별도 문서 [observability.md](observability.md) 참고. 핵심 SLO:
- Bronze 처리 지연 p95 < 5분
- Consumer lag p95 < 1,000
- Silver/Gold 배치 매일 07:30 KST 이전 완료

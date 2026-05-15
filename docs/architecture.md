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

## 100× 스케일 대응

현재 일 30,000건 수준. 100배(일 3M)로 확장 시:

| 병목 | 원인 | 대응 |
|---|---|---|
| Bronze 소파일 폭발 | 1분 trigger × 3 topic × 1,440분 = 일 4,320 마이크로배치 | Compaction 자동화 · target 128MB · `min-file-size` 64MB |
| Silver MERGE INTO 느림 | 전체 파티션 스캔 | `sido`/`deal_year` 파티셔닝 + Z-order |
| Gold Athena 스캔 비용 | 전체 테이블 스캔 | Gold 자체가 사전 집계라 OK, 파티션 프루닝만 강제 |
| Kafka 처리 지연 | 단일 파티션 처리 한계 | 파티션 6→24, Consumer Group 분리 |
| Spark Streaming 메모리 | maxOffsetsPerTrigger 초과 | `maxOffsetsPerTrigger=200,000` 상향, executor 메모리 2g→4g |

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

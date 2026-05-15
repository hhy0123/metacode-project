# propberg 운영 가시성 (Observability)

실시간 스트리밍 파이프라인을 운영하려면 "지금 잘 도는가, 어디서 막혔는가"를 즉시 답할 수 있어야 한다. 이 문서는 propberg가 어떤 지표를 어디서 보고, 어디서 어떻게 알람을 받는지 정의한다.

---

## 4-Tier 모니터링 구조

가시성을 한 도구에 몰면 잡음/신호 비율이 깨진다. 역할·주기·청중을 분리한 4-tier 구조를 채택했다.

| Tier | 청중 | 주기 | 도구 | 무엇을 본다 |
|---|---|---|---|---|
| **T1 실시간 인프라** | SRE / 개발자 | 1분 | **Grafana** (localhost:3000) | Kafka 인입 rate, Consumer lag, 컴포넌트 UP/DOWN |
| **T2 파이프라인** | 데이터 엔지니어 | 5–30분 | **Airflow UI** (localhost:8082) | DAG 성공/실패, retry, SLA miss, `bronze_freshness_check` 결과 |
| **T3 비즈니스** | PM / 운영 | 시간/일 | **Superset** (localhost:8088) | Gold 대시보드 — 시도별 거래량, 가격 트렌드, anomaly 비율 |
| **T4 ad-hoc 디버깅** | 데이터 엔지니어 | on-demand | **Athena 콘솔** | `streaming_health` 테이블 직접 쿼리, 데이터 품질 검증 |

각 tier는 **하나 위 tier의 알람을 받아 한 단계 깊이 들어가는 도구**다:
- T1에서 lag 폭증 알람 → T2로 가서 어느 DAG가 지연시키는지 확인 → T4로 가서 Athena 쿼리로 어느 시도/시간대인지 specific하게 확인.

---

## SLI / SLO

| 레이어 | SLI | SLO | 측정 방법 |
|---|---|---|---|
| Producer | 새 row publish 건수 / 폴링 | 신규 데이터가 0건 연속 24h 미만 | `poll done new_rows=N` 로그 + Grafana Kafka topic record count |
| Kafka | 데이터 신선도 (lag) | Consumer lag < 1,000 (p95) | `kafka_consumergroup_lag` (kafka-exporter) |
| Bronze 스트리밍 | end-to-end 지연 | p95 < **5분** | `ingested_at - dealDay` 비교 — Athena 쿼리로 직접 측정 |
| Bronze 스트리밍 | micro-batch 처리 시간 | p95 < **30초** | Spark UI **Structured Streaming 탭** (localhost:4040) → Batch Duration 그래프 |
| Silver 배치 | 일일 완료 시각 | 매일 07:00 KST 이전 | Airflow `propberg_pipeline` 종료 시각 |
| Gold 배치 | 일일 완료 시각 | 매일 07:30 KST 이전 | Airflow `propberg_pipeline` 종료 시각 |
| 매니지먼트 | 매일 성공 | 1회/일 | Airflow `propberg_mgmt` 종료 상태 |

> 참고: Spark Structured Streaming의 micro-batch 메트릭은 Spark JMX/Prometheus 노출이 **driver의 BlockManager/Executor 일반 메트릭만** 포함되어, batch duration / input rate 같은 streaming-specific 메트릭은 Prometheus에서 직접 안 잡힌다. 따라서 Spark UI의 Structured Streaming 탭(localhost:4040) 그래프를 1차 관측 수단으로 사용하고, Grafana는 Kafka 인입/lag/서비스 health 모니터링에 한정한다.

---

## 메트릭 스택

```
Kafka          ─▶ kafka-exporter (9308) ─┐
Spark Driver   ─▶ /metrics/prometheus ───┼─▶ Prometheus (9090) ─▶ Grafana (3000)
                                         │
Spark UI       ─▶ localhost:4040/Structured Streaming (직접 관측)
```

`infra/docker/prometheus.yml`의 scrape job:
- `kafka-exporter` — broker/topic/consumer group 메트릭 (lag, throughput)
- `spark-streaming` — Spark Driver의 일반 JVM/Executor 메트릭

Grafana 대시보드는 `monitoring/grafana/dashboards/propberg_streaming.json`에 IaC로 정의되어 컨테이너 기동 시 자동 import 된다. 단 Spark Structured Streaming의 batch duration / input rate 그래프는 **Spark UI에서 직접 확인**한다 (Prometheus 노출이 streaming query 단위로 안 됨).

---

## 핵심 패널 (Grafana `propberg-streaming` 대시보드)

`monitoring/grafana/dashboards/propberg_streaming.json` 에 정의된 5개 패널.

| # | 패널 | 메트릭 | 의미 |
|---|---|---|---|
| 1 | Kafka topic 인입 rate | `rate(kafka_topic_partition_current_offset[1m])` by topic | 토픽별 새 메시지 인입 속도. 0이면 producer 멈춤 또는 dedupe 결과 새 row 없음 |
| 2 | Consumer group lag | `kafka_consumergroup_lag` by consumergroup, topic | Spark Streaming consumer가 따라잡지 못한 메시지 수. 노란색 임계 1,000 / 빨간색 10,000 |
| 3 | 토픽별 누적 메시지 수 | `kafka_topic_partition_current_offset` 합계 | 토픽이 시작부터 받은 총 메시지 수. 우상향 증가 = streaming 살아있음 |
| 4 | 토픽 partition 수 | `kafka_topic_partitions` | 100x 스케일 시 증설 필요 여부 (현재 3 / 한계 12) |
| 5 | Service health | `up` by job | 모든 scrape target의 UP/DOWN |

> Spark Structured Streaming 자체의 batch duration / input rate는 **Prometheus가 아니라 Spark UI** (`localhost:4040 → Structured Streaming 탭`)에서 본다. Spark JMX는 driver/executor 일반 메트릭만 노출해서 streaming-specific 그래프를 못 잡는다.

---

## 알람 정책

| 알람 | 조건 | 대응 |
|---|---|---|
| Consumer lag 폭증 | `kafka_consumergroup_lag > 10000` 5분 연속 | Spark Streaming 재시작 또는 Kafka 파티션 증설 |
| Topic 인입 정체 | `rate(kafka_topic_partition_current_offset[5m]) == 0` 15분 연속 | Producer 컨테이너 / 외부 API 상태 확인 |
| Producer 실패 | producer 컨테이너 로그에 `poll failed` 5회/10분 | API 키 / 외부 API rate limit 확인 |
| Service down | `up{job=~"kafka-exporter|spark-streaming"} == 0` 2분 연속 | 해당 컨테이너 헬스체크 + 재시작 |
| Bronze 신선도 실패 | Airflow `bronze_freshness_check` sensor timeout | 토픽별 임계 (molit 30분/rone 3시간/kosis 12시간) 확인 + Producer 로그 |
| Silver/Gold 배치 실패 | Airflow task fail | 재실행, 입력 데이터 검증 |
| 매니지먼트 실패 | `propberg_mgmt` DAG fail | CloudWatch Output logs에서 `[mgmt] ... FAIL` 메시지 확인 |

> 발표/MVP 환경에선 PagerDuty 등을 붙이지 않고 Grafana 알람 + Slack webhook으로 충분. 프로덕션 전환 시 OpsGenie/PagerDuty 추가.

---

## 데이터 품질 가시성 (Gold)

`propberg_gold.anomaly_transactions` — ±30% 이탈 거래는 그 자체로 "데이터가 이상한지" 감시하는 지표다.

- 일일 anomaly 비율이 10%를 넘으면 Silver 정제 로직 버그 가능성
- 특정 지역에서만 anomaly가 폭증하면 해당 지역 API 응답 변경 가능성

`propberg_gold.streaming_health` — 스트리밍 윈도우별 record_count, avg_lag_seconds를 Silver MERGE 단계에서 derive해 Athena/Superset에서 추적 가능.

---

## 런북 (간단)

### 시나리오 1: Consumer lag이 급증한다
1. Grafana에서 어떤 토픽인지 확인
2. `docker logs propberg-streaming-consumer` — 에러 여부
3. `docker exec propberg-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups`
4. lag이 단일 파티션에만 몰려있으면 Kafka 파티션 증설, 전반적이면 Spark 리소스 증설

### 시나리오 2: Bronze 적재가 멈췄다
1. `bronze_freshness_check` Airflow 센서가 fail → 알람
2. Streaming consumer 컨테이너 상태 확인
3. S3 권한, AWS_ACCESS_KEY 만료 여부 확인
4. Iceberg 메타데이터 손상 시: `CALL system.rewrite_manifests` 수동 실행

### 시나리오 3: 매니지먼트 DAG이 실패했다
1. `docker logs propberg-airflow` — Glue Job 응답
2. AWS Glue console에서 propberg-mgmt-job 로그 확인
3. compaction은 idempotent — 다음날 자동 복구
4. remove_orphans 실패는 S3 권한 문제일 가능성 高

# propberg 운영 가시성 (Observability)

실시간 스트리밍 파이프라인을 운영하려면 "지금 잘 도는가, 어디서 막혔는가"를 즉시 답할 수 있어야 한다. 이 문서는 propberg가 어떤 지표를 어디서 보고, 어디서 어떻게 알람을 받는지 정의한다.

---

## SLI / SLO

| 레이어 | SLI | SLO | 측정 방법 |
|---|---|---|---|
| Producer | API 폴링 성공률 | 99% (24h) | `propberg-*-producer` 컨테이너 로그의 `poll done` / `poll failed` 비율 |
| Kafka | 데이터 신선도 (lag) | Consumer lag < 1,000 (p95) | `kafka_consumergroup_lag` |
| Bronze 스트리밍 | 처리 지연 (end-to-end) | p95 < **5분** | `ingested_at - trade_ts` |
| Bronze 스트리밍 | 배치 처리 시간 | p95 < **30초** | `spark_streaming_lastCompletedBatch_processingDelay` |
| Silver 배치 | 일일 완료 시각 | 매일 07:00 KST 이전 | Airflow `propberg_pipeline` 종료 시각 |
| Gold 배치 | 일일 완료 시각 | 매일 07:30 KST 이전 | Airflow `propberg_pipeline` 종료 시각 |
| 매니지먼트 | 매일 성공 | 1회/일 | Airflow `propberg_mgmt` 종료 상태 |

---

## 메트릭 스택

```
Kafka  ─▶ kafka-exporter (9308)  ─┐
                                  ├─▶ Prometheus (9090) ─▶ Grafana (3000)
Spark Streaming ─▶ JMX(4040) ─────┘
```

`infra/docker/prometheus.yml`의 scrape job:
- `kafka-exporter` — broker/topic/consumer group 메트릭
- `spark-streaming` — Spark Driver의 `/metrics/executors/prometheus`
- `spark-master` — 클러스터 상태

Grafana 대시보드는 `monitoring/grafana/dashboards/propberg_streaming.json`에 IaC로 정의되어 컨테이너 기동 시 자동 import 된다.

---

## 핵심 패널

1. **Kafka topic record count** — 토픽별 초당 인입 레코드 수
2. **Consumer group lag** — 1,000건 초과 시 노란색, 10,000건 초과 시 빨간색
3. **Spark batch duration** — 1분 trigger 안에 완료되어야 함
4. **Streaming input rate** — 초당 처리 레코드 수
5. **Service health (up)** — 모든 컴포넌트 헬스체크

---

## 알람 정책

| 알람 | 조건 | 대응 |
|---|---|---|
| Consumer lag 폭증 | `lag > 10,000` (5분 연속) | Spark Streaming 재시작 또는 Kafka 파티션 증설 |
| Batch duration 초과 | `batch_duration > 50s` (3분 연속) | 메모리/코어 증설, target-file-size 조정 |
| Producer 실패 | `poll failed > 5회` (10분 내) | API 키 / 외부 API 상태 확인 |
| Iceberg 소파일 폭주 | snapshot 파일 수 > 1,000 | 임시 compaction 수동 실행 |
| Silver/Gold 배치 실패 | Airflow task fail | 재실행, 입력 데이터 검증 |

> 메타코드 부트캠프 환경에선 PagerDuty 등을 붙이지 않고 Grafana 알람 + Slack webhook으로 충분. 프로덕션 전환 시 OpsGenie/PagerDuty 추가.

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

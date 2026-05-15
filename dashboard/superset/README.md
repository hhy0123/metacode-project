# Superset 대시보드 (IaC)

Superset 대시보드/차트/데이터셋 JSON export를 이 디렉토리에 보관합니다.
컨테이너 기동 후 수동으로 import하거나, `superset import-dashboards` CLI로 자동화 가능.

## 폴더 구성

```
dashboard/superset/
├── databases/      # Athena connection 정의
├── datasets/       # Gold 테이블 데이터셋
├── charts/         # 차트 정의 (JSON)
└── dashboards/     # 대시보드 정의 (JSON)
```

## Import 방법

```bash
# Superset 컨테이너 안에서
docker exec -it propberg-superset \
  superset import-dashboards -p /app/dashboard/superset/dashboards/main.zip
```

## 권장 대시보드

1. **부동산 가격 트렌드** — `propberg_gold.monthly_price_trend`
   - 시도별 평단가 라인 차트
   - 지수와 거래량 dual axis
2. **이상 거래 모니터** — `propberg_gold.anomaly_transactions`
   - 일별 anomaly 비율
   - 지역별 분포 히트맵
3. **스트리밍 헬스** — `propberg_gold.streaming_health`
   - 토픽별 처리량
   - 평균 lag 추이

## Athena 데이터소스 연결 문자열

```
awsathena+rest://{aws_access_key}:{aws_secret_key}@athena.us-east-1.amazonaws.com/propberg_gold?s3_staging_dir=s3://propberg-lakehouse-hhy/athena-results/
```

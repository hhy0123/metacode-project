"""propberg 배치 파이프라인 — Silver → Gold.

Bronze 레이어는 `streaming-consumer` 컨테이너가 Kafka에서 직접 Iceberg로 실시간 적재.
이 DAG는 Bronze에 쌓인 데이터를 매일 06:00 KST에 Silver/Gold로 변환.
"""
from datetime import datetime, timedelta, timezone

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

S3_BUCKET = "propberg-lakehouse-hhy"
AWS_REGION = "us-east-1"

# Airflow 2.x는 start_date가 timezone-aware일 때만 schedule_interval(cron)을 그 timezone으로 해석한다.
# pendulum.datetime(..., tz=...)로 만들어야 매일 06:00 KST에 실행됨. 그렇지 않으면 cron이 UTC 기준이 되어 KST 15:00에 실행될 수 있다.
dag = DAG(
    dag_id="propberg_pipeline",
    default_args={
        "owner": "propberg",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
    schedule_interval="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # Iceberg 동시 commit 충돌 방지 + 매일 1회 보장
    tags=["propberg", "pipeline"],
    description="Bronze(streaming)에서 누적된 데이터를 매일 06:00 KST에 Silver → Gold로 변환",
)


def _bronze_freshness_check() -> bool:
    """Bronze 데이터가 최근에 적재됐는지 확인 — 스트리밍 파이프라인이 살아있는지 검증.

    토픽별로 polling 주기가 달라 임계를 차등 적용한다:
    - molit (5분 폴링) → 30분 안에 새 파일 있어야 함
    - rone (1시간 폴링)  → 3시간 안에 있어야 함
    - kosis (6시간 폴링) → 12시간 안에 있어야 함

    S3 list_objects_v2는 알파벳 순이라 단순 MaxKeys=1로는 "최근" 보장 X.
    paginator로 prefix 전체 순회 후 max(LastModified)로 진짜 최근 시각을 찾는다.
    """
    import boto3
    s3 = boto3.client("s3", region_name=AWS_REGION)
    now = datetime.now(timezone.utc)

    thresholds = {
        "bronze/raw_transactions/": timedelta(minutes=30),
        "bronze/raw_price_index/":  timedelta(hours=3),
        "bronze/raw_population/":   timedelta(hours=12),
    }

    paginator = s3.get_paginator("list_objects_v2")
    for prefix, lag_limit in thresholds.items():
        latest = None
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if latest is None or obj["LastModified"] > latest:
                    latest = obj["LastModified"]
        if latest is None:
            print(f"[bronze_freshness] FAIL {prefix}: 객체 없음")
            return False
        lag = now - latest
        if lag > lag_limit:
            print(f"[bronze_freshness] FAIL {prefix}: lag={lag} > 임계={lag_limit}")
            return False
        print(f"[bronze_freshness] OK {prefix}: lag={lag}")
    return True


bronze_health = PythonSensor(
    task_id="bronze_freshness_check",
    python_callable=_bronze_freshness_check,
    poke_interval=60,
    timeout=600,
    mode="reschedule",
    dag=dag,
)

silver_trigger = BashOperator(
    task_id="silver_trigger",
    bash_command=(
        f"aws glue start-job-run --job-name propberg-silver-job "
        f'--arguments \'{{"--s3_bucket":"{S3_BUCKET}"}}\' --region {AWS_REGION}'
    ),
    dag=dag,
)

gold_trigger = BashOperator(
    task_id="gold_trigger",
    bash_command=(
        f"aws glue start-job-run --job-name propberg-gold-job "
        f'--arguments \'{{"--s3_bucket":"{S3_BUCKET}"}}\' --region {AWS_REGION}'
    ),
    dag=dag,
)

bronze_health >> silver_trigger >> gold_trigger

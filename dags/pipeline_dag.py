"""propberg 배치 파이프라인 — Silver → Gold.

Bronze 레이어는 `streaming-consumer` 컨테이너가 Kafka에서 직접 Iceberg로 실시간 적재.
이 DAG는 Bronze에 쌓인 데이터를 매일 06:00 KST에 Silver/Gold로 변환.
"""
from datetime import datetime, timedelta

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
    tags=["propberg", "pipeline"],
    description="Bronze(streaming)에서 누적된 데이터를 매일 06:00 KST에 Silver → Gold로 변환",
)


def _bronze_freshness_check() -> bool:
    """Bronze 최신 스냅샷이 6시간 이내인지 확인 — 스트리밍이 살아있는지 검증."""
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    threshold = datetime.utcnow() - timedelta(hours=6)
    for prefix in ("bronze/raw_transactions/", "bronze/raw_price_index/", "bronze/raw_population/"):
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)
        contents = resp.get("Contents", [])
        if not contents:
            return False
        latest = max(o["LastModified"].replace(tzinfo=None) for o in contents)
        if latest < threshold:
            return False
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

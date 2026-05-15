"""propberg Iceberg 테이블 매니지먼트 DAG.

매일 03:00 KST 자동 실행 — 스트리밍 인제스천이 만든 소파일/스냅샷 누적을 정리한다.

순서:
1. rewrite_manifests   — 매니페스트 정리 (스캔 빠르게)
2. compaction          — 소파일 → 128MB 병합
3. expire_snapshots    — 7일 이상 스냅샷 제거 (최근 3개 보존)
4. remove_orphans      — 3일 이상 미사용 파일 삭제 (시간 단위)

타임존: schedule을 KST 기준으로 해석하기 위해 start_date를 pendulum으로 timezone-aware하게 만든다.
Airflow 2.x는 start_date의 timezone이 timezone-aware일 때만 cron을 그 timezone으로 해석한다.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

S3_BUCKET = "propberg-lakehouse-hhy"
AWS_REGION = "us-east-1"

default_args = {
    "owner": "propberg",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}


def _glue(operation: str) -> str:
    return (
        f"aws glue start-job-run --job-name propberg-mgmt-job "
        f'--arguments \'{{"--operation":"{operation}","--s3_bucket":"{S3_BUCKET}"}}\' '
        f"--region {AWS_REGION}"
    )


with DAG(
    dag_id="propberg_mgmt",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 매일 1회 보장 + 동시 매니지먼트 commit 충돌 방지
    tags=["propberg", "management"],
    description="매일 03:00 KST — 매니페스트/Compaction/스냅샷/Orphan 정리",
) as dag:

    rewrite_manifests = BashOperator(
        task_id="rewrite_manifests",
        bash_command=_glue("rewrite_manifests"),
    )

    compaction = BashOperator(
        task_id="compaction",
        bash_command=_glue("compaction"),
    )

    expire_snapshots = BashOperator(
        task_id="expire_snapshots",
        bash_command=_glue("expire_snapshots"),
    )

    remove_orphans = BashOperator(
        task_id="remove_orphans",
        bash_command=_glue("remove_orphans"),
    )

    rewrite_manifests >> compaction >> expire_snapshots >> remove_orphans

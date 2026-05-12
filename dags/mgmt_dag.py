from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'propberg',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

S3_BUCKET = "propberg-lakehouse-hhy"

with DAG(
    dag_id='propberg_mgmt',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['propberg', 'management'],
) as dag:

    compaction = BashOperator(
        task_id='compaction',
        bash_command=f'aws glue start-job-run --job-name propberg-mgmt-job --arguments \'{{\"--operation\":\"compaction\",\"--s3_bucket\":\"{S3_BUCKET}\"}}\' --region us-east-1',
    )

    expire_snapshots = BashOperator(
        task_id='expire_snapshots',
        bash_command=f'aws glue start-job-run --job-name propberg-mgmt-job --arguments \'{{\"--operation\":\"expire_snapshots\",\"--s3_bucket\":\"{S3_BUCKET}\"}}\' --region us-east-1',
    )

    remove_orphans = BashOperator(
        task_id='remove_orphans',
        bash_command=f'aws glue start-job-run --job-name propberg-mgmt-job --arguments \'{{\"--operation\":\"remove_orphans\",\"--s3_bucket\":\"{S3_BUCKET}\"}}\' --region us-east-1',
    )

    compaction >> expire_snapshots >> remove_orphans
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

S3_BUCKET = 'propberg-lakehouse-hhy'

dag = DAG(
    dag_id='propberg_pipeline',
    default_args={
        'owner': 'propberg',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['propberg', 'pipeline'],
)

molit_producer = BashOperator(task_id='molit_producer', bash_command='echo molit', dag=dag)
rone_producer = BashOperator(task_id='rone_producer', bash_command='echo rone', dag=dag)
kosis_producer = BashOperator(task_id='kosis_producer', bash_command='echo kosis', dag=dag)
bronze_trigger = BashOperator(task_id='bronze_trigger', bash_command='echo bronze', dag=dag)
silver_trigger = BashOperator(task_id='silver_trigger', bash_command='echo silver', dag=dag)
gold_trigger = BashOperator(task_id='gold_trigger', bash_command='echo gold', dag=dag)

[molit_producer, rone_producer, kosis_producer] >> bronze_trigger >> silver_trigger >> gold_trigger

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ex6_smoke_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    BashOperator(
        task_id="hello",
        bash_command="echo Airflow is running!",
    )

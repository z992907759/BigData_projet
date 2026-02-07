from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

SOURCE_PATH = r"C:\Users\73835\Downloads\BigData_projet-main (1)\BigData_projet-main"
TARGET_PATH = "/workspace"

with DAG(
    dag_id="ex6_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ex5_demo = DockerOperator(
        task_id="ex5_demo",
        image="python:3.11-slim",
        auto_remove=True,
        docker_url="tcp://host.docker.internal:2375",
        network_mode="bridge",
        mounts=[
            Mount(
                source=SOURCE_PATH,
                target=TARGET_PATH,
                type="bind"
            )
        ],
        command=(
            "bash -lc "
            f"'cd {TARGET_PATH}/ex05_ml_prediction_service "
            "&& chmod +x demo.sh "
            "&& ./demo.sh'"
        ),
    )

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_postgres_etl",
    start_date=datetime(2026, 3, 1),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["retail", "postgres", "etl", "s3"]
) as dag:

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="cd /opt/airflow && python -m src.main"
    )

    run_tests = BashOperator(
        task_id="run_tests",
        bash_command="cd /opt/airflow && export PYTHONPATH=/opt/airflow && pytest tests/"
    )

    run_pipeline >> run_tests
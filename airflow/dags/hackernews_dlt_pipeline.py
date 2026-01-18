"""
Airflow DAG for HackerNews dlt pipeline.

This DAG runs the HackerNews data extraction and loads it into ClickHouse.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hackernews_dlt_pipeline',
    default_args=default_args,
    description='Extract HackerNews data and load into ClickHouse',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dlt', 'hackernews', 'clickhouse'],
) as dag:
    
    # Task to run the dlt pipeline
    # Note: The dlt pipeline script needs to be run from the dlt directory
    # and requires the .dlt/secrets.toml file to be accessible
    run_pipeline = BashOperator(
        task_id='run_hackernews_pipeline',
        bash_command='cd /opt/airflow/dlt/hacker-news && python hackernews-load.py',
        env={
            'PYTHONPATH': '/opt/airflow/dlt:/opt/airflow',
        },
    )

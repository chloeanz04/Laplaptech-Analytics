from datetime import datetime

from airflow import DAG
from airflow.sdk import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

with DAG(
    dag_id="test_bigquery",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def test_connection():
        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        result = hook.get_first("SELECT 1 AS test")
        print(f"BigQuery result: {result[0]}")

    test_connection()
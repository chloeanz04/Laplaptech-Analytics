from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

with DAG(
    dag_id="test_clickhouse",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False
) as dag:

    @task
    def test_connection():
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_laplap")
        result = hook.get_first(
            "SELECT count(*) FROM laplaptech.user_event_tracking"
        )
        print(f"Total rows: {result[0]}")

    test_connection()
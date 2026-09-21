from datetime import datetime

from airflow import DAG
from airflow.sdk import task

from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from google.cloud import bigquery


PROJECT_ID = "laplap-analytics"

DATASET_ID = "laplap_analytics"
TABLE_ID = "fact_user_event_tracking"

FULL_TABLE_ID = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    f"{TABLE_ID}"
)


BQ_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("event_name", "STRING"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("event_data", "STRING"),
    bigquery.SchemaField("device", "STRING"),
    bigquery.SchemaField(
        "event_local_timestamp",
        "INTEGER"
    ),
    bigquery.SchemaField(
        "event_received_on_server_timestamp",
        "INTEGER"
    ),
    bigquery.SchemaField(
        "session_id",
        "STRING"
    ),
    bigquery.SchemaField(
        "user_pseudo_id",
        "STRING"
    ),
    bigquery.SchemaField(
        "app_version",
        "STRING"
    ),
    bigquery.SchemaField(
        "elton_created_at",
        "TIMESTAMP"
    ),
]


with DAG(
    dag_id="clickhouse_to_bigquery_test",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=[ "clickhouse", "bigquery"]
) as dag:
    @task
    def create_bq_table():
        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        client = hook.get_client()
        dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"

        client.create_dataset(dataset, exists_ok=True)

        print(f"Dataset ready: {dataset_id}")

        table = bigquery.Table(FULL_TABLE_ID, schema=BQ_SCHEMA)

        client.create_table(table,exists_ok=True)
        print(f"Table ready: {FULL_TABLE_ID}")

    @task
    def extract_clickhouse():

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_laplap")

        sql = """
        SELECT
            id,
            event_name,
            user_id,
            event_data,
            device,
            event_local_timestamp,
            event_received_on_server_timestamp,
            session_id,
            user_psuedo_id,
            app_version,
            elton_created_at

        FROM laplaptech.user_event_tracking

        LIMIT 100
        """

        rows = hook.get_records(sql)
        print(f"Extracted rows: {len(rows)}")

        return rows

    @task
    def load_bigquery(rows):
        hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        client = hook.get_client()
        records = []

        for row in rows:
            records.append(
                {
                    "id": row[0],
                    "event_name": row[1],
                    "user_id": str(row[2]) if row[2] is not None else None,
                    "event_data": row[3],
                    "device": row[4],
                    "event_local_timestamp": row[5],
                    "event_received_on_server_timestamp": row[6],
                    "session_id": row[7],
                    "user_pseudo_id": row[8],
                    "app_version": row[9],
                    "elton_created_at": str(row[10])
                }
            )

        if not records:
            print("No rows to load")
            return

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

        job = client.load_table_from_json(
            records,
            FULL_TABLE_ID,
            job_config=job_config
        )

        job.result()

        print(f"Inserted {len(records)} rows")

    table = create_bq_table()
    data = extract_clickhouse()
    load = load_bigquery(data)

    table >> data >> load
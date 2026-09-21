from datetime import datetime, timedelta
import os
import re
import tempfile

import pyarrow.compute as pc
import pyarrow.parquet as pq

from airflow import DAG
from airflow.models import Variable
from airflow.sdk import task

from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery


PROJECT_ID = "laplap-analytics"
DATASET_ID = "laplap_analytics"
CLICKHOUSE_DATABASE = "laplaptech"
BQ_LOCATION = "US"

# Dimensions use a small lookback because duplicates are harmless there:
# the latest-record view collapses them by primary key.
DIMENSION_LOOKBACK_MINUTES = 10

# Event rows are append-only and should not be reloaded with a lookback,
# otherwise the same events can be counted twice downstream.
EVENT_LOOKBACK_MINUTES = 0


TABLE_CONFIGS = {
    "user_event_tracking": {
        "target_table": "bronze_user_event_tracking",
        "mode": "append",
        "watermark_expression": "elton_created_at",
        "watermark_column": "elton_created_at",
        "watermark_variable": "laplap_user_event_tracking_watermark",
        "lookback_minutes": EVENT_LOOKBACK_MINUTES,
        "partition_field": "elton_created_at",
        "cluster_fields": ["user_psuedo_id", "session_id"],
        "latest_view": None,
        "primary_key": None,
    },
    "brand": {
        "target_table": "bronze_brand_raw",
        "mode": "append",
        "watermark_expression": """
            greatest(
                ifNull(created_on, toDateTime64('1970-01-01 00:00:00', 3)),
                ifNull(changed_on, toDateTime64('1970-01-01 00:00:00', 3))
            )
        """,
        "watermark_column": "changed_on",
        "watermark_variable": "laplap_brand_watermark",
        "lookback_minutes": DIMENSION_LOOKBACK_MINUTES,
        "partition_field": None,
        "cluster_fields": ["id"],
        "latest_view": "vw_brand_latest",
        "primary_key": "id",
    },
    "cpu_model": {
        "target_table": "bronze_cpu_model_raw",
        "mode": "append",
        "watermark_expression": """
            greatest(
                ifNull(created_on, toDateTime64('1970-01-01 00:00:00', 3)),
                ifNull(changed_on, toDateTime64('1970-01-01 00:00:00', 3))
            )
        """,
        "watermark_column": "changed_on",
        "watermark_variable": "laplap_cpu_model_watermark",
        "lookback_minutes": DIMENSION_LOOKBACK_MINUTES,
        "partition_field": None,
        "cluster_fields": ["id"],
        "latest_view": "vw_cpu_model_latest",
        "primary_key": "id",
    },
    "gpu_model": {
        "target_table": "bronze_gpu_model_raw",
        "mode": "append",
        "watermark_expression": """
            greatest(
                ifNull(created_on, toDateTime64('1970-01-01 00:00:00', 3)),
                ifNull(changed_on, toDateTime64('1970-01-01 00:00:00', 3))
            )
        """,
        "watermark_column": "changed_on",
        "watermark_variable": "laplap_gpu_model_watermark",
        "lookback_minutes": DIMENSION_LOOKBACK_MINUTES,
        "partition_field": None,
        "cluster_fields": ["id"],
        "latest_view": "vw_gpu_model_latest",
        "primary_key": "id",
    },
    "laptop_model": {
        "target_table": "bronze_laptop_model_raw",
        "mode": "append",
        "watermark_expression": """
            greatest(
                ifNull(created_on, toDateTime64('1970-01-01 00:00:00', 3)),
                ifNull(changed_on, toDateTime64('1970-01-01 00:00:00', 3))
            )
        """,
        "watermark_column": "changed_on",
        "watermark_variable": "laplap_laptop_model_watermark",
        "lookback_minutes": DIMENSION_LOOKBACK_MINUTES,
        "partition_field": None,
        "cluster_fields": ["id"],
        "latest_view": "vw_laptop_model_latest",
        "primary_key": "id",
    },
    "laptop_benchmark_result": {
        "target_table": "bronze_laptop_benchmark_result",
        "mode": "replace",
        "watermark_expression": None,
        "watermark_column": None,
        "watermark_variable": None,
        "lookback_minutes": 0,
        "partition_field": None,
        "cluster_fields": ["laptop_model_id"],
        "latest_view": None,
        "primary_key": None,
    },
}


def bq_table_id(table_name):
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def sanitize_identifier(value):
    value = re.sub(r"[^a-zA-Z0-9_-]", "_", value)[:100]
    if not value or not re.match(r"^[A-Za-z_]", value):
        value = f"job_{value}"
    return value


def strip_nullable(clickhouse_type):
    while clickhouse_type.startswith("Nullable("):
        clickhouse_type = clickhouse_type[len("Nullable("):-1]
    return clickhouse_type


def strip_low_cardinality(clickhouse_type):
    while clickhouse_type.startswith("LowCardinality("):
        clickhouse_type = clickhouse_type[len("LowCardinality("):-1]
    return clickhouse_type


def clickhouse_type_to_bq(clickhouse_type):
    data_type = strip_low_cardinality(strip_nullable(clickhouse_type))

    if data_type.startswith("DateTime"):
        return "TIMESTAMP"
    if data_type in ("Date", "Date32"):
        return "DATE"
    if data_type.startswith(("Int", "UInt")):
        return "INT64"
    if data_type.startswith("Float"):
        return "FLOAT64"
    if data_type == "Bool":
        return "BOOL"
    if data_type.startswith("Decimal"):
        return "NUMERIC"
    if data_type in ("String", "FixedString", "UUID", "IPv4", "IPv6"):
        return "STRING"
    if data_type.startswith("Enum"):
        return "STRING"

    raise ValueError(f"Unsupported ClickHouse type: {clickhouse_type}")


def get_clickhouse_schema(clickhouse_hook, source_table):
    rows = clickhouse_hook.get_records(
        f"DESCRIBE TABLE {CLICKHOUSE_DATABASE}.{source_table}"
    )

    return [
        bigquery.SchemaField(
            row[0],
            clickhouse_type_to_bq(row[1]),
            mode="NULLABLE",
        )
        for row in rows
    ]


def ensure_dataset(client):
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset already exists: {dataset_id}")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset)
        print(f"Created dataset: {dataset_id}")


def ensure_target_table(client, table_id, schema, partition_field=None, cluster_fields=None):
    try:
        table = client.get_table(table_id)
        existing = {field.name for field in table.schema}
        missing = [field for field in schema if field.name not in existing]

        if missing:
            table.schema = list(table.schema) + missing
            client.update_table(table, ["schema"])
            print(f"Added {len(missing)} new fields to {table_id}")
        return
    except NotFound:
        pass

    table = bigquery.Table(table_id, schema=schema)

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )

    if cluster_fields:
        table.clustering_fields = cluster_fields

    client.create_table(table)
    print(f"Created table: {table_id}")


def get_watermark(variable_name):
    if not variable_name:
        return None

    value = Variable.get(variable_name, default_var=None)
    return datetime.fromisoformat(value) if value else None


def set_watermark(variable_name, value):
    if not variable_name or value is None:
        return

    Variable.set(variable_name, value.isoformat())


def format_clickhouse_timestamp(value):
    if value.tzinfo is not None:
        value = value.replace(tzinfo=None)
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def build_extract_query(source_table, schema, watermark_expression, watermark, lookback_minutes):
    columns = ",\n        ".join(f"`{field.name}`" for field in schema)

    query = f"""
    SELECT
        {columns}
    FROM
        {CLICKHOUSE_DATABASE}.{source_table}
    """

    if watermark_expression is None or watermark is None:
        return query

    if lookback_minutes > 0:
        cutoff = watermark - timedelta(minutes=lookback_minutes)
        operator = ">="
    else:
        cutoff = watermark
        operator = ">"

    cutoff_string = format_clickhouse_timestamp(cutoff)

    query += f"""
    WHERE
        {watermark_expression}
        {operator}
        toDateTime64('{cutoff_string}', 3)
    """

    return query


def stream_to_parquet(clickhouse_hook, query, watermark_column=None):
    client = clickhouse_hook.get_client()
    file_handle = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    file_path = file_handle.name
    file_handle.close()

    writer = None
    row_count = 0
    max_watermark = None

    try:
        with client.query_arrow_stream(query) as stream:
            for batch in stream:
                if writer is None:
                    writer = pq.ParquetWriter(
                        file_path,
                        batch.schema,
                        compression="zstd",
                    )

                writer.write_batch(batch)
                row_count += batch.num_rows

                if watermark_column and watermark_column in batch.column_names:
                    batch_max = pc.max(batch.column(watermark_column)).as_py()
                    if batch_max is not None and (
                        max_watermark is None or batch_max > max_watermark
                    ):
                        max_watermark = batch_max
    finally:
        if writer is not None:
            writer.close()
        client.close()

    return file_path, row_count, max_watermark


def _submit_load_job(client, file_path, table_id, job_config, job_id):
    file_size = os.path.getsize(file_path)

    try:
        with open(file_path, "rb") as file_obj:
            return client.load_table_from_file(
                file_obj,
                table_id,
                rewind=True,
                size=file_size,
                job_config=job_config,
                job_id=job_id,
                location=BQ_LOCATION,
            )
    except Conflict:
        existing_job = client.get_job(job_id, location=BQ_LOCATION)
        try:
            existing_job.result()
            print(f"Reusing existing successful load job: {job_id}")
            return existing_job
        except Exception as error:
            print(f"Existing load job {job_id} failed: {error}")
            return None


def load_parquet_to_bigquery(
    client,
    file_path,
    table_id,
    write_disposition,
    job_id,
    allow_field_addition=False,
):
    schema_update_options = (
        [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        if allow_field_addition
        else None
    )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema_update_options=schema_update_options,
    )

    load_job = _submit_load_job(
        client,
        file_path,
        table_id,
        job_config,
        job_id,
    )

    if load_job is None:
        load_job = _submit_load_job(
            client,
            file_path,
            table_id,
            job_config,
            f"{job_id}_retry",
        )

    if load_job is None:
        raise RuntimeError(f"Could not submit/reuse load job for {table_id}")

    load_job.result()
    print(f"BigQuery load completed: {table_id} (job_id={load_job.job_id})")


def create_latest_view(client, raw_table_id, view_name, primary_key):
    view_id = bq_table_id(view_name)

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY `{primary_key}`
                ORDER BY
                    `changed_on` DESC,
                    `created_on` DESC,
                    `elton_created_at` DESC
            ) AS rn
        FROM `{raw_table_id}`
    )
    SELECT * EXCEPT(rn)
    FROM ranked
    WHERE rn = 1
    """

    client.query(query, location=BQ_LOCATION).result()
    print(f"Created/replaced latest view: {view_id}")


@task
def setup_bigquery():
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = hook.get_client(project_id=PROJECT_ID)
    ensure_dataset(client)
    print("BigQuery ingestion infrastructure ready")


def build_ingestion_task(source_table, config):
    @task(task_id=f"ingest_{source_table}")
    def ingest():
        clickhouse_hook = ClickHouseHook(
            clickhouse_conn_id="clickhouse_laplap"
        )
        bigquery_hook = BigQueryHook(
            gcp_conn_id="google_cloud_default"
        )
        bq_client = bigquery_hook.get_client(
            project_id=PROJECT_ID
        )

        target_table_id = bq_table_id(config["target_table"])
        mode = config["mode"]

        print(
            f"Starting ingestion: {source_table} → {target_table_id}"
        )

        schema = get_clickhouse_schema(
            clickhouse_hook,
            source_table,
        )

        ensure_target_table(
            client=bq_client,
            table_id=target_table_id,
            schema=schema,
            partition_field=config["partition_field"],
            cluster_fields=config["cluster_fields"],
        )

        watermark = get_watermark(config["watermark_variable"])
        print(f"Current watermark: {watermark}")

        query = build_extract_query(
            source_table=source_table,
            schema=schema,
            watermark_expression=config["watermark_expression"],
            watermark=watermark,
            lookback_minutes=config["lookback_minutes"],
        )

        print(f"Extract query:\n{query}")

        file_path = None

        try:
            file_path, row_count, extracted_max_watermark = stream_to_parquet(
                clickhouse_hook=clickhouse_hook,
                query=query,
                watermark_column=config["watermark_column"],
            )

            print(f"Extracted rows: {row_count}")

            if row_count > 0:
                run_id = sanitize_identifier(
                    os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual_run")
                )
                job_id = sanitize_identifier(
                    f"laplap_{source_table}_{run_id}"
                )

                if mode == "replace":
                    load_parquet_to_bigquery(
                        client=bq_client,
                        file_path=file_path,
                        table_id=target_table_id,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        job_id=job_id,
                    )
                else:
                    load_parquet_to_bigquery(
                        client=bq_client,
                        file_path=file_path,
                        table_id=target_table_id,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                        job_id=job_id,
                        allow_field_addition=True,
                    )

                if mode == "append" and extracted_max_watermark is not None:
                    set_watermark(
                        config["watermark_variable"],
                        extracted_max_watermark,
                    )
                    print(
                        f"Updated Airflow watermark: {extracted_max_watermark}"
                    )
            else:
                print(f"No new data for {source_table}")

            if config["latest_view"]:
                create_latest_view(
                    client=bq_client,
                    raw_table_id=target_table_id,
                    view_name=config["latest_view"],
                    primary_key=config["primary_key"],
                )

            print(
                f"Successfully processed {source_table}; "
                f"rows extracted = {row_count}"
            )

        finally:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)

    return ingest()


with DAG(
    dag_id="laplap_daily_ingestion_free",
    start_date=datetime(2026, 9, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=[
        "laplap",
        "ingestion",
        "clickhouse",
        "bigquery",
        "free-tier",
    ],
) as dag:
    setup = setup_bigquery()

    for source_table, config in TABLE_CONFIGS.items():
        task_instance = build_ingestion_task(
            source_table,
            config,
        )
        setup >> task_instance
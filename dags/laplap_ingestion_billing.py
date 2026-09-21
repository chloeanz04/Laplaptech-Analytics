# from datetime import datetime, timedelta, timezone
# from decimal import Decimal
# import hashlib
# import os
# import re
# import tempfile

# import pyarrow.parquet as pq

# from airflow import DAG
# from airflow.sdk import task

# from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
# from google.cloud import bigquery
# from google.api_core.exceptions import NotFound


# PROJECT_ID = "laplap-analytics"
# DATASET_ID = "laplap_analytics"
# CLICKHOUSE_DATABASE = "laplaptech"

# BQ_LOCATION = "US"

# CONTROL_TABLE = "ingestion_control"

# LOOKBACK_MINUTES = 10


# TABLE_CONFIGS = {
#     "user_event_tracking": {
#         "target_table": "bronze_user_event_tracking",
#         "mode": "merge",
#         "watermark_expression": "elton_created_at",
#         "watermark_column": "elton_created_at",
#         "partition_field": "elton_created_at",
#         "cluster_fields": [
#             "user_psuedo_id",
#             "session_id",
#         ],
#         "dedupe_order": "elton_created_at DESC",
#     },

#     "brand": {
#         "target_table": "bronze_brand",
#         "mode": "merge",
#         "watermark_expression": """
#             greatest(
#                 ifNull(
#                     created_on,
#                     toDateTime64('1970-01-01 00:00:00', 3)
#                 ),
#                 ifNull(
#                     changed_on,
#                     toDateTime64('1970-01-01 00:00:00', 3)
#                 )
#             )
#         """,
#         "watermark_column": "changed_on",
#         "partition_field": None,
#         "cluster_fields": ["id"],
#         "dedupe_order": "changed_on DESC, created_on DESC",
#     },

#     "cpu_model": {
#         "target_table": "bronze_cpu_model",
#         "mode": "merge",
#         "watermark_expression": """
#             greatest(
#                 ifNull(
#                     created_on,
#                     toDateTime64('1970-01-01 00:00:00', 3)
#                 ),
#                 ifNull(
#                     changed_on,
#                     toDateTime64('1970-01-01 00:00:00', 3)
#                 )
#             )
#         """,
#         "watermark_column": "changed_on",
#         "partition_field": None,
#         "cluster_fields": ["id"],
#         "dedupe_order": "changed_on DESC, created_on DESC",
#     },

#     "gpu_model": {
#         "target_table": "bronze_gpu_model",
#         "mode": "merge",
#         "watermark_expression": """
#             greatest(
#                 ifNull(
#                     created_on,
#                     toDateTime64('1970-01-01 00:00:00', 3)
#                 ),
#                 ifNull(
#                     changed_on,
#                     toDateTime64('1970-01-01 00:00:00', 3)
#                 )
#             )
#         """,
#         "watermark_column": "changed_on",
#         "partition_field": None,
#         "cluster_fields": ["id"],
#         "dedupe_order": "changed_on DESC, created_on DESC",
#     },

#     "laptop_model": {
#         "target_table": "bronze_laptop_model",
#         "mode": "merge",
#         "watermark_expression": """
#             greatest(
#                 ifNull(
#                     created_on,
#                     toDateTime64('1970-01-01 00:00:00', 3)
#                 ),
#                 ifNull(
#                     changed_on,
#                     toDateTime64('1970-01-01 00:00:00', 3)
#                 )
#             )
#         """,
#         "watermark_column": "changed_on",
#         "partition_field": None,
#         "cluster_fields": ["id"],
#         "dedupe_order": "changed_on DESC, created_on DESC",
#     },

#     "laptop_benchmark_result": {
#         "target_table": "bronze_laptop_benchmark_result",
#         "mode": "replace",
#         "watermark_expression": None,
#         "watermark_column": None,
#         "partition_field": None,
#         "cluster_fields": ["laptop_model_id"],
#         "dedupe_order": "id DESC",
#     },
# }

# def utc_now():
#     return datetime.now(timezone.utc)


# def bq_table_id(table_name):
#     return (
#         f"{PROJECT_ID}."
#         f"{DATASET_ID}."
#         f"{table_name}"
#     )


# def sanitize_identifier(value):
#     value = re.sub(
#         r"[^a-zA-Z0-9_]",
#         "_",
#         value
#     )

#     return value[:100]


# def make_staging_table_id(
#     target_table,
#     run_id
# ):
#     digest = hashlib.md5(
#         f"{run_id}_{target_table}".encode()
#     ).hexdigest()[:12]

#     staging_name = (f"_stg_{target_table}_{digest}")

#     return bq_table_id(staging_name)


# def strip_nullable(clickhouse_type):
#     while clickhouse_type.startswith("Nullable("):
#         clickhouse_type = clickhouse_type[len("Nullable("):-1]
#     return clickhouse_type


# def strip_low_cardinality(clickhouse_type):
#     while clickhouse_type.startswith("LowCardinality("):
#         clickhouse_type = clickhouse_type[len("LowCardinality("):-1]
#     return clickhouse_type


# def clickhouse_type_to_bq(clickhouse_type):
#     data_type = strip_nullable(clickhouse_type)
#     data_type = strip_low_cardinality(data_type)

#     if data_type.startswith("DateTime"):
#         return "TIMESTAMP"

#     if data_type in ("Date", "Date32",):
#         return "DATE"

#     if data_type.startswith("Int"):
#         return "INT64"

#     if data_type.startswith("UInt"):
#         return "INT64"

#     if data_type.startswith("Float"):
#         return "FLOAT64"

#     if data_type == "Bool":
#         return "BOOL"

#     if data_type.startswith("Decimal"):
#         return "NUMERIC"

#     if data_type in ("String", "FixedString", "UUID", "IPv4",  "IPv6",):
#         return "STRING"

#     if data_type.startswith("Enum"):
#         return "STRING"

#     raise ValueError(
#         f"Unsupported ClickHouse type: "
#         f"{clickhouse_type}"
#     )


# def get_clickhouse_schema(clickhouse_hook, source_table):
#     rows = clickhouse_hook.get_records(
#         f"""
#         DESCRIBE TABLE
#         {CLICKHOUSE_DATABASE}.{source_table}
#         """
#     )

#     schema = []

#     for row in rows:
#         column_name = row[0]
#         clickhouse_type = row[1]

#         bq_type = clickhouse_type_to_bq(clickhouse_type)

#         schema.append(
#             bigquery.SchemaField(
#                 column_name,
#                 bq_type,
#                 mode="NULLABLE"
#             )
#         )

#     return schema


# def ensure_dataset(client):
#     dataset_id = (
#         f"{PROJECT_ID}.{DATASET_ID}"
#     )

#     try:

#         client.get_dataset(dataset_id)

#         print(
#             f"Dataset already exists: "
#             f"{dataset_id}"
#         )

#     except Exception:

#         dataset = bigquery.Dataset(dataset_id)

#         dataset.location = BQ_LOCATION

#         client.create_dataset(dataset)

#         print(
#             f"Created dataset: "
#             f"{dataset_id}"
#         )


# def ensure_control_table(client):
#     table_id = bq_table_id(CONTROL_TABLE)

#     schema = [
#         bigquery.SchemaField(
#             "source_table",
#             "STRING",
#             mode="REQUIRED"
#         ),
#         bigquery.SchemaField(
#             "target_table",
#             "STRING",
#             mode="REQUIRED"
#         ),
#         bigquery.SchemaField(
#             "watermark_value",
#             "TIMESTAMP"
#         ),
#         bigquery.SchemaField(
#             "last_success_at",
#             "TIMESTAMP"
#         ),
#         bigquery.SchemaField(
#             "last_rows_loaded",
#             "INT64"
#         ),
#         bigquery.SchemaField(
#             "status",
#             "STRING"
#         ),
#         bigquery.SchemaField(
#             "updated_at",
#             "TIMESTAMP"
#         ),
#     ]

#     table = bigquery.Table(table_id, schema=schema)

#     client.create_table(table, exists_ok=True)


# def ensure_target_table(client, table_id, schema, partition_field=None, cluster_fields=None):
#     try:
#         table = client.get_table(table_id)

#         existing_fields = {field.name: field for field in table.schema}

#         missing_fields = [field for field in schema if field.name not in existing_fields]

#         if missing_fields:
#             table.schema = list(table.schema) + missing_fields

#             client.update_table(table, ["schema"])

#             print(f"Added {len(missing_fields)} new fields to {table_id}")
#         return

#     except NotFound:
#         pass

#     table = bigquery.Table(table_id, schema=schema)

#     if partition_field:
#         table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field=partition_field)

#     if cluster_fields:
#         table.clustering_fields = cluster_fields

#     client.create_table(table)

#     print(f"Created table: {table_id}")


# def get_watermark(client, source_table):
#     table_id = bq_table_id(CONTROL_TABLE)

#     query = f"""
#     SELECT watermark_value
#     FROM `{table_id}`
#     WHERE source_table = @source_table
#     LIMIT 1
#     """

#     job_config = bigquery.QueryJobConfig(
#         query_parameters=[
#             bigquery.ScalarQueryParameter(
#                 "source_table",
#                 "STRING",
#                 source_table
#             )
#         ]
#     )

#     result = client.query(query, job_config=job_config).result()

#     row = next(iter(result), None)

#     if row is None:
#         return None

#     return row.watermark_value


# def update_control(client, source_table, target_table, watermark_value, rows_loaded, status, run_timestamp):
#     table_id = bq_table_id(CONTROL_TABLE)

#     query = f"""
#     MERGE `{table_id}` AS target

#     USING (
#         SELECT
#             @source_table AS source_table,
#             @target_table AS target_table,
#             @watermark_value AS watermark_value,
#             @last_success_at AS last_success_at,
#             @last_rows_loaded AS last_rows_loaded,
#             @status AS status,
#             @updated_at AS updated_at
#     ) AS source

#     ON target.source_table = source.source_table

#     WHEN MATCHED THEN
#         UPDATE SET
#             target.target_table = source.target_table,
#             target.watermark_value = source.watermark_value,
#             target.last_success_at = source.last_success_at,
#             target.last_rows_loaded = source.last_rows_loaded,
#             target.status = source.status,
#             target.updated_at = source.updated_at

#     WHEN NOT MATCHED THEN
#         INSERT (
#             source_table,
#             target_table,
#             watermark_value,
#             last_success_at,
#             last_rows_loaded,
#             status,
#             updated_at
#         )
#         VALUES (
#             source.source_table,
#             source.target_table,
#             source.watermark_value,
#             source.last_success_at,
#             source.last_rows_loaded,
#             source.status,
#             source.updated_at
#         )
#     """

#     job_config = bigquery.QueryJobConfig(
#         query_parameters=[
#             bigquery.ScalarQueryParameter("source_table", "STRING", source_table),
#             bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
#             bigquery.ScalarQueryParameter("watermark_value", "TIMESTAMP", watermark_value),
#             bigquery.ScalarQueryParameter("last_success_at", "TIMESTAMP", run_timestamp),
#             bigquery.ScalarQueryParameter("last_rows_loaded", "INT64", rows_loaded),
#             bigquery.ScalarQueryParameter("status", "STRING", status),
#             bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", run_timestamp),
#         ]
#     )

#     client.query(query, job_config=job_config).result()


# def build_extract_query(source_table,  schema, watermark_expression, watermark, mode):
#     columns = ",\n            ".join(f"`{field.name}`" for field in schema)

#     query = f"""
#     SELECT
#         {columns}
#     FROM
#         {CLICKHOUSE_DATABASE}.{source_table}
#     """

#     if mode == "replace":
#         return query

#     if watermark is None:
#         return query

#     lookback_time = watermark - timedelta( minutes=LOOKBACK_MINUTES)
#     watermark_string = lookback_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

#     query += f"""
#     WHERE
#         {watermark_expression}
#         >= toDateTime64(
#             '{watermark_string}',
#             3
#         )
#     """

#     return query


# def get_source_max_watermark(clickhouse_hook, source_table, watermark_expression, watermark):
#     if watermark_expression is None:
#         return None

#     query = f"""
#     SELECT
#         max(
#             {watermark_expression}
#         )
#     FROM
#         {CLICKHOUSE_DATABASE}.{source_table}
#     """

#     params = {}

#     if watermark is not None:
#         lookback_time = watermark - timedelta(minutes=LOOKBACK_MINUTES)

#         watermark_string = lookback_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

#         query += f"""
#         WHERE
#             {watermark_expression}
#             >= toDateTime64(
#                 '{watermark_string}',
#                 3
#             )
#         """

#     result = clickhouse_hook.get_first(query, parameters=params)

#     if not result:
#         return None

#     return result[0]


# def stream_to_parquet(clickhouse_hook, query):
#     client = clickhouse_hook.get_client()

#     file_handle = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)

#     file_path = file_handle.name

#     file_handle.close()

#     writer = None
#     row_count = 0

#     try:
#         with client.query_arrow_stream(query) as stream:
#             for batch in stream:
#                 if writer is None:
#                     writer = pq.ParquetWriter(file_path, batch.schema, compression="zstd")
#                 writer.write_batch(batch)
#                 row_count += (batch.num_rows)

#     finally:
#         if writer is not None:
#             writer.close()
#         client.close()

#     return file_path, row_count


# def load_parquet_to_bigquery(client, file_path, table_id, write_disposition):
#     job_config = bigquery.LoadJobConfig(source_format=(bigquery.SourceFormat.PARQUET), write_disposition=write_disposition)

#     file_size = os.path.getsize(file_path)

#     with open(file_path, "rb") as file_obj:
#         load_job = (
#             client.load_table_from_file(
#                 file_obj,
#                 table_id,
#                 rewind=True,
#                 size=file_size,
#                 job_config=job_config
#             )
#         )

#     load_job.result()


# def merge_staging_to_target(client, staging_table_id, target_table_id, columns, dedupe_order):
#     update_columns = [column for column in columns if column != "id"]

#     update_sql = ",\n            ".join(
#         f"target.`{column}` = source.`{column}`"
#         for column in update_columns
#     )

#     insert_columns = ", ".join(
#         f"`{column}`"
#         for column in columns
#     )

#     insert_values = ", ".join(
#         f"source.`{column}`"
#         for column in columns
#     )

#     query = f"""
#     MERGE `{target_table_id}` AS target

#     USING (
#         SELECT *
#         EXCEPT(row_num)

#         FROM (
#             SELECT
#                 *,
#                 ROW_NUMBER() OVER (
#                     PARTITION BY id
#                     ORDER BY {dedupe_order}
#                 ) AS row_num
#             FROM `{staging_table_id}`
#         )

#         WHERE row_num = 1
#     ) AS source

#     ON target.`id` = source.`id`

#     WHEN MATCHED THEN
#         UPDATE SET
#             {update_sql}

#     WHEN NOT MATCHED THEN
#         INSERT (
#             {insert_columns}
#         )
#         VALUES (
#             {insert_values}
#         )
#     """

#     client.query(query).result()


# @task
# def setup_bigquery():
#     hook = BigQueryHook(gcp_conn_id="google_cloud_default")
#     client = hook.get_client(project_id=PROJECT_ID)
#     ensure_dataset(client)
#     ensure_control_table(client)
#     print("BigQuery ingestion infrastructure ready")


# def build_ingestion_task(source_table, config):
#     @task(task_id=f"ingest_{source_table}")
#     def ingest():
#         clickhouse_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_laplap")
#         bigquery_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
#         bq_client = bigquery_hook.get_client( project_id=PROJECT_ID)

#         target_table = config["target_table"]
#         mode = config["mode"]
#         target_table_id = bq_table_id(target_table)

#         print(f"Starting ingestion: {source_table} → {target_table_id}")
#         schema = get_clickhouse_schema(clickhouse_hook, source_table)
#         ensure_target_table(bq_client, target_table_id, schema, config["partition_field"], config["cluster_fields"])
#         watermark = get_watermark(bq_client, source_table)

#         print(f"Current watermark: {watermark}")
#         query = build_extract_query(source_table, schema, config["watermark_expression"], watermark, mode)

#         print(f"Extract query:\n{query}")
#         file_path = None

#         try:

#             file_path, row_count = stream_to_parquet(clickhouse_hook, query)

#             print(f"Extracted rows: {row_count}")

#             if row_count == 0:
#                 print(f"No new data for {source_table}")
#                 # now = datetime.utcnow()
#                 now = utc_now()
#                 update_control(bq_client, source_table, target_table, watermark, 0, "success", now)
#                 return

#             if mode == "replace":
#                 load_parquet_to_bigquery(bq_client, file_path, target_table_id, bigquery.WriteDisposition.WRITE_TRUNCATE)
#                 new_watermark = get_source_max_watermark(clickhouse_hook, source_table, config["watermark_expression"], None)

#             else:
#                 run_id = sanitize_identifier(os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual_run"))

#                 staging_table_id = make_staging_table_id(target_table, run_id)

#                 try:
#                     load_parquet_to_bigquery(bq_client, file_path, staging_table_id, bigquery.WriteDisposition.WRITE_TRUNCATE)
#                     merge_staging_to_target(bq_client, staging_table_id, target_table_id, [field.name for field in schema], config["dedupe_order"])

#                 finally:
#                     bq_client.delete_table(staging_table_id, not_found_ok=True)

#                 new_watermark = get_source_max_watermark(clickhouse_hook, source_table, config["watermark_expression"], None)

#                 if (watermark is not None and new_watermark is not None and new_watermark < watermark):
#                     new_watermark = watermark

#             # now = datetime.utcnow()
#             now = utc_now()

#             update_control(bq_client, source_table, target_table, new_watermark, row_count, "success", now)
#             print(f"Successfully loaded {row_count} rows into {target_table_id}")

#         except Exception:
#             try:
#                 # now = datetime.utcnow()
#                 now = utc_now()
#                 update_control(bq_client, source_table, target_table, watermark, 0, "failed", now)
#             except Exception as control_error:
#                 print(f"Could not update control table: {control_error}")
#             raise

#         finally:
#             if (file_path and os.path.exists(file_path)):
#                 os.remove(file_path)
#     return ingest()


# with DAG(
#     dag_id="laplap_daily_ingestion",start_date=datetime(2026, 9, 1),
#     schedule="0 2 * * *",
#     catchup=False,
#     max_active_runs=1,
#     tags=["laplap", "ingestion", "clickhouse", "bigquery"]
# ) as dag:

#     setup = setup_bigquery()
#     tasks = []
    
#     for source_table, config in (TABLE_CONFIGS.items()):

#         task_instance = build_ingestion_task(
#             source_table,
#             config
#         )

#         setup >> task_instance

#         tasks.append(
#             task_instance
#         )
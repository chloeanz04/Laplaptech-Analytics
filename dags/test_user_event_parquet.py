from datetime import datetime
import os
import tempfile

import pyarrow.parquet as pq
from airflow import DAG
from airflow.sdk import task
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


PROJECT_ID = "laplap-analytics"
DATASET_ID = "laplap_analytics"
TARGET_TABLE = "bronze_user_event_tracking_parquet_test"

TARGET_TABLE_ID = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    f"{TARGET_TABLE}"
)


with DAG(
    dag_id="test_user_event_parquet",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=[
        "test",
        "clickhouse",
        "parquet",
        "bigquery"
    ],
) as dag:

    @task
    def extract_and_load():

        clickhouse_hook = ClickHouseHook(
            clickhouse_conn_id="clickhouse_laplap"
        )

        bigquery_hook = BigQueryHook(
            gcp_conn_id="google_cloud_default"
        )

        bq_client = bigquery_hook.get_client(
            project_id=PROJECT_ID
        )

        dataset_id = f"{PROJECT_ID}.{DATASET_ID}"

        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"

        bq_client.create_dataset(
            dataset,
            exists_ok=True
        )

        print(
            f"Dataset ready: {dataset_id}"
        )

        sql = """
        SELECT *
        FROM laplaptech.user_event_tracking
        LIMIT 1000
        """

        file_handle = tempfile.NamedTemporaryFile(
            suffix=".parquet",
            delete=False
        )

        file_path = file_handle.name
        file_handle.close()

        writer = None
        client = None
        row_count = 0

        try:

            client = clickhouse_hook.get_client()

            with client.query_arrow_stream(sql) as stream:

                for batch in stream:

                    if writer is None:

                        writer = pq.ParquetWriter(
                            file_path,
                            batch.schema,
                            compression="zstd"
                        )

                    writer.write_batch(
                        batch
                    )

                    row_count += batch.num_rows

            if writer is not None:
                writer.close()
                writer = None

            client.close()
            client = None

            print(
                f"Extracted {row_count} rows "
                f"from ClickHouse"
            )

            if row_count == 0:

                print(
                    "No rows returned."
                )

                return

            file_size = os.path.getsize(
                file_path
            )

            print(
                f"Parquet file size: "
                f"{file_size / 1024:.2f} KB"
            )

            # Validate the Parquet file before upload
            parquet_file = pq.ParquetFile(
                file_path
            )

            print(
                f"Parquet rows: "
                f"{parquet_file.metadata.num_rows}"
            )

            print(
                f"Parquet columns: "
                f"{parquet_file.metadata.num_columns}"
            )

            job_config = bigquery.LoadJobConfig(
                source_format=(
                    bigquery.SourceFormat.PARQUET
                ),
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                ),
                autodetect=True
            )

            with open(
                file_path,
                "rb"
            ) as file_obj:

                load_job = (
                    bq_client.load_table_from_file(
                        file_obj,
                        TARGET_TABLE_ID,
                        rewind=True,
                        size=file_size,
                        job_config=job_config
                    )
                )

            load_job.result()

            table = bq_client.get_table(
                TARGET_TABLE_ID
            )

            print(
                f"BigQuery table ready: "
                f"{TARGET_TABLE_ID}"
            )

            print(
                f"BigQuery row count: "
                f"{table.num_rows}"
            )

            if table.num_rows != row_count:

                raise ValueError(
                    f"Row count mismatch: "
                    f"ClickHouse={row_count}, "
                    f"BigQuery={table.num_rows}"
                )

            print(
                "ClickHouse → Arrow → Parquet "
                "→ BigQuery test SUCCESS"
            )

        finally:

            if writer is not None:
                writer.close()

            if client is not None:
                client.close()

            if os.path.exists(file_path):
                os.remove(file_path)

    extract_and_load()
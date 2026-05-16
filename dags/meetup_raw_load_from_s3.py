from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.operators.slack_webhook import \
    SlackWebhookOperator
from airflow.task.trigger_rule import TriggerRule

from meetup_pipeline.config.datasets import RAW_DATASETS
from meetup_pipeline.config.settings import settings
from meetup_pipeline.ingestion.normalization import normalize_csv_bytes_to_utf8
from meetup_pipeline.monitoring.audit import insert_file_audit

FILES = RAW_DATASETS
BUCKET = settings.s3.bucket
SOURCE_PREFIX = settings.s3.source_prefix
NORMALIZED_PREFIX = settings.s3.normalized_prefix

DEFAULT_RETRY_ARGS = {
    "owner": "cristian",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


@task(
    retries=3,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
)
def normalize_one_file(file_info: dict) -> dict:
    hook = S3Hook(aws_conn_id="aws_default")

    file_name = file_info["file_name"]
    table_name = file_info["table_name"]

    source_key = f"{SOURCE_PREFIX}{file_name}"
    target_key = f"{NORMALIZED_PREFIX}{file_name}"

    s3_obj = hook.get_key(key=source_key, bucket_name=BUCKET)
    raw_bytes = s3_obj.get()["Body"].read()

    normalized_text, detected_encoding = normalize_csv_bytes_to_utf8(raw_bytes)

    hook.load_string(
        string_data=normalized_text,
        key=target_key,
        bucket_name=BUCKET,
        replace=True,
        encoding="utf-8",
    )

    return {
        "file_name": file_name,
        "table_name": table_name,
        "normalized_key": target_key,
        "detected_encoding": detected_encoding,
    }



@task
def build_sql(file_info: dict) -> str:
    file_name = file_info["file_name"]
    table_name = file_info["table_name"]

    return f"""
    CREATE OR REPLACE TABLE RAW.{table_name}
    USING TEMPLATE (
      SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID)
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION => '@RAW.MEETUP_LANDING_STAGE/{file_name}',
          FILE_FORMAT => 'RAW.CSV_FMT'
        )
      )
    );

    COPY INTO RAW.{table_name}
    FROM @RAW.MEETUP_LANDING_STAGE/{file_name}
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FMT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """

@task
def audit_normalized_file(file_info: dict) -> dict:
    insert_file_audit(
        file_name=file_info["file_name"],
        table_name=file_info["table_name"],
        source_key=f"landing/{file_info['file_name']}",
        normalized_key=file_info["normalized_key"],
        detected_encoding=file_info["detected_encoding"],
        status="SUCCESS",
        event_type="FILE_NORMALIZED",
    )
    return file_info

with DAG(
    dag_id="meetup_load_raw",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_RETRY_ARGS,
    tags=["meetup", "load_raw"],
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    normalized_files = normalize_one_file.expand(file_info=FILES)
    audited_normalized_files = audit_normalized_file.expand(file_info=normalized_files)

    sql_statements = build_sql.expand(file_info=FILES)

    load_raw = SQLExecuteQueryOperator.partial(
        task_id="meetup_raw_load_from_s3",
        conn_id="snowflake_default",
        database="MEETUP_DE",
        retries=3,
    ).expand(sql=sql_statements)

    incremental_structure = SQLExecuteQueryOperator(
        task_id="incremental_structure",
        conn_id="snowflake_default",
        database="MEETUP_DE",
        sql="""
        CREATE TABLE IF NOT EXISTS MEETUP_DE.RAW.EVENTS_STAGE_15M
        LIKE MEETUP_DE.RAW.EVENTS;
        """,
        retries=1,
    )

    build_analytics_initial = SQLExecuteQueryOperator(
        task_id="build_analytics_initial",
        conn_id="snowflake_default",
        database="MEETUP_DE",
        sql="snowflake/ddl/structure.sql",
        split_statements=True,
        retries=2,
    )
    
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id="slack_webhook_default",
        message="Carga RAW en Snowflake completada correctamente para todos los archivos.",
        retries=2,
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id="slack_webhook_default",
        message="Falló la normalización o la carga RAW en Snowflake.",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=2,
    )

    (
        normalized_files
        >> audited_normalized_files
        >> sql_statements
        >> load_raw
        >> incremental_structure
        >> build_analytics_initial
        >> notify_success
    )
    [normalized_files, load_raw] >> notify_failure



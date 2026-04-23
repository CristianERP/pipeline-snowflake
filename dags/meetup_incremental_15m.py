from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.operators.slack_webhook import \
    SlackWebhookOperator
from airflow.sdk import DAG, task
from airflow.task.trigger_rule import TriggerRule
from src.monitoring.audit import (insert_file_audit, update_pipeline_run_end,
                                  upsert_pipeline_run_start)
from src.quality.checks import run_stage_quality_checks
from src.services.delta_generator import generate_events_delta_file
from src.services.storage import upload_file_to_s3

BUCKET = "meetup-pipeline-2026"
INCREMENTAL_PREFIX = "incremental/events/"

DEFAULT_ARGS = {
    "owner": "cristian",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


@task(retries=0)
def validate_events_stage():
    return run_stage_quality_checks()

@task(retries=1)
def generate_events_delta() -> dict:
    return generate_events_delta_file(INCREMENTAL_PREFIX)


@task(
    retries=3,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
)
def upload_delta_to_s3(delta_info: dict) -> dict:
    return upload_file_to_s3(delta_info=delta_info, bucket=BUCKET)

@task(retries=0)
def build_copy_sql(delta_info: dict) -> str:
    return f"""
    COPY INTO MEETUP_DE.RAW.EVENTS_STAGE_15M
    FROM @MEETUP_DE.RAW.MEETUP_INCREMENTAL_STAGE/{delta_info['file_name']}
    FILE_FORMAT = (FORMAT_NAME = 'MEETUP_DE.RAW.CSV_FMT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """

@task
def mark_pipeline_started():
    upsert_pipeline_run_start()

@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def mark_pipeline_success():
    update_pipeline_run_end(status="SUCCESS")

@task(trigger_rule=TriggerRule.ONE_FAILED)
def mark_pipeline_failed():
    update_pipeline_run_end(
        status="FAILED",
        error_message="Check failed task in Airflow logs."
    )

@task
def audit_delta_generated(delta_info: dict) -> dict:
    insert_file_audit(
        file_name=delta_info["file_name"],
        s3_key=delta_info["s3_key"],
        row_count=delta_info["row_count"],
        status="SUCCESS",
        event_type="DELTA_GENERATED",
    )
    return delta_info

@task
def audit_delta_uploaded(delta_info: dict) -> dict:
    insert_file_audit(
        file_name=delta_info["file_name"],
        s3_key=delta_info["s3_key"],
        row_count=delta_info.get("row_count"),
        status="SUCCESS",
        event_type="DELTA_UPLOADED",
    )
    return delta_info

with DAG(
    dag_id="meetup_incremental_15m",
    start_date=datetime(2026, 4, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["meetup", "incremental"],
) as dag:
    
    run_started = mark_pipeline_started()
    
    delta_info = generate_events_delta()
    audited_delta = audit_delta_generated(delta_info)
    
    uploaded_delta = upload_delta_to_s3(audited_delta)
    audited_upload = audit_delta_uploaded(uploaded_delta)

    copy_sql = build_copy_sql(audited_delta)

    truncate_stage_table = SQLExecuteQueryOperator(
        task_id="truncate_stage_table",
        conn_id="snowflake_default",
        sql="TRUNCATE TABLE MEETUP_DE.RAW.EVENTS_STAGE_15M;",
        retries=1,
    )

    copy_delta_to_stage = SQLExecuteQueryOperator(
        task_id="copy_delta_to_stage",
        conn_id="snowflake_default",
        sql=copy_sql,
        retries=3,
    )

    stage_quality = validate_events_stage()

    merge_events = SQLExecuteQueryOperator(
        task_id="merge_events",
        conn_id="snowflake_default",
        sql="sql/merge_events.sql",
        retries=2,
    )

    rebuild = SQLExecuteQueryOperator(
        task_id="rebuild_analytics",
        conn_id="snowflake_default",
        sql="sql/rebuild.sql",
        split_statements=True,
        retries=2,
    )

    export_processed = SQLExecuteQueryOperator(
        task_id="export_processed_tables",
        conn_id="snowflake_default",
        sql="sql/export_data.sql",
        split_statements=True,
        retries=3,
    )

    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id="slack_webhook_default",
        message="Proceso incremental de EVENTS ejecutado correctamente.",
        retries=2,
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id="slack_webhook_default",
        message="Falló el proceso incremental de EVENTS.",
        trigger_rule=TriggerRule.ONE_FAILED,
        retries=2,
    )

    run_success = mark_pipeline_success()
    run_failed = mark_pipeline_failed()

    (
        run_started
        >> delta_info
        >> audited_delta
        >> uploaded_delta
        >> audited_upload
        >> copy_sql
        >> truncate_stage_table
        >> copy_delta_to_stage
        >> stage_quality
        >> merge_events
        >> rebuild
        >> export_processed
    )
    export_processed >> [notify_success, run_success]
    
    failure_upstreams = [
    delta_info,
    audited_delta,
    uploaded_delta,
    audited_upload,
    truncate_stage_table,
    copy_delta_to_stage,
    stage_quality,
    merge_events,
    rebuild,
    export_processed,
]

for task in failure_upstreams:
    task >> notify_failure
    task >> run_failed
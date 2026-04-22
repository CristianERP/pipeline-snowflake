from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.operators.slack_webhook import \
    SlackWebhookOperator
from airflow.sdk import DAG, task
from airflow.task.trigger_rule import TriggerRule
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

@task
def generate_events_delta() -> dict:
    return generate_events_delta_file(INCREMENTAL_PREFIX)


@task
def upload_delta_to_s3(delta_info: dict) -> dict:
    return upload_file_to_s3(delta_info=delta_info, bucket=BUCKET)

@task
def build_copy_sql(delta_info: dict) -> str:
    return f"""
    COPY INTO MEETUP_DE.RAW.EVENTS_STAGE_15M
    FROM @MEETUP_DE.RAW.MEETUP_INCREMENTAL_STAGE/{delta_info['file_name']}
    FILE_FORMAT = (FORMAT_NAME = 'MEETUP_DE.RAW.CSV_FMT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """

with DAG(
    dag_id="meetup_incremental_15m",
    start_date=datetime(2026, 4, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["meetup", "incremental"],
) as dag:
    
    stage_quality = validate_events_stage()
    delta_info = generate_events_delta()
    uploaded_delta = upload_delta_to_s3(delta_info)
    copy_sql = build_copy_sql(uploaded_delta)

    truncate_stage_table = SQLExecuteQueryOperator(
        task_id="truncate_stage_table",
        conn_id="snowflake_default",
        sql="TRUNCATE TABLE MEETUP_DE.RAW.EVENTS_STAGE_15M;",
    )

    copy_delta_to_stage = SQLExecuteQueryOperator(
        task_id="copy_delta_to_stage",
        conn_id="snowflake_default",
        sql=copy_sql,
    )

    merge_events = SQLExecuteQueryOperator(
        task_id="merge_events",
        conn_id="snowflake_default",
        sql="sql/merge_events.sql",
    )

    rebuild = SQLExecuteQueryOperator(
        task_id="rebuild_analytics",
        conn_id="snowflake_default",
        sql="sql/rebuild.sql",
        split_statements=True,
    )

    export_processed = SQLExecuteQueryOperator(
        task_id="export_processed_tables",
        conn_id="snowflake_default",
        sql="sql/export_data.sql",
        split_statements=True,
    )

    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id="slack_webhook_default",
        message="Proceso incremental de EVENTS ejecutado correctamente.",
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id="slack_webhook_default",
        message="Falló el proceso incremental de EVENTS.",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    delta_info >> uploaded_delta >> truncate_stage_table >> copy_delta_to_stage >> stage_quality >> merge_events >> rebuild >> export_processed >> notify_success
    [uploaded_delta, copy_delta_to_stage, stage_quality , merge_events, rebuild] >> notify_failure
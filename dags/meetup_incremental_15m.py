import csv
import os
import random
import tempfile
import uuid
from datetime import datetime

from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.operators.slack_webhook import \
    SlackWebhookOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import DAG, task
from airflow.task.trigger_rule import TriggerRule

BUCKET = "meetup-pipeline-2026"
INCREMENTAL_PREFIX = "incremental/events/"

@task
def validate_events_stage() -> dict:
    context = get_current_context()
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    checks = {
        "row_count": (
            """
            SELECT COUNT(*)
            FROM MEETUP_DE.RAW.EVENTS_STAGE_15M
            """,
            lambda v: v > 0,
            "EVENTS_STAGE_15M está vacía"
        ),
        "null_event_id": (
            """
            SELECT COUNT(*)
            FROM MEETUP_DE.RAW.EVENTS_STAGE_15M
            WHERE "event_id" IS NULL
            """,
            lambda v: v == 0,
            "Hay event_id nulos"
        ),
        "duplicate_event_id": (
            """
            SELECT COUNT(*)
            FROM (
                SELECT "event_id"
                FROM MEETUP_DE.RAW.EVENTS_STAGE_15M
                GROUP BY 1
                HAVING COUNT(*) > 1
            )
            """,
            lambda v: v == 0,
            "Hay event_id duplicados en stage"
        ),
        "negative_rsvp_values": (
            """
            SELECT COUNT(*)
            FROM MEETUP_DE.RAW.EVENTS_STAGE_15M
            WHERE COALESCE("yes_rsvp_count", 0) < 0
               OR COALESCE("maybe_rsvp_count", 0) < 0
               OR COALESCE("waitlist_count", 0) < 0
            """,
            lambda v: v == 0,
            "Hay métricas RSVP negativas"
        ),
        "invalid_status": (
            """
            SELECT COUNT(*)
            FROM MEETUP_DE.RAW.EVENTS_STAGE_15M
            WHERE "event_status" IS NOT NULL
              AND LOWER("event_status") NOT IN ('upcoming', 'cancelled', 'past', 'draft')
            """,
            lambda v: v == 0,
            "Hay event_status fuera del dominio permitido"
        ),
        "updated_before_created": (
            """
            SELECT COUNT(*)
            FROM MEETUP_DE.RAW.EVENTS_STAGE_15M
            WHERE "created" IS NOT NULL
              AND "updated" IS NOT NULL
              AND "updated" < "created"
            """,
            lambda v: v == 0,
            "Hay registros con updated < created"
        ),
    }

    rows_to_insert = []
    failures = []
    metrics = {}

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    source_table = "MEETUP_DE.RAW.EVENTS_STAGE_15M"

    for check_name, (sql, rule, error_message) in checks.items():
        value = hook.get_records(sql)[0][0]
        passed = rule(value)

        metrics[check_name] = value

        rows_to_insert.append(
            (
                dag_id,
                task_id,
                run_id,
                source_table,
                check_name,
                "PASS" if passed else "FAIL",
                int(value) if value is not None else None,
                None if passed else error_message,
            )
        )

        if not passed:
            failures.append(f"{check_name}={value} -> {error_message}")

    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.executemany(
            """
            INSERT INTO MEETUP_DE.MONITORING.DQ_RESULTS (
                dag_id,
                task_id,
                run_id,
                source_table,
                check_name,
                status,
                metric_value,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows_to_insert,
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    if failures:
        raise ValueError(" | ".join(failures))

    return metrics

@task
def generate_events_delta() -> dict:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    records = hook.get_records(
        """
        SELECT *
        FROM MEETUP_DE.RAW.EVENTS
        LIMIT 50
        """
    )

    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM MEETUP_DE.RAW.EVENTS LIMIT 1")
        columns = [desc[0] for desc in cur.description]
    finally:
        cur.close()
        conn.close()

    if not records:
        raise ValueError("RAW.EVENTS no tiene datos para generar el delta.")

    column_map = {c.upper(): c for c in columns}

    sampled_existing = random.sample(records, min(5, len(records)))
    delta_rows = []

    for row in sampled_existing:
        row_dict = dict(zip(columns, row))

        row_dict[column_map["YES_RSVP_COUNT"]] = (row_dict.get(column_map["YES_RSVP_COUNT"]) or 0) + random.randint(1, 15)
        row_dict[column_map["MAYBE_RSVP_COUNT"]] = (row_dict.get(column_map["MAYBE_RSVP_COUNT"]) or 0) + random.randint(0, 2)
        row_dict[column_map["WAITLIST_COUNT"]] = (row_dict.get(column_map["WAITLIST_COUNT"]) or 0) + random.randint(0, 5)
        row_dict[column_map["UPDATED"]] = int(datetime.now().timestamp() * 1000)

        if random.random() < 0.2:
            row_dict[column_map["EVENT_STATUS"]] = "cancelled"
        else:
            row_dict[column_map["EVENT_STATUS"]] = row_dict.get(column_map["EVENT_STATUS"]) or "upcoming"

        delta_rows.append(row_dict)

    base_new = dict(zip(columns, sampled_existing[0]))

    for _ in range(3):
        new_row = base_new.copy()
        new_row[column_map["EVENT_ID"]] = f"delta-{uuid.uuid4().hex[:12]}"
        new_row[column_map["EVENT_NAME"]] = f"delta Event {uuid.uuid4().hex[:6]}"
        new_row[column_map["YES_RSVP_COUNT"]] = random.randint(5, 40)
        new_row[column_map["MAYBE_RSVP_COUNT"]] = random.randint(0, 9)
        new_row[column_map["WAITLIST_COUNT"]] = random.randint(0, 5)
        new_row[column_map["EVENT_STATUS"]] = "upcoming"
        new_row[column_map["CREATED"]] = int(datetime.now().timestamp() * 1000)
        new_row[column_map["UPDATED"]] = int(datetime.now().timestamp() * 1000)

        delta_rows.append(new_row)

    file_name = f"events_delta_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    file_path = os.path.join(tempfile.gettempdir(), file_name)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(delta_rows)

    return {
        "file_name": file_name,
        "file_path": file_path,
        "s3_key": f"{INCREMENTAL_PREFIX}{file_name}",
        "row_count": len(delta_rows),
    }


@task
def upload_delta_to_s3(delta_info: dict) -> dict:
    hook = S3Hook(aws_conn_id="aws_default")

    hook.load_file(
        filename=delta_info["file_path"],
        key=delta_info["s3_key"],
        bucket_name=BUCKET,
        replace=True,
    )

    return delta_info


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
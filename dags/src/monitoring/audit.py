from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import get_current_context


def _get_context_values():
    context = get_current_context()
    return {
        "dag_id": context["dag"].dag_id,
        "task_id": context["task"].task_id,
        "run_id": context["run_id"],
        "logical_date": context["logical_date"].replace(tzinfo=None),
    }


def upsert_pipeline_run_start():
    values = _get_context_values()
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            MERGE INTO MEETUP_DE.MONITORING.PIPELINE_RUNS t
            USING (
                SELECT
                    %s AS dag_id,
                    %s AS run_id,
                    %s AS logical_date
            ) s
            ON t.dag_id = s.dag_id AND t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET
                status = 'RUNNING',
                started_at = COALESCE(t.started_at, CURRENT_TIMESTAMP()),
                updated_at = CURRENT_TIMESTAMP(),
                error_message = NULL
            WHEN NOT MATCHED THEN INSERT (
                dag_id, run_id, logical_date, status, started_at, updated_at
            )
            VALUES (
                s.dag_id, s.run_id, s.logical_date, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            );
            """,
            (values["dag_id"], values["run_id"], values["logical_date"]),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def update_pipeline_run_end(status: str, error_message: str | None = None):
    values = _get_context_values()
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE MEETUP_DE.MONITORING.PIPELINE_RUNS
            SET
                status = %s,
                ended_at = CURRENT_TIMESTAMP(),
                error_message = %s,
                updated_at = CURRENT_TIMESTAMP()
            WHERE dag_id = %s AND run_id = %s
            """,
            (status, error_message, values["dag_id"], values["run_id"]),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def insert_file_audit(
    *,
    file_name: str,
    table_name: str | None = None,
    source_key: str | None = None,
    normalized_key: str | None = None,
    s3_key: str | None = None,
    detected_encoding: str | None = None,
    row_count: int | None = None,
    status: str = "SUCCESS",
    event_type: str,
):
    values = _get_context_values()
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO MEETUP_DE.MONITORING.FILE_AUDIT (
                dag_id,
                run_id,
                task_id,
                file_name,
                table_name,
                source_key,
                normalized_key,
                s3_key,
                detected_encoding,
                row_count,
                status,
                event_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                values["dag_id"],
                values["run_id"],
                values["task_id"],
                file_name,
                table_name,
                source_key,
                normalized_key,
                s3_key,
                detected_encoding,
                row_count,
                status,
                event_type,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
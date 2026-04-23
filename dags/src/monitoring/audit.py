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
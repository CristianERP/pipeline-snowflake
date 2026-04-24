from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import get_current_context

from meetup_pipeline.quality.monitoring import insert_dq_results


def run_stage_quality_checks() -> dict:
    context = get_current_context()
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    source_table = "MEETUP_DE.RAW.EVENTS_STAGE_15M"

    stage_row_count = hook.get_records(
        f'SELECT COUNT(*) FROM {source_table}'
    )[0][0]

    expected_delta_rows = hook.get_records(
        f"""
        SELECT COALESCE(MAX(row_count), 0)
        FROM MEETUP_DE.MONITORING.FILE_AUDIT
        WHERE dag_id = '{dag_id}'
          AND run_id = '{run_id}'
          AND event_type IN ('DELTA_GENERATED', 'DELTA_UPLOADED')
          AND status = 'SUCCESS'
        """
    )[0][0]

    checks = {
        "row_count": (
            f"""
            SELECT COUNT(*)
            FROM {source_table}
            """,
            lambda v: v > 0,
            "EVENTS_STAGE_15M está vacía"
        ),
        "null_event_id": (
            f"""
            SELECT COUNT(*)
            FROM {source_table}
            WHERE "event_id" IS NULL
            """,
            lambda v: v == 0,
            "Hay event_id nulos"
        ),
        "duplicate_event_id": (
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT "event_id"
                FROM {source_table}
                GROUP BY 1
                HAVING COUNT(*) > 1
            )
            """,
            lambda v: v == 0,
            "Hay event_id duplicados"
        ),
        "negative_rsvp_values": (
            f"""
            SELECT COUNT(*)
            FROM {source_table}
            WHERE COALESCE("yes_rsvp_count", 0) < 0
               OR COALESCE("maybe_rsvp_count", 0) < 0
               OR COALESCE("waitlist_count", 0) < 0
            """,
            lambda v: v == 0,
            "Hay métricas RSVP negativas"
        ),
        "invalid_status": (
            f"""
            SELECT COUNT(*)
            FROM {source_table}
            WHERE "event_status" IS NOT NULL
              AND LOWER("event_status") NOT IN ('upcoming', 'cancelled')
            """,
            lambda v: v == 0,
            "Hay event_status fuera del dominio permitido"
        ),
        "stage_matches_delta_row_count": (
            f'SELECT COUNT(*) FROM {source_table}',
            lambda v: v == expected_delta_rows,
            f"Stage row_count ({stage_row_count}) no coincide con delta row_count ({expected_delta_rows})",
        ),
    }

    

    rows_to_insert = []
    failures = []
    metrics = {}

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

    insert_dq_results(hook, rows_to_insert)

    if failures:
        raise ValueError(" | ".join(failures))

    return metrics
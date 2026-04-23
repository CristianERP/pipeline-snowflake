from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import get_current_context
from src.quality.monitoring import insert_dq_results


def run_stage_quality_checks() -> dict:
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
            "Hay event_id duplicados"
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
              AND LOWER("event_status") NOT IN ('upcoming', 'cancelled')
            """,
            lambda v: v == 0,
            "Hay event_status fuera del dominio permitido"
        )
    }

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    source_table = "MEETUP_DE.RAW.EVENTS_STAGE_15M"

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
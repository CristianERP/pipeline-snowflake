import csv
import os
import random
import tempfile
import uuid
from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def fetch_events_sample(limit: int = 50):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    records = hook.get_records(
        f"""
        SELECT *
        FROM MEETUP_DE.RAW.EVENTS
        LIMIT {limit}
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

    return records, columns


def build_delta_rows(records, columns):
    column_map = {c.upper(): c for c in columns}
    sampled_existing = random.sample(records, min(5, len(records)))
    delta_rows = []

    now_ms = int(datetime.now().timestamp() * 1000)

    for row in sampled_existing:
        row_dict = dict(zip(columns, row))

        row_dict[column_map["YES_RSVP_COUNT"]] = (
            row_dict.get(column_map["YES_RSVP_COUNT"]) or 0
        ) + random.randint(1, 15)

        row_dict[column_map["MAYBE_RSVP_COUNT"]] = (
            row_dict.get(column_map["MAYBE_RSVP_COUNT"]) or 0
        ) + random.randint(0, 1)

        row_dict[column_map["WAITLIST_COUNT"]] = (
            row_dict.get(column_map["WAITLIST_COUNT"]) or 0
        ) + random.randint(0, 5)

        row_dict[column_map["UPDATED"]] = now_ms
        row_dict[column_map["EVENT_STATUS"]] = (
            "cancelled" if random.random() < 0.2
            else row_dict.get(column_map["EVENT_STATUS"]) or "upcoming"
        )

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
        new_row[column_map["CREATED"]] = now_ms
        new_row[column_map["UPDATED"]] = now_ms
        delta_rows.append(new_row)

    return delta_rows


def write_delta_csv(columns, delta_rows, incremental_prefix: str):
    file_name = f"events_delta_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    file_path = os.path.join(tempfile.gettempdir(), file_name)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(delta_rows)

    return {
        "file_name": file_name,
        "file_path": file_path,
        "s3_key": f"{incremental_prefix}{file_name}",
        "row_count": len(delta_rows),
    }


def generate_events_delta_file(incremental_prefix: str) -> dict:
    records, columns = fetch_events_sample()
    delta_rows = build_delta_rows(records, columns)
    return write_delta_csv(columns, delta_rows, incremental_prefix)
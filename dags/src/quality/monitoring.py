def insert_dq_results(hook, rows_to_insert):
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
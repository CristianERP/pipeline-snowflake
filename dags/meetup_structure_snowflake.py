from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DEFAULT_RETRY_ARGS = {
    "owner": "cristian",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="meetup_structure",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_RETRY_ARGS,
    tags=["meetup", "structure"],
) as dag:

    create_foundation = SQLExecuteQueryOperator(
        task_id="create_structure",
        conn_id="snowflake_default",
        sql="sql/structure.sql",
        split_statements=True,
    )

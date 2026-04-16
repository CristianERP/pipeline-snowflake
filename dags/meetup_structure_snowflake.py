from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="meetup_structure",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["meetup", "structure"],
) as dag:

    create_foundation = SQLExecuteQueryOperator(
        task_id="create_structure",
        conn_id="snowflake_default",
        sql="sql/structure.sql",
        split_statements=True,
    )

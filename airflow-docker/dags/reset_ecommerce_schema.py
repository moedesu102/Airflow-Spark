from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag


@dag(
    dag_id="reset_ecommerce_schema",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "ddl", "ecommerce", "reset"],
)
def reset_ecommerce_schema():
    drop_all_ecommerce_tables = SQLExecuteQueryOperator(
        task_id="drop_all_ecommerce_tables",
        conn_id="postgres",
        autocommit=True,
        sql="""
        DROP TABLE IF EXISTS clickstream CASCADE;
        DROP TABLE IF EXISTS order_promotions CASCADE;
        DROP TABLE IF EXISTS order_items CASCADE;
        DROP TABLE IF EXISTS orders CASCADE;
        DROP TABLE IF EXISTS inventory CASCADE;
        DROP TABLE IF EXISTS warehouses CASCADE;
        DROP TABLE IF EXISTS promotions CASCADE;
        DROP TABLE IF EXISTS products CASCADE;
        DROP TABLE IF EXISTS categories CASCADE;
        DROP TABLE IF EXISTS marketing_channels CASCADE;
        DROP TABLE IF EXISTS users CASCADE;
        DROP TABLE IF EXISTS watermark CASCADE;
        """,
    )

    drop_all_ecommerce_tables


reset_ecommerce_schema()

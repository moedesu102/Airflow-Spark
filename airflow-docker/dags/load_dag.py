from datetime import datetime

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag
 
 
POSTGRES_JDBC_PACKAGES = "org.postgresql:postgresql:42.7.3"


@dag(
    dag_id="ecommerce_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "bronze", "silver", "gold", "postgres"],
)
def ecommerce_etl_pipeline():
    tables = [
        "categories",
        "products",
        "users",
        "orders",
        "order_items",
        "promotions",
        "order_promotions",
        "warehouses",
        "inventory",
        "marketing_channels",
        "clickstream",
    ]

    spark_conf = {
        "spark.master": "spark://spark-master:7077",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    }

    batch_bronze = SparkSubmitOperator(
        task_id="batch_bronze",
        application="/opt/airflow/spark_scripts/bronze/run_bronze.py",
        conn_id="spark_default",
        conf=spark_conf,
        packages=POSTGRES_JDBC_PACKAGES,
        application_args=[
            "--tables", ",".join(tables),
            "--bronze_out", "/opt/spark-data/bronze",
        ],
    )

    batch_silver = SparkSubmitOperator(
        task_id="batch_silver",
        application="/opt/airflow/spark_scripts/silver/run_silver.py",
        conn_id="spark_default",
        conf=spark_conf,
        application_args=[
            "--tables", ",".join(tables),
            "--bronze_path", "/opt/spark-data/bronze",
            "--silver_out", "/opt/spark-data/silver",
        ],
    )

    batch_gold = SparkSubmitOperator(
        task_id="batch_gold",
        application="/opt/airflow/spark_scripts/gold/run_gold.py",
        conn_id="spark_default",
        conf={
            **spark_conf,
            "spark.sql.parquet.writeLegacyFormat": "false",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
        },
        application_args=[
            "--silver_path", "/opt/spark-data/silver",
            "--gold_path", "/opt/spark-data/gold",
        ],
    )

    batch_bronze >> batch_silver >> batch_gold

ecommerce_etl_pipeline()
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag
from datetime import datetime

@dag(
    dag_id="user_transaction_analysis_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "pyspark", "etl"]
)
def user_transaction_analysis_dag():

    spark_task = SparkSubmitOperator(
        task_id="run_pyspark_analysis",
        application="/opt/airflow/spark_scripts/user_transaction_analysis.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077"
        }
    )

    spark_task

user_transaction_analysis_dag()
 
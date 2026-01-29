from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag

# Định nghĩa DAG với decorator, không truyền tham số vào DAG() nữa
@dag
def spark_airflow_demo():
    # Sử dụng SparkSubmitOperator để gửi job tới Spark Cluster
    spark_job = SparkSubmitOperator(
        task_id="spark_demo_task",
        application="/opt/airflow/spark_scripts/demo_script.py",  # Đường dẫn tới file Python trên Airflow container
        conn_id="spark_default",  # Kết nối Spark
        conf={'spark.master': 'spark://spark-master:7077'}
    )

    spark_job  # Kết nối các task trong DAG

spark_airflow_demo()
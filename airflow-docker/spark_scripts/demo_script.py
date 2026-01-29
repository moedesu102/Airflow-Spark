from pyspark.sql import SparkSession

# Tạo một Spark session
spark = SparkSession.builder.appName("SparkAirflowDemo").getOrCreate()

# Tạo DataFrame đơn giản và thực hiện một số thao tác
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
df = spark.createDataFrame(data, ["name", "value"])

df.show()

# Lưu DataFrame ra một file Parquet
# df.write.mode("overwrite").parquet("/opt/airflow/spark_scripts/output_data.parquet")

df.write.mode("overwrite").parquet("/tmp/output_data.parquet")
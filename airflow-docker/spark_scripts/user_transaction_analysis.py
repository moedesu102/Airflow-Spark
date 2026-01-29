from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg

spark = SparkSession.builder \
    .appName("UserTransactionAnalysis") \
    .getOrCreate()

users_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("/opt/spark-data/users.csv")

transactions_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("/opt/spark-data/transactions.csv")

joined_df = users_df.join(transactions_df, on="user_id", how="inner")

agg_df = joined_df.groupBy(
    "region", "user_id", "name"
).agg(
    count("transaction_id").alias("transaction_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount")
)

# Kiểm tra data trước khi write
print(f"agg_df count: {agg_df.count()}")  # Phải > 0
agg_df.show(10)  # Xem dữ liệu có không

agg_df.write \
    .mode("overwrite") \
    .partitionBy("region", "user_id") \
    .parquet("/opt/spark-data/output")

spark.stop()

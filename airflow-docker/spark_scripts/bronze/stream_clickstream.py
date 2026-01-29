import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    DoubleType,
)


CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", LongType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("channel_id", IntegerType(), True),
    StructField("event_time", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("campaign_name", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("is_returning", StringType(), True),
    StructField("is_read", StringType(), True),
])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        default=os.environ.get("ECOM_CLICKSTREAM_INPUT", "/opt/spark-data/clickstream_input"),
    )
    parser.add_argument(
        "--bronze_out",
        default=os.environ.get("ECOM_BRONZE_OUT", "/opt/spark-data/bronze"),
    )
    parser.add_argument(
        "--checkpoint_path",
        default=os.environ.get("ECOM_CHECKPOINT_PATH", "/opt/spark-data/checkpoints/clickstream"),
    )
    parser.add_argument(
        "--mode",
        choices=["continuous", "once"],
        default="once",
        help="'once' for Airflow (process available files then exit), 'continuous' for long-running",
    )
    parser.add_argument(
        "--trigger_interval",
        default="10 seconds",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ClickstreamStreaming").getOrCreate()

    output_path = os.path.join(args.bronze_out, "clickstream")

    if args.mode == "once":
        read_path = args.input_path
        if os.path.isdir(read_path):
            read_path = os.path.join(read_path, "*.csv")

        clickstream_df = (
            spark.read
            .format("csv")
            .schema(CLICKSTREAM_SCHEMA)
            .option("header", "false")
            .load(read_path)
        )

        unread_df = clickstream_df.filter(
            F.col("is_read").isNull() | (F.trim(F.col("is_read")) == "") | (F.lower(F.col("is_read")) == "null")
        )

        if not unread_df.take(1):
            print("Streaming mode: once")
            print(f"Input: {args.input_path}")
            print("No unread rows (is_read is not null).")
            spark.stop()
            return

        clickstream_with_meta = (
            unread_df
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("event_time", F.to_timestamp(F.col("event_time")))
            .withColumn(
                "is_returning",
                F.when(F.lower(F.col("is_returning")) == "true", F.lit(True)).otherwise(F.lit(False))
            )
        )

        clickstream_with_meta.write.mode("append").parquet(output_path)

        if os.path.isfile(args.input_path):
            now_str = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
            updated_df = clickstream_df.withColumn(
                "is_read",
                F.when(
                    F.col("is_read").isNull() | (F.trim(F.col("is_read")) == "") | (F.lower(F.col("is_read")) == "null"),
                    F.lit(now_str),
                ).otherwise(F.col("is_read")),
            )

            tmp_out = f"{args.input_path}.tmp"
            (
                updated_df
                .select(*[F.col(c).cast("string") for c in updated_df.columns])
                .coalesce(1)
                .write.mode("overwrite")
                .option("header", "false")
                .option("quote", "\u0000")
                .csv(tmp_out)
            )

            part_file = None
            for name in os.listdir(tmp_out):
                if name.startswith("part-") and name.endswith(".csv"):
                    part_file = os.path.join(tmp_out, name)
                    break

            if part_file is not None:
                os.replace(part_file, args.input_path)
            for name in os.listdir(tmp_out):
                try:
                    os.remove(os.path.join(tmp_out, name))
                except OSError:
                    pass
            try:
                os.rmdir(tmp_out)
            except OSError:
                pass

        print("Streaming mode: once")
        print(f"Input: {args.input_path}")
        print(f"Output: {output_path}")
        spark.stop()
        return

    clickstream_stream = (
        spark.readStream
        .format("csv")
        .schema(CLICKSTREAM_SCHEMA)
        .option("header", "false")
        .option("maxFilesPerTrigger", 1)
        .load(args.input_path)
    )

    clickstream_stream = clickstream_stream.filter(
        F.col("is_read").isNull() | (F.trim(F.col("is_read")) == "") | (F.lower(F.col("is_read")) == "null")
    )

    clickstream_with_meta = (
        clickstream_stream
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("event_time", F.to_timestamp(F.col("event_time")))
        .withColumn(
            "is_returning",
            F.when(F.lower(F.col("is_returning")) == "true", F.lit(True)).otherwise(F.lit(False))
        )
    )

    query = (
        clickstream_with_meta.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", args.checkpoint_path)
        .outputMode("append")
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    print("Streaming mode: continuous")
    print(f"Monitoring: {args.input_path}")
    print(f"Output: {output_path}")
    print(f"Checkpoint: {args.checkpoint_path}")

    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()

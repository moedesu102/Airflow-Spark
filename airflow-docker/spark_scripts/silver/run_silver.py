import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType


TABLE_PRIMARY_KEYS: dict[str, list[str]] = {
    "categories": ["category_id"],
    "products": ["product_id"],
    "users": ["user_id"],
    "orders": ["order_id"],
    "order_items": ["order_item_id"],
    "promotions": ["promo_id"],
    "order_promotions": ["order_id", "promo_id"],
    "warehouses": ["warehouse_id"],
    "inventory": ["inventory_id"],
    "marketing_channels": ["channel_id"],
    "clickstream": ["event_id"],
}


def _to_silver(df, table: str, ingestion_ts: str, ingestion_date: str):
    cleaned = df

    for field in cleaned.schema.fields:
        if isinstance(field.dataType, StringType):
            cleaned = cleaned.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)) == "", F.lit(None)).otherwise(F.trim(F.col(field.name))),
            )

    if table == "users":
        if "name" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "name",
                F.initcap(F.regexp_replace(F.col("name"), r"\s+", " ")),
            )
        if "email" in cleaned.columns:
            cleaned = cleaned.withColumn("email", F.lower(F.col("email")))
            cleaned = cleaned.withColumn(
                "email",
                F.when(
                    F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
                    F.col("email"),
                ).otherwise(F.lit(None)),
            )
        if "phone" in cleaned.columns:
            cleaned = cleaned.withColumn("phone", F.regexp_replace(F.col("phone"), r"[^0-9]", ""))
            cleaned = cleaned.withColumn(
                "phone",
                F.when(F.length(F.col("phone")).between(9, 15), F.col("phone")).otherwise(F.lit(None)),
            )

    for field in cleaned.schema.fields:
        if not isinstance(field.dataType, StringType):
            continue
        if field.name.endswith("_id"):
            cleaned = cleaned.withColumn(
                field.name,
                F.when(F.col(field.name).rlike(r"^\d+$"), F.col(field.name).cast("long")).otherwise(F.lit(None)),
            )
        if field.name.endswith("_at"):
            cleaned = cleaned.withColumn(field.name, F.to_timestamp(F.col(field.name)))

    cleaned = cleaned.withColumn("_ingestion_ts", F.lit(ingestion_ts)).withColumn(
        "_ingestion_date", F.lit(ingestion_date)
    )

    pk_cols = TABLE_PRIMARY_KEYS.get(table, [])
    if pk_cols:
        cleaned = cleaned.dropna(subset=pk_cols)

        order_cols = []
        if "updated_at" in cleaned.columns:
            order_cols.append(F.col("updated_at").desc_nulls_last())
        order_cols.append(F.col("_ingestion_ts").desc())

        w = Window.partitionBy(*[F.col(c) for c in pk_cols]).orderBy(*order_cols)
        cleaned = cleaned.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
    else:
        cleaned = cleaned.dropDuplicates()

    return cleaned


def _write_silver(df, silver_out: str, table: str, mode: str) -> None:
    out = os.path.join(silver_out, table)

    write_df = df
    if os.environ.get("ECOM_DEBUG_SINGLE_PARTITION", "0") == "1":
        write_df = write_df.repartition(1)
    write_df.write.mode(mode).partitionBy("_ingestion_date").parquet(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", required=True)
    parser.add_argument("--bronze_path", default=os.environ.get("ECOM_BRONZE_OUT", "/opt/spark-data/bronze"))
    parser.add_argument("--silver_out", default=os.environ.get("ECOM_SILVER_OUT", "/opt/spark-data/silver"))
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    os.makedirs(args.silver_out, exist_ok=True)

    spark = SparkSession.builder.appName("EcommerceSilverBatch").getOrCreate()

    now = datetime.utcnow()
    ingestion_ts = now.isoformat(sep=" ", timespec="seconds")
    ingestion_date = now.date().isoformat()

    for table in tables:
        bronze_path = os.path.join(args.bronze_path, table)
        if not os.path.exists(bronze_path):
            continue

        bronze_df = spark.read.parquet(bronze_path)

        if not bronze_df.take(1):
            continue

        silver_df = _to_silver(bronze_df, table, ingestion_ts, ingestion_date)
        _write_silver(silver_df, args.silver_out, table, mode="append")

    spark.stop()


if __name__ == "__main__":
    main()

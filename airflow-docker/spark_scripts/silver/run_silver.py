import argparse
import os
from datetime import datetime
from urllib.parse import urlparse

import psycopg2
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


def _get_pg_connection(args: argparse.Namespace):
    parsed = urlparse(args.jdbc_url.replace("jdbc:", ""))
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=args.jdbc_user,
        password=args.jdbc_password,
    )


def _ensure_watermark_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver_watermarks (
                table_name VARCHAR(255) PRIMARY KEY,
                last_watermark TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
            )
        """)
    conn.commit()


def _get_watermark(conn, table: str) -> datetime:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_watermark FROM silver_watermarks WHERE table_name = %s",
            (table,)
        )
        row = cur.fetchone()
        if row:
            return row[0]
        return datetime(1970, 1, 1)


def _update_watermark(conn, table: str, new_watermark: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO silver_watermarks (table_name, last_watermark)
            VALUES (%s, %s)
            ON CONFLICT (table_name)
            DO UPDATE SET last_watermark = EXCLUDED.last_watermark
        """, (table, new_watermark))
    conn.commit()


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
    parser.add_argument("--jdbc_url", default=os.environ.get("ECOM_JDBC_URL", "jdbc:postgresql://postgres:5432/airflow"))
    parser.add_argument("--jdbc_user", default=os.environ.get("ECOM_JDBC_USER", "airflow"))
    parser.add_argument("--jdbc_password", default=os.environ.get("ECOM_JDBC_PASSWORD", "airflow"))
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    os.makedirs(args.silver_out, exist_ok=True)

    conn = _get_pg_connection(args)
    _ensure_watermark_table(conn)

    spark = SparkSession.builder.appName("EcommerceSilverBatch").getOrCreate()

    now = datetime.utcnow()
    ingestion_ts = now.isoformat(sep=" ", timespec="seconds")
    ingestion_date = now.date().isoformat()

    for table in tables:
        bronze_path = os.path.join(args.bronze_path, table)
        if not os.path.exists(bronze_path):
            continue

        watermark = _get_watermark(conn, table)
        watermark_str = watermark.strftime("%Y-%m-%d %H:%M:%S")

        bronze_df = spark.read.parquet(bronze_path)
        bronze_df = bronze_df.filter(F.col("_ingestion_ts") > watermark_str)

        if not bronze_df.take(1):
            continue

        silver_df = _to_silver(bronze_df, table, ingestion_ts, ingestion_date)
        _write_silver(silver_df, args.silver_out, table, mode="append")

        _update_watermark(conn, table, now)

    spark.stop()
    conn.close()


if __name__ == "__main__":
    main()

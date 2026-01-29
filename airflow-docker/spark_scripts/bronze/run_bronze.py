import argparse
import os
from datetime import datetime
from urllib.parse import urlparse

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TABLES_WITH_UPDATED_AT = {
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
}


def _get_jdbc_options(args: argparse.Namespace) -> dict[str, str]:
    return {
        "url": args.jdbc_url,
        "user": args.jdbc_user,
        "password": args.jdbc_password,
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
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
            CREATE TABLE IF NOT EXISTS bronze_watermarks (
                table_name VARCHAR(255) PRIMARY KEY,
                last_watermark TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
            )
        """)
    conn.commit()


def _get_watermark(conn, table: str) -> datetime:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_watermark FROM bronze_watermarks WHERE table_name = %s",
            (table,)
        )
        row = cur.fetchone()
        if row:
            return row[0]
        return datetime(1970, 1, 1)


def _update_watermark(conn, table: str, new_watermark: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bronze_watermarks (table_name, last_watermark)
            VALUES (%s, %s)
            ON CONFLICT (table_name)
            DO UPDATE SET last_watermark = EXCLUDED.last_watermark
        """, (table, new_watermark))
    conn.commit()


def _read_table_incremental(spark: SparkSession, args: argparse.Namespace, table: str, watermark: datetime):
    options = _get_jdbc_options(args)

    if table in TABLES_WITH_UPDATED_AT:
        watermark_str = watermark.strftime("%Y-%m-%d %H:%M:%S")
        query = f"(SELECT * FROM {table} WHERE updated_at > '{watermark_str}') AS src"
        return spark.read.format("jdbc").options(**options).option("dbtable", query).load()

    return spark.read.format("jdbc").options(**options).option("dbtable", table).load()


def _write_bronze(df, bronze_out: str, table: str, ingestion_ts: str, mode: str) -> None:
    out = os.path.join(bronze_out, table)

    write_df = df.withColumn("_ingestion_ts", F.lit(ingestion_ts))
    if os.environ.get("ECOM_DEBUG_SINGLE_PARTITION", "0") == "1":
        write_df = write_df.repartition(1)
    write_df.write.mode(mode).parquet(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", required=True)
    parser.add_argument("--jdbc_url", default=os.environ.get("ECOM_JDBC_URL", "jdbc:postgresql://postgres:5432/airflow"))
    parser.add_argument("--jdbc_user", default=os.environ.get("ECOM_JDBC_USER", "airflow"))
    parser.add_argument("--jdbc_password", default=os.environ.get("ECOM_JDBC_PASSWORD", "airflow"))
    parser.add_argument("--bronze_out", default=os.environ.get("ECOM_BRONZE_OUT", "/opt/spark-data/bronze"))
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    os.makedirs(args.bronze_out, exist_ok=True)

    conn = _get_pg_connection(args)
    _ensure_watermark_table(conn)

    spark = SparkSession.builder.appName("EcommerceBronzeBatch").getOrCreate()

    now = datetime.utcnow()
    ingestion_ts = now.isoformat(sep=" ", timespec="seconds")

    extracted_tables = []
    for table in tables:
        watermark = _get_watermark(conn, table)
        df = _read_table_incremental(spark, args, table, watermark)

        if not df.take(1):
            continue

        bronze_mode = "append" if table in TABLES_WITH_UPDATED_AT else "overwrite"
        _write_bronze(df, args.bronze_out, table, ingestion_ts, bronze_mode)
        extracted_tables.append(table)
        
        if table in TABLES_WITH_UPDATED_AT:
            _update_watermark(conn, table, now)
    spark.stop()
    conn.close()


if __name__ == "__main__":
    main()
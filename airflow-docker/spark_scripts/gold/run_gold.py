import argparse
import os
import shutil
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


PREVIEW_ROWS = int(os.environ.get("ECOM_PREVIEW_ROWS", "20"))


def _preview_df(df, name: str) -> None:
    if PREVIEW_ROWS <= 0:
        return
    print(f"=== Preview: {name} (top {PREVIEW_ROWS}) ===")
    df.show(PREVIEW_ROWS, truncate=False)


def _read_silver(spark: SparkSession, silver_path: str, table: str):
    path = os.path.join(silver_path, table)
    return spark.read.parquet(path)


def _write_gold(df, gold_path: str, table: str, mode: str = "overwrite") -> None:
    if os.environ.get("ECOM_DEBUG_SINGLE_PARTITION", "0") == "1":
        df = df.repartition(1)
    
    # For overwrite mode, use a temporary path then move it
    if mode == "overwrite":
        timestamp = int(time.time())
        temp_out = os.path.join(gold_path, f"{table}_temp_{timestamp}")
        final_out = os.path.join(gold_path, table)
        
        # Write to temporary path first
        (df.write
         .mode("overwrite")
         .option("spark.sql.parquet.writeLegacyFormat", "false")
         .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
         .parquet(temp_out))
        
        # Try to move the temporary directory to final location
        try:
            if os.path.exists(final_out):
                shutil.rmtree(final_out, ignore_errors=True)
            # Move temp directory to final location
            shutil.move(temp_out, final_out)
        except Exception:
            pass
    else:
        # For non-overwrite modes, write directly
        out = os.path.join(gold_path, table)
        (df.write
         .mode(mode)
         .option("spark.sql.parquet.writeLegacyFormat", "false")
         .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
         .parquet(out))


def build_dim_users(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """Dimension table for users"""
    users = _read_silver(spark, silver_path, "users")
    
    dim_users = users.select(
        F.col("user_id"),
        F.col("full_name"),
        F.col("email"),
        F.col("phone"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("gender"),
        F.col("date_of_birth"),
        F.col("loyalty_segment"),
        F.col("registration_source"),
        F.col("is_active"),
        F.col("created_at").alias("registered_at"),
    )
    
    _write_gold(dim_users, gold_path, "dim_users")


def build_dim_products(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """Dimension table for products with category info"""
    products = _read_silver(spark, silver_path, "products")
    categories = _read_silver(spark, silver_path, "categories")
    
    dim_products = products.join(
        categories.select(
            F.col("category_id"),
            F.col("category_name"),
        ),
        on="category_id",
        how="left"
    ).select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("sku"),
        F.col("category_id"),
        F.col("category_name"),
        F.col("brand"),
        F.col("base_price"),
        F.col("cost_price"),
        F.col("status"),
        F.col("created_at"),
    )
    
    _write_gold(dim_products, gold_path, "dim_products")


def build_dim_promotions(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """Dimension table for promotions"""
    promotions = _read_silver(spark, silver_path, "promotions")
    
    dim_promotions = promotions.select(
        F.col("promo_id"),
        F.col("promo_code"),
        F.col("discount_percent"),
        F.col("discount_type"),
        F.col("start_date"),
        F.col("end_date"),
        F.col("min_order_value"),
        F.col("max_discount_amount"),
        F.col("campaign_name"),
    )
    
    _write_gold(dim_promotions, gold_path, "dim_promotions")


def build_dim_marketing_channels(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """Dimension table for marketing channels"""
    channels = _read_silver(spark, silver_path, "marketing_channels")
    
    dim_channels = channels.select(
        F.col("channel_id"),
        F.col("channel_name"),
        F.col("channel_type"),
        F.col("platform"),
        F.col("utm_source"),
        F.col("utm_medium"),
    )
    
    _write_gold(dim_channels, gold_path, "dim_marketing_channels")


def build_fact_orders(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """Fact table for orders with order items aggregated"""
    orders = _read_silver(spark, silver_path, "orders")
    order_items = _read_silver(spark, silver_path, "order_items")
    order_promotions = _read_silver(spark, silver_path, "order_promotions")
    
    order_items_agg = order_items.groupBy("order_id").agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("line_total").alias("items_total"),
        F.count("order_item_id").alias("num_items"),
    )
    
    promo_agg = order_promotions.groupBy("order_id").agg(
        F.sum("applied_amount").alias("promo_discount_total"),
        F.count("promo_id").alias("num_promos_applied"),
    )
    
    fact_orders = orders.join(order_items_agg, on="order_id", how="left") \
        .join(promo_agg, on="order_id", how="left") \
        .select(
            F.col("order_id"),
            F.col("user_id"),
            F.col("order_date"),
            F.to_date("order_date").alias("order_date_key"),
            F.col("total_amount"),
            F.col("subtotal_amount"),
            F.col("tax_amount"),
            F.col("shipping_fee"),
            F.col("discount_amount"),
            F.coalesce(F.col("promo_discount_total"), F.lit(0)).alias("promo_discount_total"),
            F.coalesce(F.col("num_promos_applied"), F.lit(0)).alias("num_promos_applied"),
            F.col("status"),
            F.col("payment_method"),
            F.col("payment_status"),
            F.col("source_channel"),
            F.col("device_type"),
            F.coalesce(F.col("total_quantity"), F.lit(0)).alias("total_quantity"),
            F.coalesce(F.col("num_items"), F.lit(0)).alias("num_line_items"),
        )
    
    _write_gold(fact_orders, gold_path, "fact_orders")


def build_fact_order_items(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """Fact table for order line items"""
    order_items = _read_silver(spark, silver_path, "order_items")
    orders = _read_silver(spark, silver_path, "orders")
    
    fact_order_items = order_items.join(
        orders.select("order_id", "user_id", "order_date"),
        on="order_id",
        how="left"
    ).select(
        F.col("order_item_id"),
        F.col("order_id"),
        F.col("user_id"),
        F.col("product_id"),
        F.col("order_date"),
        F.to_date("order_date").alias("order_date_key"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_amount"),
        F.col("line_total"),
    )
    
    _write_gold(fact_order_items, gold_path, "fact_order_items")


def build_q1_conversion_funnel(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """
    Q1: Conversion Funnel theo danh mục
    Tính tỷ lệ từ View → Add to cart → Purchase theo từng category
    """
    clickstream = _read_silver(spark, silver_path, "clickstream")
    categories = _read_silver(spark, silver_path, "categories")
    
    funnel = clickstream.groupBy("category_id").agg(
        F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_carts"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
        F.countDistinct(F.when(F.col("event_type") == "view", F.col("user_id"))).alias("unique_viewers"),
        F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("unique_purchasers"),
    )
    
    q1_result = funnel.join(
        categories.select("category_id", "category_name"),
        on="category_id",
        how="left"
    ).withColumn(
        "view_to_cart_rate",
        F.when(F.col("views") > 0, F.round(F.col("add_to_carts") / F.col("views") * 100, 2)).otherwise(0)
    ).withColumn(
        "cart_to_purchase_rate",
        F.when(F.col("add_to_carts") > 0, F.round(F.col("purchases") / F.col("add_to_carts") * 100, 2)).otherwise(0)
    ).withColumn(
        "overall_conversion_rate",
        F.when(F.col("views") > 0, F.round(F.col("purchases") / F.col("views") * 100, 2)).otherwise(0)
    ).select(
        "category_id",
        "category_name",
        "views",
        "add_to_carts",
        "purchases",
        "unique_viewers",
        "unique_purchasers",
        "view_to_cart_rate",
        "cart_to_purchase_rate",
        "overall_conversion_rate",
    )
    
    _preview_df(q1_result, "gold_q1_conversion_funnel")
    _write_gold(q1_result, gold_path, "gold_q1_conversion_funnel")


def build_q2_rfm_segmentation(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """
    Q2: VIP & Churn risk theo RFM
    - Recency (R): số ngày từ lần mua gần nhất
    - Frequency (F): tổng số đơn
    - Monetary (M): tổng chi tiêu
    """
    orders = _read_silver(spark, silver_path, "orders")
    users = _read_silver(spark, silver_path, "users")
    
    today = F.current_date()
    
    rfm = orders.filter(F.col("status").isin("Paid", "Shipped")) \
        .groupBy("user_id").agg(
            F.datediff(today, F.max(F.to_date("order_date"))).alias("recency_days"),
            F.count("order_id").alias("frequency"),
            F.sum("total_amount").alias("monetary"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
    
    rfm_with_scores = rfm.withColumn(
        "r_score",
        F.when(F.col("recency_days") <= 30, 5)
         .when(F.col("recency_days") <= 60, 4)
         .when(F.col("recency_days") <= 90, 3)
         .when(F.col("recency_days") <= 180, 2)
         .otherwise(1)
    ).withColumn(
        "f_score",
        F.when(F.col("frequency") >= 10, 5)
         .when(F.col("frequency") >= 5, 4)
         .when(F.col("frequency") >= 3, 3)
         .when(F.col("frequency") >= 2, 2)
         .otherwise(1)
    ).withColumn(
        "m_score",
        F.when(F.col("monetary") >= 1000, 5)
         .when(F.col("monetary") >= 500, 4)
         .when(F.col("monetary") >= 200, 3)
         .when(F.col("monetary") >= 100, 2)
         .otherwise(1)
    ).withColumn(
        "rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score")
    ).withColumn(
        "customer_segment",
        F.when(F.col("rfm_score") >= 13, "VIP")
         .when(F.col("rfm_score") >= 10, "Loyal")
         .when(F.col("rfm_score") >= 7, "Potential")
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "At Risk")
         .when(F.col("r_score") <= 2, "Churn Risk")
         .otherwise("New/Casual")
    )
    
    q2_result = rfm_with_scores.join(
        users.select("user_id", "full_name", "email", "loyalty_segment"),
        on="user_id",
        how="left"
    ).select(
        "user_id",
        "full_name",
        "email",
        "loyalty_segment",
        "recency_days",
        "frequency",
        F.round("monetary", 2).alias("monetary"),
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "customer_segment",
        "first_order_date",
        "last_order_date",
    )
    
    _preview_df(q2_result, "gold_q2_rfm_segmentation")
    _write_gold(q2_result, gold_path, "gold_q2_rfm_segmentation")


def build_q3_promotion_effectiveness(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """
    Q3: Hiệu quả khuyến mãi
    So sánh doanh thu trung bình trong thời gian khuyến mãi vs ngoài khuyến mãi
    """
    orders = _read_silver(spark, silver_path, "orders")
    order_promotions = _read_silver(spark, silver_path, "order_promotions")
    promotions = _read_silver(spark, silver_path, "promotions")
    
    orders_with_promo = orders.join(
        order_promotions.select("order_id", "promo_id", "applied_amount"),
        on="order_id",
        how="left"
    )
    
    orders_with_promo = orders_with_promo.withColumn(
        "has_promotion",
        F.when(F.col("promo_id").isNotNull(), True).otherwise(False)
    )
    
    comparison = orders_with_promo.groupBy("has_promotion").agg(
        F.count("order_id").alias("num_orders"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum("discount_amount").alias("total_discount"),
        F.avg("discount_amount").alias("avg_discount"),
    )
    
    promo_detail = orders_with_promo.filter(F.col("promo_id").isNotNull()) \
        .join(promotions.select("promo_id", "promo_code", "discount_percent", "campaign_name"), on="promo_id", how="left") \
        .groupBy("promo_id", "promo_code", "discount_percent", "campaign_name").agg(
            F.count("order_id").alias("times_used"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("applied_amount").alias("total_discount_given"),
        ).withColumn(
            "revenue_per_discount",
            F.when(F.col("total_discount_given") > 0, 
                   F.round(F.col("total_revenue") / F.col("total_discount_given"), 2)
            ).otherwise(None)
        )
    
    _preview_df(comparison, "gold_q3_promo_comparison")
    _preview_df(promo_detail, "gold_q3_promo_detail")
    _write_gold(comparison, gold_path, "gold_q3_promo_comparison")
    _write_gold(promo_detail, gold_path, "gold_q3_promo_detail")


def build_q4_inventory_analysis(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """
    Q4: Slow-moving & Low stock
    DSI = (Average Inventory / Cost of Goods Sold) × 365
    """
    inventory = _read_silver(spark, silver_path, "inventory")
    products = _read_silver(spark, silver_path, "products")
    order_items = _read_silver(spark, silver_path, "order_items")
    orders = _read_silver(spark, silver_path, "orders")
    
    order_items_with_date = order_items.join(
        orders.select("order_id", "order_date", "status"),
        on="order_id",
        how="left"
    ).filter(F.col("status").isin("Paid", "Shipped"))
    
    sales_last_90_days = order_items_with_date.filter(
        F.col("order_date") >= F.date_sub(F.current_date(), 90)
    ).groupBy("product_id").agg(
        F.sum("quantity").alias("qty_sold_90d"),
        F.sum("line_total").alias("revenue_90d"),
    )
    
    sales_last_365_days = order_items_with_date.filter(
        F.col("order_date") >= F.date_sub(F.current_date(), 365)
    ).groupBy("product_id").agg(
        F.sum("quantity").alias("qty_sold_365d"),
        F.sum("line_total").alias("revenue_365d"),
    )
    
    inventory_agg = inventory.groupBy("product_id").agg(
        F.sum("stock_on_hand").alias("total_stock"),
        F.sum("reserved_quantity").alias("total_reserved"),
        F.avg("reorder_level").alias("avg_reorder_level"),
    )
    
    q4_result = products.select(
        "product_id", "product_name", "sku", "cost_price", "base_price", "status"
    ).join(inventory_agg, on="product_id", how="left") \
     .join(sales_last_90_days, on="product_id", how="left") \
     .join(sales_last_365_days, on="product_id", how="left") \
     .withColumn("qty_sold_90d", F.coalesce(F.col("qty_sold_90d"), F.lit(0))) \
     .withColumn("qty_sold_365d", F.coalesce(F.col("qty_sold_365d"), F.lit(0))) \
     .withColumn("total_stock", F.coalesce(F.col("total_stock"), F.lit(0))) \
     .withColumn(
         "cogs_365d",
         F.col("qty_sold_365d") * F.coalesce(F.col("cost_price"), F.lit(0))
     ).withColumn(
         "dsi",
         F.when(F.col("cogs_365d") > 0,
                F.round((F.col("total_stock") * F.col("cost_price")) / F.col("cogs_365d") * 365, 1)
         ).otherwise(None)
     ).withColumn(
         "available_stock",
         F.col("total_stock") - F.coalesce(F.col("total_reserved"), F.lit(0))
     ).withColumn(
         "daily_sales_rate",
         F.round(F.col("qty_sold_90d") / 90, 2)
     ).withColumn(
         "days_of_stock",
         F.when(F.col("daily_sales_rate") > 0,
                F.round(F.col("available_stock") / F.col("daily_sales_rate"), 0)
         ).otherwise(None)
     ).withColumn(
         "inventory_status",
         F.when(F.col("available_stock") <= 0, "Out of Stock")
          .when(F.col("available_stock") < F.col("avg_reorder_level"), "Low Stock")
          .when((F.col("dsi").isNotNull()) & (F.col("dsi") > 180), "Slow Moving")
          .when((F.col("dsi").isNotNull()) & (F.col("dsi") > 90), "Moderate")
          .otherwise("Healthy")
     ).select(
         "product_id",
         "product_name",
         "sku",
         "status",
         "total_stock",
         "available_stock",
         "avg_reorder_level",
         "qty_sold_90d",
         "qty_sold_365d",
         "daily_sales_rate",
         "days_of_stock",
         "dsi",
         "inventory_status",
     )
    
    _preview_df(q4_result, "gold_q4_inventory_analysis")
    _write_gold(q4_result, gold_path, "gold_q4_inventory_analysis")


def build_q5_attribution(spark: SparkSession, silver_path: str, gold_path: str) -> None:
    """
    Q5: Attribution cho quảng cáo
    - First-Touch: 100% cho nguồn click đầu tiên
    - Last-Touch: 100% cho nguồn click cuối trước khi mua
    - Linear: chia đều credit cho mọi nguồn đã tương tác trong 30 ngày
    """
    clickstream = _read_silver(spark, silver_path, "clickstream")
    orders = _read_silver(spark, silver_path, "orders")
    marketing_channels = _read_silver(spark, silver_path, "marketing_channels")
    
    purchases = clickstream.filter(F.col("event_type") == "purchase") \
        .select(
            F.col("user_id"),
            F.col("session_id"),
            F.col("event_time").alias("purchase_time"),
        ).distinct()
    
    user_touchpoints = clickstream.filter(F.col("event_type").isin("view", "add_to_cart")) \
        .join(purchases, on="user_id", how="inner") \
        .filter(
            (F.col("event_time") < F.col("purchase_time")) &
            (F.col("event_time") >= F.date_sub(F.col("purchase_time"), 30))
        ).select(
            "user_id",
            "channel_id",
            "event_time",
            "purchase_time",
            "event_type",
        )
    
    w_first = Window.partitionBy("user_id", "purchase_time").orderBy("event_time")
    w_last = Window.partitionBy("user_id", "purchase_time").orderBy(F.desc("event_time"))
    
    touchpoints_ranked = user_touchpoints.withColumn(
        "first_touch_rank", F.row_number().over(w_first)
    ).withColumn(
        "last_touch_rank", F.row_number().over(w_last)
    )
    
    first_touch = touchpoints_ranked.filter(F.col("first_touch_rank") == 1) \
        .groupBy("channel_id").agg(
            F.count("*").alias("first_touch_conversions"),
        )
    
    last_touch = touchpoints_ranked.filter(F.col("last_touch_rank") == 1) \
        .groupBy("channel_id").agg(
            F.count("*").alias("last_touch_conversions"),
        )
    
    touchpoint_counts = user_touchpoints.groupBy("user_id", "purchase_time").agg(
        F.count("*").alias("total_touchpoints")
    )
    
    linear_touchpoints = user_touchpoints.join(
        touchpoint_counts, on=["user_id", "purchase_time"], how="left"
    ).withColumn(
        "linear_credit", 1.0 / F.col("total_touchpoints")
    )
    
    linear_attribution = linear_touchpoints.groupBy("channel_id").agg(
        F.sum("linear_credit").alias("linear_conversions"),
    )
    
    q5_result = marketing_channels.select("channel_id", "channel_name", "channel_type") \
        .join(first_touch, on="channel_id", how="left") \
        .join(last_touch, on="channel_id", how="left") \
        .join(linear_attribution, on="channel_id", how="left") \
        .withColumn("first_touch_conversions", F.coalesce(F.col("first_touch_conversions"), F.lit(0))) \
        .withColumn("last_touch_conversions", F.coalesce(F.col("last_touch_conversions"), F.lit(0))) \
        .withColumn("linear_conversions", F.coalesce(F.round(F.col("linear_conversions"), 2), F.lit(0))) \
        .select(
            "channel_id",
            "channel_name",
            "channel_type",
            "first_touch_conversions",
            "last_touch_conversions",
            "linear_conversions",
        )
    
    _preview_df(q5_result, "gold_q5_attribution")
    _write_gold(q5_result, gold_path, "gold_q5_attribution")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_path", default=os.environ.get("ECOM_SILVER_OUT", "/opt/spark-data/silver"))
    parser.add_argument("--gold_path", default=os.environ.get("ECOM_GOLD_OUT", "/opt/spark-data/gold"))
    parser.add_argument("--build_dims", default="true")
    parser.add_argument("--build_facts", default="true")
    parser.add_argument("--build_q1", default="true")
    parser.add_argument("--build_q2", default="true")
    parser.add_argument("--build_q3", default="true")
    parser.add_argument("--build_q4", default="true")
    parser.add_argument("--build_q5", default="true")
    args = parser.parse_args()

    os.makedirs(args.gold_path, exist_ok=True)

    spark = SparkSession.builder.appName("EcommerceGoldLayer").getOrCreate()

    if args.build_dims.lower() == "true":
        build_dim_users(spark, args.silver_path, args.gold_path)
        build_dim_products(spark, args.silver_path, args.gold_path)
        build_dim_promotions(spark, args.silver_path, args.gold_path)
        build_dim_marketing_channels(spark, args.silver_path, args.gold_path)

    if args.build_facts.lower() == "true":
        build_fact_orders(spark, args.silver_path, args.gold_path)
        build_fact_order_items(spark, args.silver_path, args.gold_path)

    if args.build_q1.lower() == "true":
        build_q1_conversion_funnel(spark, args.silver_path, args.gold_path)

    if args.build_q2.lower() == "true":
        build_q2_rfm_segmentation(spark, args.silver_path, args.gold_path)

    if args.build_q3.lower() == "true":
        build_q3_promotion_effectiveness(spark, args.silver_path, args.gold_path)

    if args.build_q4.lower() == "true":
        build_q4_inventory_analysis(spark, args.silver_path, args.gold_path)

    if args.build_q5.lower() == "true":
        build_q5_attribution(spark, args.silver_path, args.gold_path)

    spark.stop()


if __name__ == "__main__":
    main()

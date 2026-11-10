"""
PART 2 / PART 1 bridge (Spark batch job): daily product performance metrics.

Computes, per (product_id, date): views, cart_adds, purchases (units), revenue.
Output -> data/clean/product_daily (Parquet). hbase/load_product_metrics.py then writes it into
the HBase `product_metrics` table (row key product_id#yyyymmdd).

Run:  python spark/04_product_metrics.py     (after spark/01_clean.py)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import functions as F

import common

spark = common.get_spark("04_product_metrics")
C = common.CLEAN_DIR

views = (spark.read.parquet(f"{C}/fact_page_views")
         .filter("page_type = 'product_detail' AND product_id IS NOT NULL")
         .groupBy("product_id", "date").agg(F.count("*").alias("views")))

cart = (spark.read.parquet(f"{C}/fact_cart_items")
        .groupBy("product_id", "date").agg(F.countDistinct("session_id").alias("cart_adds")))

sales = (spark.read.parquet(f"{C}/fact_txn_items")
         .groupBy("product_id", "date")
         .agg(F.sum("quantity").alias("purchases"), F.round(F.sum("line_total"), 2).alias("revenue")))

daily = (views.join(cart, ["product_id", "date"], "full_outer")
         .join(sales, ["product_id", "date"], "full_outer")
         .na.fill({"views": 0, "cart_adds": 0, "purchases": 0, "revenue": 0.0}))

daily.write.mode("overwrite").parquet(f"{C}/product_daily")
print(f"product_daily rows: {daily.count():,}")
daily.orderBy(F.desc("views")).show(10)
spark.stop()

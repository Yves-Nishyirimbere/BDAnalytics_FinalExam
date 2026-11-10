"""
PART 2 (Spark batch job 1): clean & normalise the raw JSON into an analytics-ready Parquet "lake".

What this job does
------------------
1. Reads ALL raw JSON with EXPLICIT schemas (no inference -> faster, and it avoids a classic trap:
   `cart_contents` is keyed by product_id, so inference would create a struct with thousands of
   fields. We model it as MapType instead).
2. Cleans: parses ISO strings to timestamps, de-duplicates on primary keys, fills missing values,
   standardises text, removes impossible values (negative durations/quantities).
3. Enriches: assigns the missing product subcategory (deterministic MD5 hash, identical to the
   Python rule used for MongoDB), flags data-quality problems instead of silently dropping rows.
4. Normalises nested structures into flat fact tables (page views, cart items, transaction items).
5. Writes Parquet (columnar + compressed), partitioned by date for the big fact tables.
6. Writes a data-quality report to results/data_quality_report.json.

Optimisation notes (for the report)
-----------------------------------
* explicit schemas, column pruning and Parquet output make every later job much cheaper than
  re-parsing JSON;  partitionBy(date) enables partition pruning for time-range queries;
* the JSON files are one big array per file (`multiLine=true`), which Spark cannot split, so
  parallelism = number of session files. In production we would emit JSON Lines instead.

Run:  python spark/01_clean.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType, MapType,
                               StringType, StructField, StructType)

import common

common.ensure_dirs()
spark = common.get_spark("01_clean")
RAW = common.DATA_DIR
OUT = common.CLEAN_DIR

# ------------------------------------------------------------------ explicit schemas
S = StringType()
geo = StructType([StructField("city", S), StructField("state", S), StructField("country", S)])

users_schema = StructType([
    StructField("user_id", S), StructField("geo_data", geo),
    StructField("registration_date", S), StructField("last_active", S)])

categories_schema = StructType([
    StructField("category_id", S), StructField("name", S),
    StructField("subcategories", ArrayType(StructType([
        StructField("subcategory_id", S), StructField("name", S),
        StructField("profit_margin", DoubleType())])))])

products_schema = StructType([
    StructField("product_id", S), StructField("name", S), StructField("category_id", S),
    StructField("base_price", DoubleType()), StructField("current_stock", LongType()),
    StructField("is_active", BooleanType()),
    StructField("price_history", ArrayType(StructType([
        StructField("price", DoubleType()), StructField("date", S)]))),
    StructField("creation_date", S)])

sessions_schema = StructType([
    StructField("session_id", S), StructField("user_id", S),
    StructField("start_time", S), StructField("end_time", S),
    StructField("duration_seconds", LongType()),
    StructField("geo_data", StructType([StructField("city", S), StructField("state", S),
                                        StructField("country", S), StructField("ip_address", S)])),
    StructField("device_profile", StructType([StructField("type", S), StructField("os", S),
                                              StructField("browser", S)])),
    StructField("viewed_products", ArrayType(S)),
    StructField("page_views", ArrayType(StructType([
        StructField("timestamp", S), StructField("page_type", S), StructField("product_id", S),
        StructField("category_id", S), StructField("view_duration", LongType())]))),
    StructField("cart_contents", MapType(S, StructType([
        StructField("quantity", LongType()), StructField("price", DoubleType())]))),
    StructField("conversion_status", S), StructField("referrer", S)])

transactions_schema = StructType([
    StructField("transaction_id", S), StructField("session_id", S), StructField("user_id", S),
    StructField("timestamp", S),
    StructField("items", ArrayType(StructType([
        StructField("product_id", S), StructField("quantity", LongType()),
        StructField("unit_price", DoubleType()), StructField("subtotal", DoubleType())]))),
    StructField("subtotal", DoubleType()), StructField("discount", DoubleType()),
    StructField("total", DoubleType()), StructField("payment_method", S), StructField("status", S)])


def read_json(name_or_glob, schema):
    return spark.read.schema(schema).option("multiLine", True).json(os.path.join(RAW, name_or_glob))


raw_users = read_json("users.json", users_schema)
raw_categories = read_json("categories.json", categories_schema)
raw_products = read_json("products.json", products_schema)
raw_sessions = read_json("sessions_*.json", sessions_schema)
raw_txn = read_json("transactions.json", transactions_schema)

dq = {}  # data-quality report

# ------------------------------------------------------------------ categories
categories = raw_categories.dropDuplicates(["category_id"]).withColumnRenamed("name", "category_name")
subcategories = (categories.select("category_id", F.explode("subcategories").alias("s"))
                 .select("category_id", "s.subcategory_id", F.col("s.name").alias("subcategory_name"),
                         F.col("s.profit_margin").alias("profit_margin")))

# ------------------------------------------------------------------ users
users = (raw_users.dropDuplicates(["user_id"])
         .withColumn("registration_ts", F.to_timestamp("registration_date"))
         .withColumn("last_active_ts", F.to_timestamp("last_active"))
         .withColumn("registration_month", F.date_format("registration_ts", "yyyy-MM"))
         .withColumn("country", F.coalesce(F.upper("geo_data.country"), F.lit("UNKNOWN")))
         .withColumn("state", F.coalesce(F.upper("geo_data.state"), F.lit("UNKNOWN")))
         .withColumn("city", F.coalesce("geo_data.city", F.lit("UNKNOWN")))
         .withColumn("dq_last_active_before_registration", F.col("last_active_ts") < F.col("registration_ts"))
         .drop("geo_data", "registration_date", "last_active"))

# ------------------------------------------------------------------ products
# Deterministic subcategory assignment: first 8 hex chars of md5(product_id) mod #subcategories.
# (identical to common.subcategory_index used when loading MongoDB)
idx = F.pmod(F.conv(F.substring(F.md5("product_id"), 1, 8), 16, 10).cast("long"), F.size("subcategories"))
products = (raw_products.dropDuplicates(["product_id"])
            .join(categories.select("category_id", "category_name", "subcategories"), "category_id", "left")
            .withColumn("sub", F.element_at("subcategories", (idx + 1).cast("int")))
            .withColumn("subcategory_id", F.col("sub.subcategory_id"))
            .withColumn("subcategory_name", F.col("sub.name"))
            .withColumn("profit_margin", F.col("sub.profit_margin"))
            .withColumn("base_price", F.coalesce("base_price", F.element_at("price_history", -1).price))
            .withColumn("is_active", F.coalesce("is_active", F.lit(False)))
            .withColumn("current_stock", F.greatest(F.coalesce("current_stock", F.lit(0)), F.lit(0)))
            .withColumn("creation_ts", F.to_timestamp("creation_date"))
            .withColumn("n_price_changes", F.size("price_history") - 1)
            .drop("subcategories", "sub", "price_history", "creation_date"))

# ------------------------------------------------------------------ transactions
txn_raw_n = raw_txn.count()
txn = (raw_txn.dropDuplicates(["transaction_id"])
       .filter(F.col("user_id").isNotNull() & F.col("items").isNotNull() & (F.size("items") > 0))
       .withColumn("ts", F.to_timestamp("timestamp"))
       .withColumn("date", F.to_date("ts"))
       .withColumn("status", F.lower(F.trim("status")))
       .withColumn("payment_method", F.lower(F.trim("payment_method")))
       .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
       .withColumn("has_session", F.col("session_id").isNotNull())
       .withColumn("n_items", F.size("items"))
       .withColumn("units", F.expr("aggregate(items, 0L, (acc, x) -> acc + x.quantity)"))
       # flag (do not drop) rows whose arithmetic does not add up
       .withColumn("dq_total_mismatch", F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) > 0.02)
       .withColumn("dq_items_mismatch", F.abs(F.expr("aggregate(items, 0D, (acc, x) -> acc + x.subtotal)")
                                              - F.col("subtotal")) > 0.02)
       .drop("timestamp").cache())

txn_items = (txn.select("transaction_id", "user_id", "session_id", "date", "ts",
                        F.explode("items").alias("i"))
             .select("transaction_id", "user_id", "session_id", "date", "ts",
                     F.col("i.product_id").alias("product_id"), F.col("i.quantity").alias("quantity"),
                     F.col("i.unit_price").alias("unit_price"), F.col("i.subtotal").alias("line_total"))
             .filter(F.col("quantity") > 0)
             .join(products.select("product_id", "category_id", "subcategory_id", "profit_margin"),
                   "product_id", "left")
             .withColumn("est_profit", F.round(F.col("line_total") * F.col("profit_margin"), 2)))
txn = txn.drop("items")

# ------------------------------------------------------------------ sessions
sessions_base = (raw_sessions.dropDuplicates(["session_id"])
                 .filter(F.col("user_id").isNotNull() & (F.col("duration_seconds") >= 0))
                 .withColumn("start_ts", F.to_timestamp("start_time"))
                 .withColumn("end_ts", F.to_timestamp("end_time"))
                 .withColumn("date", F.to_date("start_ts"))
                 .withColumn("conversion_status", F.lower(F.coalesce("conversion_status", F.lit("browsed"))))
                 .withColumn("referrer", F.coalesce("referrer", F.lit("unknown")))
                 .cache())

txn_session_ids = txn.filter("has_session").select("session_id").distinct().withColumn("has_transaction", F.lit(True))

fact_sessions = (sessions_base
                 .join(txn_session_ids, "session_id", "left")
                 .withColumn("has_transaction", F.coalesce("has_transaction", F.lit(False)))
                 .select("session_id", "user_id", "start_ts", "end_ts", "date",
                         F.hour("start_ts").alias("hour"), "duration_seconds",
                         F.col("device_profile.type").alias("device_type"),
                         F.col("device_profile.os").alias("os"),
                         F.col("device_profile.browser").alias("browser"),
                         F.col("geo_data.country").alias("country"), F.col("geo_data.state").alias("state"),
                         "referrer", "conversion_status",
                         F.size("page_views").alias("n_page_views"),
                         F.size("viewed_products").alias("n_viewed_products"),
                         F.size("cart_contents").alias("n_cart_products"),
                         "has_transaction")
                 .withColumn("dq_converted_without_txn",
                             (F.col("conversion_status") == "converted") & ~F.col("has_transaction")))

fact_page_views = (sessions_base
                   .select("session_id", "user_id", "date", F.posexplode("page_views").alias("pos", "pv"))
                   .select("session_id", "user_id", "date", "pos",
                           F.to_timestamp("pv.timestamp").alias("ts"),
                           F.col("pv.page_type").alias("page_type"),
                           F.col("pv.product_id").alias("product_id"),
                           F.col("pv.category_id").alias("category_id"),
                           F.col("pv.view_duration").alias("view_duration")))

fact_cart_items = (sessions_base
                   .select("session_id", "user_id", "date", F.explode("cart_contents").alias("product_id", "c"))
                   .select("session_id", "user_id", "date", "product_id",
                           F.col("c.quantity").alias("quantity"), F.col("c.price").alias("price"))
                   .filter(F.col("quantity") > 0))

# ------------------------------------------------------------------ data-quality report
dq["raw_counts"] = {"users": raw_users.count(), "products": raw_products.count(),
                    "categories": raw_categories.count(), "sessions": raw_sessions.count(),
                    "transactions": txn_raw_n}
n_txn = txn.count()
n_sess = fact_sessions.count()
dq["clean_counts"] = {"sessions": n_sess, "transactions": n_txn, "page_views": fact_page_views.count(),
                      "cart_items": fact_cart_items.count(), "txn_items": txn_items.count()}
dq["transactions_without_session"] = txn.filter(~F.col("has_session")).count()
dq["transactions_total_mismatch"] = txn.filter("dq_total_mismatch").count()
dq["transactions_items_mismatch"] = txn.filter("dq_items_mismatch").count()
dq["sessions_converted_without_transaction"] = fact_sessions.filter("dq_converted_without_txn").count()
dq["sessions_by_conversion_status"] = {r["conversion_status"]: r["count"] for r in
                                       fact_sessions.groupBy("conversion_status").count().collect()}
dq["transactions_by_status"] = {r["status"]: r["count"] for r in txn.groupBy("status").count().collect()}
dq["inactive_products"] = products.filter(~F.col("is_active")).count()
dq["users_last_active_before_registration"] = users.filter("dq_last_active_before_registration").count()
dq["note_subcategory"] = "products.json has no subcategory_id; assigned deterministically via md5(product_id)"

# ------------------------------------------------------------------ write Parquet
def write(df, name, partition=None):
    w = df.write.mode("overwrite")
    if partition:
        w = w.partitionBy(partition)
    w.parquet(os.path.join(OUT, name))
    print(f"  wrote {name}")


print("Writing Parquet to", OUT)
write(users, "dim_users")
write(categories.drop("subcategories"), "dim_categories")
write(subcategories, "dim_subcategories")
write(products, "dim_products")
write(fact_sessions, "fact_sessions", "date")
write(fact_page_views, "fact_page_views", "date")
write(fact_cart_items, "fact_cart_items", "date")
write(txn, "fact_transactions", "date")
write(txn_items, "fact_txn_items", "date")

with open(os.path.join(common.RESULTS_DIR, "data_quality_report.json"), "w") as f:
    json.dump(dq, f, indent=2)
print("\n=== DATA QUALITY REPORT ===")
print(json.dumps(dq, indent=2))
spark.stop()

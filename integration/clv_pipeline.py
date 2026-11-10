"""
PART 3: Integrated analytical query - Customer Lifetime Value (CLV) with engagement.

BUSINESS QUESTION
    Which customers are the most valuable going forward, how concentrated is our revenue in
    them, and does browsing engagement (session frequency / duration) go together with spend?

DATA SOURCES (and why each lives where it does)
    MongoDB : user profile + transaction history (embedded `purchase_summary`, built by the
              $merge pipeline) -> orders, spend, average order value, country/state
    HBase   : session activity stream (`user_sessions`) -> session count, session duration,
              converted / abandoned sessions per user. HBase is the right home for this
              high-volume, time-ordered event data.
    Spark   : the ONLY engine that sees both. It pulls the two extracts, joins them on user_id
              and does the computation that neither store can do alone.

PROCESSING STEPS
    1. Extract users+purchase summary from MongoDB      (pymongo -> Spark DataFrame, explicit schema)
    2. Extract per-session engagement from HBase        (happybase scan of 4 small columns -> Spark)
    3. Aggregate HBase sessions per user in Spark       (groupBy: sessions, avg duration, ...)
    4. Join on user_id (left join: users with no sessions in the HBase subset keep 0 sessions)
    5. CLV = avg_order_value x purchase_frequency_per_month x expected_lifespan_months
       (simple, transparent "historical" CLV; assumptions are explicit parameters below)
    6. Rank buyers into value tiers with percent_rank() and summarise tiers + engagement
    7. Write results/clv_users.csv, clv_tier_summary.csv, clv_engagement.json

PERFORMANCE NOTES
    * Only 4 tiny HBase columns are scanned (never the wide `pv` family) with a large scan batch.
    * The Mongo extract projects only the needed fields.
    * Joins are on a 10k-row dimension -> Spark would broadcast it automatically; for the
      real-scale case the HBase side would be pre-aggregated by Spark/MapReduce rather than
      scanned through a single client.

Run:  python integration/clv_pipeline.py              (needs MongoDB + HBase running)
      python integration/clv_pipeline.py --offline    (no NoSQL stores: same maths on Parquet)
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import Window
from pyspark.sql import functions as F

import common
import stores

LIFESPAN_MONTHS = float(os.getenv("CLV_LIFESPAN_MONTHS", "12"))  # assumption: expected customer lifetime

ap = argparse.ArgumentParser()
ap.add_argument("--offline", action="store_true", help="skip MongoDB/HBase, use Parquet instead")
args = ap.parse_args()

common.ensure_dirs()
spark = common.get_spark("clv_pipeline")
C = common.CLEAN_DIR

# ---------------------------------------------------------------- 1 + 2: extract
if args.offline:
    print("[offline mode] building the same inputs from Parquet")
    txn = spark.read.parquet(f"{C}/fact_transactions")
    t = txn.groupBy("user_id").agg(F.count("*").cast("long").alias("orders"),
                                   F.sum("total").alias("total_spent"),
                                   F.min("ts").alias("first_purchase"), F.max("ts").alias("last_purchase"))
    users = (spark.read.parquet(f"{C}/dim_users").join(t, "user_id", "left")
             .select("user_id", "country", "state", "registration_ts",
                     F.coalesce("orders", F.lit(0)).cast("long").alias("orders"),
                     F.coalesce("total_spent", F.lit(0.0)).alias("total_spent"),
                     F.when(F.col("orders") > 0, F.col("total_spent") / F.col("orders")).otherwise(0.0)
                      .alias("avg_order_value"), "first_purchase", "last_purchase"))
    eng = spark.read.parquet(f"{C}/fact_sessions").select(
        "user_id", "session_id", "duration_seconds", "conversion_status", "device_type")
else:
    print("[1] extracting users + purchase summary from MongoDB ...")
    users = stores.mongo_users_df(spark)
    print("[2] extracting session engagement from HBase ...")
    eng = stores.hbase_sessions_df(spark)
print(f"    users: {users.count():,}   hbase/session rows: {eng.count():,}")

# ---------------------------------------------------------------- 3: aggregate engagement per user
eng_user = eng.groupBy("user_id").agg(
    F.count("*").alias("sessions"),
    F.round(F.avg("duration_seconds"), 1).alias("avg_session_sec"),
    F.sum(F.when(F.col("conversion_status") == "converted", 1).otherwise(0)).alias("converted_sessions"),
    F.sum(F.when(F.col("conversion_status") == "abandoned", 1).otherwise(0)).alias("abandoned_sessions"))

# ---------------------------------------------------------------- 4: join
df = (users.join(eng_user, "user_id", "left")
      .na.fill({"sessions": 0, "converted_sessions": 0, "abandoned_sessions": 0, "avg_session_sec": 0.0}))

# ---------------------------------------------------------------- 5: CLV
bounds = users.agg(F.min("first_purchase").alias("a"), F.max("last_purchase").alias("b")).first()
window_days = max((bounds["b"] - bounds["a"]).days, 1)
months = window_days / 30.4
print(f"[5] observation window = {window_days} days ({months:.2f} months); lifespan assumption = {LIFESPAN_MONTHS} months")

df = (df.withColumn("orders_per_month", F.col("orders") / F.lit(months))
        .withColumn("clv", F.round(F.col("avg_order_value") * F.col("orders_per_month") * F.lit(LIFESPAN_MONTHS), 2)))

# ---------------------------------------------------------------- 6: tiers (buyers only)
w = Window.orderBy(F.desc("clv"))
buyers = (df.filter("orders > 0").withColumn("pr", F.percent_rank().over(w))
          .withColumn("tier", F.when(F.col("pr") < 0.10, "1-Platinum (top 10%)")
                               .when(F.col("pr") < 0.30, "2-Gold (next 20%)")
                               .when(F.col("pr") < 0.60, "3-Silver (next 30%)")
                               .otherwise("4-Bronze (bottom 40%)")))
total_clv = buyers.agg(F.sum("clv")).first()[0]
summary = (buyers.groupBy("tier")
           .agg(F.count("*").alias("customers"), F.round(F.avg("clv"), 2).alias("avg_clv"),
                F.round(F.sum("clv"), 2).alias("sum_clv"), F.round(F.avg("orders"), 2).alias("avg_orders"),
                F.round(F.avg("avg_order_value"), 2).alias("avg_order_value"),
                F.round(F.avg("sessions"), 2).alias("avg_sessions"),
                F.round(F.avg("avg_session_sec"), 0).alias("avg_session_sec"))
           .withColumn("pct_of_total_clv", F.round(100 * F.col("sum_clv") / F.lit(total_clv), 1))
           .orderBy("tier"))

print("\n=== CLV tiers (buyers only) ===")
sp = summary.toPandas()
print(sp.to_string(index=False))

corr_sessions = df.stat.corr("sessions", "total_spent")
corr_duration = buyers.stat.corr("avg_session_sec", "clv")
print(f"\ncorrelation(sessions, total_spent), all users : {corr_sessions:.3f}")
print(f"correlation(avg session seconds, CLV), buyers : {corr_duration:.3f}")

print("\n=== Top 10 customers by CLV ===")
top = buyers.orderBy(F.desc("clv")).select("user_id", "country", "orders", "avg_order_value", "clv",
                                           "sessions", "avg_session_sec", "tier").limit(10).toPandas()
print(top.to_string(index=False))

# ---------------------------------------------------------------- 7: outputs
(buyers.select("user_id", "country", "state", "orders", "total_spent", "avg_order_value", "orders_per_month",
               "clv", "sessions", "avg_session_sec", "converted_sessions", "abandoned_sessions", "tier")
 .toPandas().to_csv(f"{common.RESULTS_DIR}/clv_users.csv", index=False))
sp.to_csv(f"{common.RESULTS_DIR}/clv_tier_summary.csv", index=False)
with open(f"{common.RESULTS_DIR}/clv_engagement.json", "w") as f:
    json.dump({"window_days": window_days, "lifespan_months": LIFESPAN_MONTHS,
               "corr_sessions_vs_total_spent": corr_sessions, "corr_avgduration_vs_clv": corr_duration,
               "buyers": buyers.count(), "non_buyers": df.filter("orders = 0").count(),
               "source": "parquet(offline)" if args.offline else "MongoDB+HBase"}, f, indent=2)
print("\nwrote results/clv_users.csv, clv_tier_summary.csv, clv_engagement.json")
spark.stop()

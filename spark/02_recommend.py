"""
PART 2 (Spark batch job 2): product recommendation indicators (market-basket / co-occurrence).

  (a) "Frequently BOUGHT together"  - baskets = transactions       (transaction items)
  (b) "Frequently VIEWED together"  - baskets = sessions           (page views)

For every product pair (A, B) that appears in the same basket we compute:
    support(A,B)      = baskets containing both / all baskets
    confidence(A->B)  = baskets containing both / baskets containing A     ("users who bought A also bought B")
    lift(A,B)         = support(A,B) / (support(A) * support(B))           (>1 : more often than chance)

Implementation: self-join of the (basket, product) table on basket with product_a < product_b
(so each unordered pair is counted once), then aggregate. A minimum pair count removes one-off
coincidences. NOTE: the dataset is synthetic and products are drawn at random, so lift values are
expected to hover around chance level - the value here is the METHOD (and it scales: it is a
distributed join + aggregation).

Run:  python spark/02_recommend.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import Window
from pyspark.sql import functions as F

import common

common.ensure_dirs()
spark = common.get_spark("02_recommend")
C = common.CLEAN_DIR
MIN_PAIR_BOUGHT = int(os.getenv("MIN_PAIR_BOUGHT", "2"))
MIN_PAIR_VIEWED = int(os.getenv("MIN_PAIR_VIEWED", "3"))

products = spark.read.parquet(f"{C}/dim_products").select("product_id", "name", "category_id")


def pair_metrics(baskets, basket_col, min_pair):
    """baskets: DataFrame(basket_col, product_id), distinct. Returns pair metrics DataFrame."""
    n_baskets = baskets.select(basket_col).distinct().count()
    prod_cnt = baskets.groupBy("product_id").agg(F.countDistinct(basket_col).alias("n"))

    a, b = baskets.alias("a"), baskets.alias("b")
    pairs = (a.join(b, (F.col(f"a.{basket_col}") == F.col(f"b.{basket_col}")) &
                    (F.col("a.product_id") < F.col("b.product_id")))
             .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
             .agg(F.count("*").alias("pair_count"))
             .filter(F.col("pair_count") >= min_pair))

    pa, pb = prod_cnt.alias("pa"), prod_cnt.alias("pb")
    m = (pairs.join(pa, F.col("product_a") == F.col("pa.product_id")).drop("product_id")
         .withColumnRenamed("n", "n_a")
         .join(pb, F.col("product_b") == F.col("pb.product_id")).drop("product_id")
         .withColumnRenamed("n", "n_b")
         .withColumn("support", F.col("pair_count") / n_baskets)
         .withColumn("confidence_a_to_b", F.col("pair_count") / F.col("n_a"))
         .withColumn("confidence_b_to_a", F.col("pair_count") / F.col("n_b"))
         .withColumn("lift", F.col("pair_count") * n_baskets / (F.col("n_a") * F.col("n_b"))))
    return m, n_baskets


def recommendations_per_product(m, top_n=3):
    """Turn pair metrics into 'if you like X, recommend Y' rows (both directions), top_n per product."""
    ab = m.select(F.col("product_a").alias("product_id"), F.col("product_b").alias("recommended"),
                  F.col("confidence_a_to_b").alias("confidence"), "lift", "pair_count")
    ba = m.select(F.col("product_b").alias("product_id"), F.col("product_a").alias("recommended"),
                  F.col("confidence_b_to_a").alias("confidence"), "lift", "pair_count")
    w = Window.partitionBy("product_id").orderBy(F.desc("confidence"), F.desc("lift"))
    return (ab.unionByName(ba).withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") <= top_n))


def named(df, left="product_a", right="product_b"):
    return (df.join(products.select(F.col("product_id").alias(left),
                                    F.col("name").alias(f"{left}_name")), left)
              .join(products.select(F.col("product_id").alias(right),
                                    F.col("name").alias(f"{right}_name")), right))


# ---------------------------------------------------------------- (a) bought together
bought = spark.read.parquet(f"{C}/fact_txn_items").select("transaction_id", "product_id").distinct()
m_bought, n_txn = pair_metrics(bought, "transaction_id", MIN_PAIR_BOUGHT)
top_bought = named(m_bought).orderBy(F.desc("lift"), F.desc("pair_count"))
print(f"\n=== Frequently BOUGHT together ({n_txn:,} baskets, min pair count {MIN_PAIR_BOUGHT}) ===")
top_bought.select("product_a_name", "product_b_name", "pair_count", "support",
                  "confidence_a_to_b", "lift").show(10, truncate=40)
top_bought.limit(200).toPandas().to_csv(f"{common.RESULTS_DIR}/spark_bought_together.csv", index=False)
(recommendations_per_product(m_bought).toPandas()
 .to_csv(f"{common.RESULTS_DIR}/spark_recommendations_bought.csv", index=False))

# ---------------------------------------------------------------- (b) viewed together
viewed = (spark.read.parquet(f"{C}/fact_page_views")
          .filter("page_type = 'product_detail' AND product_id IS NOT NULL")
          .select("session_id", "product_id").distinct())
m_viewed, n_sess = pair_metrics(viewed, "session_id", MIN_PAIR_VIEWED)
top_viewed = named(m_viewed).orderBy(F.desc("lift"), F.desc("pair_count"))
print(f"\n=== Frequently VIEWED together ({n_sess:,} sessions with a product view, min pair count {MIN_PAIR_VIEWED}) ===")
top_viewed.select("product_a_name", "product_b_name", "pair_count", "support",
                  "confidence_a_to_b", "lift").show(10, truncate=40)
top_viewed.limit(200).toPandas().to_csv(f"{common.RESULTS_DIR}/spark_viewed_together.csv", index=False)

# sanity statistic worth quoting in the report: how far from chance are the lifts?
for label, m in (("bought", m_bought), ("viewed", m_viewed)):
    s = m.agg(F.count("*").alias("pairs"), F.round(F.avg("lift"), 2).alias("avg_lift"),
              F.round(F.max("lift"), 2).alias("max_lift")).first()
    print(f"[{label}] pairs kept={s['pairs']:,}  avg lift={s['avg_lift']}  max lift={s['max_lift']}")
spark.stop()

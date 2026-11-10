"""
PART 2 (Spark SQL analytics): complex SQL over the cleaned Parquet tables.

  Q1  Top-3 categories per month                      (CTE + RANK() window)
  Q2  Conversion rate by device and by referrer       (GROUPING SETS)
  Q3  Daily revenue with a 7-day moving average       (window frame)
  Q4  Customer spend quartiles + engagement           (3-way join, NTILE window)
  Q5  Revenue vs estimated profit by category         (joins to dimension tables)
  Q6  Conversion funnel (strict, per session)         (conditional aggregation)
  Q7  Product popularity: viewed vs sold              (two aggregates joined to the catalog)
  Q8  Cross-store join: MongoDB users x HBase sessions (data that "lives" in the NoSQL stores)

Every result is printed and written to results/*.csv (used by viz/make_charts.py).

Run:  python spark/03_sql_analytics.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common
import stores

common.ensure_dirs()
spark = common.get_spark("03_sql_analytics")
C = common.CLEAN_DIR

# ---- register the cleaned Parquet tables as SQL views ------------------------------------------
for view, path in {
    "users": "dim_users", "products": "dim_products", "categories": "dim_categories",
    "sessions": "fact_sessions", "page_views": "fact_page_views",
    "txn": "fact_transactions", "txn_items": "fact_txn_items",
}.items():
    spark.read.parquet(f"{C}/{path}").createOrReplaceTempView(view)


def q(title, sql, csv=None, n=12):
    print(f"\n=== {title} ===")
    pdf = spark.sql(sql).toPandas()
    print(pdf.head(n).to_string(index=False))
    if csv:
        pdf.to_csv(f"{common.RESULTS_DIR}/{csv}", index=False)
    return pdf


# ---- Q1 -------------------------------------------------------------------------------------
q("Q1 Top-3 categories by revenue in each month", """
WITH monthly AS (
    SELECT date_format(i.date, 'yyyy-MM') AS month, c.category_name,
           ROUND(SUM(i.line_total), 2) AS revenue, SUM(i.quantity) AS units
    FROM txn_items i
    JOIN products p   ON i.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    GROUP BY 1, 2)
SELECT month, category_name, revenue, units, rnk FROM (
    SELECT *, RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rnk FROM monthly)
WHERE rnk <= 3 ORDER BY month, rnk
""", "sql_top_categories_month.csv")

# ---- Q2 -------------------------------------------------------------------------------------
q("Q2 Conversion rate by device type and by referrer", """
SELECT device_type, referrer, COUNT(*) AS sessions,
       ROUND(100 * AVG(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END), 2) AS pct_converted,
       ROUND(100 * AVG(CASE WHEN has_transaction THEN 1 ELSE 0 END), 2)                 AS pct_with_transaction,
       ROUND(AVG(duration_seconds), 0) AS avg_duration_s
FROM sessions
GROUP BY GROUPING SETS ((device_type), (referrer))
ORDER BY device_type NULLS LAST, referrer
""", "sql_conversion_by_segment.csv")

# ---- Q3 -------------------------------------------------------------------------------------
q("Q3 Daily revenue with 7-day moving average", """
WITH daily AS (SELECT date, SUM(total) AS revenue, COUNT(*) AS orders FROM txn GROUP BY date)
SELECT date, ROUND(revenue, 2) AS revenue, orders,
       ROUND(AVG(revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_ma7
FROM daily ORDER BY date
""", "sql_daily_revenue.csv", n=8)

# ---- Q4 -------------------------------------------------------------------------------------
q("Q4 Customer spend quartiles and their engagement", """
WITH spend AS (SELECT user_id, COUNT(*) AS orders, SUM(total) AS total_spent FROM txn GROUP BY user_id),
     sess  AS (SELECT user_id, COUNT(*) AS sessions, AVG(duration_seconds) AS avg_dur FROM sessions GROUP BY user_id),
     joined AS (
        SELECT u.user_id, COALESCE(s.orders, 0) AS orders, COALESCE(s.total_spent, 0) AS total_spent,
               COALESCE(e.sessions, 0) AS sessions, e.avg_dur
        FROM users u LEFT JOIN spend s ON u.user_id = s.user_id
                     LEFT JOIN sess  e ON u.user_id = e.user_id),
     q AS (SELECT *, NTILE(4) OVER (ORDER BY total_spent) AS spend_quartile FROM joined)
SELECT spend_quartile, COUNT(*) AS users, ROUND(AVG(orders), 2) AS avg_orders,
       ROUND(AVG(total_spent), 2) AS avg_spend, ROUND(AVG(sessions), 1) AS avg_sessions,
       ROUND(AVG(avg_dur), 0) AS avg_session_seconds
FROM q GROUP BY spend_quartile ORDER BY spend_quartile
""", "sql_spend_quartiles.csv")

# ---- Q5 -------------------------------------------------------------------------------------
q("Q5 Revenue vs estimated profit by category (profit = line total x subcategory margin)", """
SELECT c.category_name, ROUND(SUM(i.line_total), 2) AS revenue, ROUND(SUM(i.est_profit), 2) AS est_profit,
       ROUND(100 * SUM(i.est_profit) / SUM(i.line_total), 1) AS margin_pct
FROM txn_items i JOIN categories c ON i.category_id = c.category_id
GROUP BY c.category_name ORDER BY est_profit DESC
""", "sql_profit_by_category.csv")

# ---- Q6 -------------------------------------------------------------------------------------
funnel = spark.sql("""
WITH stage AS (
    SELECT pv.session_id,
           MAX(CASE WHEN page_type = 'product_detail' THEN 1 ELSE 0 END) AS viewed,
           MAX(CASE WHEN page_type = 'cart'           THEN 1 ELSE 0 END) AS cart,
           MAX(CASE WHEN page_type = 'checkout'       THEN 1 ELSE 0 END) AS checkout,
           MAX(CASE WHEN page_type = 'confirmation'   THEN 1 ELSE 0 END) AS confirmation
    FROM page_views pv GROUP BY pv.session_id)
SELECT COUNT(*)                                                    AS all_sessions,
       SUM(viewed)                                                 AS viewed_product,
       SUM(viewed * cart)                                          AS added_to_cart,
       SUM(viewed * cart * checkout)                               AS reached_checkout,
       SUM(viewed * cart * checkout * confirmation)                AS reached_confirmation,
       SUM(viewed * cart * checkout * confirmation * CAST(s.has_transaction AS INT)) AS purchased
FROM stage JOIN sessions s ON stage.session_id = s.session_id
""").first()
import pandas as pd
stages = ["all_sessions", "viewed_product", "added_to_cart", "reached_checkout", "reached_confirmation", "purchased"]
fdf = pd.DataFrame({"stage": stages, "sessions": [int(funnel[s]) for s in stages]})
fdf["pct_of_all"] = (100 * fdf.sessions / fdf.sessions.iloc[0]).round(2)
fdf["step_conversion_pct"] = (100 * fdf.sessions / fdf.sessions.shift(1)).round(2)
print("\n=== Q6 Strict conversion funnel ===")
print(fdf.to_string(index=False))
fdf.to_csv(f"{common.RESULTS_DIR}/sql_funnel.csv", index=False)

# ---- Q7 -------------------------------------------------------------------------------------
q("Q7 Product popularity: views vs units sold", """
WITH v AS (SELECT product_id, COUNT(*) AS views FROM page_views WHERE page_type = 'product_detail' GROUP BY 1),
     s AS (SELECT product_id, SUM(quantity) AS units_sold, SUM(line_total) AS revenue FROM txn_items GROUP BY 1)
SELECT p.product_id, p.name, c.category_name, COALESCE(v.views, 0) AS views,
       COALESCE(s.units_sold, 0) AS units_sold, ROUND(COALESCE(s.revenue, 0), 2) AS revenue,
       ROUND(100 * COALESCE(s.units_sold, 0) / NULLIF(v.views, 0), 2) AS units_per_100_views
FROM products p JOIN categories c ON p.category_id = c.category_id
LEFT JOIN v ON p.product_id = v.product_id LEFT JOIN s ON p.product_id = s.product_id
ORDER BY views DESC
""", "sql_product_popularity.csv", n=8)

# ---- Q8 -------------------------------------------------------------------------------------
print("\n=== Q8 Cross-store SQL: MongoDB users (profile + purchase summary) x HBase sessions ===")
try:
    stores.mongo_users_df(spark).createOrReplaceTempView("mongo_users")
    src_m = "MongoDB"
except Exception as e:  # keeps the job runnable when Mongo is down
    print(f"  (MongoDB unavailable: {type(e).__name__}) -> using the equivalent Spark table instead")
    spark.sql("""SELECT u.user_id, u.country, u.state, u.registration_ts,
                        CAST(COALESCE(t.orders,0) AS BIGINT) AS orders, CAST(COALESCE(t.spent,0) AS DOUBLE) AS total_spent
                 FROM users u LEFT JOIN (SELECT user_id, COUNT(*) orders, SUM(total) spent FROM txn GROUP BY user_id) t
                 ON u.user_id = t.user_id""").createOrReplaceTempView("mongo_users")
    src_m = "Spark-fallback"
try:
    stores.hbase_sessions_df(spark).createOrReplaceTempView("hbase_sessions")
    src_h = "HBase"
except Exception as e:
    print(f"  (HBase unavailable: {type(e).__name__}) -> using the equivalent Spark table instead")
    spark.sql("SELECT user_id, session_id, duration_seconds, conversion_status, device_type FROM sessions"
              ).createOrReplaceTempView("hbase_sessions")
    src_h = "Spark-fallback"
print(f"  sources used: users <- {src_m}, sessions <- {src_h}")
q("Q8 Average spend and engagement of buyers by country (top 10)", """
SELECT m.country, COUNT(*) AS buyers, ROUND(AVG(m.total_spent), 2) AS avg_spend,
       ROUND(AVG(h.sessions), 1) AS avg_sessions, ROUND(AVG(h.avg_dur), 0) AS avg_session_seconds
FROM mongo_users m
LEFT JOIN (SELECT user_id, COUNT(*) AS sessions, AVG(duration_seconds) AS avg_dur
           FROM hbase_sessions GROUP BY user_id) h ON m.user_id = h.user_id
WHERE m.orders > 0
GROUP BY m.country HAVING COUNT(*) >= 20 ORDER BY avg_spend DESC LIMIT 10
""", "sql_cross_store_country.csv")
spark.stop()

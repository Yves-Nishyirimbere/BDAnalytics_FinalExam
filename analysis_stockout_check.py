import pandas as pd
pp = pd.read_csv("results/sql_product_popularity.csv")
pr = pd.read_parquet("data/clean/dim_products")[["product_id", "current_stock", "is_active", "creation_ts"]]
d = pp.merge(pr, on="product_id")
d = d[d.views > 0].copy()
d["low_view_tail"] = d["views"] < d["views"].quantile(0.10)
print(d.groupby("low_view_tail").agg(
    products=("product_id", "count"), avg_views=("views", "mean"),
    avg_current_stock=("current_stock", "mean"),
    pct_zero_stock=("current_stock", lambda s: 100 * (s == 0).mean()),
    pct_active=("is_active", lambda s: 100 * s.mean()),
    latest_creation=("creation_ts", "max")).to_string())

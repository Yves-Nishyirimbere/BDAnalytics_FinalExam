"""
PART 4: static visualisations built from the result files written by the MongoDB / Spark /
integration steps (results/*.csv). Output: figures/*.png (180 dpi, ready to embed in the report).

  fig1_sales_performance.png      daily revenue + 7-day average, and top categories by revenue
  fig2_customer_segmentation.png  users & avg spend by purchase-frequency segment, CLV tier value share
  fig3_product_performance.png    views vs units sold per product (+ top sellers highlighted)
  fig4_conversion_funnel.png      strict funnel: view -> cart -> checkout -> confirmation -> purchase
  fig5_conversion_by_segment.png  conversion by device and by referrer

Run:  python viz/make_charts.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

import common

common.ensure_dirs()
sns.set_theme(style="whitegrid", context="talk", palette="deep")
R = common.RESULTS_DIR
BLUE, ORANGE, GREY = "#2b6cb0", "#dd6b20", "#a0aec0"


def read(name):
    path = os.path.join(R, name)
    if not os.path.exists(path):
        sys.exit(f"missing {path} - run the earlier pipeline steps first")
    return pd.read_csv(path)


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(common.FIGURES_DIR, name), dpi=180, bbox_inches="tight")
    plt.close(fig)
    print("saved figures/" + name)


# ------------------------------------------------------------------ Fig 1: sales performance
daily = read("sql_daily_revenue.csv")
daily["date"] = pd.to_datetime(daily["date"])
# first/last day of the window are PARTIAL days (generator cuts at "now") -> drop them so the
# chart does not show a fake collapse, and recompute the moving average on the remaining days
daily = daily.sort_values("date").iloc[1:-1].copy()
daily["revenue_ma7"] = daily["revenue"].rolling(7, min_periods=1).mean()
cat = read("mongo_revenue_category_month.csv")
top_cat = (cat.groupby("category_name")["revenue"].sum().sort_values(ascending=False).head(10)[::-1])

fig, ax = plt.subplots(1, 2, figsize=(18, 6.5), gridspec_kw={"width_ratios": [1.3, 1]})
ax[0].plot(daily["date"], daily["revenue"], color=GREY, lw=1.5, label="Daily revenue")
ax[0].plot(daily["date"], daily["revenue_ma7"], color=BLUE, lw=3, label="7-day moving average")
ax[0].set(title="Daily revenue, partial edge days excluded (Spark SQL)", xlabel="", ylabel="Revenue per day",
          ylim=(0, daily["revenue"].max() * 1.15))      # zero-based: day-to-day swings are only a few %
ax[0].yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"${v / 1e6:.1f}M"))
ax[0].legend(loc="lower right", fontsize=12)
ax[0].tick_params(axis="x", rotation=30)
ax[1].barh(top_cat.index, top_cat.values, color=BLUE)
ax[1].set(title="Top 10 categories by revenue (MongoDB)", xlabel="Revenue")
ax[1].xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"${v / 1e6:.0f}M"))
ax[1].tick_params(axis="y", labelsize=11)
save(fig, "fig1_sales_performance.png")

# ------------------------------------------------------------------ Fig 2: customer segmentation
seg = read("mongo_user_segments.csv")
# $bucketAuto boundaries are [min, max) - the upper bound is EXCLUSIVE except for the last bucket
def _label(i, r, last):
    lo, hi = int(r.min_orders), int(r.max_orders) if i == last else int(r.max_orders) - 1
    return f"{lo}" if lo == hi else f"{lo}-{hi}"
seg["label"] = [_label(i, r, len(seg) - 1) for i, r in enumerate(seg.itertuples())]
tiers = read("clv_tier_summary.csv")
tiers["short"] = tiers["tier"].str.replace(r"^\d-", "", regex=True).str.split(" ").str[0]

fig, ax = plt.subplots(1, 2, figsize=(18, 6.5))
bars = ax[0].bar(seg["label"], seg["users"], color=BLUE)
ax[0].set(title="Customers by purchase frequency (MongoDB $bucketAuto)", ylabel="Customers",
          xlabel="Orders per customer")
ax2 = ax[0].twinx()
ax2.plot(seg["label"], seg["avg_spend"], color=ORANGE, marker="o", lw=3)
ax2.set_ylabel("Avg spend per customer ($)", color=ORANGE)
ax2.grid(False)
for b, u in zip(bars, seg["users"]):
    ax[0].text(b.get_x() + b.get_width() / 2, b.get_height(), f"{u:,}", ha="center", va="bottom", fontsize=12)
ax[1].bar(tiers["short"], tiers["pct_of_total_clv"], color=[BLUE, "#4a90c2", "#8fb8de", GREY])
for i, (p, n) in enumerate(zip(tiers["pct_of_total_clv"], tiers["customers"])):
    ax[1].text(i, p, f"{p:.0f}%\n({n:,} cust.)", ha="center", va="bottom", fontsize=12)
ax[1].set(title="Share of total CLV by value tier",
          ylabel="% of total CLV", ylim=(0, tiers["pct_of_total_clv"].max() * 1.25))
save(fig, "fig2_customer_segmentation.png")

# ------------------------------------------------------------------ Fig 3: product performance
pp_all = read("sql_product_popularity.csv")
pp = pp_all[pp_all["views"] > 0].copy()          # inactive products have 0 views & 0 sales: excluded
n_excluded = len(pp_all) - len(pp)
fig, ax = plt.subplots(figsize=(14, 7.5))
rng = np.random.default_rng(1)
jx = pp["views"] + rng.uniform(-0.25, 0.25, len(pp))   # jitter: integer counts overlap heavily
jy = pp["units_sold"] + rng.uniform(-0.25, 0.25, len(pp))
ax.scatter(jx, jy, s=18, alpha=0.35, color=BLUE, edgecolors="none")
top = pp.nlargest(5, "units_sold")
for (_, r), mk in zip(top.iterrows(), ["o", "s", "D", "^", "v"]):   # one legend entry per top seller -> no text overlap
    ax.scatter(r["views"], r["units_sold"], s=130, marker=mk, color=ORANGE, edgecolors="black",
               linewidths=0.8, zorder=5, label=f"{r['product_id']}  ({int(r['units_sold'])} units)")
if pp["views"].std() > 0 and pp["units_sold"].std() > 0:
    k = np.polyfit(pp["views"], pp["units_sold"], 1)
    xs = np.linspace(pp["views"].min(), pp["views"].max(), 50)
    r_all = pp["views"].corr(pp["units_sold"])
    core = pp[pp["views"] >= pp["views"].quantile(0.25)]      # drop the low-view tail
    r_core = core["views"].corr(core["units_sold"])
    ax.plot(xs, np.polyval(k, xs), color="black", ls="--", lw=2, label=f"linear trend, all products (r = {r_all:.2f})")
    ax.plot([], [], " ", label=f"r = {r_core:.2f} among the 75% of products with most views")
ax.set(title="Product performance: views vs units sold (each dot = 1 product)",
       xlabel=f"Product page views  ({n_excluded} inactive products with 0 views not shown)", ylabel="Units sold")
ax.legend(fontsize=11, loc="upper left", bbox_to_anchor=(1.01, 1.0), title="Top-5 sellers", title_fontsize=12,
          frameon=False)   # outside the axes so it never hides data
save(fig, "fig3_product_performance.png")

# ------------------------------------------------------------------ Fig 4: conversion funnel
fn = read("sql_funnel.csv")
labels = ["All sessions", "Viewed a product", "Added to cart", "Reached checkout", "Reached confirmation", "Purchased"]
fig, ax = plt.subplots(figsize=(12, 6.5))
y = np.arange(len(fn))[::-1]
ax.barh(y, fn["sessions"], color=sns.color_palette("Blues_r", len(fn)))
ax.set_yticks(y)
ax.set_yticklabels(labels)
for yi, s, p, st in zip(y, fn["sessions"], fn["pct_of_all"], fn["step_conversion_pct"]):
    step = "" if np.isnan(st) else f"  (step: {st:.0f}%)"
    ax.text(s, yi, f"  {int(s):,} - {p:.0f}%{step}", va="center", fontsize=13)
ax.set(title="Session conversion funnel (Spark SQL on page-view events)", xlabel="Sessions")
ax.set_xlim(0, fn["sessions"].max() * 1.45)
ax.grid(axis="y", visible=False)
save(fig, "fig4_conversion_funnel.png")

# ------------------------------------------------------------------ Fig 5: conversion by segment
cv = read("sql_conversion_by_segment.csv")
dev = cv[cv["device_type"].notna()][["device_type", "pct_converted", "pct_with_transaction"]]
ref = cv[cv["referrer"].notna()][["referrer", "pct_converted", "pct_with_transaction"]]
fig, ax = plt.subplots(1, 2, figsize=(17, 6), sharey=True)
same = np.allclose(cv["pct_converted"], cv["pct_with_transaction"], atol=0.05)
for a, d, col, title in ((ax[0], dev, "device_type", "By device type"), (ax[1], ref, "referrer", "By referrer")):
    x = np.arange(len(d))
    if same:   # every converted session has a transaction -> two identical bars would only add clutter
        bars = a.bar(x, d["pct_converted"], 0.55, color=BLUE, label="Converted sessions (each has a transaction)")
        for b_, v in zip(bars, d["pct_converted"]):
            a.text(b_.get_x() + b_.get_width() / 2, v, f"{v:.1f}%", ha="center", va="bottom", fontsize=12)
    else:
        a.bar(x - 0.2, d["pct_converted"], 0.4, label="Session marked 'converted'", color=BLUE)
        a.bar(x + 0.2, d["pct_with_transaction"], 0.4, label="Transaction recorded", color=ORANGE)
    a.set_xticks(x)
    a.set_xticklabels(d[col], rotation=15)
    a.set(title=title, ylim=(0, max(cv["pct_converted"].max() * 1.35, 1)))
    a.grid(axis="x", visible=False)
ax[0].set_ylabel("% of sessions")
ax[0].legend(fontsize=12, loc="upper center", ncol=2)
save(fig, "fig5_conversion_by_segment.png")
print("all figures written to", common.FIGURES_DIR)

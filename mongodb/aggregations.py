"""
PART 1 (MongoDB): analytical aggregation pipelines.

  A1  Revenue by category and month           (revenue analytics)
  A2  User segmentation by purchase frequency  (basic segmentation, $bucketAuto)
  A3  Product popularity: top sold vs most viewed (two collections, joined in Python)
  A4  Revenue by US state                      ($lookup into users)
  A5  Build embedded user summary              ($merge writes back into `users`)

Each result is printed and saved to results/*.csv (used later for the charts).

Run:  python mongodb/aggregations.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from pymongo import MongoClient

import common

common.ensure_dirs()
db = MongoClient(common.MONGO_URI, serverSelectionTimeoutMS=5000)[common.MONGO_DB]


def run(name, coll, pipeline, csv=None, show=10):
    t0 = time.time()
    rows = list(db[coll].aggregate(pipeline, allowDiskUse=True))
    dt = time.time() - t0
    df = pd.DataFrame(rows)
    print(f"\n=== {name}  ({len(df)} rows, {dt:.2f}s) ===")
    if not df.empty:
        print(df.head(show).to_string(index=False))
        if csv:
            df.to_csv(os.path.join(common.RESULTS_DIR, csv), index=False)
    return df


# --------------------------------------------------------------------------------------------
# A1. Revenue by category and month
#     $unwind explodes the embedded items; category_id is already denormalised on each item,
#     so no join is needed for the grouping. $lookup only adds the readable name at the end
#     (after grouping, i.e. on a few hundred rows instead of every line item).
# --------------------------------------------------------------------------------------------
A1 = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": {"category_id": "$items.category_id",
                "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}},
        "revenue": {"$sum": "$items.subtotal"},
        "units": {"$sum": "$items.quantity"},
        "orders": {"$addToSet": "$transaction_id"},
    }},
    {"$project": {"_id": 0, "category_id": "$_id.category_id", "month": "$_id.month",
                  "revenue": {"$round": ["$revenue", 2]}, "units": 1,
                  "orders": {"$size": "$orders"}}},
    {"$lookup": {"from": "categories", "localField": "category_id",
                 "foreignField": "category_id", "as": "cat"}},
    {"$addFields": {"category_name": {"$arrayElemAt": ["$cat.name", 0]}}},
    {"$project": {"cat": 0}},
    {"$sort": {"month": 1, "revenue": -1}},
]

# --------------------------------------------------------------------------------------------
# A2. Segment users by number of orders.
#     $bucketAuto chooses boundaries from the data distribution, so segments stay balanced
#     whatever the dataset size (a fixed [1,2,3,5,...] split breaks when you scale the data).
#     NOTE: bucket ranges are [min, max) - max is EXCLUSIVE except in the last bucket.
# --------------------------------------------------------------------------------------------
A2 = [
    {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "total_spent": {"$sum": "$total"}}},
    {"$bucketAuto": {
        "groupBy": "$orders", "buckets": 5,
        "output": {"users": {"$sum": 1},
                   "avg_orders": {"$avg": "$orders"},
                   "avg_spend": {"$avg": "$total_spent"},
                   "total_revenue": {"$sum": "$total_spent"}}}},
    {"$project": {"_id": 0, "min_orders": "$_id.min", "max_orders": "$_id.max",
                  "users": 1, "avg_orders": {"$round": ["$avg_orders", 2]},
                  "avg_spend": {"$round": ["$avg_spend", 2]},
                  "total_revenue": {"$round": ["$total_revenue", 2]}}},
]

# --------------------------------------------------------------------------------------------
# A3a. Top-selling products (units + revenue) - transactions collection
# A3b. Most-viewed products                    - sessions collection (viewed_products array)
# --------------------------------------------------------------------------------------------
A3_SOLD = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id", "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"}}},
    {"$sort": {"units_sold": -1}},
    {"$limit": 500},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "p"}},
    {"$project": {"_id": 0, "product_id": "$_id", "name": {"$arrayElemAt": ["$p.name", 0]},
                  "category_name": {"$arrayElemAt": ["$p.category_name", 0]},
                  "units_sold": 1, "revenue": {"$round": ["$revenue", 2]}}},
]
A3_VIEWED = [
    {"$unwind": "$viewed_products"},
    {"$group": {"_id": "$viewed_products", "sessions_viewed": {"$sum": 1}}},
    {"$sort": {"sessions_viewed": -1}},
    {"$limit": 500},
    {"$project": {"_id": 0, "product_id": "$_id", "sessions_viewed": 1}},
]

# --------------------------------------------------------------------------------------------
# A4. Revenue by US state ($lookup transactions -> users)
# --------------------------------------------------------------------------------------------
A4 = [
    {"$group": {"_id": "$user_id", "spent": {"$sum": "$total"}, "orders": {"$sum": 1}}},
    {"$lookup": {"from": "users", "localField": "_id", "foreignField": "_id", "as": "u"}},
    {"$unwind": "$u"},
    {"$group": {"_id": "$u.geo_data.state", "revenue": {"$sum": "$spent"},
                "orders": {"$sum": "$orders"}, "customers": {"$sum": 1}}},
    {"$project": {"_id": 0, "state": "$_id", "revenue": {"$round": ["$revenue", 2]},
                  "orders": 1, "customers": 1}},
    {"$sort": {"revenue": -1}},
]

# --------------------------------------------------------------------------------------------
# A5. Embed a purchase summary into every user (computed pattern) with $merge
# --------------------------------------------------------------------------------------------
A5 = [
    {"$group": {"_id": "$user_id", "total_orders": {"$sum": 1}, "total_spent": {"$sum": "$total"},
                "first_purchase": {"$min": "$timestamp"}, "last_purchase": {"$max": "$timestamp"}}},
    {"$project": {"purchase_summary": {
        "total_orders": "$total_orders", "total_spent": {"$round": ["$total_spent", 2]},
        "avg_order_value": {"$round": [{"$divide": ["$total_spent", "$total_orders"]}, 2]},
        "first_purchase": "$first_purchase", "last_purchase": "$last_purchase"}}},
    {"$merge": {"into": "users", "on": "_id", "whenMatched": "merge", "whenNotMatched": "discard"}},
]


def main():
    run("A1 Revenue by category & month", "transactions", A1, "mongo_revenue_category_month.csv")
    run("A2 User segments by purchase frequency", "transactions", A2, "mongo_user_segments.csv")

    sold = run("A3a Top-selling products", "transactions", A3_SOLD, show=5)
    viewed = run("A3b Most-viewed products", "sessions", A3_VIEWED, show=5)
    both = sold.merge(viewed, on="product_id", how="outer").fillna(
        {"units_sold": 0, "sessions_viewed": 0, "revenue": 0})
    both.to_csv(os.path.join(common.RESULTS_DIR, "mongo_product_popularity.csv"), index=False)

    run("A4 Revenue by state", "transactions", A4, "mongo_revenue_state.csv", show=8)

    list(db.transactions.aggregate(A5))  # $merge pipelines return nothing; list() forces execution
    n = db.users.count_documents({"purchase_summary": {"$exists": True}})
    print(f"\n=== A5 purchase_summary embedded into {n:,} user documents ===")
    print(db.users.find_one({"purchase_summary": {"$exists": True}},
                            {"user_id": 1, "geo_data": 1, "purchase_summary": 1}))


if __name__ == "__main__":
    main()

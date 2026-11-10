"""
PART 1 (MongoDB): schema design + data loading.

Collections and the reasoning behind each design choice
-------------------------------------------------------
categories   : kept as the original hierarchical document (category -> subcategories[]).
               Small reference data, read as a whole.
products     : category name + subcategory (id, name, profit_margin) are EMBEDDED (denormalised)
               so a product page needs ONE read; price_history is an embedded array because it is
               only ever read together with the product.
users        : _id = user_id. Profile + (after loading transactions) an embedded SUMMARY
               (total_orders, total_spent, ...) - the "computed pattern": fast profile reads
               without scanning transactions.
transactions : line items EMBEDDED in an `items` array (an order is always read with its items).
               Each item also carries category_id/subcategory_id copied from products, so revenue
               by category needs no $lookup (read-optimised denormalisation).
sessions     : full journey document with embedded page_views[] and cart_contents - the
               document model's strongest use case. Only a SUBSET is loaded (see
               MONGO_SESSIONS_LIMIT); Spark processes the complete set.

Run:  python mongodb/load_mongo.py
"""
import glob
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pymongo import ASCENDING, MongoClient
from pymongo.errors import BulkWriteError

import common

BATCH = 5000


def to_dt(s):
    """ISO-8601 string -> datetime (stored as a real BSON Date so $dateToString etc. work)."""
    return datetime.fromisoformat(s) if s else None


def load_json(name):
    with open(os.path.join(common.DATA_DIR, name)) as f:
        return json.load(f)


def insert_batches(coll, docs):
    """insert_many in batches with error handling; returns number inserted."""
    inserted, buf = 0, []
    for d in docs:
        buf.append(d)
        if len(buf) >= BATCH:
            inserted += _flush(coll, buf)
            buf = []
    if buf:
        inserted += _flush(coll, buf)
    return inserted


def _flush(coll, buf):
    try:
        return len(coll.insert_many(buf, ordered=False).inserted_ids)
    except BulkWriteError as e:  # keep going on duplicates etc.
        n = e.details.get("nInserted", 0)
        print(f"  ! bulk write error on {coll.name}: {len(e.details['writeErrors'])} failed docs")
        return n


def main():
    t0 = time.time()
    client = MongoClient(common.MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")  # fail fast with a clear message if Mongo is down
    db = client[common.MONGO_DB]
    for name in ("categories", "products", "users", "transactions", "sessions"):
        db[name].drop()  # makes the script re-runnable

    # ---- categories ------------------------------------------------------------------
    categories = load_json("categories.json")
    db.categories.insert_many(categories)
    cat_by_id = {c["category_id"]: c for c in categories}
    print(f"categories   : {len(categories):>8,}")

    # ---- products (embed category + subcategory) -----------------------------------------
    products = load_json("products.json")
    prod_info = {}
    docs = []
    for p in products:
        cat = cat_by_id[p["category_id"]]
        subs = cat["subcategories"]
        sub = subs[common.subcategory_index(p["product_id"], len(subs))]
        p["_id"] = p["product_id"]
        p["category_name"] = cat["name"]
        p["subcategory_id"] = sub["subcategory_id"]
        p["subcategory"] = sub
        p["creation_date"] = to_dt(p["creation_date"])
        for ph in p["price_history"]:
            ph["date"] = to_dt(ph["date"])
        prod_info[p["product_id"]] = (p["category_id"], sub["subcategory_id"])
        docs.append(p)
    print(f"products     : {insert_batches(db.products, docs):>8,}")

    # ---- users ------------------------------------------------------------------------
    users = load_json("users.json")
    for u in users:
        u["_id"] = u["user_id"]
        u["registration_date"] = to_dt(u["registration_date"])
        u["last_active"] = to_dt(u["last_active"])
    print(f"users        : {insert_batches(db.users, users):>8,}")

    # ---- transactions (embed items, enrich with category) ----------------------------------
    txns = load_json("transactions.json")
    for t in txns:
        t["_id"] = t["transaction_id"]
        t["timestamp"] = to_dt(t["timestamp"])
        t["item_count"] = sum(i["quantity"] for i in t["items"])
        for i in t["items"]:
            i["category_id"], i["subcategory_id"] = prod_info[i["product_id"]]
    print(f"transactions : {insert_batches(db.transactions, txns):>8,}")

    # ---- sessions (subset, embedded page_views) ---------------------------------------------
    n_sessions, buf = 0, []
    for path in sorted(glob.glob(os.path.join(common.DATA_DIR, "sessions_*.json"))):
        with open(path) as f:
            chunk = json.load(f)
        for s in chunk:
            if n_sessions >= common.MONGO_SESSIONS_LIMIT:
                break
            s["_id"] = s["session_id"]
            s["start_time"] = to_dt(s["start_time"])
            s["end_time"] = to_dt(s["end_time"])
            for pv in s["page_views"]:
                pv["timestamp"] = to_dt(pv["timestamp"])
            buf.append(s)
            n_sessions += 1
            if len(buf) >= BATCH:
                _flush(db.sessions, buf)
                buf = []
        if n_sessions >= common.MONGO_SESSIONS_LIMIT:
            break
    if buf:
        _flush(db.sessions, buf)
    print(f"sessions     : {n_sessions:>8,} (subset)")

    # ---- indexes matching our query patterns -----------------------------------------------
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])
    db.products.create_index([("category_id", ASCENDING)])
    db.sessions.create_index([("user_id", ASCENDING), ("start_time", ASCENDING)])
    db.sessions.create_index([("conversion_status", ASCENDING)])
    print("indexes created")
    print(f"done in {time.time() - t0:.1f}s -> database '{common.MONGO_DB}'")


if __name__ == "__main__":
    main()

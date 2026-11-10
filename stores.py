"""
Helpers that pull data OUT of MongoDB and HBase into Spark DataFrames.

We deliberately avoid the Spark connectors (setup is time-consuming and version-sensitive).
Instead each system is read with its native Python client and the rows are handed to Spark with
an EXPLICIT schema. This is the "extract -> Spark" integration pattern used in Part 3.
"""
import common
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField, StructType,
                               TimestampType)

MONGO_USERS_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("country", StringType()),
    StructField("state", StringType()), StructField("registration_ts", TimestampType()),
    StructField("orders", LongType()), StructField("total_spent", DoubleType()),
    StructField("avg_order_value", DoubleType()), StructField("first_purchase", TimestampType()),
    StructField("last_purchase", TimestampType())])

HBASE_SESSIONS_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("session_id", StringType()),
    StructField("duration_seconds", LongType()), StructField("conversion_status", StringType()),
    StructField("device_type", StringType())])


def mongo_users_df(spark):
    """MongoDB `users` collection (profile + embedded purchase_summary) -> Spark DataFrame."""
    from pymongo import MongoClient

    db = MongoClient(common.MONGO_URI, serverSelectionTimeoutMS=5000)[common.MONGO_DB]
    rows = []
    for u in db.users.find({}, {"user_id": 1, "geo_data": 1, "registration_date": 1, "purchase_summary": 1}):
        ps = u.get("purchase_summary") or {}
        rows.append((u["user_id"], u["geo_data"].get("country"), u["geo_data"].get("state"),
                     u.get("registration_date"), int(ps.get("total_orders", 0)),
                     float(ps.get("total_spent", 0.0)), float(ps.get("avg_order_value", 0.0)),
                     ps.get("first_purchase"), ps.get("last_purchase")))
    return spark.createDataFrame(rows, MONGO_USERS_SCHEMA)


def hbase_sessions_df(spark, limit=None):
    """HBase `user_sessions` (only the small `info` columns we need) -> Spark DataFrame."""
    import happybase

    conn = happybase.Connection(common.HBASE_HOST, port=common.HBASE_PORT, timeout=120000)
    table = conn.table("user_sessions")
    cols = [b"info:session_id", b"info:duration_seconds", b"info:conversion_status", b"info:device_type"]
    rows = []
    # a full-table scan limited to 4 tiny columns; batch_size = rows fetched per RPC round trip
    for key, d in table.scan(columns=cols, batch_size=5000, limit=limit):
        rows.append((key.decode().split("#")[0], d[b"info:session_id"].decode(),
                     int(d[b"info:duration_seconds"]), d[b"info:conversion_status"].decode(),
                     d[b"info:device_type"].decode()))
    conn.close()
    return spark.createDataFrame(rows, HBASE_SESSIONS_SCHEMA)

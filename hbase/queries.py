"""
PART 1 (HBase): query examples.

  Q1  Latest N sessions of a user            (prefix scan - newest first thanks to reverse ts)
  Q2  All sessions of a user in a time window (start/stop-row range scan)
  Q3  Full detail (page views + cart) of one session
  Q4  Daily metrics of one product in a date range (range scan on product_metrics)

Run:  python hbase/queries.py                 (auto-picks a user that exists)
      python hbase/queries.py --user user_000042 --product prod_00123
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import happybase

import common


def ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def latest_sessions(table, user_id, n=5):
    """Q1: prefix scan. Keys start with user_id#<reverse ts>, so the first rows are the newest."""
    rows = table.scan(row_prefix=f"{user_id}#".encode(), limit=n,
                      columns=[b"info"])  # only the 'info' family -> skips the wide pv family
    return [(k.decode(), {c.decode(): v.decode() for c, v in d.items()}) for k, d in rows]


def sessions_in_window(table, user_id, start: datetime, end: datetime):
    """Q2: range scan. Reverse timestamps flip the order, so END maps to the start row."""
    row_start = f"{user_id}#{common.MAX_TS_MS - ms(end):013d}".encode()
    row_stop = f"{user_id}#{common.MAX_TS_MS - ms(start) + 1:013d}".encode()
    return list(table.scan(row_start=row_start, row_stop=row_stop, columns=[b"info"]))


def product_history(table, product_id, d1: str, d2: str):
    """Q4: range scan over <product_id>#yyyymmdd keys (d1/d2 as yyyymmdd, d2 inclusive)."""
    start = f"{product_id}#{d1}".encode()
    stop = f"{product_id}#{int(d2) + 1}".encode()
    return list(table.scan(row_start=start, row_stop=stop))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user")
    ap.add_argument("--product")
    args = ap.parse_args()

    conn = happybase.Connection(common.HBASE_HOST, port=common.HBASE_PORT, timeout=60000)
    sessions = conn.table("user_sessions")
    metrics = conn.table("product_metrics")

    user = args.user
    if not user:  # pick the first user that has data
        key, _ = next(sessions.scan(limit=1, columns=[b"info:session_id"]))
        user = key.decode().split("#")[0]

    # ---- Q1 -------------------------------------------------------------------------
    t0 = time.time()
    rows = latest_sessions(sessions, user, 5)
    print(f"\n[Q1] latest 5 sessions of {user}  ({(time.time() - t0) * 1000:.1f} ms)")
    for key, d in rows:
        print(f"  {key}\n     {d['info:start_time']}  {d['info:duration_seconds']}s  "
              f"{d['info:device_type']}/{d['info:browser']}  {d['info:referrer']}  -> {d['info:conversion_status']}")

    # ---- Q2 -------------------------------------------------------------------------
    if rows:
        newest = datetime.fromisoformat(rows[0][1]["info:start_time"])
        t0 = time.time()
        win = sessions_in_window(sessions, user, newest - timedelta(days=30), newest)
        print(f"\n[Q2] {user}: {len(win)} sessions in the 30 days up to {newest:%Y-%m-%d}  "
              f"({(time.time() - t0) * 1000:.1f} ms)")

        # ---- Q3 ---------------------------------------------------------------------
        full = sessions.row(rows[0][0].encode())
        pvs = sorted((c.decode(), v.decode()) for c, v in full.items() if c.startswith(b"pv:"))
        cart = {c.decode().split(":")[1]: v.decode() for c, v in full.items() if c.startswith(b"cart:")}
        print(f"\n[Q3] newest session in full: {len(pvs)} page views, cart = {cart or 'empty'}")
        for q, v in pvs[:6]:
            print(f"  {q}: {v}")

    # ---- Q4 -------------------------------------------------------------------------
    product = args.product
    if not product:
        key, _ = next(metrics.scan(limit=1))
        product = key.decode().split("#")[0]
    t0 = time.time()
    hist = product_history(metrics, product, "20000101", "20991231")
    print(f"\n[Q4] daily metrics of {product}: {len(hist)} days  ({(time.time() - t0) * 1000:.1f} ms)")
    for k, d in hist[:8]:
        print(f"  {k.decode()}  views={d[b'm:views'].decode()} cart_adds={d[b'm:cart_adds'].decode()} "
              f"purchases={d[b'm:purchases'].decode()} revenue={d[b'm:revenue'].decode()}")
    conn.close()


if __name__ == "__main__":
    main()

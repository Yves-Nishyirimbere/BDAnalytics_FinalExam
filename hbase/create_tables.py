"""
PART 1 (HBase): schema creation through the Thrift server (happybase).

Table 1 : user_sessions   (time-series browsing data)
  Row key   : <user_id>#<reverse_ts_ms, 13 digits>#<session_id>
              reverse_ts = 9999999999999 - epoch_ms
              -> all sessions of a user are stored contiguously, NEWEST FIRST, so
                 "latest N sessions of user X" is a short prefix scan, and a time window is a
                 simple start/stop-row range scan. The session_id suffix guarantees uniqueness.
              -> the leading component is user_id (not a timestamp), so writes spread across
                 regions instead of hot-spotting on "now".
  Families  : info  - one value per session (session_id, start_time, duration, device, referrer,
                      conversion_status, country, state)
              pv    - page views, one sparse column per view (pv_000, pv_001, ...)
              cart  - sparse: one column per product in the cart (qualifier = product_id)
              Three families only: each family is a separate store file set, so few is better.

Table 2 : product_metrics (product performance over time)
  Row key   : <product_id>#<yyyymmdd>
              -> one product's daily history is contiguous (range scan by date)
  Family    : m - views, cart_adds, purchases, revenue (written by the Spark job)

The same commands in HBase-shell form (incl. region pre-splits) are in hbase/hbase_shell_commands.txt.

Run:  python hbase/create_tables.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import happybase

import common


def recreate(conn, name, families):
    existing = [t.decode() for t in conn.tables()]
    if name in existing:
        conn.disable_table(name)
        conn.delete_table(name)
        print(f"dropped existing table '{name}'")
    conn.create_table(name, families)
    print(f"created table '{name}' with families {list(families)}")


def main():
    conn = happybase.Connection(common.HBASE_HOST, port=common.HBASE_PORT, timeout=60000)
    recreate(conn, "user_sessions", {
        "info": dict(max_versions=1),
        "pv": dict(max_versions=1),
        "cart": dict(max_versions=1),
    })
    recreate(conn, "product_metrics", {"m": dict(max_versions=1)})
    print("tables now in HBase:", [t.decode() for t in conn.tables()])
    conn.close()


if __name__ == "__main__":
    main()

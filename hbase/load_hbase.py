"""
PART 1 (HBase): load user session data into `user_sessions`.

Run:  python hbase/load_hbase.py        (after hbase/create_tables.py)
"""
import glob
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import happybase

import common

BATCH = 1000


def epoch_ms(iso: str) -> int:
    """ISO string (naive, treated as UTC) -> epoch milliseconds."""
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp() * 1000)


def session_row_key(user_id: str, start_iso: str, session_id: str) -> str:
    rev = common.MAX_TS_MS - epoch_ms(start_iso)          # newest session sorts first
    return f"{user_id}#{rev:013d}#{session_id}"


def session_to_cells(s: dict) -> dict:
    cells = {
        b"info:session_id": s["session_id"].encode(),
        b"info:start_time": s["start_time"].encode(),
        b"info:end_time": s["end_time"].encode(),
        b"info:duration_seconds": str(s["duration_seconds"]).encode(),
        b"info:device_type": s["device_profile"]["type"].encode(),
        b"info:os": s["device_profile"]["os"].encode(),
        b"info:browser": s["device_profile"]["browser"].encode(),
        b"info:referrer": s["referrer"].encode(),
        b"info:conversion_status": s["conversion_status"].encode(),
        b"info:country": str(s["geo_data"].get("country", "")).encode(),
        b"info:state": str(s["geo_data"].get("state", "")).encode(),
        b"info:viewed_count": str(len(s["viewed_products"])).encode(),
    }
    # sparse page-view columns: ts|page_type|product_id|category_id|view_duration
    for i, pv in enumerate(s["page_views"]):
        val = f"{pv['timestamp']}|{pv['page_type']}|{pv['product_id'] or ''}|{pv['category_id'] or ''}|{pv['view_duration']}"
        cells[f"pv:pv_{i:03d}".encode()] = val.encode()
    # sparse cart columns: qualifier = product_id, value = quantity
    for pid, item in s["cart_contents"].items():
        cells[f"cart:{pid}".encode()] = str(item["quantity"]).encode()
    return cells


def main():
    conn = happybase.Connection(common.HBASE_HOST, port=common.HBASE_PORT, timeout=60000)
    table = conn.table("user_sessions")
    t0, n = time.time(), 0
    for path in sorted(glob.glob(os.path.join(common.DATA_DIR, "sessions_*.json"))):
        with open(path) as f:
            chunk = json.load(f)
        with table.batch(batch_size=BATCH) as b:  # buffered puts, flushed every BATCH rows
            for s in chunk:
                if n >= common.HBASE_SESSIONS_LIMIT:
                    break
                b.put(session_row_key(s["user_id"], s["start_time"], s["session_id"]).encode(),
                      session_to_cells(s))
                n += 1
        print(f"  loaded {n:,} sessions ...")
        if n >= common.HBASE_SESSIONS_LIMIT:
            break
    print(f"done: {n:,} sessions into 'user_sessions' in {time.time() - t0:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()

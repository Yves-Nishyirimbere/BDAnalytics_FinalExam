"""
PART 1/3 (HBase + Spark): load daily product metrics (computed by spark/04_product_metrics.py)
into the `product_metrics` table. Shows Spark producing aggregates that HBase serves with
fast key/range lookups.

Run:  python hbase/load_product_metrics.py   (after spark/04_product_metrics.py)
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import happybase
import pandas as pd

import common


def main():
    path = os.path.join(common.CLEAN_DIR, "product_daily")
    df = pd.read_parquet(path)  # needs pyarrow
    print(f"read {len(df):,} product-day rows from {path}")

    conn = happybase.Connection(common.HBASE_HOST, port=common.HBASE_PORT, timeout=60000)
    table = conn.table("product_metrics")
    t0 = time.time()
    with table.batch(batch_size=2000) as b:
        for r in df.itertuples(index=False):
            key = f"{r.product_id}#{pd.Timestamp(r.date):%Y%m%d}".encode()
            b.put(key, {
                b"m:views": str(int(r.views)).encode(),
                b"m:cart_adds": str(int(r.cart_adds)).encode(),
                b"m:purchases": str(int(r.purchases)).encode(),
                b"m:revenue": f"{float(r.revenue):.2f}".encode(),
            })
    print(f"loaded {len(df):,} rows into 'product_metrics' in {time.time() - t0:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()

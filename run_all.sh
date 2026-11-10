#!/usr/bin/env bash
# Runs the whole pipeline in order. Usage:  bash run_all.sh [SESSIONS] [TRANSACTIONS]
# Prereqs: MongoDB + HBase(Thrift) running, `pip install -r requirements.txt`,
#          the provided dataset_generator.py placed in the project root.
set -euo pipefail
SESSIONS=${1:-200000}
TXNS=${2:-100000}
step() { echo; echo "################  $1  ################"; }

step "0. Generate data ($SESSIONS sessions, $TXNS transactions)"
python data_generation/prepare_generator.py dataset_generator.py "$SESSIONS" "$TXNS"
(cd data && python ../data_generation/generate_data.py)

step "1. MongoDB: load + aggregation pipelines"
python mongodb/load_mongo.py
python mongodb/aggregations.py

step "2. Spark: clean -> Parquet, product daily metrics"
python spark/01_clean.py
python spark/04_product_metrics.py

step "3. HBase: create tables, load sessions + product metrics, run queries"
python hbase/create_tables.py
python hbase/load_hbase.py
python hbase/load_product_metrics.py
python hbase/queries.py

step "4. Spark: recommendations + SQL analytics"
python spark/02_recommend.py
python spark/03_sql_analytics.py

step "5. Integrated CLV query (MongoDB + HBase + Spark)"
python integration/clv_pipeline.py

step "6. Charts"
python viz/make_charts.py
echo; echo "ALL DONE -> see results/ and figures/"

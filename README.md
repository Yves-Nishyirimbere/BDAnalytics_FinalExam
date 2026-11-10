# AUCA Big Data Analytics Final Project: Distributed Multi-Model Analytics for E-commerce

MongoDB (document model) + HBase (wide-column) + Apache Spark (distributed processing) on the
synthetic e-commerce dataset from `dataset_generator.py`.

## Architecture

```
dataset_generator -> raw JSON (users, products, categories, sessions_*, transactions)
        |
        +--> MongoDB  : products / users / transactions / sessions(subset)  -> aggregation pipelines
        +--> HBase    : user_sessions (time-series) + product_metrics        -> key/range lookups
        +--> Spark    : ALL data -> clean Parquet -> recommendations, Spark SQL, product metrics
                              ^                         |
                              |   extract (pymongo / happybase)
                              +-- Integration: CLV = MongoDB (profile+orders) x HBase (engagement), joined in Spark
        results/*.csv --> viz/make_charts.py --> figures/*.png
```

## Prerequisites

* Python 3.10+, Java 11 or 17 (for PySpark)
* MongoDB on `localhost:27017`
* HBase with the **Thrift server** on `localhost:9090` (see `docker-compose.yml`)
* `pip install -r requirements.txt`
* Put the provided `dataset_generator.py` in the project root (it is a notebook-formatted file;
  `prepare_generator.py` extracts the code from it).

Connection settings can be changed with environment variables: `MONGO_URI`, `MONGO_DB`,
`HBASE_HOST`, `HBASE_PORT`, `SPARK_DRIVER_MEMORY` (default 4g).

## Run everything

```bash
bash run_all.sh 200000 100000      # sessions, transactions
```

or step by step (always from the project root):

| Step | Command | Output |
|---|---|---|
| 0 | `python data_generation/prepare_generator.py dataset_generator.py 200000 100000` then `cd data && python ../data_generation/generate_data.py` | `data/*.json` |
| 1 | `python mongodb/load_mongo.py` | collections + indexes |
| 2 | `python mongodb/aggregations.py` | `results/mongo_*.csv` |
| 3 | `python spark/01_clean.py` | `data/clean/*` Parquet, `results/data_quality_report.json` |
| 4 | `python spark/04_product_metrics.py` | `data/clean/product_daily` |
| 5 | `python hbase/create_tables.py` | HBase tables |
| 6 | `python hbase/load_hbase.py` and `python hbase/load_product_metrics.py` | HBase data |
| 7 | `python hbase/queries.py` | sample queries printed |
| 8 | `python spark/02_recommend.py` | `results/spark_*_together.csv` |
| 9 | `python spark/03_sql_analytics.py` | `results/sql_*.csv` |
| 10 | `python integration/clv_pipeline.py` (`--offline` to skip the NoSQL stores) | `results/clv_*` |
| 11 | `python viz/make_charts.py` | `figures/fig1..fig5.png` |

Spark always processes **all** sessions; MongoDB and HBase receive a subset
(`MONGO_SESSIONS_LIMIT`, `HBASE_SESSIONS_LIMIT`, default 50,000 each).

## Design decisions (summary for the report)

**MongoDB**: embed what is read together (transaction `items`, session `page_views`, product
`price_history`), denormalise category/subcategory onto products and transaction items so revenue
by category needs no join, and keep a computed `purchase_summary` on each user ($merge).

**HBase**: `user_sessions` row key = `user_id#reverse_ts#session_id` (newest first, prefix scan per
user, no write hot-spotting); 3 column families (`info`, `pv`, `cart`) with sparse columns.
`product_metrics` row key = `product_id#yyyymmdd`, family `m`, filled by a Spark job.

**Spark**: explicit schemas (cart_contents is a MapType), data-quality flags instead of silent
drops, Parquet partitioned by date, co-occurrence recommendations (support/confidence/lift),
8 Spark SQL analyses, and a cross-store query reading from MongoDB and HBase.

## Known data caveats (mention under Limitations)

* The generator does not assign `subcategory_id` to products. We assign it deterministically from
  `md5(product_id)`, identically in Python (MongoDB) and Spark.
* Users have no age/demographics, so segmentation uses purchase frequency, spend and CLV tier.
* Data is random: product co-occurrence lift and view-to-sales correlation are near chance level,
  and conversion barely differs by device or referrer.
* Dates are relative to the time the generator runs. The first and last days are partial days
  (excluded from the daily revenue chart).
* About half of transactions have no `session_id`, and some "converted" sessions have no
  transaction (inventory failures); both are flagged in `data_quality_report.json`.
* CLV is a simple historical estimate (AOV x orders per month x 12-month lifespan assumption,
  `CLV_LIFESPAN_MONTHS`).
* A Java stack trace about `sessions_*.json` during `01_clean.py` is a harmless Spark warning.

## Report
- Technical report (PDF): `report/Technical_Report_NISHYIRIMBERE_101388.pdf`
- Screenshots of each stage running: `screenshots/`

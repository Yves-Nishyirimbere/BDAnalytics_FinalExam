"""
Shared configuration and helpers used by every script in the project.
All settings can be overridden with environment variables, so nothing is hard-coded.

Always run scripts from the project root, e.g.:  python mongodb/load_mongo.py
"""
import hashlib
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

# ---- Paths -----------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", os.path.join(ROOT, "data"))          # raw JSON from generator
CLEAN_DIR = os.getenv("CLEAN_DIR", os.path.join(ROOT, "data", "clean"))  # Spark Parquet output
RESULTS_DIR = os.getenv("RESULTS_DIR", os.path.join(ROOT, "results"))
FIGURES_DIR = os.getenv("FIGURES_DIR", os.path.join(ROOT, "figures"))

# ---- MongoDB ---------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "ecommerce")

# ---- HBase (via Thrift server, port 9090) -----------------------------------
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))

# ---- How many sessions to load into the operational stores -------------------
# (Spark always processes ALL sessions; MongoDB/HBase get a representative subset.)
MONGO_SESSIONS_LIMIT = int(os.getenv("MONGO_SESSIONS_LIMIT", "50000"))
HBASE_SESSIONS_LIMIT = int(os.getenv("HBASE_SESSIONS_LIMIT", "50000"))

# Largest 13-digit millisecond value, used to build reverse timestamps for HBase row keys
MAX_TS_MS = 9_999_999_999_999


def subcategory_index(product_id: str, n_subcategories: int) -> int:
    """
    The generator does NOT assign subcategory_id to products (the PDF sample implies it does).
    We assign one deterministically from an MD5 hash of the product_id so the SAME value is
    produced in MongoDB (Python) and in Spark (SQL md5) -> consistent across all systems.
    """
    return int(hashlib.md5(product_id.encode()).hexdigest()[:8], 16) % n_subcategories


def get_spark(app_name: str):
    """Create a local SparkSession with sensible settings for a single-machine run."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_dirs():
    for d in (CLEAN_DIR, RESULTS_DIR, FIGURES_DIR):
        os.makedirs(d, exist_ok=True)

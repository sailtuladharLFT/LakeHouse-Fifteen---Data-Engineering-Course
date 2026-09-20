from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = (
    SparkSession.builder
    .appName("Spark UI Tour")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.port", "4050")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

print(f"\n  Open this in your browser --> {sc.uiWebUrl}")
print("  Waiting 10 seconds for you to open the UI...\n")
time.sleep(10)


# ══════════════════════════════════════════════════════════════
#  100 million numbers — too large for pandas
# ══════════════════════════════════════════════════════════════
numbers = spark.range(0, 100_000_000)


# ──────────────────────────────────────────────────────────────
#  JOB 1 — Simple count  (1 stage, no shuffle)
# ──────────────────────────────────────────────────────────────
print("=" * 55)
print("  JOB 1 STARTING: count()")
print("  Expect: 1 stage, no shuffle")
print("=" * 55)

result = numbers.count()

print(f"  Result: {result:,}")
print("  Pausing 8s — check Jobs tab in UI...\n")
time.sleep(8)


# ──────────────────────────────────────────────────────────────
#  JOB 2 — Filter then count  (1 stage — filter is narrow)
# ──────────────────────────────────────────────────────────────
print("=" * 55)
print("  JOB 2 STARTING: filter().count()")
print("  Expect: 1 stage, still no shuffle")
print("=" * 55)

evens = numbers.filter(F.col("id") % 2 == 0)
result = evens.count()

print(f"  Result: {result:,}")
print("  Pausing 8s — check Jobs tab in UI...\n")
time.sleep(8)


# ──────────────────────────────────────────────────────────────
#  JOB 3 — GroupBy then count  (2 stages — 1 shuffle)
# ──────────────────────────────────────────────────────────────
print("=" * 55)
print("  JOB 3 STARTING: groupBy().count()")
print("  Expect: 2 stages — groupBy causes a SHUFFLE")
print("=" * 55)

result = (
    numbers
    .withColumn("bucket", F.col("id") % 10)
    .groupBy("bucket")
    .count()
    .count()
)

print(f"  Distinct buckets: {result}")
print("  Pausing 8s — click Job 3 in UI, look for 2 stages...\n")
time.sleep(8)


# ──────────────────────────────────────────────────────────────
#  JOB 4 — GroupBy + Sort + Show  (3 stages — 2 shuffles)
# ──────────────────────────────────────────────────────────────
print("=" * 55)
print("  JOB 4 STARTING: groupBy().count().orderBy()")
print("  Expect: 3 stages — groupBy + orderBy = 2 shuffles")
print("=" * 55)

(
    numbers
    .withColumn("bucket", F.col("id") % 10)
    .groupBy("bucket")
    .count()
    .orderBy("bucket")
    .show()
)

print("  Pausing 15s — click Job 4 in UI, look for 3 stages + 2 Exchange nodes in DAG...\n")
time.sleep(15)


# ══════════════════════════════════════════════════════════════
print("=" * 55)
print(f"  All jobs done. Spark UI: {sc.uiWebUrl}")
print("  Keeping Spark alive for 60s so you can explore...")
print("=" * 55)
time.sleep(60)

spark.stop()

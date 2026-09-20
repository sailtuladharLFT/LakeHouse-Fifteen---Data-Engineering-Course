from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = (
    SparkSession.builder
    .appName("Large Scale Word Count")
    .master("local[*]")
    # Tuned for local mode — fewer shuffle partitions avoids tiny file overhead
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

print(f"\n{'='*60}")
print(f"  Spark UI: {sc.uiWebUrl}")
print(f"{'='*60}")
print("  Open the Spark UI now, then watch jobs appear below.\n")
time.sleep(3)

# ──────────────────────────────────────────────────────────────
# Dataset: 5 million sentences × 10 words = ~50 million words
# Way beyond what pandas can hold in memory comfortably
# ──────────────────────────────────────────────────────────────

SENTENCES = 5_000_000

# 36-word vocabulary; modulo picks words so some are far more common
vocab = [
    "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
    "spark", "data", "pipeline", "cluster", "stage", "job", "task",
    "shuffle", "partition", "filter", "reduce", "join", "sort", "count",
    "stream", "batch", "cache", "memory", "lake", "warehouse", "schema",
    "python", "scala", "java", "sql", "code", "function", "transform",
]
n = len(vocab)
vocab_array = F.array([F.lit(w) for w in vocab])

# Build sentences from row IDs — pure columnar, no UDFs, no Python loops at runtime
sentences_df = spark.range(SENTENCES).select(
    F.concat_ws(" ", *[
        F.element_at(vocab_array, ((F.col("id") * (i * 7 + 3) + i * 11) % n + 1).cast("int"))
        for i in range(10)
    ]).alias("text")
)


# ──────────────────────────────────────────────────────────────
# JOB 1 — Count sentences
# Stages: 1  (scan range → project → partial count → final count)
# No shuffle, so single stage.
# ──────────────────────────────────────────────────────────────
print("[JOB 1] Counting sentences ...")
t0 = time.time()
sentence_count = sentences_df.count()
print(f"  Sentences: {sentence_count:,}  ({time.time()-t0:.1f}s)\n")


# ──────────────────────────────────────────────────────────────
# JOB 2 — Explode sentences into individual word rows, then count
# Stages: 1  (scan → project → explode → count)
# Still no shuffle — explode is a narrow transformation.
# ──────────────────────────────────────────────────────────────
words_df = sentences_df.select(
    F.explode(F.split(F.col("text"), " ")).alias("word")
)

print("[JOB 2] Counting total words after explode ...")
t0 = time.time()
total_words = words_df.count()
print(f"  Total words: {total_words:,}  ({time.time()-t0:.1f}s)\n")


# ──────────────────────────────────────────────────────────────
# JOB 3 — Word frequency (groupBy → count)
# Stages: 2  ← SHUFFLE BOUNDARY HERE
#   Stage A: scan + explode + partial aggregate (map-side combine)
#   Stage B: exchange (shuffle) + final aggregate
# ──────────────────────────────────────────────────────────────
word_counts = (
    words_df
    .groupBy("word")
    .count()
    .withColumnRenamed("count", "frequency")
)

print("[JOB 3] Computing word frequencies (groupBy — triggers shuffle) ...")
t0 = time.time()
distinct_words = word_counts.count()
print(f"  Distinct words: {distinct_words:,}  ({time.time()-t0:.1f}s)\n")


# ──────────────────────────────────────────────────────────────
# JOB 4 — Sort by frequency descending (show top 20)
# Stages: 3  ← TWO SHUFFLE BOUNDARIES
#   Stage A: re-scan + explode + partial aggregate
#   Stage B: shuffle + final aggregate
#   Stage C: range-partition shuffle for sort + collect for show()
# ──────────────────────────────────────────────────────────────
top_words = word_counts.orderBy(F.desc("frequency"))

print("[JOB 4] Sorting by frequency (orderBy — second shuffle) ...")
t0 = time.time()
top_words.show(20, truncate=False)
print(f"  ({time.time()-t0:.1f}s)\n")


# ──────────────────────────────────────────────────────────────
# JOB 5 — Filter, sort again, and write to Parquet
# Stages: 3  (same pattern as Job 4 but ends in a write, not collect)
# ──────────────────────────────────────────────────────────────
output_path = "/tmp/word_count_output"

print(f"[JOB 5] Writing filtered results to Parquet → {output_path} ...")
t0 = time.time()
(
    word_counts
    .filter(F.col("frequency") > 500)
    .orderBy(F.desc("frequency"))
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(output_path)
)
print(f"  Done.  ({time.time()-t0:.1f}s)\n")


# ──────────────────────────────────────────────────────────────
# Keep Spark alive so you can browse the full UI
# ──────────────────────────────────────────────────────────────
print(f"{'='*60}")
print(f"  All 5 jobs complete.")
print(f"  Spark UI: {sc.uiWebUrl}")
print(f"{'='*60}")
input("\nPress Enter to stop Spark and exit...")

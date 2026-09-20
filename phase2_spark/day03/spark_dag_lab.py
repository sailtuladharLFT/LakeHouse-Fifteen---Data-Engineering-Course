from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Spark DAG Lab")
    .master("local[*]")
    .getOrCreate()
)

sc = spark.sparkContext

print("Spark application started")
print("Application ID:", sc.applicationId)
print("Spark UI:", sc.uiWebUrl)


# --------------------------------------------------
# 1. Create a large distributed dataset
# --------------------------------------------------

data = spark.range(0, 10_000_000)

print("Number of rows:", data.count())
print("Initial partitions:", data.rdd.getNumPartitions())


# --------------------------------------------------
# 2. Narrow transformations
# --------------------------------------------------

filtered = data.filter(data.id % 2 == 0)

transformed = filtered.withColumn(
    "group_id",
    filtered.id % 100
)


# --------------------------------------------------
# 3. Wide transformation
# --------------------------------------------------

grouped = (
    transformed
    .groupBy("group_id")
    .count()
)


# --------------------------------------------------
# 4. Transformation after shuffle
# --------------------------------------------------

result = grouped.filter(
    grouped["count"] > 50
)


# --------------------------------------------------
# 5. Action
# --------------------------------------------------

final_count = result.count()

print("Final result count:", final_count)

print("\nSpark UI is live at:", sc.uiWebUrl)
input("Press Enter to stop the Spark session and exit...")

# spark.stop()
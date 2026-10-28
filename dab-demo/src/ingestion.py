from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

# Sample raw sales data
sales_data = [
    (1001, "C001", "Laptop", 1, 1200.00),
    (1002, "C002", "Mouse", 2, 25.00),
    (1003, "C001", "Keyboard", 1, 75.00),
    (1004, "C003", "Monitor", 2, 250.00),
    (1005, "C002", "Mouse", 1, 25.00),
]


columns = [
    "order_id",
    "customer_id",
    "product",
    "quantity",
    "unit_price",
]

# Creating a Data frame

sales_df = spark.createDataFrame(sales_data , columns)



# Add ingestion timestamp
sales_df = sales_df.withColumn(
    "ingested_at",
    current_timestamp()
)

# Write to The Bronze Delta table

target_table = "dev_catalog.bronze.sales_raw"

(
    sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Successfully ingested {sales_df.count()} records into {target_table}")
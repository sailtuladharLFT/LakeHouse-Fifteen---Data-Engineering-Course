from pyspark import pipelines as dp
from pyspark.sql import functions as F


# SILVER: Cleaned sales records

@dp.materialized_view(
    name="dev_catalog.silver.sales_clean",
    comment="Cleaned and deduplicated sales records",
)
@dp.expect_or_drop(
    "valid_order_id",
    "order_id IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_product",
    "product IS NOT NULL",
)
@dp.expect_or_drop(
    "valid_quantity",
    "quantity > 0",
)
@dp.expect_or_drop(
    "valid_unit_price",
    "unit_price >= 0",
)
def sales_clean():
    return (
        spark.read.table("dev_catalog.bronze.sales_raw")
        .dropDuplicates(["order_id"])
        .withColumn(
            "total_amount",
            F.col("quantity") * F.col("unit_price"),
        )
    )


# GOLD: Product-level sales aggregation

@dp.materialized_view(
    name="dev_catalog.gold.sales_by_product",
    comment="Aggregated sales metrics by product",
)
def sales_by_product():
    return (
        spark.read.table("dev_catalog.silver.sales_clean")
        .groupBy("product")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_amount").alias("total_revenue"),
        )
    )
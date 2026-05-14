from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    round,
    desc
)

spark = (
    SparkSession.builder
    .appName("Postgres To ClickHouse DataMarts")
    .getOrCreate()
)

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
db = os.environ["POSTGRES_DB"]

postgresUrl = f"jdbc:postgresql://postgres:5432/{db}"

postgresProperties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

clickhouseUrl = "jdbc:clickhouse://clickhouse:8123/default"

clickhouseProperties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": ""
}

factSalesDf = spark.read.jdbc(
    postgresUrl,
    "fact_sales",
    properties=postgresProperties
)

dimCustomerDf = spark.read.jdbc(
    postgresUrl,
    "dim_customer",
    properties=postgresProperties
)

dimProductDf = spark.read.jdbc(
    postgresUrl,
    "dim_product",
    properties=postgresProperties
)

dimDateDf = spark.read.jdbc(
    postgresUrl,
    "dim_date",
    properties=postgresProperties
)

dimStoreDf = spark.read.jdbc(
    postgresUrl,
    "dim_store",
    properties=postgresProperties
)

dimSupplierDf = spark.read.jdbc(
    postgresUrl,
    "dim_supplier",
    properties=postgresProperties
)

# product sales
dmProductSalesDf = (
    factSalesDf.alias("f")
    .join(
        dimProductDf.alias("p"),
        col("f.product_id") == col("p.product_id")
    )
    .groupBy(
        col("p.product_id").alias("product_id"),
        col("p.name").alias("product_name"),
        col("p.category").alias("category"),
        col("p.rating").alias("rating"),
        col("p.reviews").alias("reviews")
    )
    .agg(
        sum("f.sale_quantity").alias("total_sales"),
        round(
            sum("f.sale_total_price"),
            2
        ).alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
    .limit(10)
)

# customer sales
dmCustomerSalesDf = (
    factSalesDf.alias("f")
    .join(
        dimCustomerDf.alias("c"),
        col("f.customer_id") == col("c.customer_id")
    )
    .groupBy(
        col("c.customer_id").alias("customer_id"),
        col("c.first_name").alias("first_name"),
        col("c.last_name").alias("last_name"),
        col("c.country").alias("country")
    )
    .agg(
        round(
            sum("f.sale_total_price"),
            2
        ).alias("total_spent"),

        round(
            avg("f.sale_total_price"),
            2
        ).alias("avg_check")
    )
    .orderBy(desc("total_spent"))
    .limit(10)
)

# time sales
dmTimeSalesDf = (
    factSalesDf.alias("f")
    .join(
        dimDateDf.alias("d"),
        col("f.date_id") == col("d.date_id")
    )
    .groupBy(
        col("d.year").alias("year"),
        col("d.month").alias("month"),
        col("d.quarter").alias("quarter")
    )
    .agg(
        round(
            sum("f.sale_total_price"),
            2
        ).alias("total_revenue"),

        round(
            avg("f.sale_total_price"),
            2
        ).alias("avg_order_size"),

        sum("f.sale_quantity").alias("total_sales")
    )
    .orderBy(
        "year",
        "month"
    )
)

# store sales
dmStoreSalesDf = (
    factSalesDf.alias("f")
    .join(
        dimStoreDf.alias("s"),
        col("f.store_id") == col("s.store_id")
    )
    .groupBy(
        col("s.store_id").alias("store_id"),
        col("s.name").alias("store_name"),
        col("s.city").alias("city"),
        col("s.country").alias("country")
    )
    .agg(
        round(
            sum("f.sale_total_price"),
            2
        ).alias("total_revenue"),

        round(
            avg("f.sale_total_price"),
            2
        ).alias("avg_check"),

        sum("f.sale_quantity").alias("total_sales")
    )
    .orderBy(desc("total_revenue"))
    .limit(5)
)

# supplier sales
dmSupplierSalesDf = (
    factSalesDf.alias("f")
    .join(
        dimSupplierDf.alias("s"),
        col("f.supplier_id") == col("s.supplier_id")
    )
    .join(
        dimProductDf.alias("p"),
        col("f.product_id") == col("p.product_id")
    )
    .groupBy(
        col("s.supplier_id").alias("supplier_id"),
        col("s.name").alias("supplier_name"),
        col("s.country").alias("country")
    )
    .agg(
        round(
            sum("f.sale_total_price"),
            2
        ).alias("total_revenue"),

        round(
            avg("p.price"),
            2
        ).alias("avg_product_price"),

        sum("f.sale_quantity").alias("total_sales")
    )
    .orderBy(desc("total_revenue"))
    .limit(5)
)

# product quality
dmProductQualityDf = (
    factSalesDf.alias("f")
    .join(
        dimProductDf.alias("p"),
        col("f.product_id") == col("p.product_id")
    )
    .groupBy(
        col("p.product_id").alias("product_id"),
        col("p.name").alias("product_name"),
        col("p.rating").alias("rating"),
        col("p.reviews").alias("reviews")
    )
    .agg(
        sum("f.sale_quantity").alias("total_sales"),

        round(
            sum("f.sale_total_price"),
            2
        ).alias("total_revenue")
    )
    .orderBy(desc("rating"))
)

marts = [
    ("dm_product_sales", dmProductSalesDf),
    ("dm_customer_sales", dmCustomerSalesDf),
    ("dm_time_sales", dmTimeSalesDf),
    ("dm_store_sales", dmStoreSalesDf),
    ("dm_supplier_sales", dmSupplierSalesDf),
    ("dm_product_quality", dmProductQualityDf)
]

for tableName, df in marts:

    (
        df.write
        .format("jdbc")
        .mode("overwrite")
        .option("url", clickhouseUrl)
        .option("dbtable", tableName)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("user", "default")
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
        .save()
    )

    print(f"Created ClickHouse mart: {tableName}")

print("All ClickHouse marts created successfully")

spark.stop()

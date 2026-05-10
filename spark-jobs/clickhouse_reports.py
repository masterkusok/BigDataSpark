from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    count,
    round,
    countDistinct,
    desc
)
import os

# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("PetShop Analytics DataMarts")
    .getOrCreate()
)

# =========================================================
# PostgreSQL connection
# =========================================================

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
db = os.environ["POSTGRES_DB"]

jdbcUrl = f"jdbc:postgresql://postgres:5432/{db}"

connectionProperties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

# =========================================================
# Read DWH tables
# =========================================================

factSalesDf = spark.read.jdbc(
    jdbcUrl,
    "fact_sales",
    properties=connectionProperties
)

dimCustomerDf = spark.read.jdbc(
    jdbcUrl,
    "dim_customer",
    properties=connectionProperties
)

dimProductDf = spark.read.jdbc(
    jdbcUrl,
    "dim_product",
    properties=connectionProperties
)

dimDateDf = spark.read.jdbc(
    jdbcUrl,
    "dim_date",
    properties=connectionProperties
)

dimStoreDf = spark.read.jdbc(
    jdbcUrl,
    "dim_store",
    properties=connectionProperties
)

dimSupplierDf = spark.read.jdbc(
    jdbcUrl,
    "dim_supplier",
    properties=connectionProperties
)

# =========================================================
# SALES + PRODUCT MART
# =========================================================

salesProductDf = (
    factSalesDf.alias("f")
    .join(
        dimProductDf.alias("p"),
        col("f.product_id") == col("p.product_id")
    )
    .select(
        col("f.*"),

        col("p.name").alias("product_name"),
        col("p.category"),
        col("p.rating"),
        col("p.reviews")
    )
)

# ---------------------------------------------------------
# Top 10 products
# ---------------------------------------------------------

topProductsDf = (
    salesProductDf
    .groupBy(
        "product_id",
        "product_name"
    )
    .agg(
        sum("sale_quantity").alias("total_quantity"),
        round(
            sum("sale_total_price"),
            2
        ).alias("total_revenue")
    )
    .orderBy(desc("total_quantity"))
    .limit(10)
)

# ---------------------------------------------------------
# Revenue by category
# ---------------------------------------------------------

revenueByCategoryDf = (
    salesProductDf
    .groupBy("category")
    .agg(
        round(
            sum("sale_total_price"),
            2
        ).alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
)

# ---------------------------------------------------------
# Product ratings
# ---------------------------------------------------------

productRatingsDf = (
    salesProductDf
    .select(
        "product_id",
        "product_name",
        "rating",
        "reviews"
    )
    .dropDuplicates(["product_id"])
)

# =========================================================
# SALES + CUSTOMER MART
# =========================================================

salesCustomerDf = (
    factSalesDf.alias("f")
    .join(
        dimCustomerDf.alias("c"),
        col("f.customer_id") == col("c.customer_id")
    )
    .select(
        col("f.*"),

        col("c.first_name"),
        col("c.last_name"),
        col("c.country").alias("customer_country")
    )
)

# ---------------------------------------------------------
# Top customers
# ---------------------------------------------------------

topCustomersDf = (
    salesCustomerDf
    .groupBy(
        "customer_id",
        "first_name",
        "last_name",
        "customer_country"
    )
    .agg(
        round(
            sum("sale_total_price"),
            2
        ).alias("total_spent")
    )
    .orderBy(desc("total_spent"))
    .limit(10)
)

# ---------------------------------------------------------
# Customers by country
# ---------------------------------------------------------

customersByCountryDf = (
    salesCustomerDf
    .groupBy("customer_country")
    .agg(
        countDistinct("customer_id").alias("customers_count")
    )
    .orderBy(desc("customers_count"))
)

# ---------------------------------------------------------
# Average customer check
# ---------------------------------------------------------

avgCustomerCheckDf = (
    salesCustomerDf
    .groupBy(
        "customer_id",
        "first_name",
        "last_name"
    )
    .agg(
        round(
            avg("sale_total_price"),
            2
        ).alias("avg_check")
    )
)

# =========================================================
# SALES + TIME MART
# =========================================================

salesTimeDf = (
    factSalesDf.alias("f")
    .join(
        dimDateDf.alias("d"),
        col("f.date_id") == col("d.date_id")
    )
    .select(
        col("f.*"),

        col("d.full_date"),
        col("d.month"),
        col("d.year"),
        col("d.quarter")
    )
)

# ---------------------------------------------------------
# Monthly sales
# ---------------------------------------------------------

monthlySalesDf = (
    salesTimeDf
    .groupBy(
        "year",
        "month"
    )
    .agg(
        round(
            sum("sale_total_price"),
            2
        ).alias("monthly_revenue")
    )
    .orderBy(
        "year",
        "month"
    )
)

# ---------------------------------------------------------
# Yearly sales
# ---------------------------------------------------------

yearlySalesDf = (
    salesTimeDf
    .groupBy("year")
    .agg(
        round(
            sum("sale_total_price"),
            2
        ).alias("yearly_revenue")
    )
    .orderBy("year")
)

# ---------------------------------------------------------
# Average monthly order
# ---------------------------------------------------------

avgMonthlyOrderDf = (
    salesTimeDf
    .groupBy(
        "year",
        "month"
    )
    .agg(
        round(
            avg("sale_total_price"),
            2
        ).alias("avg_order_size")
    )
)

# =========================================================
# SALES + STORE MART
# =========================================================

salesStoreDf = (
    factSalesDf.alias("f")
    .join(
        dimStoreDf.alias("s"),
        col("f.store_id") == col("s.store_id")
    )
    .select(
        col("f.*"),

        col("s.name").alias("store_name"),
        col("s.city").alias("store_city"),
        col("s.country").alias("store_country")
    )
)

# ---------------------------------------------------------
# Top stores
# ---------------------------------------------------------

topStoresDf = (
    salesStoreDf
    .groupBy(
        "store_id",
        "store_name",
        "store_city",
        "store_country"
    )
    .agg(
        round(
            sum("sale_total_price"),
            2
        ).alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
    .limit(5)
)

# ---------------------------------------------------------
# Sales geography
# ---------------------------------------------------------

salesGeoDf = (
    salesStoreDf
    .groupBy(
        "store_country",
        "store_city"
    )
    .agg(
        round(
            sum("sale_total_price"),
            2
        ).alias("total_revenue")
    )
)

# ---------------------------------------------------------
# Average store check
# ---------------------------------------------------------

avgStoreCheckDf = (
    salesStoreDf
    .groupBy(
        "store_id",
        "store_name"
    )
    .agg(
        round(
            avg("sale_total_price"),
            2
        ).alias("avg_check")
    )
)

# =========================================================
# SALES + SUPPLIER MART
# =========================================================

salesSupplierDf = (
    factSalesDf.alias("f")
    .join(
        dimSupplierDf.alias("s"),
        col("f.supplier_id") == col("s.supplier_id")
    )
    .join(
        dimProductDf.alias("p"),
        col("f.product_id") == col("p.product_id")
    )
    .select(
        col("f.*"),

        col("s.name").alias("supplier_name"),
        col("s.country").alias("supplier_country"),

        col("p.price"),
        col("p.name").alias("product_name")
    )
)

# ---------------------------------------------------------
# Top suppliers
# ---------------------------------------------------------

topSuppliersDf = (
    salesSupplierDf
    .groupBy(
        "supplier_id",
        "supplier_name",
        "supplier_country"
    )
    .agg(
        round(
            sum("sale_total_price"),
            2
        ).alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
    .limit(5)
)

# ---------------------------------------------------------
# Average product price by supplier
# ---------------------------------------------------------

supplierAvgPriceDf = (
    salesSupplierDf
    .groupBy(
        "supplier_id",
        "supplier_name"
    )
    .agg(
        round(
            avg("price"),
            2
        ).alias("avg_product_price")
    )
)

# ---------------------------------------------------------
# Supplier country sales
# ---------------------------------------------------------

supplierCountryDf = (
    salesSupplierDf
    .groupBy("supplier_country")
    .agg(
        round(
            sum("sale_total_price"),
            2
        ).alias("total_revenue")
    )
)

# =========================================================
# PRODUCT QUALITY MART
# =========================================================

# ---------------------------------------------------------
# Highest rated products
# ---------------------------------------------------------

highestRatedDf = (
    dimProductDf
    .select(
        col("product_id"),
        col("name").alias("product_name"),
        "rating",
        "reviews"
    )
    .orderBy(desc("rating"))
)

# ---------------------------------------------------------
# Lowest rated products
# ---------------------------------------------------------

lowestRatedDf = (
    dimProductDf
    .select(
        col("product_id"),
        col("name").alias("product_name"),
        "rating",
        "reviews"
    )
    .orderBy(col("rating"))
)

# ---------------------------------------------------------
# Rating vs sales correlation
# ---------------------------------------------------------

ratingSalesDf = (
    salesProductDf
    .groupBy(
        "product_id",
        "product_name",
        "rating"
    )
    .agg(
        sum("sale_quantity").alias("total_sales")
    )
)

correlationValue = (
    ratingSalesDf
    .stat
    .corr("rating", "total_sales")
)

correlationDf = spark.createDataFrame(
    [
        (
            "rating_sales_correlation",
            correlationValue
        )
    ],
    [
        "metric",
        "value"
    ]
)

# ---------------------------------------------------------
# Most reviewed products
# ---------------------------------------------------------

mostReviewedDf = (
    dimProductDf
    .select(
        col("product_id"),
        col("name").alias("product_name"),
        "reviews"
    )
    .orderBy(desc("reviews"))
)

# =========================================================
# Save marts into PostgreSQL
# =========================================================

dataMarts = [
    ("dm_top_products", topProductsDf),
    ("dm_revenue_by_category", revenueByCategoryDf),
    ("dm_product_ratings", productRatingsDf),

    ("dm_top_customers", topCustomersDf),
    ("dm_customers_by_country", customersByCountryDf),
    ("dm_avg_customer_check", avgCustomerCheckDf),

    ("dm_monthly_sales", monthlySalesDf),
    ("dm_yearly_sales", yearlySalesDf),
    ("dm_avg_monthly_order", avgMonthlyOrderDf),

    ("dm_top_stores", topStoresDf),
    ("dm_sales_geo", salesGeoDf),
    ("dm_avg_store_check", avgStoreCheckDf),

    ("dm_top_suppliers", topSuppliersDf),
    ("dm_supplier_avg_price", supplierAvgPriceDf),
    ("dm_supplier_country_sales", supplierCountryDf),

    ("dm_highest_rated_products", highestRatedDf),
    ("dm_lowest_rated_products", lowestRatedDf),
    ("dm_rating_sales_correlation", correlationDf),
    ("dm_most_reviewed_products", mostReviewedDf)
]

for tableName, df in dataMarts:
    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=jdbcUrl,
            table=tableName,
            properties=connectionProperties
        )
    )

print("Analytics DataMarts created successfully")

spark.stop()
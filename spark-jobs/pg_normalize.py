'''
Spark job for normalizing postgres mock_data table into a snowflake.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
    monotonically_increasing_id,
    date_format,
    dayofmonth,
    month,
    year,
    quarter,
    coalesce,
    lit
)
from pyspark.sql.window import Window
import os

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
db = os.environ["POSTGRES_DB"]

spark = (
    SparkSession.builder
    .appName("Postgres ETL")
    .getOrCreate()
)

jdbc_url = f"jdbc:postgresql://postgres:5432/{db}"

connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

mockDataDf = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="mock_data",
        properties=connection_properties
    )
)

dimPetBreedDf = (
    mockDataDf
    .select(
        col("customer_pet_breed").alias("breed_name")
    )
    .where(col("customer_pet_breed").isNotNull())
    .distinct()
)

breedWindow = Window.orderBy("breed_name")

dimPetBreedDf = (
    dimPetBreedDf
    .withColumn(
        "pet_breed_id",
        row_number().over(breedWindow)
    )
    .select(
        "pet_breed_id",
        "breed_name"
    )
)

customerWindow = Window.partitionBy(
    "sale_customer_id"
).orderBy("sale_customer_id")

dimCustomerDf = (
    mockDataDf
    .join(
        dimPetBreedDf,
        mockDataDf.customer_pet_breed == dimPetBreedDf.breed_name,
        "left"
    )
    .withColumn(
        "rn",
        row_number().over(customerWindow)
    )
    .filter(col("rn") == 1)
    .select(
        col("sale_customer_id").alias("customer_id"),
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("customer_pet_type").alias("pet_type"),
        col("customer_pet_name").alias("pet_name"),
        col("pet_breed_id")
    )
)

sellerWindow = Window.partitionBy(
    "sale_seller_id"
).orderBy("sale_seller_id")

dimSellerDf = (
    mockDataDf
    .withColumn(
        "rn",
        row_number().over(sellerWindow)
    )
    .filter(col("rn") == 1)
    .select(
        col("sale_seller_id").alias("seller_id"),
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    )
)

# =========================================================
# DIM SUPPLIER
# =========================================================

dimSupplierDf = (
    mockDataDf
    .filter(col("supplier_name").isNotNull())
    .select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country")
    )
    .distinct()
)

supplierWindow = Window.orderBy("name")

dimSupplierDf = (
    dimSupplierDf
    .withColumn(
        "supplier_id",
        row_number().over(supplierWindow)
    )
    .select(
        "supplier_id",
        "name",
        "contact",
        "email",
        "phone",
        "address",
        "city",
        "country"
    )
)

# =========================================================
# DIM STORE
# =========================================================

dimStoreDf = (
    mockDataDf
    .filter(col("store_name").isNotNull())
    .select(
        col("store_name").alias("name"),
        col("store_location").alias("address"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    )
    .distinct()
)

storeWindow = Window.orderBy("name")

dimStoreDf = (
    dimStoreDf
    .withColumn(
        "store_id",
        row_number().over(storeWindow)
    )
    .select(
        "store_id",
        "name",
        "address",
        "city",
        "state",
        "country",
        "phone",
        "email"
    )
)

dimPetCategoryDf = (
    mockDataDf
    .select(
        col("pet_category").alias("category_name")
    )
    .where(col("pet_category").isNotNull())
    .distinct()
)

petCategoryWindow = Window.orderBy("category_name")

dimPetCategoryDf = (
    dimPetCategoryDf
    .withColumn(
        "pet_category_id",
        row_number().over(petCategoryWindow)
    )
    .select(
        "pet_category_id",
        "category_name"
    )
)

productWindow = Window.partitionBy(
    "sale_product_id"
).orderBy("sale_product_id")

dimProductDf = (
    mockDataDf
    .join(
        dimPetCategoryDf,
        mockDataDf.pet_category == dimPetCategoryDf.category_name,
        "left"
    )
    .withColumn(
        "rn",
        row_number().over(productWindow)
    )
    .filter(col("rn") == 1)
    .select(
        col("sale_product_id").alias("product_id"),
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        col("product_price").alias("price"),
        col("product_quantity").alias("quantity"),
        col("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_brand").alias("brand"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date"),
        col("pet_category_id")
    )
)

dimDateDf = (
    mockDataDf
    .filter(col("sale_date").isNotNull())
    .select(
        col("sale_date").alias("full_date")
    )
    .distinct()
)

dimDateDf = (
    dimDateDf
    .withColumn(
        "date_id",
        date_format(col("full_date"), "yyyyMMdd").cast("integer")
    )
    .withColumn(
        "day",
        dayofmonth(col("full_date"))
    )
    .withColumn(
        "month",
        month(col("full_date"))
    )
    .withColumn(
        "year",
        year(col("full_date"))
    )
    .withColumn(
        "quarter",
        quarter(col("full_date"))
    )
    .select(
        "date_id",
        "full_date",
        "day",
        "month",
        "year",
        "quarter"
    )
)

factSalesDf = (
    mockDataDf
    .join(
        dimStoreDf,
        (
            (dimStoreDf.name == mockDataDf.store_name)
            &
            (dimStoreDf.city == mockDataDf.store_city)
            &
            (
                coalesce(dimStoreDf.state, lit(""))
                ==
                coalesce(mockDataDf.store_state, lit(""))
            )
        ),
        "inner"
    )
    .join(
        dimSupplierDf,
        (
            (dimSupplierDf.name == mockDataDf.supplier_name)
            &
            (dimSupplierDf.contact == mockDataDf.supplier_contact)
        ),
        "inner"
    )
    .select(
        col("sale_customer_id").alias("customer_id"),
        col("sale_seller_id").alias("seller_id"),
        col("sale_product_id").alias("product_id"),
        col("store_id"),
        col("supplier_id"),
        date_format(
            col("sale_date"),
            "yyyyMMdd"
        ).cast("integer").alias("date_id"),
        col("sale_quantity").alias("sale_quantity"),
        col("sale_total_price").alias("sale_total_price")
    )
)

tables = [
    ("dim_pet_breed", dimPetBreedDf),
    ("dim_customer", dimCustomerDf),
    ("dim_seller", dimSellerDf),
    ("dim_supplier", dimSupplierDf),
    ("dim_store", dimStoreDf),
    ("dim_pet_category", dimPetCategoryDf),
    ("dim_product", dimProductDf),
    ("dim_date", dimDateDf),
    ("fact_sales", factSalesDf)
]

for tableName, df in tables:
    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=jdbc_url,
            table=tableName,
            properties=connection_properties
        )
    )

print("DWH ETL completed successfully")
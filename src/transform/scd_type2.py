#!/usr/bin/env python3
"""
src/transform/scd_type2.py
Star schema builder with SCD Type 2 on product (price) and customer (loyalty tier).
Reads from staging/ writes to transform/ as CSV, then bq load into BigQuery.
"""

import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, row_number, when, sum as spark_sum, max as spark_max, min as spark_min,
    lag, lead, expr, to_date, date_add, concat
)
from pyspark.sql.window import Window

# === AUTO-DETECT PROJECT ID ===
try:
    PROJECT_ID = subprocess.check_output(['gcloud', 'config', 'get-value', 'project']).decode().strip()
except Exception as e:
    PROJECT_ID = "YOUR_PROJECT_ID_HERE"
    print(f"⚠️ Could not auto-detect project ID: {e}. Using fallback.")

BUCKET_NAME = f"retail-raw-{PROJECT_ID}"
STAGING_PATH = f"gs://{BUCKET_NAME}/staging/"
OUTPUT_BASE = f"gs://{BUCKET_NAME}/transform/"
print(f"🔍 Project ID: {PROJECT_ID}")
print(f"🔍 Output base: {OUTPUT_BASE}")

# === CREATE SPARK SESSION ===
ADC_PATH = "/tmp/tmp.d1fW6vl8XD/application_default_credentials.json"
spark = (
    SparkSession.builder
    .appName("SCDType2_StarSchema")
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.fs.gs.project.id", PROJECT_ID)
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        ADC_PATH
    )
    .getOrCreate()
)
print("✅ Spark Session Created")

spark.sparkContext.setLogLevel("WARN")

# =============================================
# 1. READ STAGING DATA
# =============================================
df_orders = (
    spark.read
    .option("header", True)
    .csv(STAGING_PATH)
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("total_amount", col("quantity") * col("unit_price"))
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("store_id", col("store_id").cast("int"))
)

df_orders.cache()
print(f"📊 Loaded {df_orders.count()} rows from staging")

# =============================================
# 2. BUILD DIM_DATE
# =============================================
# NOTE: date_min/date_max come back as plain Python `date` objects (from .first()),
# not Spark columns — so we compute the day-count in Python, and we must cast
# spark.range()'s `id` (BIGINT) to INT before feeding it to date_add().
date_min, date_max = df_orders.select(spark_min("order_date"), spark_max("order_date")).first()
num_days = (date_max - date_min).days

date_seq = spark.range(0, num_days + 1).select(
    expr(f"date_add('{date_min}', cast(id as int))").alias("date")
)
# substr() only works on string/binary types, not on a `date` column directly —
# cast to string first.
dim_date = date_seq.withColumn("year", col("date").cast("string").substr(1, 4).cast("int")) \
                   .withColumn("month", col("date").cast("string").substr(6, 2).cast("int")) \
                   .withColumn("day", col("date").cast("string").substr(9, 2).cast("int")) \
                   .withColumn("quarter", when(col("month") <= 3, 1)
                               .when(col("month") <= 6, 2)
                               .when(col("month") <= 9, 3)
                               .otherwise(4)) \
                   .withColumn("day_of_week", expr("date_format(date, 'E')")) \
                   .withColumn("is_weekend", when(col("day_of_week").isin(["Sat", "Sun"]), 1).otherwise(0))
dim_date.write.mode("overwrite").option("header", True).csv(OUTPUT_BASE + "dim_date/")
print(f"✅ dim_date written")

# =============================================
# 3. BUILD DIM_STORE
# =============================================
store_ids = df_orders.select("store_id").distinct().collect()
store_list = [row.store_id for row in store_ids]
store_map = []
for sid in store_list:
    store_map.append({
        "store_id": sid,
        "store_name": f"Store {sid}",
        "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"][sid % 5],
        "region": ["East", "West", "Midwest", "South", "Southwest"][sid % 5]
    })
df_store = spark.createDataFrame(store_map)
df_store.write.mode("overwrite").option("header", True).csv(OUTPUT_BASE + "dim_store/")
print(f"✅ dim_store written")

# =============================================
# 4. BUILD DIM_PRODUCT (SCD Type 2 on price)
# =============================================
df_product_day = df_orders.groupBy("product_id", "order_date") \
                          .agg(spark_min("unit_price").alias("unit_price"))
window_spec = Window.partitionBy("product_id").orderBy("order_date")
df_with_lag = df_product_day.withColumn("prev_price", lag("unit_price").over(window_spec))
df_price_start = df_with_lag.filter(
    (col("prev_price").isNull()) | (col("unit_price") != col("prev_price"))
).select("product_id", "order_date", "unit_price") \
 .withColumn("valid_from", col("order_date"))
window_spec2 = Window.partitionBy("product_id").orderBy("valid_from")
df_price_versions = df_price_start.withColumn(
    "valid_to",
    lead("valid_from").over(window_spec2)
).withColumn(
    "valid_to",
    when(col("valid_to").isNull(), lit(None).cast("date"))
     .otherwise(date_add(col("valid_to"), -1))
)
df_product_versions = df_price_versions.withColumn(
    "product_name",
    when(col("product_id") % 3 == 0, "Widget")
    .when(col("product_id") % 3 == 1, "Gadget")
    .otherwise("Doohickey")
).withColumn(
    "category",
    when(col("product_id") % 2 == 0, "Electronics")
    .otherwise("Accessories")
).withColumn(
    "is_current",
    when(col("valid_to").isNull(), 1).otherwise(0)
)
window_sk = Window.partitionBy("product_id").orderBy("valid_from")
df_product = df_product_versions.withColumn(
    "product_sk",
    row_number().over(window_sk)
).select("product_sk", "product_id", "product_name", "category",
         "unit_price", "valid_from", "valid_to", "is_current")
df_product.write.mode("overwrite").option("header", True).csv(OUTPUT_BASE + "dim_product/")
print(f"✅ dim_product written")

# =============================================
# 5. BUILD DIM_CUSTOMER (SCD Type 2 on loyalty tier)
# =============================================
df_customer_spend = df_orders.groupBy("customer_id", "order_date") \
                             .agg(spark_sum("total_amount").alias("daily_spend"))
window_cust = Window.partitionBy("customer_id").orderBy("order_date") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_cumulative = df_customer_spend.withColumn(
    "cumulative_spend",
    spark_sum("daily_spend").over(window_cust)
)
df_tier = df_cumulative.withColumn(
    "tier",
    when(col("cumulative_spend") < 100, "Bronze")
    .when((col("cumulative_spend") >= 100) & (col("cumulative_spend") < 500), "Silver")
    .otherwise("Gold")
)
window_tier = Window.partitionBy("customer_id").orderBy("order_date")
df_with_prev_tier = df_tier.withColumn("prev_tier", lag("tier").over(window_tier))
df_tier_start = df_with_prev_tier.filter(
    (col("prev_tier").isNull()) | (col("tier") != col("prev_tier"))
).select("customer_id", "order_date", "tier") \
 .withColumn("valid_from", col("order_date"))
window_tier2 = Window.partitionBy("customer_id").orderBy("valid_from")
df_customer_versions = df_tier_start.withColumn(
    "valid_to",
    lead("valid_from").over(window_tier2)
).withColumn(
    "valid_to",
    when(col("valid_to").isNull(), lit(None).cast("date"))
     .otherwise(date_add(col("valid_to"), -1))
)
df_customer_versions = df_customer_versions.withColumn(
    "customer_name",
    concat(lit("Customer "), col("customer_id"))
).withColumn(
    "email",
    concat(col("customer_name"), lit("@example.com"))
).withColumn(
    "city",
    when(col("customer_id") % 4 == 0, "New York")
    .when(col("customer_id") % 4 == 1, "Los Angeles")
    .when(col("customer_id") % 4 == 2, "Chicago")
    .otherwise("Houston")
).withColumn(
    "is_current",
    when(col("valid_to").isNull(), 1).otherwise(0)
)
window_cust_sk = Window.partitionBy("customer_id").orderBy("valid_from")
df_customer = df_customer_versions.withColumn(
    "customer_sk",
    row_number().over(window_cust_sk)
).select("customer_sk", "customer_id", "customer_name", "email",
         "city", "tier", "valid_from", "valid_to", "is_current")
df_customer.write.mode("overwrite").option("header", True).csv(OUTPUT_BASE + "dim_customer/")
print(f"✅ dim_customer written")

# =============================================
# 6. BUILD FACT_SALES
# =============================================
dim_product_df = spark.read.option("header", True).csv(OUTPUT_BASE + "dim_product/")
dim_customer_df = spark.read.option("header", True).csv(OUTPUT_BASE + "dim_customer/")
dim_store_df = spark.read.option("header", True).csv(OUTPUT_BASE + "dim_store/")
dim_date_df = spark.read.option("header", True).csv(OUTPUT_BASE + "dim_date/")

dim_product_df = dim_product_df.withColumn("product_id", col("product_id").cast("int")) \
                               .withColumn("product_sk", col("product_sk").cast("int")) \
                               .withColumn("valid_from", to_date(col("valid_from"))) \
                               .withColumn("valid_to", to_date(col("valid_to")))
dim_customer_df = dim_customer_df.withColumn("customer_id", col("customer_id").cast("int")) \
                                 .withColumn("customer_sk", col("customer_sk").cast("int")) \
                                 .withColumn("valid_from", to_date(col("valid_from"))) \
                                 .withColumn("valid_to", to_date(col("valid_to")))
dim_store_df = dim_store_df.withColumn("store_id", col("store_id").cast("int"))
dim_date_df = dim_date_df.withColumn("date", to_date(col("date")))

fact = df_orders.join(
    dim_product_df,
    (df_orders.product_id == dim_product_df.product_id) &
    (df_orders.order_date >= dim_product_df.valid_from) &
    ((df_orders.order_date <= dim_product_df.valid_to) | dim_product_df.valid_to.isNull()),
    "left"
).join(
    dim_customer_df,
    (df_orders.customer_id == dim_customer_df.customer_id) &
    (df_orders.order_date >= dim_customer_df.valid_from) &
    ((df_orders.order_date <= dim_customer_df.valid_to) | dim_customer_df.valid_to.isNull()),
    "left"
).join(
    dim_store_df,
    df_orders.store_id == dim_store_df.store_id,
    "left"
).join(
    dim_date_df,
    df_orders.order_date == dim_date_df.date,
    "left"
)

fact_final = fact.select(
    df_orders["order_id"],
    dim_product_df["product_sk"],
    dim_customer_df["customer_sk"],
    df_orders["store_id"],
    dim_date_df["date"].alias("order_date"),
    df_orders["quantity"],
    df_orders["unit_price"],
    df_orders["total_amount"],
    df_orders["channel"]
)

fact_final.write.mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_BASE + "fact_sales/")

print("✅ fact_sales written")

print("=" * 60)
print("STAR SCHEMA BUILD COMPLETE")
print(f"dim_date rows: {dim_date.count()}")
print(f"dim_store rows: {df_store.count()}")
print(f"dim_product rows: {df_product.count()}")
print(f"dim_customer rows: {df_customer.count()}")
print(f"fact_sales rows: {fact_final.count()}")
print("=" * 60)
spark.stop()

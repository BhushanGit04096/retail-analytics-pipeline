from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit, when, input_file_name
import subprocess
import os

# === HARDCODE YOUR PROJECT ID (REPLACE WITH YOUR ACTUAL ID) ===
PROJECT_ID = "playground-s-11-4db4d9ec"   # <-- CHANGE THIS IF DIFFERENT
BUCKET_NAME = f"retail-raw-{PROJECT_ID}"
INPUT_PATH = f"gs://{BUCKET_NAME}/date=*/channel=*/orders.csv"
STAGING_PATH = f"gs://{BUCKET_NAME}/staging/"
QUARANTINE_PATH = f"gs://{BUCKET_NAME}/quarantine/"

print(f"🔍 Reading data from: {INPUT_PATH}")

# === GET ACCESS TOKEN FROM GCLOUD ===
access_token = subprocess.check_output(['gcloud', 'auth', 'print-access-token']).decode().strip()

# === INIT SPARK WITH GCS CONNECTOR + ACCESS TOKEN ===
spark = SparkSession.builder \
    .appName("RetailDQ") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.fs.gs.project.id", PROJECT_ID) \
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "false") \
    .config("spark.hadoop.fs.gs.auth.access.token.enable", "true") \
    .config("spark.hadoop.fs.gs.auth.access.token", access_token) \
    .getOrCreate()

# === READ DATA ===
df = spark.read.option("header", True).csv(INPUT_PATH)
df = df.withColumn("source_file", input_file_name())

total_count = df.count()
print(f"📊 Total rows read: {total_count}")

# === 1. NULL CHECKS ===
null_checks = df.select(
    spark_sum(col("order_id").isNull().cast("int")).alias("null_order_id"),
    spark_sum(col("customer_id").isNull().cast("int")).alias("null_customer_id"),
    spark_sum(col("product_id").isNull().cast("int")).alias("null_product_id"),
    spark_sum(col("store_id").isNull().cast("int")).alias("null_store_id")
).collect()[0]

# === 2. DUPLICATE CHECK ===
duplicate_count = df.groupBy("order_id").count().filter(col("count") > 1).count()
print(f"🔁 Duplicate order_ids found: {duplicate_count}")

# === 3. REFERENTIAL INTEGRITY ===
valid_product_ids = [str(i) for i in range(1, 501)]
invalid_product_count = df.filter(~col("product_id").isin(valid_product_ids)).count()
print(f"🚫 Rows with invalid product_id: {invalid_product_count}")

# === FLAG BAD ROWS ===
df_flagged = df.withColumn(
    "dq_failed",
    when(
        (col("order_id").isNull()) |
        (col("customer_id").isNull()) |
        (col("product_id").isNull()) |
        (col("store_id").isNull()) |
        (~col("product_id").isin(valid_product_ids)),
        lit(True)
    ).otherwise(lit(False))
)

df_passed = df_flagged.filter(col("dq_failed") == False).drop("dq_failed", "source_file")
df_failed = df_flagged.filter(col("dq_failed") == True).drop("dq_failed")

print(f"✅ Passed: {df_passed.count()} rows")
print(f"❌ Failed: {df_failed.count()} rows")

# === WRITE ===
if df_passed.count() > 0:
    df_passed.write.mode("overwrite").option("header", True).csv(STAGING_PATH)
    print(f"✅ Wrote passed data to: {STAGING_PATH}")

if df_failed.count() > 0:
    df_failed.write.mode("overwrite").option("header", True).csv(QUARANTINE_PATH)
    print(f"⚠️ Wrote failed data to: {QUARANTINE_PATH}")

# === SUMMARY ===
print("=" * 50)
print("DATA QUALITY SUMMARY")
print(f"Total rows: {total_count}")
print(f"Null order_id: {null_checks.null_order_id}")
print(f"Null customer_id: {null_checks.null_customer_id}")
print(f"Null product_id: {null_checks.null_product_id}")
print(f"Null store_id: {null_checks.null_store_id}")
print(f"Duplicates: {duplicate_count}")
print(f"Invalid Products: {invalid_product_count}")
print(f"Passed: {df_passed.count()}")
print(f"Failed: {df_failed.count()}")
print("=" * 50)

spark.stop()

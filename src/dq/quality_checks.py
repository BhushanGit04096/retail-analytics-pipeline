# NOTE:
# Before publishing to GitHub or deploying to Dataproc, replace the
# authentication section with production IAM/service account authentication.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit, when, input_file_name
from pyspark import StorageLevel
import subprocess


def main():

    # ==============================
    # AUTO-DETECT PROJECT ID
    # ==============================
    try:
        PROJECT_ID = subprocess.check_output(
            ["gcloud", "config", "get-value", "project"]
        ).decode().strip()
    except Exception:
        raise Exception("Unable to detect GCP Project ID.")

    BUCKET_NAME = f"retail-raw-{PROJECT_ID}"

    INPUT_PATH = f"gs://{BUCKET_NAME}/date=*/channel=*/orders.csv"
    STAGING_PATH = f"gs://{BUCKET_NAME}/staging/"
    QUARANTINE_PATH = f"gs://{BUCKET_NAME}/quarantine/"

    print("=" * 60)
    print("RETAIL DATA QUALITY PIPELINE")
    print("=" * 60)
    print(f"Project ID : {PROJECT_ID}")
    print(f"Bucket     : {BUCKET_NAME}")
    print(f"Input Path : {INPUT_PATH}")
    print("=" * 60)

    # ==============================
    # CREATE SPARK SESSION
    # ==============================
    ADC_PATH = "/tmp/tmp.zoEsahDyyc/application_default_credentials.json"

    spark = (
        SparkSession.builder
        .appName("RetailDQ")
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

    # ==============================
    # READ DATA
    # ==============================
    print("\nReading data from Cloud Storage...")

    df = (
        spark.read
        .option("header", True)
        .csv(INPUT_PATH)
        .withColumn("source_file", input_file_name())
    )

    df.persist(StorageLevel.MEMORY_AND_DISK)

    print("\nSchema:")
    df.printSchema()

    print("\nSample Data:")
    df.show(5, truncate=False)

    total_count = df.count()
    print(f"\nTotal Rows Read : {total_count}")

    # ==============================
    # NULL CHECKS
    # ==============================
    null_checks = (
        df.select(
            spark_sum(col("order_id").isNull().cast("int")).alias("null_order_id"),
            spark_sum(col("customer_id").isNull().cast("int")).alias("null_customer_id"),
            spark_sum(col("product_id").isNull().cast("int")).alias("null_product_id"),
            spark_sum(col("store_id").isNull().cast("int")).alias("null_store_id"),
        ).collect()[0]
    )

    # ==============================
    # DUPLICATE CHECK
    # ==============================
    duplicate_count = (
        df.groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    # ==============================
    # REFERENTIAL INTEGRITY
    # ==============================
    valid_product_ids = [str(i) for i in range(1, 501)]

    invalid_product_count = (
        df.filter(~col("product_id").isin(valid_product_ids))
        .count()
    )

    # ==============================
    # FLAG BAD ROWS
    # ==============================
    df_flagged = df.withColumn(
        "dq_failed",
        when(
            (
                col("order_id").isNull()
                | col("customer_id").isNull()
                | col("product_id").isNull()
                | col("store_id").isNull()
                | (~col("product_id").isin(valid_product_ids))
            ),
            lit(True),
        ).otherwise(lit(False)),
    )

    df_passed = (
        df_flagged.filter(col("dq_failed") == False)
        .drop("dq_failed", "source_file")
    )

    df_failed = (
        df_flagged.filter(col("dq_failed") == True)
        .drop("dq_failed")
    )

    passed_count = df_passed.count()
    failed_count = df_failed.count()

    print(f"Passed Rows : {passed_count}")
    print(f"Failed Rows : {failed_count}")

    # ==============================
    # WRITE OUTPUT
    # ==============================
    if passed_count > 0:
        (
            df_passed.write.mode("overwrite")
            .option("header", True)
            .csv(STAGING_PATH)
        )
        print(f"Written clean data to {STAGING_PATH}")

    if failed_count > 0:
        (
            df_failed.write.mode("overwrite")
            .option("header", True)
            .csv(QUARANTINE_PATH)
        )
        print(f"Written failed data to {QUARANTINE_PATH}")

    # ==============================
    # SUMMARY
    # ==============================
    print("\n" + "=" * 60)
    print("DATA QUALITY SUMMARY")
    print("=" * 60)
    print(f"Total Rows          : {total_count}")
    print(f"Null order_id       : {null_checks.null_order_id}")
    print(f"Null customer_id    : {null_checks.null_customer_id}")
    print(f"Null product_id     : {null_checks.null_product_id}")
    print(f"Null store_id       : {null_checks.null_store_id}")
    print(f"Duplicate order_ids : {duplicate_count}")
    print(f"Invalid product_ids : {invalid_product_count}")
    print(f"Passed Rows         : {passed_count}")
    print(f"Failed Rows         : {failed_count}")
    print("=" * 60)

    df.unpersist()
    spark.stop()

    print("DQ Pipeline Completed Successfully.")


if __name__ == "__main__":
    main()

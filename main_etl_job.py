import sys
from datetime import datetime
from pyhocon import ConfigFactory
from pyspark.sql import SparkSession, DataFrame, functions as F, Window

def create_spark_session(spark_conf: dict) -> SparkSession:
    """
    Create and return a SparkSession based on the provided configuration dictionary.
    """
    builder = SparkSession.builder.appName(spark_conf.get("appName", "MyETLPipeline"))
    for key, value in spark_conf.items():
        if key != "appName":
            builder.config(key, value)
    return builder.getOrCreate()

# -------------------------------------------------------------------
#                         BRONZE LAYER
# -------------------------------------------------------------------

def get_max_transfer_id(spark: SparkSession, table_path: str) -> int:
    """
    Helper to fetch the maximum transferId from a Bronze table for incremental ingestion.
    If the table doesn't exist or is empty, return 0.
    """
    # In Iceberg, we can attempt to read the table. If it doesn’t exist, handle exception:
    try:
        df = spark.read.format("iceberg").load(table_path)
        max_val = df.agg(F.max("transferId")).collect()[0][0]
        return max_val if max_val is not None else 0
    except:
        # Table might not exist yet
        return 0

def load_customers_bronze(spark: SparkSession, config: dict):
    """
    Reads all customers from MSSQL and writes to customers_bronze.
    """
    # MSSQL JDBC config
    mssql_conf = config["data-sources"]["mssql"]
    df_customers = (
        spark.read
        .format("jdbc")
        .option("url", mssql_conf["url"])
        .option("dbtable", "dbo.customers")  # Adjust schema/table name
        .option("user", mssql_conf["user"])
        .option("password", mssql_conf["password"])
        .option("driver", mssql_conf["driver"])
        .load()
    )

    # Write to customers_bronze
    bronze_path = config["paths"]["bronze"]
    df_customers.write.format("iceberg") \
        .mode("overwrite") \
        .option("path", f"{bronze_path}/customers_bronze") \
        .save()

def load_transfers_bronze(spark: SparkSession, config: dict):
    """
    Incrementally reads transfers from MSSQL, splitting out success vs failed to
    transfers_bronze / transfers_failed_bronze.
    """
    mssql_conf = config["data-sources"]["mssql"]

    # Get existing max(transferId) from the Bronze table
    bronze_path = config["paths"]["bronze"]
    existing_max_id = get_max_transfer_id(spark, f"{bronze_path}/transfers_bronze")

    # Read from MSSQL where transferId > existing_max_id
    df_transfers = (
        spark.read
        .format("jdbc")
        .option("url", mssql_conf["url"])
        .option("dbtable", f"(SELECT * FROM dbo.transfers WHERE transferId > {existing_max_id}) as tmp")
        .option("user", mssql_conf["user"])
        .option("password", mssql_conf["password"])
        .option("driver", mssql_conf["driver"])
        .load()
    )

    # Split succeeded vs failed
    df_succeeded = df_transfers.filter(F.col("transferStatus") == 1)
    df_failed = df_transfers.filter(F.col("transferStatus") == 0)

    # Append mode for Bronze
    df_succeeded.write.format("iceberg") \
        .mode("append") \
        .option("path", f"{bronze_path}/transfers_bronze") \
        .save()

    df_failed.write.format("iceberg") \
        .mode("append") \
        .option("path", f"{bronze_path}/transfers_failed_bronze") \
        .save()

def load_balance_bronze(spark: SparkSession, config: dict):
    """
    Incrementally reads balance table from MSSQL, checking the max(transferId) in balance_bronze.
    """
    mssql_conf = config["data-sources"]["mssql"]
    bronze_path = config["paths"]["bronze"]
    existing_max_id = get_max_transfer_id(spark, f"{bronze_path}/balance_bronze")

    df_balance = (
        spark.read
        .format("jdbc")
        .option("url", mssql_conf["url"])
        .option("dbtable", f"(SELECT * FROM dbo.balance WHERE transferId > {existing_max_id}) as tmp")
        .option("user", mssql_conf["user"])
        .option("password", mssql_conf["password"])
        .option("driver", mssql_conf["driver"])
        .load()
    )

    df_balance.write.format("iceberg") \
        .mode("append") \
        .option("path", f"{bronze_path}/balance_bronze") \
        .save()

def run_bronze_pipeline(spark: SparkSession, config: dict):
    """
    Full Bronze pipeline: Load customers, transfers, and balance into Bronze.
    """
    load_customers_bronze(spark, config)
    load_transfers_bronze(spark, config)
    load_balance_bronze(spark, config)

# -------------------------------------------------------------------
#                         SILVER LAYER
# -------------------------------------------------------------------

def scd_type_2_customers(spark: SparkSession, config: dict):
    """
    Reads from customers_bronze and merges changes into customers_silver
    using a simple SCD Type 2 approach:
      - If no changes, keep existing row open.
      - If changed, close the old row (set validTo) and insert a new row with updated values.
    """
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]

    # Try to read existing Silver table. If it doesn't exist, handle exception.
    try:
        df_silver_current = spark.read.format("iceberg").load(f"{silver_path}/customers_silver")
    except:
        # No silver table yet (initial load scenario)
        df_silver_current = spark.createDataFrame([], schema="""
            customerId INT,
            firstName STRING,
            lastName STRING,
            birthDate DATE,
            customerStatus INT,
            validFrom TIMESTAMP,
            validTo TIMESTAMP
        """)

    df_bronze = spark.read.format("iceberg").load(f"{bronze_path}/customers_bronze")

    # We'll define a "current" row in silver as one that has validTo = null
    # We compare Bronze to current silver. If there's a difference, we close out the old record and insert a new one.
    # For demonstration, we assume the entire row except validFrom/validTo is "business columns".

    # 1) Join Bronze to the "current" rows in Silver by customerId
    #    We find new/updated records vs. unchanged.

    # Filter to only "open" rows in Silver
    df_silver_open = df_silver_current.filter(F.col("validTo").isNull())

    join_cond = (df_bronze.customerId == df_silver_open.customerId)
    # We'll do a full outer join, or left join if you prefer. 
    # But for simplicity let's do a left join from Bronze => Silver open.
    df_joined = df_bronze.alias("b").join(
        df_silver_open.alias("s"), 
        on=join_cond, 
        how="left"
    )

    # 2) Identify changed rows. We'll say it's changed if any relevant column is different.
    #    We'll handle birthDate, customerStatus, firstName, lastName. 
    #    We'll ignore validFrom and validTo columns from the silver in the comparison.

    is_changed_expr = (
        (F.col("b.firstName") != F.col("s.firstName")) |
        (F.col("b.lastName")  != F.col("s.lastName"))  |
        (F.col("b.birthDate") != F.col("s.birthDate")) |
        (F.col("b.customerStatus") != F.col("s.customerStatus"))
    )

    # 3) Prepare new "open" rows for changed records
    df_changed_new = df_joined.filter(is_changed_expr | F.col("s.customerId").isNull()).select(
        F.col("b.customerId"),
        F.col("b.firstName"),
        F.col("b.lastName"),
        F.col("b.birthDate"),
        F.col("b.customerStatus"),
        F.lit(F.current_timestamp()).alias("validFrom"),
        F.lit(None).cast("timestamp").alias("validTo")
    )

    # 4) Prepare old rows that need to be closed (set validTo) for those changed records
    #    We only close rows if they exist in silver (s.customerId not null).
    df_changed_old = df_joined.filter(is_changed_expr & F.col("s.customerId").isNotNull()).select(
        "s.*"
    ).withColumn("validTo", F.current_timestamp())

    # 5) Unchanged rows remain as-is. We'll filter them out from df_silver_open except those we explicitly close.
    #    So let's define them as "open rows minus the ones in df_changed_old".
    df_unchanged = df_silver_open.join(
        df_changed_old.select("customerId"), 
        on="customerId", 
        how="left_anti"  # keep only those not matched in changed_old
    )

    # 6) Combine everything:
    #    - unchanged open rows
    #    - changed old rows (closed)
    #    - changed new rows
    df_silver_final = df_unchanged.unionByName(df_changed_old).unionByName(df_changed_new)

    # 7) Write final result to Silver
    df_silver_final.write.format("iceberg") \
        .mode("overwrite") \
        .option("path", f"{silver_path}/customers_silver") \
        .save()

def copy_transfers_to_silver(spark: SparkSession, config: dict):
    """
    Reads from transfers_bronze and writes (or merges) to transfers_silver.
    For simplicity, we'll do a full overwrite. 
    In real practice, you might do incremental merges or partition-based updates.
    """
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]

    df_bronze_transfers = spark.read.format("iceberg").load(f"{bronze_path}/transfers_bronze")

    # For demonstration: full overwrite
    df_bronze_transfers.write.format("iceberg") \
        .mode("overwrite") \
        .option("path", f"{silver_path}/transfers_silver") \
        .save()

def copy_balance_to_silver(spark: SparkSession, config: dict):
    """
    Reads from balance_bronze and writes to balance_silver.
    Again, demonstration with simple overwrite.
    """
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]

    df_balance_bronze = spark.read.format("iceberg").load(f"{bronze_path}/balance_bronze")

    df_balance_bronze.write.format("iceberg") \
        .mode("overwrite") \
        .option("path", f"{silver_path}/balance_silver") \
        .save()

def run_silver_pipeline(spark: SparkSession, config: dict):
    """
    Runs all silver transformations:
      - SCD Type 2 for customers
      - Simple copy for transfers
      - Simple copy for balance
    """
    scd_type_2_customers(spark, config)
    copy_transfers_to_silver(spark, config)
    copy_balance_to_silver(spark, config)

# -------------------------------------------------------------------
#                           MAIN
# -------------------------------------------------------------------

def main(config_path: str):
    # 1. Parse the config
    config = ConfigFactory.parse_file(config_path)
    layer = config["layer"].lower()

    # 2. Create Spark session
    spark_conf = config["spark-config"]
    spark = create_spark_session(spark_conf)

    # 3. Run pipeline steps
    if layer == "bronze":
        run_bronze_pipeline(spark, config)
    elif layer == "silver":
        run_silver_pipeline(spark, config)
    elif layer == "gold":
        # For your scenario, you might transform data from silver → gold, 
        # but your question focuses on bronze & silver details. 
        # Implement your gold pipeline logic here if needed.
        pass
    else:
        raise ValueError(f"Unknown layer: {layer}")

    # 4. Stop Spark
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main_etl_job.py <path_to_config_file>")
        sys.exit(1)
    main(sys.argv[1])

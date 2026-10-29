# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

# --------------------------------------------------
# STEP 1: Configuration
# --------------------------------------------------

BASE_PATH = "/Volumes/krishiflow/source/source_data/"

LOAD_BATCH_ID = datetime.now().strftime(
    "bronze_load_%Y%m%d_%H%M%S"
)

datasets = [
    # Master/reference datasets
    {
        "folder": "location",
        "format": "csv",
        "table": "location_raw",
        "source": "synthetic_master_data"
    },
    {
        "folder": "crop",
        "format": "csv",
        "table": "crop_raw",
        "source": "synthetic_master_data"
    },
    {
        "folder": "farmer",
        "format": "csv",
        "table": "farmer_raw",
        "source": "synthetic_master_data"
    },
    {
        "folder": "farm",
        "format": "csv",
        "table": "farm_raw",
        "source": "synthetic_master_data"
    },
    {
        "folder": "collection_center",
        "format": "csv",
        "table": "collection_center_raw",
        "source": "synthetic_master_data"
    },
    {
        "folder": "storage",
        "format": "csv",
        "table": "storage_facility_raw",
        "source": "synthetic_master_data"
    },
    {
        "folder": "market",
        "format": "csv",
        "table": "market_raw",
        "source": "synthetic_master_data"
    },

    # Supply-chain transaction datasets
    {
        "folder": "harvest",
        "format": "csv",
        "table": "harvest_raw",
        "source": "synthetic_transaction_data"
    },
    {
        "folder": "collection",
        "format": "csv",
        "table": "collection_raw",
        "source": "synthetic_transaction_data"
    },
    {
        "folder": "storage_transaction",
        "format": "csv",
        "table": "storage_raw",
        "source": "synthetic_transaction_data"
    },
    {
        "folder": "transport",
        "format": "json",
        "table": "transport_raw",
        "source": "synthetic_transaction_data"
    },
    {
        "folder": "sale",
        "format": "csv",
        "table": "sale_raw",
        "source": "synthetic_transaction_data"
    },

    # Real weather observations from Open-Meteo
    {
        "folder": "weather/daily_observations",
        "format": "json",
        "table": "weather_raw",
        "source": "open_meteo_api_landing"
    }
]

# --------------------------------------------------
# STEP 2: Ensure the Bronze schema exists
# --------------------------------------------------

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS krishiflow.bronze"
)

# --------------------------------------------------
# STEP 3: Read each source and write its Bronze table
# --------------------------------------------------

results = []

for config in datasets:

    folder = config["folder"]
    file_format = config["format"]
    table_name = config["table"]
    source_name = config["source"]

    input_path = BASE_PATH + folder + "/"
    full_table_name = "krishiflow.bronze." + table_name

    print("\n" + "=" * 60)
    print("Dataset:", folder)
    print("Format:", file_format)
    print("Target:", full_table_name)

    # Confirm the source folder contains files
    files = [
        item for item in dbutils.fs.ls(input_path)
        if not item.name.startswith(("_", "."))
    ]

    if not files:
        raise Exception(
            "No data files found in: " + input_path
        )

    # Read CSV while preserving source values as strings
    if file_format == "csv":
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .load(input_path)
        )

    # Read newline-delimited JSON files
    elif file_format == "json":
        df = (
            spark.read
            .format("json")
            .option("mode", "PERMISSIVE")
            .load(input_path)
        )

    else:
        raise Exception(
            "Unsupported format: " + file_format
        )

    # Capture the original source column names
    original_columns = df.columns

    if len(original_columns) == 0:
        raise Exception(
            "No columns detected in: " + input_path
        )

    print("Columns:", original_columns)

    # Preserve existing source metadata when present.
    # Add standard metadata only when it is absent.
    if "_ingestion_timestamp" not in df.columns:
        df = df.withColumn(
            "_ingestion_timestamp",
            F.current_timestamp()
        )

    if "_source" not in df.columns:
        df = df.withColumn(
            "_source",
            F.lit(source_name)
        )

    if "_batch_id" not in df.columns:
        df = df.withColumn(
            "_batch_id",
            F.lit(LOAD_BATCH_ID)
        )

    if "_raw_file_path" not in df.columns:
        df = df.withColumn(
            "_raw_file_path",
            F.lit(input_path)
        )

    # Add Bronze-specific metadata without overwriting
    # metadata already present in the source
    df = (
        df
        .withColumn(
            "_bronze_load_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_bronze_batch_id",
            F.lit(LOAD_BATCH_ID)
        )
        .withColumn(
            "_source_folder",
            F.lit(folder)
        )
    )

    # Count records before writing
    record_count = df.count()

    if record_count == 0:
        raise Exception(
            "No records found in: " + input_path
        )

    print("Records:", record_count)

    # Initial load: replace this target's previous snapshot
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    print("SUCCESS:", full_table_name)

    results.append(
        (
            folder,
            file_format,
            full_table_name,
            record_count,
            "SUCCESS"
        )
    )

# --------------------------------------------------
# STEP 4: Show the ingestion summary
# --------------------------------------------------

summary_df = spark.createDataFrame(
    results,
    [
        "source_folder",
        "format",
        "target_table",
        "records_loaded",
        "status"
    ]
)

display(summary_df)

print("Bronze batch completed:", LOAD_BATCH_ID)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN krishiflow.bronze;
# MAGIC

# COMMAND ----------

# Profile Bronze Tables
from pyspark.sql import functions as F

# All 14 Bronze tables
table_names = [
    "location_raw",
    "crop_raw",
    "farmer_raw",
    "farm_raw",
    "collection_center_raw",
    "storage_facility_raw",
    "market_raw",
    "harvest_raw",
    "collection_raw",
    "storage_raw",
    "transport_raw",
    "sale_raw",
    "weather_raw",
    "market_price_raw"
]

profile_results = []

for table_name in table_names:
    full_name = f"krishiflow.bronze.{table_name}"

    df = spark.table(full_name)

    profile_results.append((
        table_name,
        df.count(),
        len(df.columns),
        ", ".join(df.columns)
    ))

profile_df = spark.createDataFrame(
    profile_results,
    [
        "table_name",
        "record_count",
        "column_count",
        "columns"
    ]
)

display(profile_df.orderBy("table_name"))
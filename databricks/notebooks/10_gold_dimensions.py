# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# STEP 1: Create Gold schema
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS krishiflow.gold")

# ============================================================
# STEP 2: Create dimension tables from Silver
# ============================================================

dimension_configs = [
    {
        "source": "krishiflow.silver.location",
        "target": "krishiflow.gold.dim_location",
        "pk": "location_id",
        "columns": [
            "location_id",
            "state",
            "district",
            "city",
            "latitude",
            "longitude"
        ]
    },
    {
        "source": "krishiflow.silver.crop",
        "target": "krishiflow.gold.dim_crop",
        "pk": "crop_id",
        "columns": [
            "crop_id",
            "crop_name",
            "crop_category",
            "unit_of_measure"
        ]
    },
    {
        "source": "krishiflow.silver.farmer",
        "target": "krishiflow.gold.dim_farmer",
        "pk": "farmer_id",
        "columns": [
            "farmer_id",
            "farmer_name",
            "location_id",
            "farm_count"
        ]
    },
    {
        "source": "krishiflow.silver.farm",
        "target": "krishiflow.gold.dim_farm",
        "pk": "farm_id",
        "columns": [
            "farm_id",
            "farmer_id",
            "location_id",
            "area_acres",
            "farm_type"
        ]
    },
    {
        "source": "krishiflow.silver.market",
        "target": "krishiflow.gold.dim_market",
        "pk": "market_id",
        "columns": [
            "market_id",
            "market_name",
            "location_id",
            "market_type"
        ]
    },
    {
        "source": "krishiflow.silver.storage_facility",
        "target": "krishiflow.gold.dim_storage",
        "pk": "storage_facility_id",
        "columns": [
            "storage_facility_id",
            "storage_name",
            "location_id",
            "storage_type",
            "capacity_kg"
        ]
    },
    {
        "source": "krishiflow.silver.collection_center",
        "target": "krishiflow.gold.dim_collection_center",
        "pk": "collection_center_id",
        "columns": [
            "collection_center_id",
            "center_name",
            "location_id",
            "capacity_kg"
        ]
    }
]

dimension_results = []

for config in dimension_configs:

    source_table = config["source"]
    target_table = config["target"]
    primary_key = config["pk"]

    print("\nProcessing:", source_table)

    source_df = spark.table(source_table)

    # Select only business columns, excluding Silver metadata
    dimension_df = source_df.select(*config["columns"])

    # Ensure primary keys are populated and unique
    null_keys = dimension_df.filter(
        F.col(primary_key).isNull()
    ).count()

    duplicate_keys = (
        dimension_df
        .groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if null_keys > 0 or duplicate_keys > 0:
        raise Exception(
            f"Invalid dimension key in {source_table}: "
            f"{null_keys} null keys, "
            f"{duplicate_keys} duplicate keys"
        )

    # Replace the target with the current validated dimension
    (
        dimension_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    target_count = spark.table(target_table).count()

    print("Created:", target_table)
    print("Records:", target_count)

    dimension_results.append(
        (target_table, target_count, "SUCCESS")
    )

# ============================================================
# STEP 3: Build dim_date from all relevant date ranges
# ============================================================

date_sources = [
    ("krishiflow.silver.harvest", "harvest_date"),
    ("krishiflow.silver.collection", "collection_date"),
    ("krishiflow.silver.storage_transaction", "entry_date"),
    ("krishiflow.silver.storage_transaction", "exit_date"),
    ("krishiflow.silver.transport", "dispatch_timestamp"),
    ("krishiflow.silver.transport", "arrival_timestamp"),
    ("krishiflow.silver.sale", "sale_date"),
    ("krishiflow.silver.weather", "weather_date"),
    ("krishiflow.silver.market_price", "arrival_date")
]

date_frames = []

for table_name, date_column in date_sources:

    current_dates = (
        spark.table(table_name)
        .select(
            F.to_date(F.col(date_column)).alias("full_date")
        )
        .filter(F.col("full_date").isNotNull())
    )

    date_frames.append(current_dates)

all_dates = date_frames[0]

for current_dates in date_frames[1:]:
    all_dates = all_dates.unionByName(current_dates)

date_bounds = all_dates.agg(
    F.min("full_date").alias("min_date"),
    F.max("full_date").alias("max_date")
).first()

if date_bounds["min_date"] is None:
    raise Exception("No valid dates found in Silver datasets.")

start_date = str(date_bounds["min_date"])
end_date = str(date_bounds["max_date"])

print("\nDate dimension range:", start_date, "to", end_date)

# Generate every date between the minimum and maximum dates
calendar_df = spark.sql(f"""
    SELECT explode(
        sequence(
            DATE '{start_date}',
            DATE '{end_date}',
            INTERVAL 1 DAY
        )
    ) AS full_date
""")

dim_date = (
    calendar_df
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )
    .withColumn("calendar_year", F.year("full_date"))
    .withColumn("quarter_number", F.quarter("full_date"))
    .withColumn("month_number", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn("day_of_week_number", F.dayofweek("full_date"))
    .withColumn("day_of_year", F.dayofyear("full_date"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn(
        "is_weekend",
        F.dayofweek("full_date").isin([1, 7])
    )
)

date_target = "krishiflow.gold.dim_date"

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(date_target)
)

dimension_results.append(
    (
        date_target,
        spark.table(date_target).count(),
        "SUCCESS"
    )
)

# ============================================================
# STEP 4: Display build summary
# ============================================================

results_df = spark.createDataFrame(
    dimension_results,
    ["table_name", "record_count", "status"]
)

display(results_df.orderBy("table_name"))

print("\nGold dimensions created successfully.")

# COMMAND ----------

spark.sql("SHOW TABLES IN krishiflow.gold").show(
    truncate=False
)
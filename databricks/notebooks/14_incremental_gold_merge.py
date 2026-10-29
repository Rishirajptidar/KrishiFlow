# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark.sql("CREATE SCHEMA IF NOT EXISTS krishiflow.gold")

# ============================================================
# STEP 1: Prepare Gold dimension data
# ============================================================

dimension_configs = [
    (
        "krishiflow.silver.location",
        "krishiflow.gold.dim_location",
        "location_id",
        ["location_id", "state", "district", "city", "latitude", "longitude"]
    ),
    (
        "krishiflow.silver.crop",
        "krishiflow.gold.dim_crop",
        "crop_id",
        ["crop_id", "crop_name", "crop_category", "unit_of_measure"]
    ),
    (
        "krishiflow.silver.farmer",
        "krishiflow.gold.dim_farmer",
        "farmer_id",
        ["farmer_id", "farmer_name", "location_id", "farm_count"]
    ),
    (
        "krishiflow.silver.farm",
        "krishiflow.gold.dim_farm",
        "farm_id",
        ["farm_id", "farmer_id", "location_id", "area_acres", "farm_type"]
    ),
    (
        "krishiflow.silver.market",
        "krishiflow.gold.dim_market",
        "market_id",
        ["market_id", "market_name", "location_id", "market_type"]
    ),
    (
        "krishiflow.silver.storage_facility",
        "krishiflow.gold.dim_storage",
        "storage_facility_id",
        [
            "storage_facility_id", "storage_name",
            "location_id", "storage_type", "capacity_kg"
        ]
    ),
    (
        "krishiflow.silver.collection_center",
        "krishiflow.gold.dim_collection_center",
        "collection_center_id",
        [
            "collection_center_id", "center_name",
            "location_id", "capacity_kg"
        ]
    )
]

merge_jobs = []

for source_table, target_table, key, columns in dimension_configs:
    source_df = spark.table(source_table).select(*columns)
    merge_jobs.append((target_table, source_df, [key]))


# ============================================================
# STEP 2: Prepare the date dimension
# Extend its existing calendar when new dates arrive.
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
    date_frames.append(
        spark.table(table_name)
        .select(F.to_date(date_column).alias("full_date"))
        .filter(F.col("full_date").isNotNull())
    )

all_dates = date_frames[0]

for frame in date_frames[1:]:
    all_dates = all_dates.unionByName(frame)

date_bounds = all_dates.agg(
    F.min("full_date").alias("min_date"),
    F.max("full_date").alias("max_date")
).first()

if date_bounds["min_date"] is None:
    raise Exception("No valid dates available for the date dimension.")

start_date = str(date_bounds["min_date"])
end_date = str(date_bounds["max_date"])

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
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("calendar_year", F.year("full_date"))
    .withColumn("quarter_number", F.quarter("full_date"))
    .withColumn("month_number", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn("day_of_week_number", F.dayofweek("full_date"))
    .withColumn("day_of_year", F.dayofyear("full_date"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
)

merge_jobs.append(
    ("krishiflow.gold.dim_date", dim_date, ["date_key"])
)


# ============================================================
# STEP 3: Prepare Gold fact data
# ============================================================

# Fact: Harvest
harvest = spark.table("krishiflow.silver.harvest")

fact_harvest = harvest.select(
    "harvest_id",
    F.date_format(F.to_date("harvest_date"), "yyyyMMdd").cast("int").alias("date_key"),
    "harvest_date",
    "farm_id",
    "crop_id",
    "quantity_harvested_kg",
    "area_harvested_acres"
).withColumn(
    "yield_kg_per_acre",
    F.round(F.col("quantity_harvested_kg") / F.col("area_harvested_acres"), 2)
)

merge_jobs.append(
    ("krishiflow.gold.fact_harvest", fact_harvest, ["harvest_id"])
)


# Fact: Collection
collection = spark.table("krishiflow.silver.collection")

fact_collection = collection.select(
    "collection_id",
    F.date_format(F.to_date("collection_date"), "yyyyMMdd").cast("int").alias("date_key"),
    "collection_date",
    "harvest_id",
    "farm_id",
    "collection_center_id",
    "quantity_received_kg"
)

merge_jobs.append(
    ("krishiflow.gold.fact_collection", fact_collection, ["collection_id"])
)


# Fact: Storage
storage = spark.table("krishiflow.silver.storage_transaction")

fact_storage = (
    storage
    .withColumn(
        "entry_date_key",
        F.date_format(F.to_date("entry_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "exit_date_key",
        F.when(
            F.col("exit_date").isNotNull(),
            F.date_format(F.to_date("exit_date"), "yyyyMMdd").cast("int")
        )
    )
    .withColumn(
        "storage_quantity_difference_kg",
        F.round(F.col("quantity_in_kg") - F.col("quantity_out_kg"), 2)
    )
    .withColumn(
        "storage_quantity_difference_pct",
        F.when(
            F.col("quantity_in_kg") > 0,
            F.round(
                (F.col("quantity_in_kg") - F.col("quantity_out_kg"))
                / F.col("quantity_in_kg") * 100,
                2
            )
        )
    )
    .select(
        "storage_transaction_id",
        "collection_id",
        "storage_facility_id",
        "entry_date_key",
        "exit_date_key",
        "entry_date",
        "exit_date",
        "quantity_in_kg",
        "quantity_out_kg",
        "storage_quantity_difference_kg",
        "storage_quantity_difference_pct"
    )
)

merge_jobs.append(
    ("krishiflow.gold.fact_storage", fact_storage, ["storage_transaction_id"])
)


# Fact: Transport
transport = spark.table("krishiflow.silver.transport")

fact_transport = (
    transport
    .withColumn(
        "dispatch_date_key",
        F.date_format(F.to_date("dispatch_timestamp"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "arrival_date_key",
        F.date_format(F.to_date("arrival_timestamp"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "transport_quantity_loss_kg",
        F.round(F.col("quantity_dispatched_kg") - F.col("quantity_received_kg"), 2)
    )
    .withColumn(
        "transport_quantity_loss_pct",
        F.when(
            F.col("quantity_dispatched_kg") > 0,
            F.round(
                (F.col("quantity_dispatched_kg") - F.col("quantity_received_kg"))
                / F.col("quantity_dispatched_kg") * 100,
                2
            )
        )
    )
    .withColumn(
        "transit_hours",
        F.round(
            (
                F.col("arrival_timestamp").cast("long")
                - F.col("dispatch_timestamp").cast("long")
            ) / 3600,
            2
        )
    )
    .select(
        "transport_id",
        "storage_transaction_id",
        "crop_id",
        "market_id",
        "source_location_id",
        "destination_location_id",
        "dispatch_date_key",
        "arrival_date_key",
        "dispatch_timestamp",
        "arrival_timestamp",
        "distance_km",
        "quantity_dispatched_kg",
        "quantity_received_kg",
        "transport_quantity_loss_kg",
        "transport_quantity_loss_pct",
        "transit_hours"
    )
)

merge_jobs.append(
    ("krishiflow.gold.fact_transport", fact_transport, ["transport_id"])
)


# Fact: Sales
sales = spark.table("krishiflow.silver.sale")

fact_sales = (
    sales
    .withColumn(
        "revenue",
        F.round(F.col("quantity_sold_kg") * F.col("selling_price_per_kg"), 2)
    )
    .select(
        "sale_id",
        F.date_format(F.to_date("sale_date"), "yyyyMMdd").cast("int").alias("date_key"),
        "sale_date",
        "transport_id",
        "market_id",
        "crop_id",
        "quantity_sold_kg",
        "selling_price_per_kg",
        "revenue"
    )
)

merge_jobs.append(
    ("krishiflow.gold.fact_sales", fact_sales, ["sale_id"])
)


# Fact: Government mandi prices
market_price = spark.table("krishiflow.silver.market_price")

fact_market_price = (
    market_price
    .withColumn(
        "date_key",
        F.date_format(F.to_date("arrival_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "price_spread",
        F.round(F.col("max_price") - F.col("min_price"), 2)
    )
    .select(
        "market_price_id",
        "date_key",
        "arrival_date",
        "state",
        "district",
        "market",
        "commodity",
        "variety",
        "grade",
        "min_price",
        "max_price",
        "modal_price",
        "price_spread"
    )
)

merge_jobs.append(
    ("krishiflow.gold.fact_market_price", fact_market_price, ["market_price_id"])
)


# Fact: Weather
weather = spark.table("krishiflow.silver.weather")

fact_weather = weather.select(
    "weather_id",
    F.date_format(F.to_date("weather_date"), "yyyyMMdd").cast("int").alias("date_key"),
    "weather_date",
    "location_id",
    "temperature_c",
    "humidity_percentage",
    "rainfall_mm",
    "wind_speed_kmph"
)

merge_jobs.append(
    ("krishiflow.gold.fact_weather", fact_weather, ["weather_id"])
)


# ============================================================
# STEP 4: Generic Delta MERGE function
# ============================================================

def merge_gold_table(target_table, source_df, keys):

    # Verify incoming primary keys
    null_condition = None

    for key in keys:
        condition = F.col(key).isNull()
        null_condition = (
            condition if null_condition is None
            else null_condition | condition
        )

    null_keys = source_df.filter(null_condition).count()

    duplicate_keys = (
        source_df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if null_keys > 0 or duplicate_keys > 0:
        raise Exception(
            f"Source key validation failed for {target_table}. "
            f"Null-key rows: {null_keys}; duplicate-key groups: {duplicate_keys}"
        )

    source_count = source_df.count()
    exists = spark.catalog.tableExists(target_table)

    if exists:
        target_before = spark.table(target_table).count()

        target_columns = spark.table(target_table).columns

        if set(target_columns) != set(source_df.columns):
            raise Exception(
                f"Schema mismatch for {target_table}.\n"
                f"Existing columns: {target_columns}\n"
                f"Incoming columns: {source_df.columns}"
            )

        # Keep the target column order for readability
        source_df = source_df.select(*target_columns)

        merge_condition = " AND ".join(
            [f"t.`{key}` = s.`{key}`" for key in keys]
        )

        (
            DeltaTable.forName(spark, target_table)
            .alias("t")
            .merge(source_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        target_before = 0

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

    target_after = spark.table(target_table).count()

    return (
        target_table,
        source_count,
        target_before,
        target_after,
        target_after - target_before,
        "SUCCESS"
    )


# ============================================================
# STEP 5: Execute all Gold upserts
# ============================================================

merge_results = []

for target_table, source_df, keys in merge_jobs:

    print("\nIncrementally refreshing:", target_table)

    result = merge_gold_table(
        target_table,
        source_df,
        keys
    )

    merge_results.append(result)

    print("Incoming rows:", result[1])
    print("Rows before:", result[2])
    print("Rows after:", result[3])
    print("New rows added:", result[4])


# ============================================================
# STEP 6: Display summary
# ============================================================

summary_schema = """
    target_table STRING,
    incoming_rows LONG,
    rows_before LONG,
    rows_after LONG,
    new_rows_added LONG,
    status STRING
"""

summary_df = spark.createDataFrame(
    merge_results,
    schema=summary_schema
)

display(summary_df.orderBy("target_table"))

print("Incremental Gold refresh completed.")
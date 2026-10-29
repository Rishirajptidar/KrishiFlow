# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# STEP 1: Configuration
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS krishiflow.gold")

results = []

# ============================================================
# STEP 2: FACT HARVEST
# One row per harvest event
# ============================================================

harvest = spark.table("krishiflow.silver.harvest")

fact_harvest = harvest.select(
    "harvest_id",
    F.date_format(
        F.to_date("harvest_date"), "yyyyMMdd"
    ).cast("int").alias("date_key"),
    "harvest_date",
    "farm_id",
    "crop_id",
    "quantity_harvested_kg",
    "area_harvested_acres"
).withColumn(
    "yield_kg_per_acre",
    F.round(
        F.col("quantity_harvested_kg")
        / F.col("area_harvested_acres"),
        2
    )
)

# ============================================================
# STEP 3: FACT COLLECTION
# One row per collection event
# ============================================================

collection = spark.table("krishiflow.silver.collection")

fact_collection = collection.select(
    "collection_id",
    F.date_format(
        F.to_date("collection_date"), "yyyyMMdd"
    ).cast("int").alias("date_key"),
    "collection_date",
    "harvest_id",
    "farm_id",
    "collection_center_id",
    "quantity_received_kg"
)

# ============================================================
# STEP 4: FACT STORAGE
# One row per storage transaction
# ============================================================

storage = spark.table(
    "krishiflow.silver.storage_transaction"
)

fact_storage = (
    storage
    .withColumn(
        "entry_date_key",
        F.date_format(
            F.to_date("entry_date"), "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "exit_date_key",
        F.when(
            F.col("exit_date").isNotNull(),
            F.date_format(
                F.to_date("exit_date"), "yyyyMMdd"
            ).cast("int")
        )
    )
    .withColumn(
        "storage_quantity_difference_kg",
        F.round(
            F.col("quantity_in_kg") - F.col("quantity_out_kg"),
            2
        )
    )
    .withColumn(
        "storage_quantity_difference_pct",
        F.when(
            F.col("quantity_in_kg") > 0,
            F.round(
                (
                    F.col("quantity_in_kg")
                    - F.col("quantity_out_kg")
                ) / F.col("quantity_in_kg") * 100,
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

# ============================================================
# STEP 5: FACT TRANSPORT
# One row per transport event
# ============================================================

transport = spark.table("krishiflow.silver.transport")

fact_transport = (
    transport
    .withColumn(
        "dispatch_date_key",
        F.date_format(
            F.to_date("dispatch_timestamp"), "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "arrival_date_key",
        F.date_format(
            F.to_date("arrival_timestamp"), "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "transport_quantity_loss_kg",
        F.round(
            F.col("quantity_dispatched_kg")
            - F.col("quantity_received_kg"),
            2
        )
    )
    .withColumn(
        "transport_quantity_loss_pct",
        F.when(
            F.col("quantity_dispatched_kg") > 0,
            F.round(
                (
                    F.col("quantity_dispatched_kg")
                    - F.col("quantity_received_kg")
                ) / F.col("quantity_dispatched_kg") * 100,
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

# ============================================================
# STEP 6: FACT SALES
# One row per sales event
# ============================================================

sales = spark.table("krishiflow.silver.sale")

fact_sales = (
    sales
    .withColumn(
        "revenue",
        F.round(
            F.col("quantity_sold_kg")
            * F.col("selling_price_per_kg"),
            2
        )
    )
    .select(
        "sale_id",
        F.date_format(
            F.to_date("sale_date"), "yyyyMMdd"
        ).cast("int").alias("date_key"),
        "sale_date",
        "transport_id",
        "market_id",
        "crop_id",
        "quantity_sold_kg",
        "selling_price_per_kg",
        "revenue"
    )
)

# ============================================================
# STEP 7: FACT MARKET PRICE
# One row per cleaned mandi-price observation
# Keep external mandi names separate from synthetic markets
# ============================================================

market_price = spark.table(
    "krishiflow.silver.market_price"
)

fact_market_price = (
    market_price
    .withColumn(
        "date_key",
        F.date_format(
            F.to_date("arrival_date"), "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "price_spread",
        F.round(
            F.col("max_price") - F.col("min_price"),
            2
        )
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

# ============================================================
# STEP 8: FACT WEATHER
# One row per weather observation
# ============================================================

weather = spark.table("krishiflow.silver.weather")

fact_weather = weather.select(
    "weather_id",
    F.date_format(
        F.to_date("weather_date"), "yyyyMMdd"
    ).cast("int").alias("date_key"),
    "weather_date",
    "location_id",
    "temperature_c",
    "humidity_percentage",
    "rainfall_mm",
    "wind_speed_kmph"
)

# ============================================================
# STEP 9: Write Gold fact tables
# ============================================================

fact_configs = [
    ("fact_harvest", fact_harvest, harvest),
    ("fact_collection", fact_collection, collection),
    ("fact_storage", fact_storage, storage),
    ("fact_transport", fact_transport, transport),
    ("fact_sales", fact_sales, sales),
    ("fact_market_price", fact_market_price, market_price),
    ("fact_weather", fact_weather, weather)
]

for name, fact_df, source_df in fact_configs:

    target = "krishiflow.gold." + name

    print("\nBuilding:", target)

    source_count = source_df.count()
    fact_count = fact_df.count()

    if fact_count != source_count:
        raise Exception(
            f"Record-count mismatch for {name}: "
            f"source={source_count}, fact={fact_count}"
        )

    (
        fact_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    results.append(
        (name, source_count, fact_count, "SUCCESS")
    )

    print("Records:", fact_count)
    print("Created:", target)

# ============================================================
# STEP 10: Display build summary
# ============================================================

summary = spark.createDataFrame(
    results,
    [
        "fact_table",
        "source_records",
        "fact_records",
        "status"
    ]
)

display(summary)

print("All seven Gold fact tables created successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_harvest' AS table_name, COUNT(*) AS records
# MAGIC FROM krishiflow.gold.fact_harvest
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'fact_collection', COUNT(*)
# MAGIC FROM krishiflow.gold.fact_collection
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'fact_storage', COUNT(*)
# MAGIC FROM krishiflow.gold.fact_storage
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'fact_transport', COUNT(*)
# MAGIC FROM krishiflow.gold.fact_transport
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'fact_sales', COUNT(*)
# MAGIC FROM krishiflow.gold.fact_sales
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'fact_market_price', COUNT(*)
# MAGIC FROM krishiflow.gold.fact_market_price
# MAGIC
# MAGIC UNION ALL
# MAGIC SELECT 'fact_weather', COUNT(*)
# MAGIC FROM krishiflow.gold.fact_weather;
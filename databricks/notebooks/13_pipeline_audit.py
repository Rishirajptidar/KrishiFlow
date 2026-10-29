# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# STEP 1: Configuration
# ============================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS krishiflow.gold")

quarantine_table = "krishiflow.silver.quarantine_records"

# Bronze -> Silver table mappings
table_mappings = [
    ("location_raw", "location"),
    ("crop_raw", "crop"),
    ("farmer_raw", "farmer"),
    ("farm_raw", "farm"),
    ("collection_center_raw", "collection_center"),
    ("storage_facility_raw", "storage_facility"),
    ("market_raw", "market"),
    ("harvest_raw", "harvest"),
    ("collection_raw", "collection"),
    ("storage_raw", "storage_transaction"),
    ("transport_raw", "transport"),
    ("sale_raw", "sale"),
    ("weather_raw", "weather"),
    ("market_price_raw", "market_price")
]

reconciliation_rows = []
audit_rows = []


def exists(table_name):
    return spark.catalog.tableExists(table_name)


def add_check(layer, table_name, check_name,
              failed_records, details, severity="HIGH"):

    failed_records = int(failed_records)

    status = "PASS" if failed_records == 0 else "FAIL"

    audit_rows.append((
        layer,
        table_name,
        check_name,
        severity,
        failed_records,
        status,
        details
    ))


# ============================================================
# STEP 2: Bronze-to-Silver reconciliation
# ============================================================

for bronze_name, silver_name in table_mappings:

    source = "krishiflow.bronze." + bronze_name
    target = "krishiflow.silver." + silver_name

    if not exists(source) or not exists(target):

        reconciliation_rows.append((
            source,
            target,
            None,
            None,
            None,
            None,
            None,
            "MISSING_TABLE"
        ))

        continue

    source_count = spark.table(source).count()
    target_count = spark.table(target).count()

    quarantine_count = 0

    if exists(quarantine_table):
        quarantine_count = (
            spark.table(quarantine_table)
            .filter(F.col("source_table") == source)
            .count()
        )

    difference = source_count - target_count

    # Positive values here indicate differences not explained
    # by currently recorded quarantine rows.
    unexplained_difference = difference - quarantine_count

    status = (
        "MATCH"
        if unexplained_difference == 0
        else "REVIEW"
    )

    reconciliation_rows.append((
        source,
        target,
        source_count,
        target_count,
        difference,
        quarantine_count,
        unexplained_difference,
        status
    ))


reconciliation_schema = """
    source_table STRING,
    silver_table STRING,
    bronze_records LONG,
    silver_records LONG,
    bronze_minus_silver LONG,
    quarantine_records LONG,
    unexplained_difference LONG,
    reconciliation_status STRING
"""

reconciliation_df = spark.createDataFrame(
    reconciliation_rows,
    schema=reconciliation_schema
).withColumn(
    "checked_at",
    F.current_timestamp()
)

(
    reconciliation_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("krishiflow.gold.pipeline_reconciliation")
)

print("Bronze-to-Silver reconciliation completed.")


# ============================================================
# STEP 3: Silver primary-key checks
# ============================================================

silver_primary_keys = [
    ("location", "location_id"),
    ("crop", "crop_id"),
    ("farmer", "farmer_id"),
    ("farm", "farm_id"),
    ("collection_center", "collection_center_id"),
    ("storage_facility", "storage_facility_id"),
    ("market", "market_id"),
    ("harvest", "harvest_id"),
    ("collection", "collection_id"),
    ("storage_transaction", "storage_transaction_id"),
    ("transport", "transport_id"),
    ("sale", "sale_id"),
    ("weather", "weather_id"),
    ("market_price", "market_price_id")
]

for table_name, pk in silver_primary_keys:

    full_name = "krishiflow.silver." + table_name

    if not exists(full_name):
        add_check(
            "SILVER", full_name, "TABLE_EXISTS",
            1, "Expected Silver table is missing."
        )
        continue

    df = spark.table(full_name)

    null_count = df.filter(F.col(pk).isNull()).count()

    duplicate_count = (
        df.groupBy(pk)
        .count()
        .filter(
            F.col(pk).isNotNull()
            & (F.col("count") > 1)
        )
        .count()
    )

    add_check(
        "SILVER", full_name,
        "PRIMARY_KEY_NOT_NULL",
        null_count,
        "Number of rows with a null primary key."
    )

    add_check(
        "SILVER", full_name,
        "PRIMARY_KEY_UNIQUE",
        duplicate_count,
        "Number of duplicated primary-key groups."
    )


# ============================================================
# STEP 4: Foreign-key checks
# ============================================================

foreign_keys = [
    ("farmer", "location_id", "location", "location_id"),
    ("farm", "farmer_id", "farmer", "farmer_id"),
    ("farm", "location_id", "location", "location_id"),
    ("collection_center", "location_id", "location", "location_id"),
    ("storage_facility", "location_id", "location", "location_id"),
    ("market", "location_id", "location", "location_id"),

    ("harvest", "farm_id", "farm", "farm_id"),
    ("harvest", "crop_id", "crop", "crop_id"),

    ("collection", "harvest_id", "harvest", "harvest_id"),
    ("collection", "farm_id", "farm", "farm_id"),
    (
        "collection", "collection_center_id",
        "collection_center", "collection_center_id"
    ),

    (
        "storage_transaction", "collection_id",
        "collection", "collection_id"
    ),
    (
        "storage_transaction", "storage_facility_id",
        "storage_facility", "storage_facility_id"
    ),

    (
        "transport", "storage_transaction_id",
        "storage_transaction", "storage_transaction_id"
    ),
    ("transport", "crop_id", "crop", "crop_id"),
    ("transport", "market_id", "market", "market_id"),
    (
        "transport", "source_location_id",
        "location", "location_id"
    ),
    (
        "transport", "destination_location_id",
        "location", "location_id"
    ),

    ("sale", "transport_id", "transport", "transport_id"),
    ("sale", "market_id", "market", "market_id"),
    ("sale", "crop_id", "crop", "crop_id"),

    ("weather", "location_id", "location", "location_id")
]

for child_table, child_key, parent_table, parent_key in foreign_keys:

    child_name = "krishiflow.silver." + child_table
    parent_name = "krishiflow.silver." + parent_table

    if not exists(child_name) or not exists(parent_name):
        add_check(
            "SILVER", child_name,
            "FOREIGN_KEY_" + child_key.upper(),
            1,
            "Child or parent table is missing."
        )
        continue

    child_keys = spark.table(child_name).select(
        F.col(child_key).alias("_fk")
    )

    parent_keys = (
        spark.table(parent_name)
        .select(F.col(parent_key).alias("_pk"))
        .distinct()
    )

    unmatched = child_keys.join(
        parent_keys,
        child_keys["_fk"] == parent_keys["_pk"],
        "left_anti"
    )

    failed_count = unmatched.count()

    add_check(
        "SILVER",
        child_name,
        "FOREIGN_KEY_" + child_key.upper(),
        failed_count,
        f"References {parent_name}.{parent_key}; "
        "unmatched rows include null foreign keys."
    )


# ============================================================
# STEP 5: Business-rule checks
# ============================================================

business_rules = [
    (
        "harvest",
        "POSITIVE_HARVEST_QUANTITY",
        lambda d: F.col("quantity_harvested_kg") <= 0
    ),
    (
        "harvest",
        "POSITIVE_HARVEST_AREA",
        lambda d: F.col("area_harvested_acres") <= 0
    ),
    (
        "collection",
        "POSITIVE_COLLECTION_QUANTITY",
        lambda d: F.col("quantity_received_kg") <= 0
    ),
    (
        "storage_transaction",
        "POSITIVE_STORAGE_INBOUND",
        lambda d: F.col("quantity_in_kg") <= 0
    ),
    (
        "storage_transaction",
        "NONNEGATIVE_STORAGE_OUTBOUND",
        lambda d: F.col("quantity_out_kg") < 0
    ),
    (
        "storage_transaction",
        "STORAGE_DATE_ORDER",
        lambda d: (
            F.col("exit_date").isNotNull()
            & (F.col("exit_date") < F.col("entry_date"))
        )
    ),
    (
        "transport",
        "POSITIVE_TRANSPORT_DISTANCE",
        lambda d: F.col("distance_km") <= 0
    ),
    (
        "transport",
        "RECEIVED_NOT_ABOVE_DISPATCHED",
        lambda d: (
            F.col("quantity_received_kg")
            > F.col("quantity_dispatched_kg")
        )
    ),
    (
        "transport",
        "TRANSPORT_DATE_ORDER",
        lambda d: (
            F.col("arrival_timestamp")
            < F.col("dispatch_timestamp")
        )
    ),
    (
        "sale",
        "POSITIVE_SOLD_QUANTITY",
        lambda d: F.col("quantity_sold_kg") <= 0
    ),
    (
        "sale",
        "POSITIVE_SELLING_PRICE",
        lambda d: F.col("selling_price_per_kg") <= 0
    ),
    (
        "sale",
        "REVENUE_CALCULATION",
        lambda d: (
            F.col("revenue").isNull()
            | (
                F.abs(
                    F.col("revenue")
                    - F.col("quantity_sold_kg")
                    * F.col("selling_price_per_kg")
                ) > 0.05
            )
        )
    ),
    (
        "weather",
        "HUMIDITY_RANGE",
        lambda d: (
            (F.col("humidity_percentage") < 0)
            | (F.col("humidity_percentage") > 100)
            | F.col("humidity_percentage").isNull()
        )
    ),
    (
        "weather",
        "NONNEGATIVE_RAINFALL",
        lambda d: (
            (F.col("rainfall_mm") < 0)
            | F.col("rainfall_mm").isNull()
        )
    ),
    (
        "weather",
        "NONNEGATIVE_WIND_SPEED",
        lambda d: (
            (F.col("wind_speed_kmph") < 0)
            | F.col("wind_speed_kmph").isNull()
        )
    ),
    (
        "market_price",
        "VALID_MARKET_PRICE_RANGE",
        lambda d: (
            F.col("min_price").isNull()
            | F.col("max_price").isNull()
            | F.col("modal_price").isNull()
            | (F.col("min_price") < 0)
            | (F.col("max_price") < F.col("min_price"))
            | (F.col("modal_price") < 0)
        )
    )
]

for table_name, rule_name, predicate_fn in business_rules:

    full_name = "krishiflow.silver." + table_name

    if not exists(full_name):
        add_check(
            "SILVER", full_name, rule_name,
            1, "Expected Silver table is missing."
        )
        continue

    df = spark.table(full_name)

    failed_count = df.filter(
        predicate_fn(df)
    ).count()

    add_check(
        "SILVER",
        full_name,
        rule_name,
        failed_count,
        "Independent business-rule audit."
    )


# Coordinate validation
location_df = spark.table("krishiflow.silver.location")

invalid_latitude = location_df.filter(
    (F.col("latitude") < -90)
    | (F.col("latitude") > 90)
    | F.col("latitude").isNull()
).count()

invalid_longitude = location_df.filter(
    (F.col("longitude") < -180)
    | (F.col("longitude") > 180)
    | F.col("longitude").isNull()
).count()

add_check(
    "SILVER", "krishiflow.silver.location",
    "LATITUDE_RANGE", invalid_latitude,
    "Latitude must be between -90 and 90."
)

add_check(
    "SILVER", "krishiflow.silver.location",
    "LONGITUDE_RANGE", invalid_longitude,
    "Longitude must be between -180 and 180."
)


# ============================================================
# STEP 6: Check Gold fact keys and date dimensions
# ============================================================

fact_keys = [
    ("fact_harvest", "harvest_id", ["date_key"]),
    ("fact_collection", "collection_id", ["date_key"]),
    ("fact_storage", "storage_transaction_id", ["entry_date_key"]),
    (
        "fact_transport", "transport_id",
        ["dispatch_date_key", "arrival_date_key"]
    ),
    ("fact_sales", "sale_id", ["date_key"]),
    ("fact_market_price", "market_price_id", ["date_key"]),
    ("fact_weather", "weather_id", ["date_key"])
]

date_dimension = spark.table("krishiflow.gold.dim_date")

date_keys = date_dimension.select(
    F.col("date_key").alias("_valid_date_key")
).distinct()

for fact_name, pk, date_columns in fact_keys:

    full_name = "krishiflow.gold." + fact_name

    if not exists(full_name):
        add_check(
            "GOLD", full_name, "FACT_TABLE_EXISTS",
            1, "Expected Gold fact table is missing."
        )
        continue

    df = spark.table(full_name)

    add_check(
        "GOLD", full_name, "PRIMARY_KEY_NOT_NULL",
        df.filter(F.col(pk).isNull()).count(),
        "Fact primary keys must not be null."
    )

    add_check(
        "GOLD", full_name, "PRIMARY_KEY_UNIQUE",
        (
            df.groupBy(pk)
            .count()
            .filter(
                F.col(pk).isNotNull()
                & (F.col("count") > 1)
            )
            .count()
        ),
        "Fact primary keys must be unique."
    )

    for date_col in date_columns:

        # A storage exit date can be null, so its date key,
        # if checked in a future audit, should remain optional.
        invalid_date_rows = (
            df.select(F.col(date_col).alias("_date_key"))
            .filter(F.col("_date_key").isNotNull())
            .join(
                date_keys,
                F.col("_date_key") == F.col("_valid_date_key"),
                "left_anti"
            )
            .count()
        )

        null_date_rows = df.filter(
            F.col(date_col).isNull()
        ).count()

        add_check(
            "GOLD",
            full_name,
            "DATE_KEY_COVERAGE_" + date_col.upper(),
            invalid_date_rows + null_date_rows,
            "Date key must exist in gold.dim_date."
        )


# ============================================================
# STEP 7: Verify the analytics views
# ============================================================

view_names = [
    "vw_crop_performance",
    "vw_storage_performance",
    "vw_transport_performance",
    "vw_market_price_trend",
    "vw_supply_chain_loss"
]

for view_name in view_names:

    full_name = "krishiflow.gold." + view_name

    if not exists(full_name):
        add_check(
            "GOLD", full_name, "VIEW_EXISTS",
            1, "Analytics view is missing."
        )
    else:
        row_count = spark.table(full_name).count()

        add_check(
            "GOLD", full_name, "VIEW_HAS_ROWS",
            0 if row_count > 0 else 1,
            f"View row count: {row_count}",
            severity="MEDIUM"
        )


# ============================================================
# STEP 8: Save the data-quality audit results
# ============================================================

audit_schema = """
    layer STRING,
    object_name STRING,
    check_name STRING,
    severity STRING,
    failed_records LONG,
    status STRING,
    details STRING
"""

audit_df = spark.createDataFrame(
    audit_rows,
    schema=audit_schema
).withColumn(
    "checked_at",
    F.current_timestamp()
)

audit_target = "krishiflow.gold.data_quality_audit"

(
    audit_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(audit_target)
)

print("Data-quality audit saved:", audit_target)

# ============================================================
# STEP 9: Display audit summary
# ============================================================

display(
    spark.sql("""
        SELECT
            layer,
            status,
            COUNT(*) AS checks
        FROM krishiflow.gold.data_quality_audit
        GROUP BY layer, status
        ORDER BY layer, status
    """)
)

# Display failed checks
display(
    spark.sql("""
        SELECT
            layer,
            object_name,
            check_name,
            severity,
            failed_records,
            details
        FROM krishiflow.gold.data_quality_audit
        WHERE status = 'FAIL'
        ORDER BY severity, object_name, check_name
    """)
)

# Display Bronze-to-Silver reconciliation
display(
    spark.table("krishiflow.gold.pipeline_reconciliation")
    .orderBy(F.desc("unexplained_difference"))
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Investigate mandi price records
# MAGIC SELECT
# MAGIC     source_table,
# MAGIC     bronze_records,
# MAGIC     silver_records,
# MAGIC     bronze_minus_silver,
# MAGIC     quarantine_records,
# MAGIC     unexplained_difference,
# MAGIC     reconciliation_status
# MAGIC FROM krishiflow.gold.pipeline_reconciliation
# MAGIC WHERE source_table =
# MAGIC     'krishiflow.bronze.market_price_raw';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     layer,
# MAGIC     object_name,
# MAGIC     check_name,
# MAGIC     severity,
# MAGIC     failed_records,
# MAGIC     details
# MAGIC FROM krishiflow.gold.data_quality_audit
# MAGIC WHERE status = 'FAIL'
# MAGIC ORDER BY object_name, check_name;
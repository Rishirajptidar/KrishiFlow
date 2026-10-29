# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# --------------------------------------------------
# STEP 1: Read Bronze weather data
# --------------------------------------------------

source_table = "krishiflow.bronze.weather_raw"
target_table = "krishiflow.silver.weather"

raw_df = spark.table(source_table)

print("Bronze weather records:", raw_df.count())
print("Source columns:", raw_df.columns)

# Preserve the original record for quarantine
df = raw_df.withColumn(
    "_raw_record",
    F.to_json(
        F.struct(*[F.col(c) for c in raw_df.columns])
    )
)

# --------------------------------------------------
# STEP 2: Clean identifiers
# --------------------------------------------------

for c in ["weather_id", "location_id"]:
    cleaned = F.trim(F.col(c).cast("string"))

    df = df.withColumn(
        c,
        F.when(
            F.upper(cleaned).isin(
                "", "NA", "N/A", "NULL", "NONE", "-"
            ),
            F.lit(None).cast("string")
        ).otherwise(cleaned)
    )

# --------------------------------------------------
# STEP 3: Convert date and numeric columns
# --------------------------------------------------

# Date conversion
original_date = F.col("weather_date").cast("string")

df = df.withColumn(
    "_invalid_weather_date",
    original_date.isNotNull()
    & F.expr("try_cast(`weather_date` AS DATE)").isNull()
)

df = df.withColumn(
    "weather_date",
    F.expr("try_cast(`weather_date` AS DATE)")
)

numeric_columns = [
    "temperature_c",
    "humidity_percentage",
    "rainfall_mm",
    "wind_speed_kmph"
]

for c in numeric_columns:
    original_value = F.col(c).cast("string")
    parsed_value = F.expr(f"try_cast(`{c}` AS DOUBLE)")

    df = df.withColumn(
        "_invalid_" + c,
        original_value.isNotNull() & parsed_value.isNull()
    )

    df = df.withColumn(c, parsed_value)

# --------------------------------------------------
# STEP 4: Identify duplicate weather IDs
# --------------------------------------------------

df = df.withColumn(
    "_weather_id_count",
    F.count(F.lit(1)).over(
        Window.partitionBy("weather_id")
    )
)

# --------------------------------------------------
# STEP 5: Validate location relationships
# --------------------------------------------------

valid_locations = (
    spark.table("krishiflow.silver.location")
    .select("location_id")
    .distinct()
    .withColumn("_location_exists", F.lit(1))
)

df = df.join(
    valid_locations,
    on="location_id",
    how="left"
)

# --------------------------------------------------
# STEP 6: Apply data quality rules
# --------------------------------------------------

failures = [
    F.when(
        F.col("weather_id").isNull(),
        "DQ_REQUIRED_WEATHER_ID"
    ),
    F.when(
        F.col("location_id").isNull(),
        "DQ_REQUIRED_LOCATION_ID"
    ),
    F.when(
        F.col("weather_date").isNull(),
        "DQ_REQUIRED_OR_INVALID_WEATHER_DATE"
    ),
    F.when(
        F.col("temperature_c").isNull(),
        "DQ_REQUIRED_OR_INVALID_TEMPERATURE"
    ),
    F.when(
        F.col("humidity_percentage").isNull(),
        "DQ_REQUIRED_OR_INVALID_HUMIDITY"
    ),
    F.when(
        F.col("rainfall_mm").isNull(),
        "DQ_REQUIRED_OR_INVALID_RAINFALL"
    ),
    F.when(
        F.col("wind_speed_kmph").isNull(),
        "DQ_REQUIRED_OR_INVALID_WIND_SPEED"
    ),
    F.when(
        F.col("_invalid_weather_date"),
        "DQ_INVALID_WEATHER_DATE"
    ),
    F.when(
        F.col("_weather_id_count") > 1,
        "DQ_DUPLICATE_WEATHER_ID"
    ),
    F.when(
        F.col("_location_exists").isNull(),
        "DQ_INVALID_LOCATION_FK"
    ),
    F.when(
        F.col("weather_date") > F.current_date(),
        "DQ_FUTURE_WEATHER_DATE"
    ),
    F.when(
        F.col("humidity_percentage").isNotNull()
        & (
            (F.col("humidity_percentage") < 0)
            | (F.col("humidity_percentage") > 100)
        ),
        "DQ_HUMIDITY_OUT_OF_RANGE"
    ),
    F.when(
        F.col("rainfall_mm").isNotNull()
        & (F.col("rainfall_mm") < 0),
        "DQ_NEGATIVE_RAINFALL"
    ),
    F.when(
        F.col("wind_speed_kmph").isNotNull()
        & (F.col("wind_speed_kmph") < 0),
        "DQ_NEGATIVE_WIND_SPEED"
    ),
    F.when(
        F.col("temperature_c").isNotNull()
        & (
            (F.col("temperature_c") < -80)
            | (F.col("temperature_c") > 70)
        ),
        "DQ_TEMPERATURE_OUT_OF_RANGE"
    )
]

# Include conversion errors explicitly
for c in numeric_columns:
    failures.append(
        F.when(
            F.col("_invalid_" + c),
            "DQ_INVALID_NUMERIC_" + c.upper()
        )
    )

df_flagged = df.withColumn(
    "_failure_reason",
    F.concat_ws(" | ", *failures)
)

valid_df = df_flagged.filter(
    F.col("_failure_reason") == ""
)

invalid_df = df_flagged.filter(
    F.length(F.col("_failure_reason")) > 0
)

# --------------------------------------------------
# STEP 7: Write valid weather observations to Silver
# --------------------------------------------------

output_columns = [
    "weather_id",
    "location_id",
    "weather_date",
    "temperature_c",
    "humidity_percentage",
    "rainfall_mm",
    "wind_speed_kmph"
]

for c in [
    "_ingestion_timestamp",
    "_source",
    "_batch_id",
    "_raw_file_path",
    "_raw_payload_file"
]:
    if c in valid_df.columns:
        output_columns.append(c)

silver_weather = valid_df.select(*output_columns)

(
    silver_weather.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

# --------------------------------------------------
# STEP 8: Merge invalid records into quarantine
# --------------------------------------------------

quarantine_table = "krishiflow.silver.quarantine_records"

quarantine_df = (
    invalid_df
    .withColumn(
        "quarantine_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.lit(source_table),
                F.coalesce(
                    F.col("weather_id"),
                    F.lit("NULL")
                ),
                F.col("_failure_reason"),
                F.coalesce(
                    F.col("_batch_id").cast("string"),
                    F.lit("unknown")
                )
            ),
            256
        )
    )
    .select(
        "quarantine_id",
        F.lit(source_table).alias("source_table"),
        F.col("weather_id").alias("record_id"),
        F.col("_failure_reason").alias("validation_rule"),
        F.col("_failure_reason").alias("failure_reason"),
        F.col("_raw_record").alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
        F.coalesce(
            F.col("_batch_id").cast("string"),
            F.lit("unknown")
        ).alias("batch_id")
    )
    .dropDuplicates(["quarantine_id"])
)

if quarantine_df.limit(1).count() > 0:
    if spark.catalog.tableExists(quarantine_table):
        (
            DeltaTable.forName(spark, quarantine_table)
            .alias("target")
            .merge(
                quarantine_df.alias("source"),
                "target.quarantine_id = source.quarantine_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            quarantine_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(quarantine_table)
        )

    print("Quarantine table updated.")
else:
    print("No invalid weather records found.")

# --------------------------------------------------
# STEP 9: Report results
# --------------------------------------------------

print("Valid weather records:", valid_df.count())
print("Quarantined weather records:", invalid_df.count())

display(
    spark.sql("""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT weather_id) AS unique_weather_ids,
            COUNT(DISTINCT location_id) AS locations,
            MIN(weather_date) AS first_weather_date,
            MAX(weather_date) AS last_weather_date,
            AVG(temperature_c) AS avg_temperature_c,
            SUM(rainfall_mm) AS total_rainfall_mm
        FROM krishiflow.silver.weather
    """)
)
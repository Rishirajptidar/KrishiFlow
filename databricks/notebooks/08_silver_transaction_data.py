# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.sql("CREATE SCHEMA IF NOT EXISTS krishiflow.silver")

# --------------------------------------------------
# Configuration: source, target, types and relationships
# --------------------------------------------------

datasets = [
    {
        "source": "harvest_raw",
        "target": "harvest",
        "pk": "harvest_id",
        "strings": ["harvest_id", "farm_id", "crop_id"],
        "dates": ["harvest_date"],
        "timestamps": [],
        "numbers": {
            "quantity_harvested_kg": "DOUBLE",
            "area_harvested_acres": "DOUBLE"
        },
        "required": [
            "harvest_id", "farm_id", "crop_id",
            "harvest_date", "quantity_harvested_kg",
            "area_harvested_acres"
        ],
        "fks": {
            "farm_id": ("farm", "farm_id"),
            "crop_id": ("crop", "crop_id")
        },
        "positive": [
            "quantity_harvested_kg", "area_harvested_acres"
        ],
        "nonnegative": [],
        "not_future": ["harvest_date"],
        "date_order": []
    },
    {
        "source": "collection_raw",
        "target": "collection",
        "pk": "collection_id",
        "strings": [
            "collection_id", "harvest_id",
            "farm_id", "collection_center_id"
        ],
        "dates": ["collection_date"],
        "timestamps": [],
        "numbers": {"quantity_received_kg": "DOUBLE"},
        "required": [
            "collection_id", "harvest_id", "farm_id",
            "collection_center_id", "collection_date",
            "quantity_received_kg"
        ],
        "fks": {
            "harvest_id": ("harvest", "harvest_id"),
            "farm_id": ("farm", "farm_id"),
            "collection_center_id": (
                "collection_center", "collection_center_id"
            )
        },
        "positive": ["quantity_received_kg"],
        "nonnegative": [],
        "not_future": ["collection_date"],
        "date_order": []
    },
    {
        "source": "storage_raw",
        "target": "storage_transaction",
        "pk": "storage_transaction_id",
        "strings": [
            "storage_transaction_id", "collection_id",
            "storage_facility_id"
        ],
        "dates": ["entry_date", "exit_date"],
        "timestamps": [],
        "numbers": {
            "quantity_in_kg": "DOUBLE",
            "quantity_out_kg": "DOUBLE"
        },
        "required": [
            "storage_transaction_id", "collection_id",
            "storage_facility_id", "entry_date",
            "quantity_in_kg", "quantity_out_kg"
        ],
        "fks": {
            "collection_id": ("collection", "collection_id"),
            "storage_facility_id": (
                "storage_facility", "storage_facility_id"
            )
        },
        "positive": ["quantity_in_kg"],
        "nonnegative": ["quantity_out_kg"],
        "not_future": ["entry_date", "exit_date"],
        "date_order": [("entry_date", "exit_date")]
    },
    {
        "source": "transport_raw",
        "target": "transport",
        "pk": "transport_id",
        "strings": [
            "transport_id", "storage_transaction_id", "crop_id",
            "market_id", "source_location_id",
            "destination_location_id"
        ],
        "dates": [],
        "timestamps": ["dispatch_timestamp", "arrival_timestamp"],
        "numbers": {
            "distance_km": "DOUBLE",
            "quantity_dispatched_kg": "DOUBLE",
            "quantity_received_kg": "DOUBLE"
        },
        "required": [
            "transport_id", "storage_transaction_id", "crop_id",
            "market_id", "source_location_id",
            "destination_location_id", "dispatch_timestamp",
            "arrival_timestamp", "distance_km",
            "quantity_dispatched_kg", "quantity_received_kg"
        ],
        "fks": {
            "storage_transaction_id": (
                "storage_transaction", "storage_transaction_id"
            ),
            "crop_id": ("crop", "crop_id"),
            "market_id": ("market", "market_id"),
            "source_location_id": ("location", "location_id"),
            "destination_location_id": ("location", "location_id")
        },
        "positive": [
            "distance_km", "quantity_dispatched_kg",
            "quantity_received_kg"
        ],
        "nonnegative": [],
        "not_future": ["dispatch_timestamp", "arrival_timestamp"],
        "date_order": [("dispatch_timestamp", "arrival_timestamp")],
        "quantity_pairs": [
            ("quantity_received_kg", "quantity_dispatched_kg")
        ]
    },
    {
        "source": "sale_raw",
        "target": "sale",
        "pk": "sale_id",
        "strings": ["sale_id", "transport_id", "market_id", "crop_id"],
        "dates": ["sale_date"],
        "timestamps": [],
        "numbers": {
            "quantity_sold_kg": "DOUBLE",
            "selling_price_per_kg": "DOUBLE"
        },
        "required": [
            "sale_id", "transport_id", "market_id", "crop_id",
            "sale_date", "quantity_sold_kg",
            "selling_price_per_kg"
        ],
        "fks": {
            "transport_id": ("transport", "transport_id"),
            "market_id": ("market", "market_id"),
            "crop_id": ("crop", "crop_id")
        },
        "positive": ["quantity_sold_kg", "selling_price_per_kg"],
        "nonnegative": [],
        "not_future": ["sale_date"],
        "date_order": []
    }
]

# --------------------------------------------------
# Process one dataset
# --------------------------------------------------

metadata_columns = [
    "_ingestion_timestamp",
    "_source",
    "_batch_id",
    "_raw_file_path"
]

def process_transaction(config):

    source_table = "krishiflow.bronze." + config["source"]
    target_table = "krishiflow.silver." + config["target"]
    pk = config["pk"]

    print("\nProcessing:", source_table)

    raw = spark.table(source_table)

    # Preserve original source data for quarantine
    df = raw.withColumn(
        "_raw_record",
        F.to_json(F.struct(*[F.col(c) for c in raw.columns]))
    )

    source_count = raw.count()
    parse_checks = []

    # Clean identifiers and other text fields
    for c in config["strings"]:
        cleaned = F.trim(F.col(c).cast("string"))
        df = df.withColumn(
            c,
            F.when(
                F.upper(cleaned).isin("", "NA", "N/A", "NULL", "NONE", "-"),
                F.lit(None).cast("string")
            ).otherwise(cleaned)
        )

    # Convert dates and detect invalid date values
    for c in config["dates"]:
        original = F.col(c).cast("string")
        parsed = F.expr(f"try_cast(`{c}` AS DATE)")

        flag = "__invalid_" + c
        df = df.withColumn(
            flag, original.isNotNull() & parsed.isNull()
        )
        df = df.withColumn(c, parsed)
        parse_checks.append(
            F.when(F.col(flag), F.lit("DQ_INVALID_DATE_" + c.upper()))
        )

    # Convert timestamps and detect invalid timestamp values
    for c in config["timestamps"]:
        original = F.col(c).cast("string")
        parsed = F.expr(f"try_cast(`{c}` AS TIMESTAMP)")

        flag = "__invalid_" + c
        df = df.withColumn(
            flag, original.isNotNull() & parsed.isNull()
        )
        df = df.withColumn(c, parsed)
        parse_checks.append(
            F.when(
                F.col(flag),
                F.lit("DQ_INVALID_TIMESTAMP_" + c.upper())
            )
        )

    # Convert numeric values safely
    for c, sql_type in config["numbers"].items():
        original = F.col(c).cast("string")
        parsed = F.expr(f"try_cast(`{c}` AS {sql_type})")

        flag = "__invalid_" + c
        df = df.withColumn(
            flag, original.isNotNull() & parsed.isNull()
        )
        df = df.withColumn(c, parsed)
        parse_checks.append(
            F.when(
                F.col(flag),
                F.lit("DQ_INVALID_NUMBER_" + c.upper())
            )
        )

    failures = list(parse_checks)

    # Required fields
    for c in config["required"]:
        failures.append(
            F.when(
                F.col(c).isNull(),
                F.lit("DQ_REQUIRED_" + c.upper())
            )
        )

    # Primary-key uniqueness
    df = df.withColumn(
        "__pk_count",
        F.count(F.lit(1)).over(Window.partitionBy(pk))
    )
    failures.append(
        F.when(
            F.col(pk).isNotNull() & (F.col("__pk_count") > 1),
            F.lit("DQ_DUPLICATE_PRIMARY_KEY")
        )
    )

    # Foreign keys: left join against unique valid Silver keys
    for fk, (parent_name, parent_key) in config["fks"].items():

        helper = "__valid_" + fk

        parent_keys = (
            spark.table("krishiflow.silver." + parent_name)
            .select(
                F.col(parent_key).cast("string").alias(helper)
            )
            .filter(F.col(parent_key).isNotNull())
            .distinct()
        )

        df = df.join(
            parent_keys,
            df[fk].cast("string") == parent_keys[helper],
            "left"
        )

        failures.append(
            F.when(
                F.col(helper).isNull(),
                F.lit("DQ_INVALID_FK_" + fk.upper())
            )
        )

    # Numeric rules
    for c in config["positive"]:
        failures.append(
            F.when(
                F.col(c).isNotNull() & (F.col(c) <= 0),
                F.lit("DQ_NON_POSITIVE_" + c.upper())
            )
        )

    for c in config["nonnegative"]:
        failures.append(
            F.when(
                F.col(c).isNotNull() & (F.col(c) < 0),
                F.lit("DQ_NEGATIVE_" + c.upper())
            )
        )

    # Date/timestamp ordering
    for earlier, later in config["date_order"]:
        failures.append(
            F.when(
                F.col(earlier).isNotNull()
                & F.col(later).isNotNull()
                & (F.col(later) < F.col(earlier)),
                F.lit("DQ_INVALID_DATE_ORDER_" + earlier.upper())
            )
        )

    # Future dates/timestamps
    for c in config["not_future"]:
        failures.append(
            F.when(
                F.col(c).isNotNull()
                & (F.col(c) > F.current_timestamp()),
                F.lit("DQ_FUTURE_DATE_" + c.upper())
            )
        )

    # Business quantity comparisons
    for received, dispatched in config.get("quantity_pairs", []):
        failures.append(
            F.when(
                F.col(received).isNotNull()
                & F.col(dispatched).isNotNull()
                & (F.col(received) > F.col(dispatched)),
                F.lit("DQ_RECEIVED_EXCEEDS_DISPATCHED")
            )
        )

    df_flagged = df.withColumn(
        "_failure_reason",
        F.concat_ws(" | ", *failures)
    )

    valid = df_flagged.filter(F.col("_failure_reason") == "")
    invalid = df_flagged.filter(F.length("_failure_reason") > 0)

    # Add derived sales revenue
    if config["target"] == "sale":
        valid = valid.withColumn(
            "revenue",
            F.round(
                F.col("quantity_sold_kg") *
                F.col("selling_price_per_kg"),
                2
            )
        )

    business_columns = (
        config["strings"]
        + config["dates"]
        + config["timestamps"]
        + list(config["numbers"].keys())
    )

    if config["target"] == "sale":
        business_columns.append("revenue")

    output_columns = business_columns + [
        c for c in metadata_columns if c in valid.columns
    ]

    silver_df = valid.select(*output_columns)

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    # Standardized quarantine schema
    quarantine_df = invalid.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit(source_table),
                F.coalesce(F.col(pk).cast("string"), F.lit("NULL")),
                F.col("_failure_reason"),
                F.coalesce(F.col("_batch_id"), F.lit("unknown"))
            ),
            256
        ).alias("quarantine_id"),
        F.lit(source_table).alias("source_table"),
        F.col(pk).cast("string").alias("record_id"),
        F.col("_failure_reason").alias("validation_rule"),
        F.col("_failure_reason").alias("failure_reason"),
        F.col("_raw_record").alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
        F.coalesce(F.col("_batch_id"), F.lit("unknown")).alias("batch_id")
    ).dropDuplicates(["quarantine_id"])

    print("Source records:", source_count)
    print("Valid Silver records:", silver_df.count())
    print("Quarantined records:", invalid.count())
    print("Created:", target_table)

    return quarantine_df, (
        config["target"],
        source_count,
        silver_df.count(),
        invalid.count()
    )

# --------------------------------------------------
# Run in dependency order
# --------------------------------------------------

quarantine_frames = []
summary_rows = []

for config in datasets:
    quarantine_df, summary_row = process_transaction(config)
    quarantine_frames.append(quarantine_df)
    summary_rows.append(summary_row)

# --------------------------------------------------
# Merge into shared quarantine table
# --------------------------------------------------

quarantine_table = "krishiflow.silver.quarantine_records"

combined_quarantine = quarantine_frames[0]

for qdf in quarantine_frames[1:]:
    combined_quarantine = combined_quarantine.unionByName(qdf)

combined_quarantine = combined_quarantine.dropDuplicates(
    ["quarantine_id"]
)

if combined_quarantine.limit(1).count() > 0:
    if spark.catalog.tableExists(quarantine_table):
        (
            DeltaTable.forName(spark, quarantine_table)
            .alias("target")
            .merge(
                combined_quarantine.alias("source"),
                "target.quarantine_id = source.quarantine_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            combined_quarantine.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(quarantine_table)
        )

    print("Quarantine updated:", quarantine_table)
else:
    print("No invalid transaction records found.")

summary_df = spark.createDataFrame(
    summary_rows,
    [
        "silver_table",
        "source_records",
        "valid_records",
        "quarantined_records"
    ]
)

display(summary_df)
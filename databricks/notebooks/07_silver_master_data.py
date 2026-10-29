# Databricks notebook source
# clean the master data
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# --------------------------------------------------
# STEP 1: Configuration
# --------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS krishiflow.silver")

null_values = ["", "NA", "N/A", "NULL", "NONE", "-"]

metadata_columns = [
    "_ingestion_timestamp",
    "_source",
    "_batch_id",
    "_raw_file_path"
]

datasets = [
    {
        "source": "krishiflow.bronze.location_raw",
        "target": "krishiflow.silver.location",
        "pk": "location_id",
        "strings": [
            "location_id", "state", "district", "city"
        ],
        "numbers": {
            "latitude": "DOUBLE",
            "longitude": "DOUBLE"
        },
        "required": [
            "location_id", "state", "district", "city",
            "latitude", "longitude"
        ],
        "foreign_keys": {},
        "positive": [],
        "nonnegative": [],
        "bounds": {
            "latitude": (-90, 90),
            "longitude": (-180, 180)
        },
        "business_columns": [
            "location_id", "state", "district", "city",
            "latitude", "longitude"
        ]
    },
    {
        "source": "krishiflow.bronze.crop_raw",
        "target": "krishiflow.silver.crop",
        "pk": "crop_id",
        "strings": [
            "crop_id", "crop_name", "crop_category",
            "unit_of_measure"
        ],
        "numbers": {},
        "required": [
            "crop_id", "crop_name", "crop_category",
            "unit_of_measure"
        ],
        "foreign_keys": {},
        "positive": [],
        "nonnegative": [],
        "bounds": {},
        "business_columns": [
            "crop_id", "crop_name", "crop_category",
            "unit_of_measure"
        ]
    },
    {
        "source": "krishiflow.bronze.farmer_raw",
        "target": "krishiflow.silver.farmer",
        "pk": "farmer_id",
        "strings": [
            "farmer_id", "farmer_name", "location_id"
        ],
        "numbers": {
            "farm_count": "INT"
        },
        "required": [
            "farmer_id", "farmer_name", "location_id",
            "farm_count"
        ],
        "foreign_keys": {
            "location_id": "krishiflow.silver.location"
        },
        "positive": [],
        "nonnegative": ["farm_count"],
        "bounds": {},
        "business_columns": [
            "farmer_id", "farmer_name", "location_id",
            "farm_count"
        ]
    },
    {
        "source": "krishiflow.bronze.farm_raw",
        "target": "krishiflow.silver.farm",
        "pk": "farm_id",
        "strings": [
            "farm_id", "farmer_id", "location_id", "farm_type"
        ],
        "numbers": {
            "area_acres": "DOUBLE"
        },
        "required": [
            "farm_id", "farmer_id", "location_id",
            "area_acres", "farm_type"
        ],
        "foreign_keys": {
            "farmer_id": "krishiflow.silver.farmer",
            "location_id": "krishiflow.silver.location"
        },
        "positive": ["area_acres"],
        "nonnegative": [],
        "bounds": {},
        "business_columns": [
            "farm_id", "farmer_id", "location_id",
            "area_acres", "farm_type"
        ]
    },
    {
        "source": "krishiflow.bronze.collection_center_raw",
        "target": "krishiflow.silver.collection_center",
        "pk": "collection_center_id",
        "strings": [
            "collection_center_id", "center_name", "location_id"
        ],
        "numbers": {
            "capacity_kg": "DOUBLE"
        },
        "required": [
            "collection_center_id", "center_name",
            "location_id", "capacity_kg"
        ],
        "foreign_keys": {
            "location_id": "krishiflow.silver.location"
        },
        "positive": ["capacity_kg"],
        "nonnegative": [],
        "bounds": {},
        "business_columns": [
            "collection_center_id", "center_name",
            "location_id", "capacity_kg"
        ]
    },
    {
        "source": "krishiflow.bronze.storage_facility_raw",
        "target": "krishiflow.silver.storage_facility",
        "pk": "storage_facility_id",
        "strings": [
            "storage_facility_id", "storage_name",
            "location_id", "storage_type"
        ],
        "numbers": {
            "capacity_kg": "DOUBLE"
        },
        "required": [
            "storage_facility_id", "storage_name",
            "location_id", "storage_type", "capacity_kg"
        ],
        "foreign_keys": {
            "location_id": "krishiflow.silver.location"
        },
        "positive": ["capacity_kg"],
        "nonnegative": [],
        "bounds": {},
        "business_columns": [
            "storage_facility_id", "storage_name",
            "location_id", "storage_type", "capacity_kg"
        ]
    },
    {
        "source": "krishiflow.bronze.market_raw",
        "target": "krishiflow.silver.market",
        "pk": "market_id",
        "strings": [
            "market_id", "market_name", "location_id", "market_type"
        ],
        "numbers": {},
        "required": [
            "market_id", "market_name", "location_id", "market_type"
        ],
        "foreign_keys": {
            "location_id": "krishiflow.silver.location"
        },
        "positive": [],
        "nonnegative": [],
        "bounds": {},
        "business_columns": [
            "market_id", "market_name", "location_id", "market_type"
        ]
    }
]

# --------------------------------------------------
# STEP 2: Helper to clean and validate a dataset
# --------------------------------------------------

def process_master_dataset(config):

    source_table = config["source"]
    target_table = config["target"]
    pk = config["pk"]

    print("\nProcessing:", source_table)

    raw = spark.table(source_table)

    # Preserve the original row for quarantine
    df = raw.withColumn(
        "_raw_record",
        F.to_json(
            F.struct(*[F.col(c) for c in raw.columns])
        )
    )

    source_count = raw.count()

    # Trim strings and normalize empty values to NULL
    for c in config["strings"]:

        trimmed = F.trim(F.col(c).cast("string"))

        df = df.withColumn(
            c,
            F.when(
                F.upper(trimmed).isin(*null_values),
                F.lit(None).cast("string")
            ).otherwise(trimmed)
        )

    # Convert numeric fields safely
    for c, sql_type in config["numbers"].items():

        df = df.withColumn(
            c,
            F.expr(
                f"try_cast(`{c}` AS {sql_type})"
            )
        )

    # Count occurrences of each primary key
    df = df.withColumn(
        "_pk_count",
        F.count(F.lit(1)).over(Window.partitionBy(pk))
    )

    failures = []

    # Required-field checks
    for c in config["required"]:
        failures.append(
            F.when(
                F.col(c).isNull(),
                F.lit(f"DQ_REQUIRED_{c.upper()}")
            )
        )

    # Duplicate primary-key check
    failures.append(
        F.when(
            F.col(pk).isNotNull()
            & (F.col("_pk_count") > 1),
            F.lit("DQ_DUPLICATE_PRIMARY_KEY")
        )
    )

    # Foreign-key checks against already-cleaned Silver tables
    for fk, parent_table in config["foreign_keys"].items():

        parent_ids = [
            row[0]
            for row in (
                spark.table(parent_table)
                .select(F.col(fk).cast("string"))
                .distinct()
                .collect()
            )
            if row[0] is not None
        ]

        if parent_ids:
            invalid_fk = (
                F.col(fk).isNull()
                | ~F.col(fk).cast("string").isin(*parent_ids)
            )
        else:
            invalid_fk = F.lit(True)

        failures.append(
            F.when(
                invalid_fk,
                F.lit(f"DQ_INVALID_FK_{fk.upper()}")
            )
        )

    # Strictly positive values
    for c in config["positive"]:
        failures.append(
            F.when(
                F.col(c).isNotNull() & (F.col(c) <= 0),
                F.lit(f"DQ_NON_POSITIVE_{c.upper()}")
            )
        )

    # Nonnegative values
    for c in config["nonnegative"]:
        failures.append(
            F.when(
                F.col(c).isNotNull() & (F.col(c) < 0),
                F.lit(f"DQ_NEGATIVE_{c.upper()}")
            )
        )

    # Allowed numeric ranges
    for c, (lower, upper) in config["bounds"].items():
        failures.append(
            F.when(
                F.col(c).isNotNull()
                & (
                    (F.col(c) < lower)
                    | (F.col(c) > upper)
                ),
                F.lit(f"DQ_OUT_OF_RANGE_{c.upper()}")
            )
        )

    df = df.withColumn(
        "_failure_reason",
        F.concat_ws(" | ", *failures)
    )

    good = df.filter(F.col("_failure_reason") == "")
    bad = df.filter(F.length("_failure_reason") > 0)

    # Keep business fields and useful ingestion metadata
    output_columns = config["business_columns"] + [
        c for c in metadata_columns if c in df.columns
    ]

    silver_df = good.select(*output_columns)

    # Write this dataset's Silver table
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    valid_count = silver_df.count()
    bad_count = bad.count()

    # Prepare records for the shared quarantine table
    quarantine_df = bad.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit(source_table),
                F.coalesce(F.col(pk).cast("string"), F.lit("NULL")),
                F.col("_failure_reason"),
                F.coalesce(
                    F.col("_batch_id").cast("string"),
                    F.lit("unknown")
                )
            ),
            256
        ).alias("quarantine_id"),

        F.lit(source_table).alias("source_table"),
        F.col(pk).cast("string").alias("record_id"),
        F.col("_failure_reason").alias("validation_rule"),
        F.col("_failure_reason").alias("failure_reason"),
        F.col("_raw_record").alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
        F.coalesce(
            F.col("_batch_id").cast("string"),
            F.lit("unknown")
        ).alias("batch_id")
    )

    print("Source records:", source_count)
    print("Valid Silver records:", valid_count)
    print("Quarantined records:", bad_count)
    print("Created:", target_table)

    return quarantine_df, (
        source_table,
        target_table,
        source_count,
        valid_count,
        bad_count
    )


# --------------------------------------------------
# STEP 3: Process dimensions in dependency order
# Location and crop first, then dependent datasets
# --------------------------------------------------

quarantine_dfs = []
summary_rows = []

for config in datasets:

    quarantine_df, summary_row = process_master_dataset(config)

    quarantine_dfs.append(quarantine_df)
    summary_rows.append(summary_row)

# --------------------------------------------------
# STEP 4: Merge quarantine records safely
# Preserve existing market-price quarantine records
# --------------------------------------------------

quarantine_table = "krishiflow.silver.quarantine_records"

combined_quarantine = quarantine_dfs[0]

for qdf in quarantine_dfs[1:]:
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

    print("Quarantine table updated:", quarantine_table)

else:
    print("No invalid master records found.")

# --------------------------------------------------
# STEP 5: Display the result summary
# --------------------------------------------------

summary_df = spark.createDataFrame(
    summary_rows,
    [
        "source_table",
        "silver_table",
        "source_records",
        "valid_records",
        "quarantined_records"
    ]
)

display(summary_df)
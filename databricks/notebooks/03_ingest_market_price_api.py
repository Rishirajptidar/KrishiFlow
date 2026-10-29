# Databricks notebook source
# Step 1: Set the exact CSV path

csv_path = "/Volumes/krishiflow/source/source_data/market_price/9ef84268-d588-465a-a308-a864a43d0070.csv"

# Verify that the file exists
display(dbutils.fs.ls(
    "/Volumes/krishiflow/source/source_data/market_price/"
))

print("Input file:", csv_path)

# COMMAND ----------

from pyspark.sql import functions as F
import re
from datetime import datetime

# --------------------------------------------------
# STEP 1: Set the exact CSV file path
# --------------------------------------------------

csv_path = "/Volumes/krishiflow/source/source_data/market_price/9ef84268-d588-465a-a308-a864a43d0070.csv"

# Verify that the uploaded file exists
display(
    dbutils.fs.ls(
        "/Volumes/krishiflow/source/source_data/market_price/"
    )
)

print("Reading file:", csv_path)

# --------------------------------------------------
# STEP 2: Read the CSV file
# --------------------------------------------------

df_source = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .load(csv_path)
)

print("Original columns:", df_source.columns)

# --------------------------------------------------
# STEP 3: Standardize column names
# --------------------------------------------------

def normalize_column(name):
    name = name.replace("\ufeff", "")
    name = re.sub(
        r"_x0020_",
        "_",
        name,
        flags=re.IGNORECASE
    )
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    return name.strip("_").lower()


# Map normalized names to their original CSV column names
column_map = {}

for original_name in df_source.columns:
    normalized_name = normalize_column(original_name)

    if normalized_name and normalized_name not in column_map:
        column_map[normalized_name] = original_name


# Expected business columns
expected_columns = [
    "state",
    "district",
    "market",
    "commodity",
    "variety",
    "grade",
    "arrival_date",
    "min_price",
    "max_price",
    "modal_price"
]

# Check whether all required columns are present
missing_columns = [
    col for col in expected_columns
    if col not in column_map
]

if missing_columns:
    raise Exception(
        f"Missing columns: {missing_columns}\n"
        f"Columns found: {list(column_map.keys())}"
    )


# Select only the expected columns.
# This automatically ignores the extra empty column.
def quoted_column(name):
    return F.col("`" + name.replace("`", "``") + "`")


df_market = df_source.select(
    *[
        quoted_column(column_map[col])
        .cast("string")
        .alias(col)
        for col in expected_columns
    ]
)

print("Standardized columns:", df_market.columns)

# --------------------------------------------------
# STEP 4: Add ingestion metadata
# --------------------------------------------------

batch_id = datetime.now().strftime(
    "mandi_csv_%Y%m%d_%H%M%S"
)

df_bronze = (
    df_market
    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "_source",
        F.lit("data.gov.in_official_csv")
    )
    .withColumn(
        "_batch_id",
        F.lit(batch_id)
    )
    .withColumn(
        "_raw_file_path",
        F.lit(csv_path)
    )
)

# --------------------------------------------------
# STEP 5: Check the record count
# --------------------------------------------------

total_records = df_bronze.count()

if total_records == 0:
    raise Exception("The CSV contains no data records.")

print("Batch ID:", batch_id)
print("Total records:", total_records)

# --------------------------------------------------
# STEP 6: Create the Bronze Delta table
# --------------------------------------------------

spark.sql(
    "CREATE SCHEMA IF NOT EXISTS krishiflow.bronze"
)

target_table = "krishiflow.bronze.market_price_raw"

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print("SUCCESS! Bronze table created:", target_table)

# --------------------------------------------------
# STEP 7: Preview the ingested data
# --------------------------------------------------

display(
    spark.table(target_table).limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F
import re
from datetime import datetime

csv_path = "/Volumes/krishiflow/source/source_data/market_price/9ef84268-d588-465a-a308-a864a43d0070.csv"

print("File path:", csv_path)
print("File exists:", len(dbutils.fs.ls(csv_path)) > 0)

# COMMAND ----------

# read and inspect the csv
df_source = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .load(csv_path)
)

print("Columns found:")
print(df_source.columns)

print("Total records:", df_source.count())

display(df_source.limit(10))

# COMMAND ----------

# Standardize columns and create bronze

from pyspark.sql import functions as F
from datetime import datetime

# 1. Standardize column names and retain source values as strings
df_market = df_source.select(
    F.col("State").cast("string").alias("state"),
    F.col("District").cast("string").alias("district"),
    F.col("Market").cast("string").alias("market"),
    F.col("Commodity").cast("string").alias("commodity"),
    F.col("Variety").cast("string").alias("variety"),
    F.col("Grade").cast("string").alias("grade"),
    F.col("Arrival_Date").cast("string").alias("arrival_date"),
    F.col("Min_x0020_Price").cast("string").alias("min_price"),
    F.col("Max_x0020_Price").cast("string").alias("max_price"),
    F.col("Modal_x0020_Price").cast("string").alias("modal_price")
)

# 2. Add ingestion metadata
batch_id = datetime.now().strftime("mandi_csv_%Y%m%d_%H%M%S")

df_bronze = (
    df_market
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source", F.lit("data.gov.in_official_csv"))
    .withColumn("_batch_id", F.lit(batch_id))
    .withColumn("_raw_file_path", F.lit(csv_path))
)

# 3. Check the record count
total_records = df_bronze.count()
print("Records to ingest:", total_records)
print("Batch ID:", batch_id)

if total_records == 0:
    raise Exception("No records found. Bronze table was not written.")

# 4. Ensure the Bronze schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS krishiflow.bronze")

# 5. Write the Bronze Delta table
target_table = "krishiflow.bronze.market_price_raw"

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print("SUCCESS: Bronze table created:", target_table)

# 6. Preview the ingested records
display(spark.table(target_table).limit(10))

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT state) AS states,
    COUNT(DISTINCT commodity) AS commodities,
    COUNT(DISTINCT market) AS markets
FROM krishiflow.bronze.market_price_raw
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Layer: Clean, Validate, and Transform Market Prices.

# COMMAND ----------

# prepaere and validate data
from pyspark.sql import functions as F

# 1. Read the Bronze table
raw_df = spark.table("krishiflow.bronze.market_price_raw")

# Preserve the original Bronze row for quarantine and debugging
df = raw_df.withColumn(
    "_raw_record",
    F.to_json(F.struct(*[F.col(c).alias(c) for c in raw_df.columns]))
)

# 2. Clean text columns
text_columns = [
    "state", "district", "market", "commodity",
    "variety", "grade", "arrival_date",
    "min_price", "max_price", "modal_price"
]

null_values = ["", "NA", "N/A", "NULL", "NONE", "-"]

for c in text_columns:
    trimmed = F.trim(F.col(c).cast("string"))

    df = df.withColumn(
        c,
        F.when(
            F.upper(trimmed).isin(*null_values),
            F.lit(None).cast("string")
        ).otherwise(trimmed)
    )

# 3. Parse the arrival date using common date formats
df = df.withColumn("_arrival_date_text", F.col("arrival_date"))

date_formats = [
    "dd/MM/yyyy",
    "d/M/yyyy",
    "yyyy-MM-dd",
    "dd-MM-yyyy",
    "d-M-yyyy",
    "yyyy/MM/dd",
    "M/d/yyyy"
]

date_candidates = [
    F.expr(f"try_to_timestamp(_arrival_date_text, '{fmt}')")
    for fmt in date_formats
]

df = df.withColumn(
    "arrival_date",
    F.to_date(F.coalesce(*date_candidates))
)

# 4. Clean and convert prices into decimal numbers
for c in ["min_price", "max_price", "modal_price"]:
    clean_col = "_" + c + "_clean"

    df = df.withColumn(
        clean_col,
        F.regexp_replace(F.col(c), r"[,₹\s]", "")
    )

    df = df.withColumn(
        c,
        F.expr(f"try_cast({clean_col} AS DECIMAL(18,2))")
    )

# 5. Generate a stable ID from the business columns
business_columns = [
    "state", "district", "market", "commodity",
    "variety", "grade", "arrival_date",
    "min_price", "max_price", "modal_price"
]

id_parts = [
    F.coalesce(F.col(c).cast("string"), F.lit("<NULL>"))
    for c in business_columns
]

df = df.withColumn(
    "market_price_id",
    F.sha2(F.concat_ws("||", *id_parts), 256)
)

# 6. Remove exact duplicate business records
records_before = df.count()

df = df.dropDuplicates(["market_price_id"])

records_after = df.count()

print("Bronze records:", records_before)
print("Records after deduplication:", records_after)
print("Exact duplicates removed:", records_before - records_after)

# 7. Define data quality checks
failure_reasons = [
    F.when(F.col("state").isNull(), "DQ_REQUIRED_STATE"),
    F.when(F.col("district").isNull(), "DQ_REQUIRED_DISTRICT"),
    F.when(F.col("market").isNull(), "DQ_REQUIRED_MARKET"),
    F.when(F.col("commodity").isNull(), "DQ_REQUIRED_COMMODITY"),

    F.when(F.col("arrival_date").isNull(), "DQ_INVALID_ARRIVAL_DATE"),
    F.when(
        F.col("arrival_date") > F.current_date(),
        "DQ_FUTURE_ARRIVAL_DATE"
    ),

    F.when(F.col("min_price").isNull(), "DQ_INVALID_MIN_PRICE"),
    F.when(F.col("max_price").isNull(), "DQ_INVALID_MAX_PRICE"),
    F.when(F.col("modal_price").isNull(), "DQ_INVALID_MODAL_PRICE"),

    F.when(F.col("min_price") < 0, "DQ_NEGATIVE_MIN_PRICE"),
    F.when(F.col("max_price") < 0, "DQ_NEGATIVE_MAX_PRICE"),
    F.when(F.col("modal_price") < 0, "DQ_NEGATIVE_MODAL_PRICE"),

    F.when(
        F.col("min_price").isNotNull()
        & F.col("max_price").isNotNull()
        & (F.col("max_price") < F.col("min_price")),
        "DQ_MAX_PRICE_BELOW_MIN_PRICE"
    )
]

df_flagged = df.withColumn(
    "_failure_reason",
    F.concat_ws(" | ", *failure_reasons)
)

# 8. Display validation results
valid_count = df_flagged.filter(
    F.col("_failure_reason") == ""
).count()

quarantine_count = df_flagged.filter(
    F.length(F.col("_failure_reason")) > 0
).count()

print("Valid records:", valid_count)
print("Records requiring quarantine:", quarantine_count)

display(
    df_flagged
    .groupBy("_failure_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# save the silver and quarantine tables
from delta.tables import DeltaTable

# 1. Create the Silver schema if needed
spark.sql("CREATE SCHEMA IF NOT EXISTS krishiflow.silver")

# 2. Select valid, cleaned records
silver_columns = [
    "market_price_id",
    "state",
    "district",
    "market",
    "commodity",
    "variety",
    "grade",
    "arrival_date",
    "min_price",
    "max_price",
    "modal_price",
    "_ingestion_timestamp",
    "_source",
    "_batch_id",
    "_raw_file_path"
]

silver_df = (
    df_flagged
    .filter(F.col("_failure_reason") == "")
    .select(*silver_columns)
)

# 3. Write the Silver table
silver_target = "krishiflow.silver.market_price"

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_target)
)

print("Silver table created:", silver_target)

# 4. Prepare invalid records for quarantine
quarantine_target = "krishiflow.silver.quarantine_records"

quarantine_df = (
    df_flagged
    .filter(F.length(F.col("_failure_reason")) > 0)
    .withColumn(
        "quarantine_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("market_price_id"),
                F.col("_failure_reason"),
                F.col("_batch_id")
            ),
            256
        )
    )
    .select(
        "quarantine_id",
        F.lit("krishiflow.bronze.market_price_raw").alias("source_table"),
        F.col("market_price_id").alias("record_id"),
        F.col("_failure_reason").alias("validation_rule"),
        F.col("_failure_reason").alias("failure_reason"),
        F.col("_raw_record").alias("raw_record"),
        F.current_timestamp().alias("detected_at"),
        F.col("_batch_id").alias("batch_id")
    )
)

# Merge quarantine records without adding the same record twice
if spark.catalog.tableExists(quarantine_target):
    (
        DeltaTable.forName(spark, quarantine_target)
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
        .saveAsTable(quarantine_target)
    )

print("Quarantine table updated:", quarantine_target)

# 5. Verify the Silver result
display(
    spark.sql("""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT market_price_id) AS unique_records,
            MIN(arrival_date) AS earliest_arrival_date,
            MAX(arrival_date) AS latest_arrival_date,
            MIN(min_price) AS lowest_min_price,
            MAX(max_price) AS highest_max_price
        FROM krishiflow.silver.market_price
    """)
)

# Preview the cleaned records
display(spark.table(silver_target).limit(10))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
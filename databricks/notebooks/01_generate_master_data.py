# Databricks notebook source
spark.range(5).show()

# COMMAND ----------

# we will create 20 locations first

from pyspark.sql import Row

locations = [
    Row("LOC001", "Madhya Pradesh", "Indore", "Indore", 22.7196, 75.8577),
    Row("LOC002", "Madhya Pradesh", "Ujjain", "Ujjain", 23.1765, 75.7885),
    Row("LOC003", "Madhya Pradesh", "Dewas", "Dewas", 22.9676, 76.0534),
    Row("LOC004", "Madhya Pradesh", "Shajapur", "Shajapur", 23.4264, 76.2770),
    Row("LOC005", "Madhya Pradesh", "Dhar", "Dhar", 22.5937, 75.2977),
    Row("LOC006", "Madhya Pradesh", "Mandsaur", "Mandsaur", 24.0718, 75.0699),
    Row("LOC007", "Madhya Pradesh", "Ratlam", "Ratlam", 23.3315, 75.0367),
    Row("LOC008", "Madhya Pradesh", "Neemuch", "Neemuch", 24.4764, 74.8624),
    Row("LOC009", "Madhya Pradesh", "Khandwa", "Khandwa", 21.8240, 76.3500),
    Row("LOC010", "Madhya Pradesh", "Khargone", "Khargone", 21.8238, 75.6140),
    Row("LOC011", "Madhya Pradesh", "Bhopal", "Bhopal", 23.2599, 77.4126),
    Row("LOC012", "Madhya Pradesh", "Sehore", "Sehore", 23.2032, 77.0844),
    Row("LOC013", "Madhya Pradesh", "Vidisha", "Vidisha", 23.5251, 77.8081),
    Row("LOC014", "Maharashtra", "Nagpur", "Nagpur", 21.1458, 79.0882),
    Row("LOC015", "Rajasthan", "Kota", "Kota", 25.2138, 75.8648),
    Row("LOC016", "Gujarat", "Ahmedabad", "Ahmedabad", 23.0225, 72.5714),
    Row("LOC017", "Gujarat", "Vadodara", "Vadodara", 22.3072, 73.1812),
    Row("LOC018", "Maharashtra", "Nashik", "Nashik", 19.9975, 73.7898),
    Row("LOC019", "Uttar Pradesh", "Agra", "Agra", 27.1767, 78.0081),
    Row("LOC020", "Delhi", "Delhi", "Delhi", 28.6139, 77.2090)
]

columns = [
    "location_id",
    "state",
    "district",
    "city",
    "latitude",
    "longitude"
]

location_df = spark.createDataFrame(locations, columns)

display(location_df)

# COMMAND ----------

# let's verify that every location ID is unique.

location_id_count = location_df.select("location_id").distinct().count()

total_count = location_df.count()

print(f"Total records      : {total_count}")
print(f"Unique location IDs: {location_id_count}")

# COMMAND ----------

# write data to our source volume

location_path = "/Volumes/krishiflow/source/source_data/location"

location_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(location_path)

# COMMAND ----------

# verify the files

display(dbutils.fs.ls(location_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### crop data

# COMMAND ----------

# create the master crop data

from pyspark.sql import Row

crops = [
    Row("C001", "Wheat", "Cereal", "KG"),
    Row("C002", "Soybean", "Oilseed", "KG"),
    Row("C003", "Maize", "Cereal", "KG"),
    Row("C004", "Potato", "Vegetable", "KG"),
    Row("C005", "Onion", "Vegetable", "KG"),
    Row("C006", "Tomato", "Vegetable", "KG"),
    Row("C007", "Cotton", "Fiber", "KG"),
    Row("C008", "Gram", "Pulse", "KG"),
    Row("C009", "Chilli", "Spice", "KG"),
    Row("C010", "Garlic", "Spice", "KG"),
    Row("C011", "Mustard", "Oilseed", "KG"),
    Row("C012", "Peas", "Vegetable", "KG"),
    Row("C013", "Groundnut", "Oilseed", "KG"),
    Row("C014", "Lentil", "Pulse", "KG"),
    Row("C015", "Cabbage", "Vegetable", "KG")
]

crop_columns = [
    "crop_id",
    "crop_name",
    "crop_category",
    "unit_of_measure"
]

crop_df = spark.createDataFrame(crops, crop_columns)

display(crop_df)

# COMMAND ----------

# validate the crop IDs

total_crops = crop_df.count()
unique_crop_ids = crop_df.select("crop_id").distinct().count()

print(f"Total crops      : {total_crops}")
print(f"Unique crop IDs  : {unique_crop_ids}")

# COMMAND ----------

# now check for missing values
crop_df.select(
    "crop_id",
    "crop_name",
    "crop_category",
    "unit_of_measure"
).summary("count").show()

# COMMAND ----------

# write to the volume

crop_path = "/Volumes/krishiflow/source/source_data/crop"

crop_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(crop_path)

# COMMAND ----------

# verify the write

display(dbutils.fs.ls(crop_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Farmer Master
# MAGIC
# MAGIC Now we'll generate 500 synthetic farmers automatically instead of manually entering them.

# COMMAND ----------

from pyspark.sql import Row
import random

# Fixed seed = reproducible data
random.seed(42)

NUM_FARMERS = 500

farmer_rows = []

for i in range(1, NUM_FARMERS + 1):

    farmer_id = f"FR{i:06d}"

    # Pick an existing location
    location_id = f"LOC{random.randint(1, 20):03d}"

    farmer_name = f"Farmer_{i:04d}"

    # Number of farms owned by the farmer
    farm_count = random.randint(1, 3)

    farmer_rows.append(
        Row(
            farmer_id,
            farmer_name,
            location_id,
            farm_count
        )
    )

farmer_columns = [
    "farmer_id",
    "farmer_name",
    "location_id",
    "farm_count"
]

farmer_df = spark.createDataFrame(
    farmer_rows,
    farmer_columns
)

display(farmer_df)

# COMMAND ----------

# validate the farmer data

print("Total farmers:", farmer_df.count())

print(
    "Unique farmer IDs:",
    farmer_df.select("farmer_id").distinct().count()
)

# COMMAND ----------

# check every farmer has a valid location

farmer_df.groupBy("location_id").count().orderBy("location_id").show()

# COMMAND ----------

# save the farmer data

farmer_path = "/Volumes/krishiflow/source/source_data/farmer"

farmer_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(farmer_path)

# COMMAND ----------

# verify the write

display(dbutils.fs.ls(farmer_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create farm data

# COMMAND ----------

farm_rows = []

farm_types = [
    "IRRIGATED",
    "RAINFED",
    "MIXED"
]

farm_counter = 1

for farmer in farmer_df.collect():

    for _ in range(farmer.farm_count):

        farm_id = f"F{farm_counter:06d}"

        # Keep the farm in the farmer's location
        location_id = farmer.location_id

        area_acres = round(
            random.uniform(2, 50),
            2
        )

        farm_type = random.choice(farm_types)

        farm_rows.append(
            Row(
                farm_id,
                farmer.farmer_id,
                location_id,
                area_acres,
                farm_type
            )
        )

        farm_counter += 1


farm_columns = [
    "farm_id",
    "farmer_id",
    "location_id",
    "area_acres",
    "farm_type"
]

farm_df = spark.createDataFrame(
    farm_rows,
    farm_columns
)

display(farm_df)

# COMMAND ----------

# check total no. of farms

print("Total farms:", farm_df.count())

# COMMAND ----------

# lets verify farmer ==> farm relationship
farm_df.groupBy("farmer_id") \
    .count() \
    .orderBy("farmer_id") \
    .show(20)

# COMMAND ----------

# another strong validation
validation_df = (
    farmer_df
    .select("farmer_id", "farm_count")
    .join(
        farm_df.groupBy("farmer_id").count(),
        on="farmer_id",
        how="left"
    )
    .withColumnRenamed("count", "actual_farm_count")
)

display(validation_df)

# COMMAND ----------

# validate farm ids
print(
    "Total farm IDs:",
    farm_df.select("farm_id").count()
)

print(
    "Unique farm IDs:",
    farm_df.select("farm_id").distinct().count()
)

# COMMAND ----------

# validate farm area

farm_df.filter("area_acres <= 0").count()

# COMMAND ----------

# save the farm data

farm_path = "/Volumes/krishiflow/source/source_data/farm"

farm_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(farm_path)

# COMMAND ----------

# verify the write
display(dbutils.fs.ls(farm_path))

# COMMAND ----------

# now we will create collection centers

import random

# Fixed seed for reproducibility
rng = random.Random(100)

# Get existing location IDs
location_ids = [
    row["location_id"]
    for row in location_df.select("location_id").collect()
]

NUM_CENTERS = 40
center_rows = []

for i in range(1, NUM_CENTERS + 1):

    center_id = f"CC{i:03d}"

    location_id = rng.choice(location_ids)

    center_name = f"Collection_Center_{i:03d}"

    capacity_kg = round(
        rng.uniform(10000, 100000), 2
    )

    center_rows.append((
        center_id,
        center_name,
        location_id,
        capacity_kg
    ))

center_columns = [
    "collection_center_id",
    "center_name",
    "location_id",
    "capacity_kg"
]

collection_center_df = spark.createDataFrame(
    center_rows,
    center_columns
)

display(collection_center_df)

# COMMAND ----------

# validate the data
print("Total centers:", collection_center_df.count())

print(
    "Unique center IDs:",
    collection_center_df.select(
        "collection_center_id"
    ).distinct().count()
)

invalid_locations = (
    collection_center_df
    .join(
        location_df.select("location_id"),
        on="location_id",
        how="left_anti"
    )
    .count()
)

print("Invalid location references:", invalid_locations)

# COMMAND ----------

# save to the volume
center_path = (
    "/Volumes/krishiflow/source/source_data/collection_center"
)

collection_center_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(center_path)

display(dbutils.fs.ls(center_path))

# COMMAND ----------

# MAGIC %md
# MAGIC A storage facility holds crops after collection and before transportation or sale.
# MAGIC
# MAGIC We'll generate 30 facilities with different storage types and capacities.

# COMMAND ----------

# create storage facility master
import random

rng = random.Random(200)

location_ids = [
    row["location_id"]
    for row in location_df.select("location_id").collect()
]

storage_types = [
    "COLD_STORAGE",
    "WAREHOUSE",
    "GRAIN_SILO"
]

NUM_FACILITIES = 30
storage_rows = []

for i in range(1, NUM_FACILITIES + 1):

    storage_id = f"SF{i:03d}"

    location_id = rng.choice(location_ids)

    storage_name = f"Storage_Facility_{i:03d}"

    storage_type = rng.choice(storage_types)

    capacity_kg = round(
        rng.uniform(20000, 200000), 2
    )

    storage_rows.append((
        storage_id,
        storage_name,
        location_id,
        storage_type,
        capacity_kg
    ))

storage_columns = [
    "storage_facility_id",
    "storage_name",
    "location_id",
    "storage_type",
    "capacity_kg"
]

storage_facility_df = spark.createDataFrame(
    storage_rows,
    storage_columns
)

display(storage_facility_df)

# COMMAND ----------

# validate the storage facility data
print("Total storage facilities:", storage_facility_df.count())

print(
    "Unique facility IDs:",
    storage_facility_df.select(
        "storage_facility_id"
    ).distinct().count()
)

print(
    "Invalid location references:",
    storage_facility_df
    .join(
        location_df.select("location_id"),
        on="location_id",
        how="left_anti"
    )
    .count()
)

print(
    "Invalid capacities:",
    storage_facility_df
    .filter("capacity_kg <= 0")
    .count()
)

# COMMAND ----------

# save the data
storage_path = (
    "/Volumes/krishiflow/source/source_data/storage"
)

storage_facility_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(storage_path)

display(dbutils.fs.ls(storage_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Create Market Master
# MAGIC Our market master connects crop sales and prices to specific locations.
# MAGIC
# MAGIC For this initial synthetic dataset, we'll create one internal market record per location.

# COMMAND ----------

# Read the existing locations in a consistent order
market_locations = (
    location_df
    .select("location_id", "city")
    .orderBy("location_id")
    .collect()
)

market_rows = []

for i, location in enumerate(market_locations, start=1):

    market_id = f"MKT{i:03d}"

    market_name = f"{location['city']} Wholesale Market"

    location_id = location["location_id"]

    market_type = "WHOLESALE_MANDI"

    market_rows.append((
        market_id,
        market_name,
        location_id,
        market_type
    ))

market_columns = [
    "market_id",
    "market_name",
    "location_id",
    "market_type"
]

market_df = spark.createDataFrame(
    market_rows,
    market_columns
)

display(market_df)

# COMMAND ----------

# validate market master
print("Total markets:", market_df.count())

print(
    "Unique market IDs:",
    market_df.select("market_id").distinct().count()
)

print(
    "Invalid location references:",
    market_df
    .join(
        location_df.select("location_id"),
        on="location_id",
        how="left_anti"
    )
    .count()
)

# COMMAND ----------

# save market master
market_path = (
    "/Volumes/krishiflow/source/source_data/market"
)

market_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(market_path)

display(dbutils.fs.ls(market_path))


# COMMAND ----------

# finale verification call
master_summary = [
    ("Location", location_df),
    ("Crop", crop_df),
    ("Farmer", farmer_df),
    ("Farm", farm_df),
    ("Collection Center", collection_center_df),
    ("Storage Facility", storage_facility_df),
    ("Market", market_df)
]

for name, df in master_summary:
    print(f"{name}: {df.count()} records")

# COMMAND ----------

# Paths to our saved master data
farm_path = "/Volumes/krishiflow/source/source_data/farm"
crop_path = "/Volumes/krishiflow/source/source_data/crop"

# Read the CSV files using Spark
farm_source_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(farm_path)
)

crop_source_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(crop_path)
)

print("Farm records:", farm_source_df.count())
print("Crop records:", crop_source_df.count())

display(farm_source_df.limit(5))
display(crop_source_df.limit(5))

# COMMAND ----------

# generate 10000 harvest events
import random
from datetime import date, timedelta

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)

# Fixed random seed for reproducible data
rng = random.Random(300)

# Convert our small master datasets into Python lists
# This is fine for our current dataset size.
farms = (
    farm_source_df
    .select("farm_id", "area_acres")
    .collect()
)

crops = (
    crop_source_df
    .select("crop_id", "crop_name")
    .collect()
)

# Synthetic yield ranges in kilograms per acre
yield_ranges = {
    "Wheat": (1800, 3000),
    "Soybean": (700, 1400),
    "Maize": (2000, 4000),
    "Potato": (7000, 14000),
    "Onion": (6000, 12000),
    "Tomato": (8000, 18000),
    "Cotton": (500, 1000),
    "Gram": (700, 1400),
    "Chilli": (500, 1200),
    "Garlic": (4000, 8000),
    "Mustard": (600, 1200),
    "Peas": (4000, 8000),
    "Groundnut": (1000, 2200),
    "Lentil": (600, 1200),
    "Cabbage": (8000, 16000)
}

# Historical date range
start_date = date(2025, 9, 1)
end_date = date(2026, 8, 31)
date_range_days = (end_date - start_date).days

NUM_HARVESTS = 10000
harvest_rows = []

for i in range(1, NUM_HARVESTS + 1):

    # Choose an existing farm and crop
    farm = rng.choice(farms)
    crop = rng.choice(crops)

    # Harvest 10% to 50% of the farm's area
    farm_area = float(farm["area_acres"])
    area_harvested = round(
        farm_area * rng.uniform(0.10, 0.50), 2
    )

    # Estimate production using the crop's yield range
    min_yield, max_yield = yield_ranges[crop["crop_name"]]

    yield_per_acre = rng.uniform(min_yield, max_yield)

    quantity_harvested = round(
        area_harvested * yield_per_acre, 2
    )

    # Assign a date within the historical period
    harvest_date = start_date + timedelta(
        days=rng.randint(0, date_range_days)
    )

    harvest_rows.append((
        f"H{i:06d}",
        farm["farm_id"],
        crop["crop_id"],
        harvest_date,
        quantity_harvested,
        area_harvested
    ))

# Explicit schema: keeps data types predictable
harvest_schema = StructType([
    StructField("harvest_id", StringType(), False),
    StructField("farm_id", StringType(), False),
    StructField("crop_id", StringType(), False),
    StructField("harvest_date", DateType(), False),
    StructField("quantity_harvested_kg", DoubleType(), False),
    StructField("area_harvested_acres", DoubleType(), False)
])

harvest_df = spark.createDataFrame(
    harvest_rows,
    schema=harvest_schema
)

display(harvest_df.limit(20))

# COMMAND ----------

# validate the harvest event
from pyspark.sql.functions import col

total_harvests = harvest_df.count()

unique_harvest_ids = (
    harvest_df
    .select("harvest_id")
    .distinct()
    .count()
)

invalid_quantities = harvest_df.filter(
    col("quantity_harvested_kg") <= 0
).count()

invalid_areas = harvest_df.filter(
    col("area_harvested_acres") <= 0
).count()

invalid_farms = (
    harvest_df
    .join(
        farm_source_df.select("farm_id"),
        on="farm_id",
        how="left_anti"
    )
    .count()
)

invalid_crops = (
    harvest_df
    .join(
        crop_source_df.select("crop_id"),
        on="crop_id",
        how="left_anti"
    )
    .count()
)

future_dates = harvest_df.filter(
    col("harvest_date") > date.today()
).count()

print("Total harvests:", total_harvests)
print("Unique harvest IDs:", unique_harvest_ids)
print("Invalid quantities:", invalid_quantities)
print("Invalid harvested areas:", invalid_areas)
print("Invalid farm references:", invalid_farms)
print("Invalid crop references:", invalid_crops)
print("Future dates:", future_dates)

# COMMAND ----------

# save harvest data to the volume
harvest_path = (
    "/Volumes/krishiflow/source/source_data/harvest"
)

harvest_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(harvest_path)

print("Harvest source data saved successfully.")

display(dbutils.fs.ls(harvest_path))

# COMMAND ----------

# Load existing source files

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)

# Source paths
harvest_path = "/Volumes/krishiflow/source/source_data/harvest"
center_path = "/Volumes/krishiflow/source/source_data/collection_center"

# Explicit schema for the harvest CSV
harvest_schema = StructType([
    StructField("harvest_id", StringType(), False),
    StructField("farm_id", StringType(), False),
    StructField("crop_id", StringType(), False),
    StructField("harvest_date", DateType(), False),
    StructField("quantity_harvested_kg", DoubleType(), False),
    StructField("area_harvested_acres", DoubleType(), False)
])

# Read existing harvest data
harvest_source_df = (
    spark.read
    .option("header", True)
    .schema(harvest_schema)
    .csv(harvest_path)
)

# Read collection center master data
center_source_df = (
    spark.read
    .option("header", True)
    .csv(center_path)
    .select("collection_center_id")
    .distinct()
)

print("Harvest records:", harvest_source_df.count())
print("Collection centers:", center_source_df.count())

display(harvest_source_df.limit(5))
display(center_source_df)

# COMMAND ----------

# assign collection center to each harvest
# Sort the center IDs for a stable mapping
center_ids = [
    row["collection_center_id"]
    for row in (
        center_source_df
        .orderBy("collection_center_id")
        .collect()
    )
]

center_count = len(center_ids)

if center_count == 0:
    raise ValueError("No collection centers found.")

# Give each center an integer index
center_mapping_rows = [
    (index, center_id)
    for index, center_id in enumerate(center_ids)
]

center_index_df = spark.createDataFrame(
    center_mapping_rows,
    ["center_idx", "collection_center_id"]
)

display(center_index_df)

# COMMAND ----------

# generate the collection records
# Assign a collection center using the harvest ID
harvest_with_center_df = (
    harvest_source_df
    .withColumn(
        "_center_idx",
        F.pmod(
            F.xxhash64("harvest_id"),
            F.lit(center_count)
        ).cast("int")
    )
    .join(
        center_index_df,
        on=F.col("_center_idx") == F.col("center_idx"),
        how="left"
    )
    .drop("_center_idx", "center_idx")
)

# Generate a small, simulated handling loss of 0.5% to 3%
collection_df = (
    harvest_with_center_df
    .withColumn(
        "_loss_rate",
        F.lit(0.005) + F.rand(seed=400) * F.lit(0.025)
    )
    .withColumn(
        "collection_id",
        F.concat(
            F.lit("COL"),
            F.regexp_replace(F.col("harvest_id"), "^H", "")
        )
    )
    .withColumn(
        "collection_date",
        F.date_add(
            F.col("harvest_date"),
            F.floor(F.rand(seed=401) * 4).cast("int")
        )
    )
    .withColumn(
        "quantity_received_kg",
        F.round(
            F.col("quantity_harvested_kg")
            * (F.lit(1.0) - F.col("_loss_rate")),
            2
        )
    )
    .select(
        "collection_id",
        "harvest_id",
        "farm_id",
        "collection_center_id",
        "collection_date",
        "quantity_received_kg"
    )
)

display(collection_df.limit(20))

# COMMAND ----------

# validate the collection data
# Basic counts and uniqueness
total_collections = collection_df.count()

unique_collection_ids = (
    collection_df
    .select("collection_id")
    .distinct()
    .count()
)

unique_harvest_references = (
    collection_df
    .select("harvest_id")
    .distinct()
    .count()
)

print("Total collection records:", total_collections)
print("Unique collection IDs:", unique_collection_ids)
print("Unique harvest references:", unique_harvest_references)

# Check for invalid collection-center references
invalid_centers = (
    collection_df
    .join(
        center_source_df,
        on="collection_center_id",
        how="left_anti"
    )
    .count()
)

# Check that every collection references a real harvest
invalid_harvests = (
    collection_df
    .join(
        harvest_source_df.select("harvest_id"),
        on="harvest_id",
        how="left_anti"
    )
    .count()
)

print("Invalid center references:", invalid_centers)
print("Invalid harvest references:", invalid_harvests)

# COMMAND ----------

# Now validate quantities and dates against the original harvest records:
collection_check_df = (
    collection_df.alias("c")
    .join(
        harvest_source_df.select(
            "harvest_id",
            "harvest_date",
            "quantity_harvested_kg"
        ).alias("h"),
        on="harvest_id",
        how="inner"
    )
)

invalid_quantities = collection_check_df.filter(
    (F.col("c.quantity_received_kg") <= 0) |
    (
        F.col("c.quantity_received_kg")
        > F.col("h.quantity_harvested_kg")
    )
).count()

invalid_dates = collection_check_df.filter(
    F.col("c.collection_date") < F.col("h.harvest_date")
).count()

print("Invalid quantities:", invalid_quantities)
print("Invalid collection dates:", invalid_dates)

# COMMAND ----------

# save collection data
collection_path = (
    "/Volumes/krishiflow/source/source_data/collection"
)

collection_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(collection_path)

print("Collection source data saved successfully.")

display(dbutils.fs.ls(collection_path))

# COMMAND ----------

# load collection and storage master data
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)

# Read existing collection transactions
collection_path = (
    "/Volumes/krishiflow/source/source_data/collection"
)

collection_schema = StructType([
    StructField("collection_id", StringType(), False),
    StructField("harvest_id", StringType(), False),
    StructField("farm_id", StringType(), False),
    StructField("collection_center_id", StringType(), False),
    StructField("collection_date", DateType(), False),
    StructField("quantity_received_kg", DoubleType(), False)
])

collection_source_df = (
    spark.read
    .option("header", True)
    .schema(collection_schema)
    .csv(collection_path)
)

# Read storage facilities created in Step 9
storage_master_path = (
    "/Volumes/krishiflow/source/source_data/storage"
)

storage_master_df = (
    spark.read
    .option("header", True)
    .csv(storage_master_path)
    .select("storage_facility_id")
    .distinct()
)

print("Collection records:", collection_source_df.count())
print("Storage facilities:", storage_master_df.count())

# COMMAND ----------

# Assign a storage facility to each collection
# Only collect the small master list, not the transactions
storage_ids = [
    row["storage_facility_id"]
    for row in (
        storage_master_df
        .orderBy("storage_facility_id")
        .collect()
    )
]

storage_count = len(storage_ids)

if storage_count == 0:
    raise ValueError("No storage facilities found.")

storage_index_df = spark.createDataFrame(
    [
        (index, storage_id)
        for index, storage_id in enumerate(storage_ids)
    ],
    ["storage_idx", "storage_facility_id"]
)

print("Available storage facilities:", storage_count)

display(storage_index_df)

# COMMAND ----------

# generate the storage transaction
# Assign each collection to a storage facility
collection_with_storage_df = (
    collection_source_df
    .withColumn(
        "_storage_idx",
        F.pmod(
            F.xxhash64("collection_id"),
            F.lit(storage_count)
        ).cast("int")
    )
    .join(
        storage_index_df,
        F.col("_storage_idx") == F.col("storage_idx"),
        "left"
    )
    .drop("_storage_idx", "storage_idx")
)

# Generate realistic-looking, simulated storage movements
storage_transaction_df = (
    collection_with_storage_df

    # Generate storage duration from 1 to 8 days
    .withColumn(
        "_duration_days",
        (
            F.floor(F.rand(seed=501) * 8) + 1
        ).cast("int")
    )

    # Generate a simulated storage loss rate from 0.5% to 5%
    .withColumn(
        "_loss_rate",
        F.lit(0.005) + F.rand(seed=502) * F.lit(0.045)
    )

    # Create a unique storage transaction ID
    .withColumn(
        "storage_transaction_id",
        F.concat(
            F.lit("ST"),
            F.regexp_replace(
                F.col("collection_id"),
                "^COL",
                ""
            )
        )
    )

    # Entry date is the date of collection
    .withColumn(
        "entry_date",
        F.col("collection_date")
    )

    # Exit date is entry date plus storage duration
    .withColumn(
        "exit_date",
        F.date_add(
            F.col("collection_date"),
            F.col("_duration_days")
        )
    )

    # All collected quantity enters storage
    .withColumn(
        "quantity_in_kg",
        F.col("quantity_received_kg")
    )

    # Apply the simulated storage loss
    .withColumn(
        "quantity_out_kg",
        F.round(
            F.col("quantity_in_kg")
            * (F.lit(1.0) - F.col("_loss_rate")),
            2
        )
    )

    # Keep only the required source fields
    .select(
        "storage_transaction_id",
        "collection_id",
        "storage_facility_id",
        "entry_date",
        "exit_date",
        "quantity_in_kg",
        "quantity_out_kg"
    )
)

display(storage_transaction_df.limit(20))

# COMMAND ----------

# validate the generated data
# Check record counts and unique transaction IDs
print(
    "Total storage transactions:",
    storage_transaction_df.count()
)

print(
    "Unique transaction IDs:",
    storage_transaction_df
    .select("storage_transaction_id")
    .distinct()
    .count()
)

print(
    "Unique collection references:",
    storage_transaction_df
    .select("collection_id")
    .distinct()
    .count()
)

# COMMAND ----------

# now check the parent relationship
# Every collection reference must exist
invalid_collections = (
    storage_transaction_df
    .join(
        collection_source_df.select("collection_id"),
        on="collection_id",
        how="left_anti"
    )
    .count()
)

# Every storage facility reference must exist
invalid_facilities = (
    storage_transaction_df
    .join(
        storage_master_df,
        on="storage_facility_id",
        how="left_anti"
    )
    .count()
)

print("Invalid collection references:", invalid_collections)
print("Invalid facility references:", invalid_facilities)

# COMMAND ----------

# finaly validate quantities and dates
invalid_quantities = storage_transaction_df.filter(
    (F.col("quantity_in_kg") <= 0) |
    (F.col("quantity_out_kg") < 0) |
    (F.col("quantity_out_kg") > F.col("quantity_in_kg"))
).count()

invalid_dates = storage_transaction_df.filter(
    F.col("exit_date") < F.col("entry_date")
).count()

null_required_fields = storage_transaction_df.filter(
    F.col("storage_transaction_id").isNull() |
    F.col("collection_id").isNull() |
    F.col("storage_facility_id").isNull() |
    F.col("entry_date").isNull() |
    F.col("exit_date").isNull() |
    F.col("quantity_in_kg").isNull() |
    F.col("quantity_out_kg").isNull()
).count()

print("Invalid quantities:", invalid_quantities)
print("Invalid dates:", invalid_dates)
print("Records missing required fields:", null_required_fields)


# COMMAND ----------

# save to the volume
storage_transaction_path = (
    "/Volumes/krishiflow/source/source_data/storage_transaction"
)

storage_transaction_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(storage_transaction_path)

print("Storage transactions saved successfully.")

display(dbutils.fs.ls(storage_transaction_path))

# COMMAND ----------

# Load exixting data sources
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType
)

# Paths to existing source data
storage_path = "/Volumes/krishiflow/source/source_data/storage_transaction"
collection_path = "/Volumes/krishiflow/source/source_data/collection"
storage_master_path = "/Volumes/krishiflow/source/source_data/storage"
harvest_path = "/Volumes/krishiflow/source/source_data/harvest"
farm_path = "/Volumes/krishiflow/source/source_data/farm"
market_path = "/Volumes/krishiflow/source/source_data/market"
location_path = "/Volumes/krishiflow/source/source_data/location"

# Define the storage transaction schema explicitly
storage_schema = StructType([
    StructField("storage_transaction_id", StringType(), False),
    StructField("collection_id", StringType(), False),
    StructField("storage_facility_id", StringType(), False),
    StructField("entry_date", DateType(), False),
    StructField("exit_date", DateType(), False),
    StructField("quantity_in_kg", DoubleType(), False),
    StructField("quantity_out_kg", DoubleType(), False)
])

# Read the existing CSV datasets
storage_source_df = (
    spark.read
    .option("header", True)
    .schema(storage_schema)
    .csv(storage_path)
)

collection_source_df = (
    spark.read
    .option("header", True)
    .schema(StructType([
        StructField("collection_id", StringType(), False),
        StructField("harvest_id", StringType(), False),
        StructField("farm_id", StringType(), False),
        StructField("collection_center_id", StringType(), False),
        StructField("collection_date", DateType(), False),
        StructField("quantity_received_kg", DoubleType(), False)
    ]))
    .csv(collection_path)
)

harvest_source_df = (
    spark.read
    .option("header", True)
    .schema(StructType([
        StructField("harvest_id", StringType(), False),
        StructField("farm_id", StringType(), False),
        StructField("crop_id", StringType(), False),
        StructField("harvest_date", DateType(), False),
        StructField("quantity_harvested_kg", DoubleType(), False),
        StructField("area_harvested_acres", DoubleType(), False)
    ]))
    .csv(harvest_path)
)

# Master datasets: infer types for latitude, longitude and capacity
storage_master_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(storage_master_path)
)

market_source_df = (
    spark.read
    .option("header", True)
    .csv(market_path)
)

location_master_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(location_path)
)

print("Storage transactions:", storage_source_df.count())
print("Collection records:", collection_source_df.count())
print("Harvest records:", harvest_source_df.count())
print("Storage facilities:", storage_master_df.count())
print("Markets:", market_source_df.count())
print("Locations:", location_master_df.count())

# COMMAND ----------

# Create location indexes for destination selection
# Select one row per location, ordered consistently
location_rows = (
    location_master_df
    .select("location_id", "latitude", "longitude")
    .distinct()
    .orderBy("location_id")
    .collect()
)

location_index_df = spark.createDataFrame(
    [
        (
            index,
            row["location_id"],
            float(row["latitude"]),
            float(row["longitude"])
        )
        for index, row in enumerate(location_rows)
    ],
    [
        "location_idx",
        "location_id",
        "latitude",
        "longitude"
    ]
)

# Attach the same index to each market's location
market_index_df = (
    market_source_df
    .join(location_index_df, on="location_id", how="inner")
    .select(
        F.col("location_idx").alias("market_idx"),
        "market_id",
        "location_id",
        "market_name",
        F.col("latitude").alias("market_latitude"),
        F.col("longitude").alias("market_longitude")
    )
)

market_count = market_index_df.count()

if market_count < 2:
    raise ValueError(
        "At least two valid markets are required for transportation."
    )

print("Indexed locations:", location_index_df.count())
print("Indexed markets:", market_count)

display(market_index_df)

# COMMAND ----------

# link storage transactions to market
st = storage_source_df.alias("st")
c = collection_source_df.alias("c")
h = harvest_source_df.alias("h")
sf = storage_master_df.alias("sf")
sloc = location_index_df.alias("sloc")

# Join the existing parent records
transport_base_df = (
    st
    .join(
        c,
        F.col("st.collection_id") == F.col("c.collection_id"),
        "inner"
    )
    .join(
        h,
        F.col("c.harvest_id") == F.col("h.harvest_id"),
        "inner"
    )
    .join(
        sf,
        F.col("st.storage_facility_id")
        == F.col("sf.storage_facility_id"),
        "inner"
    )
    .join(
        sloc,
        F.col("sf.location_id") == F.col("sloc.location_id"),
        "inner"
    )
    .select(
        F.col("st.storage_transaction_id"),
        F.col("st.collection_id"),
        F.col("st.exit_date"),
        F.col("st.quantity_out_kg").alias("quantity_dispatched_kg"),
        F.col("h.crop_id"),
        F.col("sf.location_id").alias("source_location_id"),
        F.col("sloc.location_idx").alias("_source_location_idx"),
        F.col("sloc.latitude").alias("_source_latitude"),
        F.col("sloc.longitude").alias("_source_longitude")
    )
)

print("Transport-ready records:", transport_base_df.count())

display(transport_base_df.limit(10))


# COMMAND ----------

# Choose a destination market without selecting the source location itself:
transport_route_df = (
    transport_base_df
    .withColumn(
        "_destination_idx",
        F.pmod(
            F.col("_source_location_idx")
            + F.lit(1)
            + F.pmod(
                F.xxhash64("storage_transaction_id"),
                F.lit(market_count - 1)
            ),
            F.lit(market_count)
        ).cast("int")
    )
    .join(
        market_index_df.alias("m"),
        F.col("_destination_idx") == F.col("m.market_idx"),
        "inner"
    )
    .select(
        "storage_transaction_id",
        "collection_id",
        "exit_date",
        "quantity_dispatched_kg",
        "crop_id",
        "source_location_id",
        "_source_latitude",
        "_source_longitude",
        F.col("m.market_id").alias("market_id"),
        F.col("m.location_id").alias("destination_location_id"),
        F.col("m.market_latitude").alias("_destination_latitude"),
        F.col("m.market_longitude").alias("_destination_longitude")
    )
)

display(transport_route_df.limit(10))

# COMMAND ----------

# Generate timestamps, distance and shipment quantities
# Calculate great-circle distance using latitude and longitude
transport_enriched_df = (
    transport_route_df

    .withColumn(
        "_lat1",
        F.radians(F.col("_source_latitude"))
    )
    .withColumn(
        "_lat2",
        F.radians(F.col("_destination_latitude"))
    )
    .withColumn(
        "_lon1",
        F.radians(F.col("_source_longitude"))
    )
    .withColumn(
        "_lon2",
        F.radians(F.col("_destination_longitude"))
    )

    # Haversine formula: calculate the value used for distance
    .withColumn(
        "_haversine_a",
        F.pow(
            F.sin((F.col("_lat2") - F.col("_lat1")) / 2),
            2
        )
        + F.cos(F.col("_lat1"))
        * F.cos(F.col("_lat2"))
        * F.pow(
            F.sin((F.col("_lon2") - F.col("_lon1")) / 2),
            2
        )
    )

    # Protect against minor floating-point rounding errors
    .withColumn(
        "_haversine_a",
        F.least(
            F.lit(1.0),
            F.greatest(F.lit(0.0), F.col("_haversine_a"))
        )
    )

    # Straight-line distance × approximate road-distance factor
    .withColumn(
        "distance_km",
        F.round(
            F.lit(6371.0)
            * F.lit(2.0)
            * F.asin(F.sqrt(F.col("_haversine_a")))
            * F.lit(1.3),
            2
        )
    )

    # Dispatch at 08:00 on the storage exit date
    .withColumn(
        "dispatch_timestamp",
        F.to_timestamp(
            F.concat(
                F.date_format(F.col("exit_date"), "yyyy-MM-dd"),
                F.lit(" 08:00:00")
            )
        )
    )

    # Simulated delivery duration between 1 and 12 hours
    .withColumn(
        "_duration_hours",
        (
            F.floor(F.rand(seed=602) * 12) + F.lit(1)
        ).cast("int")
    )

    # Arrival timestamp = dispatch timestamp + delivery duration
    .withColumn(
        "arrival_timestamp",
        F.from_unixtime(
            F.unix_timestamp(F.col("dispatch_timestamp"))
            + F.col("_duration_hours") * F.lit(3600)
        ).cast("timestamp")
    )

    # Simulated transportation loss between 0.2% and 2%
    .withColumn(
        "_loss_rate",
        F.lit(0.002) + F.rand(seed=603) * F.lit(0.018)
    )

    # Generate the transport ID from the storage transaction ID
    .withColumn(
        "transport_id",
        F.concat(
            F.lit("TR"),
            F.regexp_replace(
                F.col("storage_transaction_id"),
                "^ST",
                ""
            )
        )
    )

    # Quantity received must not exceed the quantity dispatched
    .withColumn(
        "quantity_received_kg",
        F.round(
            F.col("quantity_dispatched_kg")
            * (F.lit(1.0) - F.col("_loss_rate")),
            2
        )
    )

    .select(
        "transport_id",
        "storage_transaction_id",
        "market_id",
        "source_location_id",
        "destination_location_id",
        "crop_id",
        "dispatch_timestamp",
        "arrival_timestamp",
        "quantity_dispatched_kg",
        "quantity_received_kg",
        "distance_km"
    )
)

display(transport_enriched_df.limit(20))

# COMMAND ----------

# validate transpost data
# check record count and uniqueness
transport_count = transport_enriched_df.count()

unique_transport_ids = (
    transport_enriched_df
    .select("transport_id")
    .distinct()
    .count()
)

unique_storage_references = (
    transport_enriched_df
    .select("storage_transaction_id")
    .distinct()
    .count()
)

print("Transport records:", transport_count)
print("Unique transport IDs:", unique_transport_ids)
print("Unique storage references:", unique_storage_references)

# COMMAND ----------

# check foreign key reference
invalid_storage_refs = (
    transport_enriched_df
    .join(
        storage_source_df.select("storage_transaction_id"),
        on="storage_transaction_id",
        how="left_anti"
    )
    .count()
)

invalid_markets = (
    transport_enriched_df
    .join(
        market_source_df.select("market_id"),
        on="market_id",
        how="left_anti"
    )
    .count()
)

invalid_source_locations = (
    transport_enriched_df
    .join(
        location_master_df.select(
            F.col("location_id").alias("source_location_id")
        ),
        on="source_location_id",
        how="left_anti"
    )
    .count()
)

invalid_destination_locations = (
    transport_enriched_df
    .join(
        location_master_df.select(
            F.col("location_id").alias("destination_location_id")
        ),
        on="destination_location_id",
        how="left_anti"
    )
    .count()
)

print("Invalid storage references:", invalid_storage_refs)
print("Invalid market references:", invalid_markets)
print("Invalid source locations:", invalid_source_locations)
print("Invalid destination locations:", invalid_destination_locations)

# COMMAND ----------

# Check quantities, routes and delivery times
invalid_quantities = transport_enriched_df.filter(
    (F.col("quantity_dispatched_kg") <= 0)
    | (F.col("quantity_received_kg") < 0)
    | (
        F.col("quantity_received_kg")
        > F.col("quantity_dispatched_kg")
    )
).count()

invalid_distances = transport_enriched_df.filter(
    F.col("distance_km") <= 0
).count()

invalid_timestamps = transport_enriched_df.filter(
    F.col("arrival_timestamp") < F.col("dispatch_timestamp")
).count()

same_source_destination = transport_enriched_df.filter(
    F.col("source_location_id")
    == F.col("destination_location_id")
).count()

null_required_fields = transport_enriched_df.filter(
    F.col("transport_id").isNull()
    | F.col("storage_transaction_id").isNull()
    | F.col("market_id").isNull()
    | F.col("source_location_id").isNull()
    | F.col("destination_location_id").isNull()
    | F.col("crop_id").isNull()
    | F.col("dispatch_timestamp").isNull()
    | F.col("arrival_timestamp").isNull()
    | F.col("quantity_dispatched_kg").isNull()
    | F.col("quantity_received_kg").isNull()
    | F.col("distance_km").isNull()
).count()

print("Invalid quantities:", invalid_quantities)
print("Invalid distances:", invalid_distances)
print("Invalid timestamps:", invalid_timestamps)
print("Same source/destination:", same_source_destination)
print("Missing required fields:", null_required_fields)

# COMMAND ----------

# save transportation as json
transport_path = (
    "/Volumes/krishiflow/source/source_data/transport"
)

transport_enriched_df.write \
    .mode("overwrite") \
    .json(transport_path)

print("Transport JSON data saved successfully.")

display(dbutils.fs.ls(transport_path))

# COMMAND ----------

# Spark will create a directory containing JSON part files and a completion marker. That is expected.
transport_saved_df = spark.read.json(transport_path)

print("Saved transport records:", transport_saved_df.count())

display(transport_saved_df.limit(5))

# COMMAND ----------

# load transportation and master data
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType
)

# Source paths
transport_path = "/Volumes/krishiflow/source/source_data/transport"
crop_path = "/Volumes/krishiflow/source/source_data/crop"
market_path = "/Volumes/krishiflow/source/source_data/market"

# Define the JSON schema explicitly
transport_schema = StructType([
    StructField("transport_id", StringType(), False),
    StructField("storage_transaction_id", StringType(), False),
    StructField("market_id", StringType(), False),
    StructField("source_location_id", StringType(), False),
    StructField("destination_location_id", StringType(), False),
    StructField("crop_id", StringType(), False),
    StructField("dispatch_timestamp", TimestampType(), False),
    StructField("arrival_timestamp", TimestampType(), False),
    StructField("quantity_dispatched_kg", DoubleType(), False),
    StructField("quantity_received_kg", DoubleType(), False),
    StructField("distance_km", DoubleType(), False)
])

# Load transportation data
transport_source_df = (
    spark.read
    .schema(transport_schema)
    .json(transport_path)
)

# Load crop master
crop_master_df = (
    spark.read
    .option("header", True)
    .csv(crop_path)
    .select("crop_id", "crop_name")
)

# Load market master
market_master_df = (
    spark.read
    .option("header", True)
    .csv(market_path)
    .select("market_id")
    .distinct()
)

print("Transport records:", transport_source_df.count())
print("Crop records:", crop_master_df.count())
print("Market records:", market_master_df.count())

display(transport_source_df.limit(5))

# COMMAND ----------

# define stimulated selling price
base_prices = {
    "Wheat": 28.0,
    "Soybean": 45.0,
    "Maize": 24.0,
    "Potato": 22.0,
    "Onion": 30.0,
    "Tomato": 28.0,
    "Cotton": 65.0,
    "Gram": 65.0,
    "Chilli": 90.0,
    "Garlic": 120.0,
    "Mustard": 55.0,
    "Peas": 35.0,
    "Groundnut": 65.0,
    "Lentil": 85.0,
    "Cabbage": 18.0
}

price_lookup_df = spark.createDataFrame(
    [
        (crop_name, price)
        for crop_name, price in base_prices.items()
    ],
    ["crop_name", "base_price_per_kg"]
)

display(price_lookup_df.orderBy("crop_name"))

# COMMAND ----------

# generate the sales records
# Join each shipment to its crop name and synthetic price
transport_with_price_df = (
    transport_source_df
    .join(
        crop_master_df,
        on="crop_id",
        how="inner"
    )
    .join(
        price_lookup_df,
        on="crop_name",
        how="inner"
    )
)

# Create deterministic variations based on the transport ID
sales_df = (
    transport_with_price_df

    # A stable numeric hash for reproducible transaction variations
    .withColumn(
        "_hash_value",
        F.pmod(
            F.xxhash64("transport_id"),
            F.lit(1000000)
        )
    )

    # Sell between 95% and 99.99% of the shipment received
    .withColumn(
        "_sold_fraction",
        F.lit(0.95)
        + (
            F.pmod(F.col("_hash_value"), F.lit(500))
            / F.lit(10000.0)
        )
    )

    # Sale can occur on arrival day or up to two days later
    .withColumn(
        "sale_date",
        F.date_add(
            F.to_date("arrival_timestamp"),
            F.pmod(F.col("_hash_value"), F.lit(3)).cast("int")
        )
    )

    # Generate the sale ID from the transport ID
    .withColumn(
        "sale_id",
        F.concat(
            F.lit("SALE"),
            F.regexp_replace(F.col("transport_id"), "^TR", "")
        )
    )

    # Calculate the quantity sold
    .withColumn(
        "quantity_sold_kg",
        F.round(
            F.col("quantity_received_kg")
            * F.col("_sold_fraction"),
            2
        )
    )

    # Vary price by -10% to +10% around the synthetic base price
    .withColumn(
        "_price_variation",
        (
            F.pmod(F.col("_hash_value"), F.lit(21))
            - F.lit(10)
        ) / F.lit(100.0)
    )

    .withColumn(
        "selling_price_per_kg",
        F.round(
            F.col("base_price_per_kg")
            * (F.lit(1.0) + F.col("_price_variation")),
            2
        )
    )

    # Keep the source fields, including transport linkage
    .select(
        "sale_id",
        "transport_id",
        "market_id",
        "crop_id",
        "sale_date",
        "quantity_sold_kg",
        "selling_price_per_kg"
    )
)

display(sales_df.limit(20))

# COMMAND ----------

# validate sales records
print("Total sales:", sales_df.count())

print(
    "Unique sale IDs:",
    sales_df.select("sale_id").distinct().count()
)

print(
    "Unique transport references:",
    sales_df.select("transport_id").distinct().count()
)

# COMMAND ----------

# now check market and crop reference
invalid_markets = (
    sales_df
    .join(
        market_master_df,
        on="market_id",
        how="left_anti"
    )
    .count()
)

invalid_crops = (
    sales_df
    .join(
        crop_master_df.select("crop_id"),
        on="crop_id",
        how="left_anti"
    )
    .count()
)

invalid_transports = (
    sales_df
    .join(
        transport_source_df.select("transport_id"),
        on="transport_id",
        how="left_anti"
    )
    .count()
)

print("Invalid market references:", invalid_markets)
print("Invalid crop references:", invalid_crops)
print("Invalid transport references:", invalid_transports)

# COMMAND ----------

# verify that quantities and dates are valid against the actual transport record.
sales_transport_check_df = (
    sales_df.alias("s")
    .join(
        transport_source_df.alias("t"),
        on="transport_id",
        how="inner"
    )
)

invalid_quantities = sales_transport_check_df.filter(
    (F.col("s.quantity_sold_kg") <= 0)
    | (
        F.col("s.quantity_sold_kg")
        > F.col("t.quantity_received_kg")
    )
).count()

invalid_dates = sales_transport_check_df.filter(
    F.col("s.sale_date") < F.to_date(F.col("t.arrival_timestamp"))
).count()

invalid_prices = sales_df.filter(
    F.col("selling_price_per_kg") <= 0
).count()

print("Invalid quantities:", invalid_quantities)
print("Sales before arrival:", invalid_dates)
print("Invalid selling prices:", invalid_prices)

# COMMAND ----------

# Finally, ensure the sale uses the same crop and market as its shipment:
mismatched_sales = sales_transport_check_df.filter(
    (F.col("s.crop_id") != F.col("t.crop_id"))
    | (F.col("s.market_id") != F.col("t.market_id"))
).count()

print("Crop/market mismatches:", mismatched_sales)

# COMMAND ----------

# save sales data to the volume

sale_path = "/Volumes/krishiflow/source/source_data/sale"

sales_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(sale_path)

print("Sales source data saved successfully.")

display(dbutils.fs.ls(sale_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # KrishiFlow — Project Progress & Notebook Reference Notes
# MAGIC
# MAGIC ## 1. Project setup
# MAGIC
# MAGIC - Platform: Databricks Free Edition.
# MAGIC - Catalog: `krishiflow` (Standard).
# MAGIC - Schemas: `source`, `bronze`, `silver`, `gold`.
# MAGIC - Volume: `source.source_data`.
# MAGIC - Master and operational data notebook: `01_generate_master_data`.
# MAGIC - Weather API notebook: `02_ingest_weather_api`.
# MAGIC
# MAGIC **Source file location:**
# MAGIC
# MAGIC `/Volumes/krishiflow/source/source_data/`
# MAGIC
# MAGIC ## 2. Master/reference data — Notebook 01
# MAGIC
# MAGIC These datasets define the entities used throughout KrishiFlow.
# MAGIC
# MAGIC | Dataset | Target records | Purpose |
# MAGIC |---|---:|---|
# MAGIC | Location | 20 | Locations and geographic coordinates |
# MAGIC | Crop | 15 | Crop names, categories and units |
# MAGIC | Farmer | 500 | Synthetic farmer records |
# MAGIC | Farm | Approximately 1,000 | Farms linked to their owners |
# MAGIC | Collection center | 40 | Centers linked to locations |
# MAGIC | Storage facility | 30 | Facilities, types and capacities |
# MAGIC | Market | 20 | Internal markets linked to locations |
# MAGIC
# MAGIC ### Important implementation details
# MAGIC
# MAGIC - IDs follow consistent patterns, such as `LOC001`, `C001`, `FR000001`, `F000001`, `CC001`, `SF001` and `MKT001`.
# MAGIC - A fixed random seed is used where appropriate for reproducibility.
# MAGIC - Every farm references an existing farmer and location.
# MAGIC - Each farmer's declared `farm_count` is used to generate the corresponding number of farms.
# MAGIC - Collection centers and storage facilities reference existing locations.
# MAGIC - Each internal market corresponds to one of the 20 locations. Its generated name is a placeholder, not a verified official mandi name.
# MAGIC
# MAGIC **Files/folders:** `location/`, `crop/`, `farmer/`, `farm/`, `collection_center/`, `storage/`, `market/`.
# MAGIC
# MAGIC ## 3. Transactional source data — Notebook 01
# MAGIC
# MAGIC ### Step 12: Harvest
# MAGIC
# MAGIC - Target: 10,000 records.
# MAGIC - Period: September 1, 2025–August 31, 2026.
# MAGIC - Each harvest references an existing farm and crop.
# MAGIC - Crop-specific synthetic yield ranges determine harvested quantities.
# MAGIC - Positive quantities, positive harvested areas, unique harvest IDs and valid references are checked.
# MAGIC
# MAGIC Output: `harvest/` — CSV.
# MAGIC
# MAGIC ### Step 13: Collection
# MAGIC
# MAGIC - Target: 10,000 records, one for each harvest.
# MAGIC - Each collection references an actual harvest, farm and collection center.
# MAGIC - Collection occurs on the harvest date or within the following three days.
# MAGIC - Simulated handling loss: 0.5%–3%.
# MAGIC - Received quantity must be positive and cannot exceed harvested quantity.
# MAGIC
# MAGIC Output: `collection/` — CSV.
# MAGIC
# MAGIC ### Step 14: Storage transactions
# MAGIC
# MAGIC - Target: 10,000 records, one for each collection.
# MAGIC - Each transaction references an existing collection and storage facility.
# MAGIC - Entry date equals collection date.
# MAGIC - Simulated storage duration: 1–8 days.
# MAGIC - Simulated storage loss: 0.5%–5%.
# MAGIC - Quantity out cannot exceed quantity in.
# MAGIC
# MAGIC Output: `storage_transaction/` — CSV.
# MAGIC
# MAGIC **Important:** `storage/` contains the storage-facility master; `storage_transaction/` contains storage movements.
# MAGIC
# MAGIC ### Step 15: Transportation
# MAGIC
# MAGIC - Target: 10,000 records, one for each storage transaction.
# MAGIC - Links each shipment to its storage transaction, crop, source location and destination market.
# MAGIC - Dispatch occurs at 08:00 on the storage exit date.
# MAGIC - Simulated delivery duration: 1–12 hours.
# MAGIC - Simulated transport loss: 0.2%–2%.
# MAGIC - Distance is estimated using geographic coordinates and a road-distance multiplier; it is not an actual road-route distance.
# MAGIC - Received quantity cannot exceed dispatched quantity.
# MAGIC
# MAGIC Output: `transport/` — JSON.
# MAGIC
# MAGIC ### Step 16: Sales
# MAGIC
# MAGIC - Target: 10,000 records, one for each transportation record.
# MAGIC - Each sale references the shipment, crop and destination market.
# MAGIC - The sale date is the arrival date or within the following two days.
# MAGIC - Synthetic crop-specific base prices are varied by approximately −10% to +10%.
# MAGIC - Sold quantity is designed to be 95%–99.99% of shipment quantity received.
# MAGIC - Revenue is a derived metric to calculate in Silver, not a source field.
# MAGIC
# MAGIC Output: `sale/` — CSV.
# MAGIC
# MAGIC ## 4. Validation checks
# MAGIC
# MAGIC The generation steps include checks for:
# MAGIC
# MAGIC - Total records and unique IDs.
# MAGIC - Valid foreign-key references.
# MAGIC - Missing required values.
# MAGIC - Positive quantities and areas.
# MAGIC - Received quantities not exceeding upstream quantities.
# MAGIC - Valid date order and delivery timestamps.
# MAGIC - Positive distances and prices.
# MAGIC - Correct crop and market references.
# MAGIC
# MAGIC **Expected core transaction counts:** 10,000 harvests, 10,000 collections, 10,000 storage transactions, 10,000 transports and 10,000 sales.
# MAGIC
# MAGIC These are targets, not independently verified results. Count a dataset as completed only after its generation, validation and save cells run successfully.
# MAGIC
# MAGIC ## 5. Weather API — Separate notebook
# MAGIC
# MAGIC Notebook: `02_ingest_weather_api`.
# MAGIC
# MAGIC Planned source: Open-Meteo Historical Weather API.
# MAGIC
# MAGIC - Locations: our 20 location records.
# MAGIC - Historical period: September 1, 2025–August 31, 2026.
# MAGIC - Frequency: daily.
# MAGIC - Expected volume: approximately 7,300 observations.
# MAGIC - Variables:
# MAGIC   - `temperature_2m_mean` → `temperature_c`
# MAGIC   - `relative_humidity_2m_mean` → `humidity_percentage`
# MAGIC   - `precipitation_sum` → `rainfall_mm`
# MAGIC   - `wind_speed_10m_mean` → `wind_speed_kmph`
# MAGIC
# MAGIC Planned output folders:
# MAGIC
# MAGIC - `weather/raw_payloads/` — retained API responses and request metadata.
# MAGIC - `weather/daily_observations/` — standardized daily observations.
# MAGIC
# MAGIC The weather ingestion code has been provided, but successful execution and output counts have not yet been confirmed.
# MAGIC
# MAGIC ## 6. Market-price API — Not yet implemented
# MAGIC
# MAGIC Planned source: Government of India mandi-price data through AGMARKNET/data.gov.in.
# MAGIC
# MAGIC Planned work:
# MAGIC
# MAGIC - Retrieve real market-price records through the available API.
# MAGIC - Map official market and commodity names to KrishiFlow's internal IDs.
# MAGIC - Preserve the original source values and price units.
# MAGIC - Convert ₹/quintal to ₹/kg during Silver transformation when the source unit is ₹/quintal.
# MAGIC - Handle missing values, duplicate observations and historical-data availability.
# MAGIC
# MAGIC ## 7. Important design decisions
# MAGIC
# MAGIC - Operational transactions are synthetic; they are not actual farm records.
# MAGIC - Weather and mandi prices will come from real external sources.
# MAGIC - Simulated loss percentages and sales prices are test assumptions, not real agricultural findings.
# MAGIC - Spark writes files into directories containing part files rather than necessarily creating a single named CSV.
# MAGIC - `.mode("overwrite")` is currently used for initial source generation. Incremental processing has not been implemented yet.
# MAGIC - Intentional bad-record injection has not been implemented yet.
# MAGIC
# MAGIC ## 8. Remaining project work
# MAGIC
# MAGIC 1. Finish and validate weather API ingestion.
# MAGIC 2. Implement market-price API ingestion.
# MAGIC 3. Build Bronze Delta tables for raw source data.
# MAGIC 4. Build Silver transformations, data-quality checks and quarantine.
# MAGIC 5. Build Gold dimensions, fact tables and analytical views.
# MAGIC 6. Implement incremental processing, including deduplication and safe reprocessing.
# MAGIC 7. Configure Lakeflow Jobs, dependencies, retries and monitoring.
# MAGIC 8. Build dashboard and analytical queries.
# MAGIC 9. Publish GitHub documentation, architecture diagrams and verified project results.
# MAGIC
# MAGIC **Overall goal:** A repeatable Databricks Data Engineering pipeline that ingests mixed sources, validates and transforms the data, tracks data quality, processes incremental updates and produces useful agricultural supply-chain analytics.
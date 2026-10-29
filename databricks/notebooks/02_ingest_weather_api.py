# Databricks notebook source
# load our 20 location records
from pyspark.sql import functions as F

location_path = "/Volumes/krishiflow/source/source_data/location"

locations_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(location_path)
    .select(
        "location_id",
        "state",
        "district",
        "city",
        "latitude",
        "longitude"
    )
    .dropDuplicates(["location_id"])
    .orderBy("location_id")
)

print("Location count:", locations_df.count())

display(locations_df)

# COMMAND ----------

# check that the coordinates are not missing
invalid_coordinates = locations_df.filter(
    F.col("latitude").isNull()
    | F.col("longitude").isNull()
    | (F.col("latitude") < -90)
    | (F.col("latitude") > 90)
    | (F.col("longitude") < -180)
    | (F.col("longitude") > 180)
).count()

print("Invalid coordinates:", invalid_coordinates)

# COMMAND ----------

# test the api with one record
import requests

API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Use the first location for a small test
test_location = locations_df.first()

test_params = {
    "latitude": float(test_location["latitude"]),
    "longitude": float(test_location["longitude"]),
    "start_date": "2025-09-01",
    "end_date": "2025-09-07",
    "daily": (
        "temperature_2m_mean,"
        "relative_humidity_2m_mean,"
        "precipitation_sum,"
        "wind_speed_10m_mean"
    ),
    "timezone": "Asia/Kolkata",
    "temperature_unit": "celsius",
    "wind_speed_unit": "kmh",
    "precipitation_unit": "mm"
}

response = requests.get(
    API_URL,
    params=test_params,
    timeout=60
)

response.raise_for_status()
test_payload = response.json()

if test_payload.get("error"):
    raise RuntimeError(test_payload.get("reason"))

print("API connection successful!")
print("Location:", test_location["location_id"])
print("API timezone:", test_payload.get("timezone"))
print("Daily variables returned:")

display(test_payload["daily"])


# COMMAND ----------

# fetch the historical weather dataset
import json
import time

from datetime import date, datetime, timezone

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    TimestampType
)

# Historical backfill period
START_DATE = "2025-09-01"
END_DATE = "2026-08-31"

BATCH_ID = "WEATHER_BACKFILL_20250901_20260831"
SOURCE_NAME = "open_meteo_historical_api"

# Landing paths inside the Unity Catalog Volume
weather_root = "/Volumes/krishiflow/source/source_data/weather"

raw_payload_path = f"{weather_root}/raw_payloads"
daily_observations_path = f"{weather_root}/daily_observations"

dbutils.fs.mkdirs(raw_payload_path)
dbutils.fs.mkdirs(daily_observations_path)

daily_variables = [
    "temperature_2m_mean",
    "relative_humidity_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_mean"
]

weather_records = []

# Only 20 master rows, so collecting these is safe
location_rows = (
    locations_df
    .orderBy("location_id")
    .collect()
)

for location in location_rows:

    location_id = location["location_id"]
    latitude = float(location["latitude"])
    longitude = float(location["longitude"])

    print(f"Fetching weather for {location_id}...")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": ",".join(daily_variables),
        "timezone": "Asia/Kolkata",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm"
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=60
    )

    response.raise_for_status()
    payload = response.json()

    if payload.get("error"):
        raise RuntimeError(
            f"API error for {location_id}: "
            f"{payload.get('reason')}"
        )

    daily = payload.get("daily", {})
    dates = daily.get("time", [])

    if not dates:
        raise ValueError(
            f"No daily observations returned for {location_id}."
        )

    # Confirm all requested variables have matching date arrays
    for variable in daily_variables:
        if (
            variable not in daily
            or len(daily[variable]) != len(dates)
        ):
            raise ValueError(
                f"Unexpected response for {location_id}: "
                f"{variable} is missing or has a different length."
            )

    # Metadata records where, when and why this response was fetched.
    ingestion_time = datetime.now(timezone.utc)
    raw_file = (
        f"{raw_payload_path}/"
        f"weather_{location_id}_{START_DATE}_{END_DATE}.json"
    )

    payload_wrapper = {
        "_source_metadata": {
            "location_id": location_id,
            "requested_latitude": latitude,
            "requested_longitude": longitude,
            "_source": SOURCE_NAME,
            "_batch_id": BATCH_ID,
            "_ingestion_timestamp": ingestion_time.isoformat()
        },
        "api_response": payload
    }

    # Save the API response with its original data nested under api_response
    dbutils.fs.put(
        raw_file,
        json.dumps(payload_wrapper, ensure_ascii=False),
        True
    )

    # Flatten daily arrays into one record per location and date
    for i, weather_date in enumerate(dates):

        def safe_float(value):
            return None if value is None else float(value)

        weather_records.append((
            f"WEA_{location_id}_{weather_date.replace('-', '')}",
            location_id,
            date.fromisoformat(weather_date),
            safe_float(daily["temperature_2m_mean"][i]),
            safe_float(daily["relative_humidity_2m_mean"][i]),
            safe_float(daily["precipitation_sum"][i]),
            safe_float(daily["wind_speed_10m_mean"][i]),
            ingestion_time.replace(tzinfo=None),
            SOURCE_NAME,
            BATCH_ID,
            raw_file
        ))

    # Brief pause between requests
    time.sleep(0.2)

print("Locations processed:", len(location_rows))
print("Raw payload files expected:", len(location_rows))
print("Daily observations collected:", len(weather_records))

# COMMAND ----------

# convert the extracted data into spark dataframe
weather_schema = StructType([
    StructField("weather_id", StringType(), False),
    StructField("location_id", StringType(), False),
    StructField("weather_date", DateType(), False),
    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_percentage", DoubleType(), True),
    StructField("rainfall_mm", DoubleType(), True),
    StructField("wind_speed_kmph", DoubleType(), True),
    StructField("_ingestion_timestamp", TimestampType(), False),
    StructField("_source", StringType(), False),
    StructField("_batch_id", StringType(), False),
    StructField("_raw_payload_file", StringType(), False)
])

weather_api_df = spark.createDataFrame(
    weather_records,
    schema=weather_schema
)

print("Weather DataFrame created.")
print("Total records:", weather_api_df.count())

display(weather_api_df.limit(20))

# COMMAND ----------

# validate the weather data
expected_days = (
    date.fromisoformat(END_DATE)
    - date.fromisoformat(START_DATE)
).days + 1

expected_records = len(location_rows) * expected_days

total_records = weather_api_df.count()

unique_weather_ids = (
    weather_api_df
    .select("weather_id")
    .distinct()
    .count()
)

duplicate_location_days = (
    weather_api_df
    .groupBy("location_id", "weather_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Expected records:", expected_records)
print("Actual records:", total_records)
print("Unique weather IDs:", unique_weather_ids)
print("Duplicate location-date pairs:", duplicate_location_days)


# COMMAND ----------

# check references and quality rules
invalid_locations = (
    weather_api_df
    .join(
        locations_df.select("location_id"),
        on="location_id",
        how="left_anti"
    )
    .count()
)

invalid_humidity = weather_api_df.filter(
    (F.col("humidity_percentage") < 0)
    | (F.col("humidity_percentage") > 100)
).count()

invalid_rainfall = weather_api_df.filter(
    F.col("rainfall_mm") < 0
).count()

invalid_wind_speed = weather_api_df.filter(
    F.col("wind_speed_kmph") < 0
).count()

invalid_dates = weather_api_df.filter(
    (F.col("weather_date") < F.lit(START_DATE).cast("date"))
    | (F.col("weather_date") > F.lit(END_DATE).cast("date"))
).count()

print("Invalid location references:", invalid_locations)
print("Invalid humidity values:", invalid_humidity)
print("Negative rainfall values:", invalid_rainfall)
print("Negative wind speeds:", invalid_wind_speed)
print("Dates outside requested range:", invalid_dates)


# COMMAND ----------

weather_api_df.select(
    *[
        F.sum(
            F.col(column).isNull().cast("int")
        ).alias(column)
        for column in [
            "temperature_c",
            "humidity_percentage",
            "rainfall_mm",
            "wind_speed_kmph"
        ]
    ]
).show()

# COMMAND ----------

# save the api data to our volume
weather_api_df.write \
    .mode("overwrite") \
    .json(daily_observations_path)

print("Daily weather observations saved.")
display(dbutils.fs.ls(daily_observations_path))

# COMMAND ----------

# verify the saved response and raw api response
weather_saved_df = (
    spark.read
    .schema(weather_schema)
    .json(daily_observations_path)
)

raw_payload_files = [
    item for item in dbutils.fs.ls(raw_payload_path)
    if item.name.endswith(".json")
]

print("Saved daily observations:", weather_saved_df.count())
print("Saved raw API payload files:", len(raw_payload_files))

display(weather_saved_df.limit(10))
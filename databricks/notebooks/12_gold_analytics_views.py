# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# STEP 1: Create business-facing Gold views
# ============================================================

view_definitions = [

    # --------------------------------------------------------
    # VIEW 1: Crop performance
    # --------------------------------------------------------
    (
        "vw_crop_performance",
        """
        CREATE OR REPLACE VIEW krishiflow.gold.vw_crop_performance AS

        WITH harvest_metrics AS (
            SELECT
                crop_id,
                COUNT(DISTINCT harvest_id) AS harvest_events,
                COUNT(DISTINCT farm_id) AS farms_contributing,
                SUM(quantity_harvested_kg) AS total_harvested_kg,
                SUM(area_harvested_acres) AS total_area_harvested_acres
            FROM krishiflow.silver.harvest
            GROUP BY crop_id
        ),

        collection_metrics AS (
            SELECT
                h.crop_id,
                COUNT(DISTINCT c.collection_id) AS collection_events,
                SUM(c.quantity_received_kg) AS total_collected_kg
            FROM krishiflow.silver.collection c
            JOIN krishiflow.silver.harvest h
                ON c.harvest_id = h.harvest_id
            GROUP BY h.crop_id
        ),

        sales_metrics AS (
            SELECT
                crop_id,
                COUNT(DISTINCT sale_id) AS sales_events,
                SUM(quantity_sold_kg) AS total_sold_kg,
                SUM(revenue) AS total_sales_revenue
            FROM krishiflow.silver.sale
            GROUP BY crop_id
        )

        SELECT
            c.crop_id,
            c.crop_name,
            c.crop_category,

            COALESCE(h.harvest_events, 0) AS harvest_events,
            COALESCE(h.farms_contributing, 0) AS farms_contributing,
            COALESCE(h.total_harvested_kg, 0) AS total_harvested_kg,
            COALESCE(h.total_area_harvested_acres, 0)
                AS total_area_harvested_acres,

            CASE
                WHEN h.total_area_harvested_acres > 0
                THEN ROUND(
                    h.total_harvested_kg
                    / h.total_area_harvested_acres,
                    2
                )
            END AS yield_kg_per_acre,

            COALESCE(cm.collection_events, 0) AS collection_events,
            COALESCE(cm.total_collected_kg, 0) AS total_collected_kg,

            COALESCE(sm.sales_events, 0) AS sales_events,
            COALESCE(sm.total_sold_kg, 0) AS total_sold_kg,
            COALESCE(sm.total_sales_revenue, 0) AS total_sales_revenue

        FROM krishiflow.gold.dim_crop c

        LEFT JOIN harvest_metrics h
            ON c.crop_id = h.crop_id

        LEFT JOIN collection_metrics cm
            ON c.crop_id = cm.crop_id

        LEFT JOIN sales_metrics sm
            ON c.crop_id = sm.crop_id
        """
    ),

    # --------------------------------------------------------
    # VIEW 2: Storage performance
    # --------------------------------------------------------
    (
        "vw_storage_performance",
        """
        CREATE OR REPLACE VIEW krishiflow.gold.vw_storage_performance AS

        SELECT
            st.storage_transaction_id,
            st.collection_id,

            st.storage_facility_id,
            ds.storage_name,
            ds.storage_type,
            ds.capacity_kg,

            loc.location_id,
            loc.state,
            loc.district,
            loc.city,

            st.entry_date,
            st.exit_date,

            st.quantity_in_kg,
            st.quantity_out_kg,

            ROUND(
                st.quantity_in_kg - st.quantity_out_kg,
                2
            ) AS storage_quantity_difference_kg,

            CASE
                WHEN st.quantity_in_kg > 0
                THEN ROUND(
                    (
                        st.quantity_in_kg - st.quantity_out_kg
                    ) / st.quantity_in_kg * 100,
                    2
                )
            END AS storage_quantity_difference_pct,

            CASE
                WHEN st.exit_date IS NOT NULL
                THEN DATEDIFF(st.exit_date, st.entry_date)
            END AS storage_days

        FROM krishiflow.silver.storage_transaction st

        LEFT JOIN krishiflow.gold.dim_storage ds
            ON st.storage_facility_id = ds.storage_facility_id

        LEFT JOIN krishiflow.gold.dim_location loc
            ON ds.location_id = loc.location_id
        """
    ),

    # --------------------------------------------------------
    # VIEW 3: Transport performance
    # --------------------------------------------------------
    (
        "vw_transport_performance",
        """
        CREATE OR REPLACE VIEW krishiflow.gold.vw_transport_performance AS

        SELECT
            t.transport_id,
            t.storage_transaction_id,

            t.crop_id,
            c.crop_name,

            t.market_id,
            m.market_name,

            t.source_location_id,
            src.state AS source_state,
            src.district AS source_district,
            src.city AS source_city,

            t.destination_location_id,
            dst.state AS destination_state,
            dst.district AS destination_district,
            dst.city AS destination_city,

            t.dispatch_timestamp,
            t.arrival_timestamp,

            t.distance_km,
            t.quantity_dispatched_kg,
            t.quantity_received_kg,

            ROUND(
                t.quantity_dispatched_kg - t.quantity_received_kg,
                2
            ) AS transport_quantity_difference_kg,

            CASE
                WHEN t.quantity_dispatched_kg > 0
                THEN ROUND(
                    (
                        t.quantity_dispatched_kg
                        - t.quantity_received_kg
                    ) / t.quantity_dispatched_kg * 100,
                    2
                )
            END AS transport_quantity_difference_pct,

            ROUND(
                (
                    UNIX_TIMESTAMP(t.arrival_timestamp)
                    - UNIX_TIMESTAMP(t.dispatch_timestamp)
                ) / 3600.0,
                2
            ) AS transit_hours

        FROM krishiflow.silver.transport t

        LEFT JOIN krishiflow.gold.dim_crop c
            ON t.crop_id = c.crop_id

        LEFT JOIN krishiflow.gold.dim_market m
            ON t.market_id = m.market_id

        LEFT JOIN krishiflow.gold.dim_location src
            ON t.source_location_id = src.location_id

        LEFT JOIN krishiflow.gold.dim_location dst
            ON t.destination_location_id = dst.location_id
        """
    ),

    # --------------------------------------------------------
    # VIEW 4: Market-price trends
    # --------------------------------------------------------
    (
        "vw_market_price_trend",
        """
        CREATE OR REPLACE VIEW krishiflow.gold.vw_market_price_trend AS

        SELECT
            arrival_date,
            state,
            district,
            market,
            commodity,

            COUNT(*) AS observation_count,
            COUNT(DISTINCT variety) AS variety_count,
            COUNT(DISTINCT grade) AS grade_count,

            MIN(min_price) AS lowest_observed_price,
            MAX(max_price) AS highest_observed_price,

            ROUND(AVG(min_price), 2) AS average_min_price,
            ROUND(AVG(max_price), 2) AS average_max_price,
            ROUND(AVG(modal_price), 2) AS average_modal_price,

            PERCENTILE_APPROX(
                modal_price, 0.5, 10000
            ) AS median_modal_price,

            ROUND(
                AVG(max_price - min_price),
                2
            ) AS average_price_spread

        FROM krishiflow.silver.market_price

        GROUP BY
            arrival_date,
            state,
            district,
            market,
            commodity
        """
    ),

    # --------------------------------------------------------
    # VIEW 5: End-to-end supply-chain quantity differences
    # Grain: one row per harvest_id
    # --------------------------------------------------------
    (
        "vw_supply_chain_loss",
        """
        CREATE OR REPLACE VIEW krishiflow.gold.vw_supply_chain_loss AS

        WITH harvest_base AS (
            SELECT
                harvest_id,
                farm_id,
                crop_id,
                harvest_date,
                quantity_harvested_kg,
                area_harvested_acres
            FROM krishiflow.silver.harvest
        ),

        collection_by_harvest AS (
            SELECT
                harvest_id,
                COUNT(DISTINCT collection_id) AS collection_events,
                SUM(quantity_received_kg) AS quantity_collected_kg
            FROM krishiflow.silver.collection
            GROUP BY harvest_id
        ),

        storage_by_harvest AS (
            SELECT
                c.harvest_id,
                COUNT(DISTINCT st.storage_transaction_id)
                    AS storage_events,
                SUM(st.quantity_in_kg) AS quantity_stored_in_kg,
                SUM(st.quantity_out_kg) AS quantity_stored_out_kg
            FROM krishiflow.silver.storage_transaction st
            JOIN krishiflow.silver.collection c
                ON st.collection_id = c.collection_id
            GROUP BY c.harvest_id
        ),

        transport_by_harvest AS (
            SELECT
                c.harvest_id,
                COUNT(DISTINCT t.transport_id) AS transport_events,
                SUM(t.quantity_dispatched_kg) AS quantity_dispatched_kg,
                SUM(t.quantity_received_kg) AS quantity_transport_received_kg
            FROM krishiflow.silver.transport t
            JOIN krishiflow.silver.storage_transaction st
                ON t.storage_transaction_id = st.storage_transaction_id
            JOIN krishiflow.silver.collection c
                ON st.collection_id = c.collection_id
            GROUP BY c.harvest_id
        ),

        sales_by_harvest AS (
            SELECT
                c.harvest_id,
                COUNT(DISTINCT s.sale_id) AS sales_events,
                SUM(s.quantity_sold_kg) AS quantity_sold_kg,
                SUM(s.revenue) AS sales_revenue
            FROM krishiflow.silver.sale s
            JOIN krishiflow.silver.transport t
                ON s.transport_id = t.transport_id
            JOIN krishiflow.silver.storage_transaction st
                ON t.storage_transaction_id = st.storage_transaction_id
            JOIN krishiflow.silver.collection c
                ON st.collection_id = c.collection_id
            GROUP BY c.harvest_id
        )

        SELECT
            h.harvest_id,
            h.harvest_date,

            h.farm_id,
            f.farmer_id,

            h.crop_id,
            crop.crop_name,
            crop.crop_category,

            h.quantity_harvested_kg,
            h.area_harvested_acres,

            COALESCE(c.collection_events, 0) AS collection_events,
            COALESCE(c.quantity_collected_kg, 0)
                AS quantity_collected_kg,

            ROUND(
                h.quantity_harvested_kg
                - COALESCE(c.quantity_collected_kg, 0),
                2
            ) AS harvest_to_collection_difference_kg,

            COALESCE(st.storage_events, 0) AS storage_events,
            COALESCE(st.quantity_stored_in_kg, 0)
                AS quantity_stored_in_kg,
            COALESCE(st.quantity_stored_out_kg, 0)
                AS quantity_stored_out_kg,

            ROUND(
                COALESCE(st.quantity_stored_in_kg, 0)
                - COALESCE(st.quantity_stored_out_kg, 0),
                2
            ) AS storage_quantity_difference_kg,

            COALESCE(t.transport_events, 0) AS transport_events,
            COALESCE(t.quantity_dispatched_kg, 0)
                AS quantity_dispatched_kg,
            COALESCE(t.quantity_transport_received_kg, 0)
                AS quantity_transport_received_kg,

            ROUND(
                COALESCE(t.quantity_dispatched_kg, 0)
                - COALESCE(t.quantity_transport_received_kg, 0),
                2
            ) AS transport_quantity_difference_kg,

            COALESCE(s.sales_events, 0) AS sales_events,
            COALESCE(s.quantity_sold_kg, 0) AS quantity_sold_kg,
            COALESCE(s.sales_revenue, 0) AS sales_revenue,

            ROUND(
                COALESCE(t.quantity_transport_received_kg, 0)
                - COALESCE(s.quantity_sold_kg, 0),
                2
            ) AS transport_received_minus_sold_kg,

            ROUND(
                h.quantity_harvested_kg
                - COALESCE(s.quantity_sold_kg, 0),
                2
            ) AS harvest_to_sale_quantity_difference_kg

        FROM harvest_base h

        LEFT JOIN collection_by_harvest c
            ON h.harvest_id = c.harvest_id

        LEFT JOIN storage_by_harvest st
            ON h.harvest_id = st.harvest_id

        LEFT JOIN transport_by_harvest t
            ON h.harvest_id = t.harvest_id

        LEFT JOIN sales_by_harvest s
            ON h.harvest_id = s.harvest_id

        LEFT JOIN krishiflow.gold.dim_farm f
            ON h.farm_id = f.farm_id

        LEFT JOIN krishiflow.gold.dim_crop crop
            ON h.crop_id = crop.crop_id
        """
    )
]

# ============================================================
# STEP 2: Create or replace the views
# ============================================================

for view_name, sql_text in view_definitions:
    print("Creating:", view_name)
    spark.sql(sql_text)
    print("SUCCESS:", "krishiflow.gold." + view_name)

print("\nAll five Gold analytics views created.")

# ============================================================
# STEP 3: Verify the views
# ============================================================

view_names = [
    "vw_crop_performance",
    "vw_storage_performance",
    "vw_transport_performance",
    "vw_market_price_trend",
    "vw_supply_chain_loss"
]

view_results = []

for view_name in view_names:
    full_name = "krishiflow.gold." + view_name

    view_results.append(
        (view_name, spark.table(full_name).count(), "SUCCESS")
    )

summary_df = spark.createDataFrame(
    view_results,
    ["view_name", "record_count", "status"]
)

display(summary_df)

# COMMAND ----------

# inspect the views
display(
    spark.table(
        "krishiflow.gold.vw_crop_performance"
    ).orderBy(
        F.desc("total_harvested_kg")
    ).limit(10)
)

display(
    spark.table(
        "krishiflow.gold.vw_supply_chain_loss"
    ).limit(10)
)

display(
    spark.table(
        "krishiflow.gold.vw_market_price_trend"
    ).orderBy(
        F.desc("arrival_date")
    ).limit(20)
)
# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Build the Big Beautiful Table (BBT)
# MAGIC
# MAGIC Merges all internal and external silver-layer data into a single wide
# MAGIC denormalized table for pricing model training.
# MAGIC
# MAGIC **Sources:**
# MAGIC - Internal: `internal_commercial_policies`, `internal_claims_history`, `internal_quote_history`
# MAGIC - External (silver): `silver_market_pricing_benchmark`, `silver_geospatial_hazard_enrichment`, `silver_credit_bureau_summary`
# MAGIC
# MAGIC **Output:** `commercial_bbt_live` — the Big Beautiful Table

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_bbt")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load all source tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, lit, round as spark_round, expr, log, greatest, least

# Internal tables (already in Databricks)
policies = spark.table(f"{fqn}.internal_commercial_policies")
claims = spark.table(f"{fqn}.internal_claims_history")
quotes = spark.table(f"{fqn}.internal_quote_history")

# External silver tables
market = spark.table(f"{fqn}.silver_market_pricing_benchmark")
geo = spark.table(f"{fqn}.silver_geospatial_hazard_enrichment")
bureau = spark.table(f"{fqn}.silver_credit_bureau_summary")

print(f"Policies: {policies.count()}, Claims: {claims.count()}, Quotes: {quotes.count()}")
print(f"Market: {market.count()}, Geo: {geo.count()}, Bureau: {bureau.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Aggregate claims to policy level

# COMMAND ----------

claims_agg = (claims
    .groupBy("policy_id")
    .agg(
        F.count("claim_id").alias("claim_count_5y"),
        F.sum("incurred_amount").alias("total_incurred_5y"),
        F.sum("paid_amount").alias("total_paid_5y"),
        F.sum("reserve").alias("total_reserve_5y"),
        F.countDistinct("peril").alias("distinct_perils"),
        F.max("loss_date").alias("last_claim_date"),
        F.sum(when(col("status") == "Open", 1).otherwise(0)).alias("open_claims_count"),
        # Peril-level breakdowns
        F.sum(when(col("peril") == "Fire", col("incurred_amount")).otherwise(0)).alias("fire_incurred"),
        F.sum(when(col("peril") == "Flood", col("incurred_amount")).otherwise(0)).alias("flood_incurred"),
        F.sum(when(col("peril") == "Theft", col("incurred_amount")).otherwise(0)).alias("theft_incurred"),
        F.sum(when(col("peril") == "Liability", col("incurred_amount")).otherwise(0)).alias("liability_incurred"),
        F.sum(when(col("peril") == "Storm", col("incurred_amount")).otherwise(0)).alias("storm_incurred"),
        F.sum(when(col("peril") == "Subsidence", col("incurred_amount")).otherwise(0)).alias("subsidence_incurred"),
        F.sum(when(col("peril") == "Escape of Water", col("incurred_amount")).otherwise(0)).alias("water_incurred"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Aggregate quotes to policy level

# COMMAND ----------

quotes_agg = (quotes
    .filter(col("converted") == "Y")
    .filter(col("policy_id").isNotNull())
    .groupBy("policy_id")
    .agg(
        F.count("quote_id").alias("quote_count"),
        F.avg("quoted_premium").alias("avg_quoted_premium"),
        F.min("quoted_premium").alias("min_quoted_premium"),
        F.max("quoted_premium").alias("max_quoted_premium"),
        F.sum(when(col("competitor_quoted") == "Y", 1).otherwise(0)).alias("competitor_quote_count"),
        F.max("quote_date").alias("last_quote_date"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Map postcodes to regions for market join

# COMMAND ----------

# Simple postcode-to-region mapping for the synthetic data
postcode_region_map = {
    "EC1": "London", "EC2": "London", "SW1": "London", "SE1": "London",
    "W1": "London", "N1": "London", "E1": "London",
    "M1": "North West", "M2": "North West",
    "B1": "Midlands", "B2": "Midlands",
    "LS1": "Yorkshire", "LS2": "Yorkshire",
    "L1": "North West", "L2": "North West",
    "CF1": "Wales",
    "EH1": "Scotland",
    "G1": "Scotland",
    "BS1": "South West",
    "NG1": "Midlands",
}

# Build mapping as DataFrame
region_rows = [(k, v) for k, v in postcode_region_map.items()]
region_df = spark.createDataFrame(region_rows, ["postcode_prefix", "region"])

# Extract postcode prefix (letters before digits) from postcode_sector
policies_with_region = (policies
    .withColumn("postcode_prefix", F.regexp_extract("postcode_sector", r"^([A-Z]+\d)", 1))
    .join(region_df, "postcode_prefix", "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: SIC code to risk tier mapping

# COMMAND ----------

sic_risk_map = {
    "1011": "Medium",   # Food processing
    "2562": "Medium",   # Machining
    "4110": "Medium",   # Construction
    "4520": "Medium",   # Vehicle maintenance
    "4711": "Low",      # Retail (non-specialised)
    "5610": "Medium",   # Restaurants
    "6201": "Low",      # Computer programming
    "6311": "Low",      # Data processing
    "6499": "Low",      # Financial services
    "6820": "Low",      # Real estate
    "7022": "Low",      # Management consultancy
    "7112": "Low",      # Engineering activities
    "8010": "High",     # Security
    "8622": "Medium",   # Medical practice
    "9311": "Medium",   # Sports facilities
}

sic_rows = [(k, v) for k, v in sic_risk_map.items()]
sic_df = spark.createDataFrame(sic_rows, ["sic_code", "industry_risk_tier"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Join everything into the BBT

# COMMAND ----------

# Build the market join key
policies_enriched = (policies_with_region
    .withColumn("market_join_key", F.concat(col("sic_code"), lit("_"), col("region")))
)

bbt = (policies_enriched
    # Claims aggregation
    .join(claims_agg, "policy_id", "left")
    # Quote aggregation
    .join(quotes_agg, "policy_id", "left")
    # Market intelligence (join on SIC + region)
    .join(
        market.select("match_key_sic_region", "market_median_rate", "competitor_a_min_premium",
                       "price_index_trend", "competitor_ratio"),
        col("market_join_key") == col("match_key_sic_region"),
        "left"
    )
    # Geospatial hazard (join on postcode)
    .join(
        geo.select("postcode_sector", "flood_zone_rating", "proximity_to_fire_station_km",
                    "crime_theft_index", "subsidence_risk", "composite_location_risk", "location_risk_tier"),
        policies_enriched.postcode_sector == geo.postcode_sector,
        "left"
    )
    .drop(geo.postcode_sector)
    # Credit bureau (join on policy_id)
    .join(
        bureau.select("policy_id", "credit_score", "ccj_count", "years_trading",
                       "director_changes", "credit_risk_tier", "business_stability_score"),
        "policy_id",
        "left"
    )
    # SIC risk tier
    .join(sic_df, "sic_code", "left")
    # Drop intermediate columns
    .drop("postcode_prefix", "market_join_key", "match_key_sic_region")
)

print(f"BBT columns: {len(bbt.columns)}")
bbt.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Generate synthetic/derived features
# MAGIC These simulate the 200+ bureau and geo proxy columns that would exist in a real BBT.

# COMMAND ----------

import hashlib

def deterministic_hash(seed_col, feature_name, min_val, max_val):
    """Generate a deterministic numeric feature from a seed column and feature name."""
    return (F.abs(F.hash(F.concat(seed_col, lit(feature_name)))) % (max_val - min_val) + min_val)

def deterministic_hash_double(seed_col, feature_name, min_val, max_val):
    """Generate a deterministic double feature."""
    return spark_round(
        (F.abs(F.hash(F.concat(seed_col, lit(feature_name)))) % ((max_val - min_val) * 100)) / 100.0 + min_val,
        2
    )

# Synthetic Bureau Features (credit/financial proxies)
bureau_features = {
    "credit_default_probability": (0.01, 0.35),
    "director_stability_score": (10, 100),
    "payment_history_score": (200, 900),
    "trade_credit_utilisation_pct": (0, 100),
    "debt_to_equity_ratio": (0.1, 5.0),
    "working_capital_ratio": (0.5, 3.5),
    "revenue_growth_3y_pct": (-30, 80),
    "employee_count_est": (1, 500),
    "industry_default_rate_pct": (0.5, 12.0),
    "supplier_concentration_score": (10, 100),
    "invoice_dispute_rate_pct": (0, 15),
    "bank_account_stability_months": (6, 240),
    "registered_charges_count": (0, 10),
    "profit_margin_est_pct": (-10, 40),
    "asset_tangibility_ratio": (0.1, 0.95),
    "interest_coverage_ratio": (0.5, 15.0),
    "accounts_filed_on_time": (0, 1),
    "company_age_months": (1, 600),
    "sector_bankruptcy_rate_pct": (0.5, 8.0),
    "management_experience_score": (10, 100),
}

# Synthetic Geo Features (location/environmental proxies)
geo_features = {
    "distance_to_coast_km": (0.1, 200.0),
    "local_unemployment_rate_pct": (2.0, 12.0),
    "traffic_density_index": (10, 100),
    "air_quality_index": (1, 10),
    "average_property_value_k": (80, 1200),
    "population_density_per_km2": (50, 15000),
    "commercial_density_score": (1, 100),
    "historic_flood_events_10y": (0, 15),
    "elevation_metres": (0, 500),
    "distance_to_hospital_km": (0.5, 40.0),
    "distance_to_motorway_km": (0.1, 50.0),
    "green_space_pct": (2, 60),
    "noise_pollution_index": (20, 90),
    "broadband_speed_mbps": (5, 1000),
    "listed_building_density": (0, 50),
    "average_wind_speed_mph": (5, 30),
    "annual_rainfall_mm": (400, 1500),
    "soil_clay_content_pct": (5, 70),
    "radon_risk_level": (1, 5),
    "tree_cover_pct": (1, 45),
}

# Apply synthetic features
for feat_name, (min_v, max_v) in bureau_features.items():
    if isinstance(min_v, float) or isinstance(max_v, float):
        bbt = bbt.withColumn(feat_name, deterministic_hash_double(col("policy_id"), feat_name, min_v, max_v))
    else:
        bbt = bbt.withColumn(feat_name, deterministic_hash(col("policy_id"), feat_name, min_v, max_v))

for feat_name, (min_v, max_v) in geo_features.items():
    if isinstance(min_v, float) or isinstance(max_v, float):
        bbt = bbt.withColumn(feat_name, deterministic_hash_double(col("postcode_sector"), feat_name, min_v, max_v))
    else:
        bbt = bbt.withColumn(feat_name, deterministic_hash(col("postcode_sector"), feat_name, min_v, max_v))

print(f"BBT columns after synthetic expansion: {len(bbt.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Add derived pricing features

# COMMAND ----------

bbt = (bbt
    # Loss ratio proxy
    .withColumn("loss_ratio_5y",
        spark_round(
            when(col("current_premium") > 0,
                 col("total_incurred_5y") / (col("current_premium") * 5))
            .otherwise(None), 3))
    # Premium per £1k sum insured
    .withColumn("rate_per_1k_si",
        spark_round(
            when(col("sum_insured") > 0,
                 col("current_premium") / (col("sum_insured") / 1000))
            .otherwise(None), 2))
    # Market position (our rate vs market median)
    .withColumn("market_position_ratio",
        spark_round(
            when(col("market_median_rate").isNotNull() & (col("market_median_rate") > 0),
                 col("rate_per_1k_si") / col("market_median_rate"))
            .otherwise(None), 3))
    # Building age
    .withColumn("building_age_years", lit(2026) - col("year_built"))
    # Combined risk score (weighted blend of location, credit, industry)
    .withColumn("combined_risk_score",
        spark_round(
            (F.coalesce(col("composite_location_risk"), lit(5.0)) * 0.35) +
            (F.coalesce(lit(10) - (col("credit_score") - 200) / 70.0, lit(5.0)) * 0.30) +
            (when(col("industry_risk_tier") == "High", 8.0)
             .when(col("industry_risk_tier") == "Medium", 5.0)
             .otherwise(2.5) * 0.20) +
            (when(col("claim_count_5y") > 3, 8.0)
             .when(col("claim_count_5y") > 1, 5.0)
             .when(col("claim_count_5y") > 0, 3.0)
             .otherwise(1.0) * 0.15),
            2))
    # Audit metadata
    .withColumn("last_updated_by", lit("system_bbt_builder"))
    .withColumn("approval_timestamp", F.current_timestamp())
    .withColumn("source_version", lit("v1.0"))
    .withColumn("bbt_build_timestamp", F.current_timestamp())
)

print(f"Final BBT columns: {len(bbt.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Write the BBT

# COMMAND ----------

bbt.write.mode("overwrite").saveAsTable(f"{fqn}.commercial_bbt_live")

row_count = spark.table(f"{fqn}.commercial_bbt_live").count()
col_count = len(spark.table(f"{fqn}.commercial_bbt_live").columns)
print(f"✓ {fqn}.commercial_bbt_live — {row_count:,} rows × {col_count} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create initial monthly snapshot

# COMMAND ----------

from datetime import datetime

snapshot_suffix = datetime.now().strftime("%Y_%m")
snapshot_table = f"{fqn}.commercial_bbt_{snapshot_suffix}"

spark.sql(f"CREATE TABLE IF NOT EXISTS {snapshot_table} DEEP CLONE {fqn}.commercial_bbt_live")
print(f"✓ Snapshot: {snapshot_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        count(*) as total_rows,
        count(DISTINCT policy_id) as unique_policies,
        count(DISTINCT sic_code) as unique_sic_codes,
        count(DISTINCT postcode_sector) as unique_postcodes,
        avg(current_premium) as avg_premium,
        avg(loss_ratio_5y) as avg_loss_ratio,
        avg(combined_risk_score) as avg_risk_score
    FROM {fqn}.commercial_bbt_live
"""))

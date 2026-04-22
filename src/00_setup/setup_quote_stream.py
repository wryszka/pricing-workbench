# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Commercial Quote Stream (live capture simulator)
# MAGIC
# MAGIC Generates synthetic data for the "quote stream" story: the live traffic flowing
# MAGIC
# MAGIC ```
# MAGIC Sales channel  →  API  →  Rating Engine  →  API  →  Sales channel
# MAGIC ```
# MAGIC
# MAGIC Three JSON payloads are captured per transaction and landed in Unity Catalog
# MAGIC so operators/actuaries can investigate anomalous quotes without copy-pasting
# MAGIC JSON out of Notepad. A flattened silver table provides the analytics view.
# MAGIC
# MAGIC **Tables:**
# MAGIC - `raw_quote_sales_requests`    — incoming request from the sales channel (JSON)
# MAGIC - `raw_quote_engine_requests`   — outgoing call to the rating engine (JSON)
# MAGIC - `raw_quote_engine_responses`  — response with the priced quote (JSON)
# MAGIC - `silver_quote_stream`         — one row per transaction, flat fields for analytics
# MAGIC
# MAGIC **Volume:** `saved_payloads` — operators can export a suspect payload for ad-hoc investigation.
# MAGIC
# MAGIC Includes two seeded outliers that reproduce the canonical "why was this business
# MAGIC charged £48M?" case used to demo the investigate-and-replay workflow.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_upt")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

random.seed(42)

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {fqn}.saved_payloads")
print(f"✓ volume {fqn}.saved_payloads ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference pools — commercial insurance flavour
# MAGIC Reuses the same postcodes and SIC codes as the UPT setup so a future "cross-reference
# MAGIC this transaction against the policy book" story remains possible.

# COMMAND ----------

COMPANY_PREFIXES = ["Ashford", "Bright", "Castle", "Delta", "Eastern", "Foundry", "Grange",
                    "Harbour", "Iron", "Junction", "Kingfield", "Lakeside", "Meridian",
                    "Northstar", "Oakwell", "Pinewood", "Quarry", "Redcliffe", "Summit",
                    "Thornfield", "Uplands", "Vanguard", "Westbridge", "Yardley"]
COMPANY_SUFFIXES = ["Manufacturing Ltd", "Services Ltd", "Trading Ltd", "Holdings Ltd",
                    "Group plc", "Logistics Ltd", "Retail Ltd", "Construction Ltd",
                    "Engineering Ltd", "Hospitality Ltd"]
SIC_DESCRIPTIONS = {
    "1011": ("Food processing",     1.20),
    "2562": ("Machining",           1.30),
    "4110": ("Construction",        1.35),
    "4520": ("Vehicle maintenance", 1.15),
    "4711": ("Retail (general)",    0.95),
    "5610": ("Restaurants",         1.10),
    "6201": ("Computer programming",0.80),
    "6311": ("Data processing",     0.85),
    "6499": ("Financial services",  0.90),
    "6820": ("Real estate",         0.95),
    "7022": ("Management consulting",0.85),
    "7112": ("Engineering",         1.00),
    "8010": ("Security services",   1.40),
    "8622": ("Medical practice",    1.05),
    "9311": ("Sports facilities",   1.10),
}
SIC_CODES = list(SIC_DESCRIPTIONS.keys())

POSTCODE_AREAS = [
    ("EC1", "London",      1.35),
    ("EC2", "London",      1.30),
    ("SW1", "London",      1.40),
    ("SE1", "London",      1.25),
    ("W1",  "London",      1.45),
    ("E1",  "London",      1.20),
    ("M1",  "North West",  1.05),
    ("B1",  "Midlands",    1.00),
    ("LS1", "Yorkshire",   0.95),
    ("L1",  "North West",  1.02),
    ("BS1", "South West",  1.00),
    ("NG1", "Midlands",    0.95),
    ("EH1", "Scotland",    0.90),
    ("G1",  "Scotland",    0.92),
    ("CF1", "Wales",       0.88),
]

CONSTRUCTION_TYPES = ["Fire Resistive", "Non-Combustible", "Joisted Masonry", "Frame", "Heavy Timber"]
ROOF_TYPES         = ["Metal Deck", "Concrete", "Tiled", "Flat Membrane", "Slated"]
FLOOD_ZONES        = ["Low", "Medium", "High"]
PREVIOUS_INSURERS  = ["Aviva", "Zurich", "Allianz", "RSA", "AIG", "QBE", "Hiscox", "None"]
CHANNELS           = ["Direct", "Broker", "Aggregator", "Renewal"]
SALES_USERS        = ["sales.agent.01", "sales.agent.02", "sales.agent.03",
                      "self_service.portal", "broker.portal.uk"]
MODEL_VERSIONS     = ["pricing_v6.2", "pricing_v6.3", "pricing_v7.0_glm", "pricing_v7.1_glm"]
CURRENT_MODEL      = "pricing_v7.1_glm"

# COMMAND ----------

def random_postcode(area: str) -> str:
    """Build a plausible UK postcode sector from an area prefix."""
    return f"{area} {random.randint(1, 9)}{random.choice('ABDEFGHJLNPQRSTUWXYZ')}{random.choice('ABDEFGHJLNPQRSTUWXYZ')}"


def pricing_factors(sic_code: str, construction: str, year_built: int,
                    buildings_si: int, contents_si: int, liability_si: int,
                    flood_zone: str, claims_5y: int,
                    postcode_loading: float, sic_loading: float) -> dict:
    base_building  = buildings_si * 0.0011
    base_contents  = contents_si  * 0.010
    base_liability = liability_si * 0.0004
    base = (base_building + base_contents + base_liability) * postcode_loading * sic_loading

    loadings = {}
    if flood_zone == "High":
        loadings["flood_risk"] = round(base * 0.30, 2)
    elif flood_zone == "Medium":
        loadings["flood_risk"] = round(base * 0.10, 2)
    if year_built < 1930:
        loadings["subsidence_age"] = round(base * 0.07, 2)
    if construction in ("Frame", "Heavy Timber"):
        loadings["construction_risk"] = round(base * 0.08, 2)
    if claims_5y > 0:
        loadings["claims_history"] = round(base * 0.14 * claims_5y, 2)

    discounts = {}
    if claims_5y == 0:
        discounts["no_claims"] = round(base * 0.08, 2)
    if random.random() < 0.25:
        discounts["multi_cover"] = round(base * 0.06, 2)

    return {
        "base_building_premium":  round(base_building,  2),
        "base_contents_premium":  round(base_contents,  2),
        "base_liability_premium": round(base_liability, 2),
        "postcode_loading":       postcode_loading,
        "sic_loading":            sic_loading,
        "base_premium":           round(base, 2),
        "loadings":               loadings,
        "discounts":              discounts,
    }


def build_quote(transaction_id: str, created_at: datetime, force_outlier: bool = False,
                force_dropout: bool = False):
    """Return (sales_req, engine_req, engine_resp, gross_premium).
    If force_dropout: engine_resp is None (journey abandoned before pricing).
    If force_outlier: price explodes to ~£48M via a seeded 'factor_override' anomaly."""
    company = f"{random.choice(COMPANY_PREFIXES)} {random.choice(COMPANY_SUFFIXES)}"
    sic = random.choice(SIC_CODES)
    sic_desc, sic_loading = SIC_DESCRIPTIONS[sic]
    area, region, pc_loading = random.choice(POSTCODE_AREAS)
    postcode = random_postcode(area)
    construction = random.choice(CONSTRUCTION_TYPES)
    year_built = random.randint(1905, 2023)
    flood_zone = random.choices(FLOOD_ZONES, weights=[0.70, 0.22, 0.08])[0]
    claims_5y = random.choices([0, 1, 2, 3, 5], weights=[0.60, 0.22, 0.10, 0.05, 0.03])[0]
    buildings_si  = random.choice([500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000, 25_000_000])
    contents_si   = random.choice([50_000, 100_000, 250_000, 500_000, 1_000_000])
    liability_si  = random.choice([1_000_000, 2_000_000, 5_000_000, 10_000_000])
    channel = random.choice(CHANNELS)
    model_version = random.choice(MODEL_VERSIONS)

    sales_req = {
        "sales_transaction_id": transaction_id,
        "timestamp": created_at.isoformat(),
        "agent_user": random.choice(SALES_USERS),
        "channel": channel,
        "business": {
            "company_name": company,
            "sic_code": sic,
            "sic_description": sic_desc,
            "company_number": f"GB{random.randint(1000000, 9999999)}",
            "years_trading": random.randint(1, 60),
        },
        "property": {
            "address_line_1": f"{random.randint(1, 250)} {random.choice(['Industrial', 'Business', 'Trade', 'Commerce'])} {random.choice(['Park', 'Estate', 'Way', 'Lane'])}",
            "postcode": postcode,
            "region": region,
            "construction_type": construction,
            "year_built": year_built,
            "roof_type": random.choice(ROOF_TYPES),
            "floor_area_sqm": random.randint(150, 15000),
            "flood_zone": flood_zone,
            "sprinklered": random.random() < 0.35,
            "alarmed": random.random() < 0.75,
        },
        "history": {
            "claims_last_5_years": claims_5y,
            "previous_insurer": random.choice(PREVIOUS_INSURERS),
            "bankruptcy_history": random.random() < 0.02,
        },
        "coverage_requested": {
            "buildings_sum_insured":     buildings_si,
            "contents_sum_insured":      contents_si,
            "public_liability_limit":    liability_si,
            "voluntary_excess":          random.choice([1_000, 2_500, 5_000, 10_000]),
            "business_interruption":     random.random() < 0.50,
        },
    }

    engine_req = {
        "quote_reference": f"Q-{uuid.uuid4().hex[:10].upper()}",
        "sales_transaction_id": transaction_id,
        "model_version": model_version,
        "timestamp": created_at.isoformat(),
        "factors": {
            "RATING_FACTOR_POSTCODE_AREA":  area,
            "RATING_FACTOR_REGION":         region,
            "RATING_FACTOR_SIC":            sic,
            "RATING_FACTOR_CONSTRUCTION":   construction,
            "RATING_FACTOR_YEARS_BUILT":    datetime.now().year - year_built,
            "RATING_FACTOR_FLOOR_AREA":     sales_req["property"]["floor_area_sqm"],
            "RATING_FACTOR_FLOOD_ZONE":     flood_zone,
            "RATING_FACTOR_CLAIMS_5Y":      claims_5y,
            "RATING_FACTOR_SPRINKLERED":    sales_req["property"]["sprinklered"],
            "RATING_FACTOR_ALARMED":        sales_req["property"]["alarmed"],
            "RATING_FACTOR_BUILDINGS_SI":   buildings_si,
            "RATING_FACTOR_CONTENTS_SI":    contents_si,
            "RATING_FACTOR_LIABILITY_LIMIT": liability_si,
            "RATING_FACTOR_VOL_EXCESS":     sales_req["coverage_requested"]["voluntary_excess"],
            "RATING_FACTOR_BUSINESS_INTERRUPTION": sales_req["coverage_requested"]["business_interruption"],
            "RATING_FACTOR_CHANNEL":        channel,
        },
    }

    if force_dropout:
        return sales_req, engine_req, None, None

    factors = pricing_factors(sic, construction, year_built, buildings_si, contents_si,
                              liability_si, flood_zone, claims_5y, pc_loading, sic_loading)
    net_premium = factors["base_premium"] + sum(factors["loadings"].values()) - sum(factors["discounts"].values())

    if force_outlier:
        # Seeded £48M-ish anomaly — the case operators need to investigate.
        # In a real incident, this comes from a bad factor override upstream or
        # a corrupted model version. We reproduce the shape, not the cause.
        net_premium = 48_212_560.0
        factors["loadings"]["factor_override_anomaly"] = 48_000_000.0

    ipt = round(net_premium * 0.12, 2)
    gross_premium = round(net_premium + ipt, 2)

    engine_resp = {
        "quote_reference": engine_req["quote_reference"],
        "sales_transaction_id": transaction_id,
        "model_version": model_version,
        "timestamp": (created_at + timedelta(milliseconds=random.randint(150, 900))).isoformat(),
        "pricing": {
            "base_building_premium":  factors["base_building_premium"],
            "base_contents_premium":  factors["base_contents_premium"],
            "base_liability_premium": factors["base_liability_premium"],
            "postcode_loading":       factors["postcode_loading"],
            "sic_loading":            factors["sic_loading"],
            "base_premium":           factors["base_premium"],
            "loadings":               factors["loadings"],
            "discounts":              factors["discounts"],
            "net_premium":            round(net_premium, 2),
            "ipt":                    ipt,
            "gross_premium":          gross_premium,
        },
        "decision": {
            "status":         "QUOTED",
            "decline_reason": None,
            "quote_expiry":   (created_at + timedelta(days=30)).date().isoformat(),
        },
    }

    return sales_req, engine_req, engine_resp, gross_premium

# COMMAND ----------

N_QUOTES = 1000
DROPOUT_RATE = 0.17
OUTLIER_TRANSACTION_IDS = ["TX-BAKERY-48M-2026Q2", "TX-OUTLIER-RETAIL-02"]

base_time = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)

sales_rows, engine_req_rows, engine_resp_rows, silver_rows = [], [], [], []

for i in range(N_QUOTES):
    created_at = base_time + timedelta(minutes=random.randint(0, 30 * 24 * 60))

    if i < len(OUTLIER_TRANSACTION_IDS):
        tx_id = OUTLIER_TRANSACTION_IDS[i]
        force_outlier, force_dropout = True, False
    else:
        tx_id = f"TX-{uuid.uuid4().hex[:8].upper()}"
        force_dropout = random.random() < DROPOUT_RATE
        force_outlier = False

    sales_req, engine_req, engine_resp, gross = build_quote(
        tx_id, created_at, force_outlier=force_outlier, force_dropout=force_dropout)

    sales_rows.append(Row(transaction_id=tx_id, created_at=created_at,
                          payload=json.dumps(sales_req)))
    engine_req_rows.append(Row(transaction_id=tx_id, created_at=created_at,
                               payload=json.dumps(engine_req)))

    bound = False
    if engine_resp is not None:
        engine_resp_rows.append(Row(
            transaction_id=tx_id,
            created_at=datetime.fromisoformat(engine_resp["timestamp"]),
            payload=json.dumps(engine_resp),
        ))
        # Very rich quotes are less likely to convert
        bind_prob = 0.65 if (gross or 0) < 50_000 else 0.45
        bound = random.random() < bind_prob

    status = "ABANDONED" if engine_resp is None else ("BOUND" if bound else "QUOTED")

    silver_rows.append(Row(
        transaction_id=tx_id,
        created_at=created_at,
        channel=sales_req["channel"],
        agent_user=sales_req["agent_user"],
        company_name=sales_req["business"]["company_name"],
        sic_code=sales_req["business"]["sic_code"],
        sic_description=sales_req["business"]["sic_description"],
        postcode=sales_req["property"]["postcode"],
        region=sales_req["property"]["region"],
        construction_type=sales_req["property"]["construction_type"],
        year_built=sales_req["property"]["year_built"],
        floor_area_sqm=sales_req["property"]["floor_area_sqm"],
        flood_zone=sales_req["property"]["flood_zone"],
        claims_last_5y=sales_req["history"]["claims_last_5_years"],
        buildings_si=sales_req["coverage_requested"]["buildings_sum_insured"],
        contents_si=sales_req["coverage_requested"]["contents_sum_insured"],
        liability_si=sales_req["coverage_requested"]["public_liability_limit"],
        model_version=engine_req["model_version"],
        gross_premium=float(gross) if gross is not None else None,
        quote_status=status,
        is_outlier=force_outlier,
    ))

print(f"Generated {len(sales_rows)} sales requests, "
      f"{len(engine_resp_rows)} engine responses, {len(silver_rows)} silver rows "
      f"(outliers: {sum(1 for r in silver_rows if r['is_outlier'])})")

# COMMAND ----------

def _write_raw(rows, table_name):
    schema_ = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("payload", StringType(), False),
    ])
    df = spark.createDataFrame(rows, schema=schema_)
    (df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{fqn}.{table_name}"))
    print(f"✓ {fqn}.{table_name} — {df.count()} rows")


_write_raw(sales_rows,       "raw_quote_sales_requests")
_write_raw(engine_req_rows,  "raw_quote_engine_requests")
_write_raw(engine_resp_rows, "raw_quote_engine_responses")

# COMMAND ----------

silver_df = spark.createDataFrame(silver_rows)
(silver_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{fqn}.silver_quote_stream"))
print(f"✓ {fqn}.silver_quote_stream — {silver_df.count()} rows")

# COMMAND ----------

# Table-level metadata — tags + comment so the stream is discoverable in Catalog Explorer
silver_table = f"{fqn}.silver_quote_stream"
spark.sql(f"""
    ALTER TABLE {silver_table}
    SET TBLPROPERTIES (
        'comment' = 'Live commercial quote stream — one row per transaction, flattened for analytics and Genie. Keyed on transaction_id. Joined to the three raw_quote_* tables for full JSON payloads.'
    )
""")
tags = {
    "business_line":    "commercial_property",
    "pricing_domain":   "quote_stream",
    "refresh_cadence":  "streaming_simulated",
    "demo_environment": "true",
}
tag_sql = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
spark.sql(f"ALTER TABLE {silver_table} SET TAGS ({tag_sql})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT quote_status, COUNT(*) AS n,
           ROUND(AVG(gross_premium), 2) AS avg_gross,
           ROUND(MAX(gross_premium), 2) AS max_gross
    FROM {fqn}.silver_quote_stream
    GROUP BY quote_status
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT transaction_id, company_name, postcode, gross_premium, is_outlier
    FROM {fqn}.silver_quote_stream
    WHERE is_outlier = true
"""))

#!/usr/bin/env python3
"""Rebuild the Modelling Mart dashboard with:
  • distinct colours per bar chart (not all blue)
  • a second 'Trends' page sourced from Delta history + monthly snapshots
PATCH keeps the same dashboard_id so the app doesn't need redeploying."""
import json
import secrets
import subprocess

DASH_ID  = "01f13edd547b1d528507be6f200b7ebc"
TABLE    = "lr_serverless_aws_us_catalog.pricing_upt.unified_pricing_table_live"
WAREHOUSE = "ab79eced8207d29b"

# Monthly snapshots that actually exist — discovered via information_schema.
SNAPSHOTS = [
    ("2026-03", "lr_serverless_aws_us_catalog.pricing_upt.unified_pricing_table_2026_03"),
    ("2026-04", "lr_serverless_aws_us_catalog.pricing_upt.unified_pricing_table_2026_04"),
    ("Live",    "lr_serverless_aws_us_catalog.pricing_upt.unified_pricing_table_live"),
]

# Databricks default palette — used one per bar chart so the dashboard is not all blue.
PALETTE = {
    "amber":  "#FFAB00",
    "green":  "#00A972",
    "red":    "#FF3621",
    "sky":    "#8BCAE7",
    "rose":   "#BF7080",
    "mint":   "#99DDB4",
    "coral":  "#FCA4A1",
}

def uid() -> str:
    return secrets.token_hex(4)

# ---------- widget factories ----------

def counter(dataset_id: str, value_field: str, title: str, number_format: str | None = None) -> dict:
    spec = {
        "version": 2,
        "widgetType": "counter",
        "encodings": {"value": {"fieldName": value_field, "displayName": title}},
        "frame": {"showTitle": True, "title": title},
    }
    if number_format:
        spec["encodings"]["value"]["format"] = number_format
    return {
        "name": uid(),
        "queries": [{
            "name": "main_query",
            "query": {
                "datasetName": dataset_id,
                "fields": [{"name": value_field, "expression": f"`{value_field}`"}],
                "disaggregated": True,
            },
        }],
        "spec": spec,
    }

def bar(dataset_id: str, x_field: str, y_field: str, x_label: str, y_label: str,
        title: str, palette: list[str], sort_by: str = "y-reversed") -> dict:
    """Bar chart with a distinct colour PER CATEGORY — binds x to the color
    encoding so Lakeview cycles the supplied palette across the bars."""
    return {
        "name": uid(),
        "queries": [{
            "name": "main_query",
            "query": {
                "datasetName": dataset_id,
                "fields": [
                    {"name": x_field, "expression": f"`{x_field}`"},
                    {"name": y_field, "expression": f"`{y_field}`"},
                ],
                "disaggregated": True,
            },
        }],
        "spec": {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {"fieldName": x_field, "scale": {"type": "categorical", "sort": {"by": sort_by}}, "displayName": x_label},
                "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_label},
                "color": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_label},
                "label": {"show": True},
            },
            "frame": {"showTitle": True, "title": title},
            "mark": {"colors": palette},
        },
    }

def line(dataset_id: str, x_field: str, y_field: str, x_label: str, y_label: str, title: str, colour: str) -> dict:
    return {
        "name": uid(),
        "queries": [{
            "name": "main_query",
            "query": {
                "datasetName": dataset_id,
                "fields": [
                    {"name": x_field, "expression": f"`{x_field}`"},
                    {"name": y_field, "expression": f"`{y_field}`"},
                ],
                "disaggregated": True,
            },
        }],
        "spec": {
            "version": 3,
            "widgetType": "line",
            "encodings": {
                "x": {"fieldName": x_field, "scale": {"type": "categorical"}, "displayName": x_label},
                "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_label},
                "label": {"show": True},
            },
            "frame": {"showTitle": True, "title": title},
            "mark": {"colors": [colour]},
        },
    }

def table(dataset_id: str, columns: list[tuple[str, str, str]], title: str) -> dict:
    """Table widget — one column per field. Omits numberFormat when the column
    isn't numeric (null values trip the 'visualisation has no fields selected'
    empty-state banner in Lakeview)."""
    cols = []
    for f, display, display_as in columns:
        col = {
            "fieldName":    f,
            "type":         "string" if display_as == "string" else "float",
            "displayAs":    display_as,
            "title":        display,
            "displayName":  display,
            "alignContent": "left" if display_as == "string" else "right",
        }
        if display_as == "number":
            col["numberFormat"] = "0,0"
        cols.append(col)
    return {
        "name": uid(),
        "queries": [{
            "name": "main_query",
            "query": {
                "datasetName": dataset_id,
                "fields": [{"name": f, "expression": f"`{f}`"} for f, _, _ in columns],
                "disaggregated": True,
            },
        }],
        "spec": {
            "version": 1,
            "widgetType": "table",
            "encodings": {"columns": cols},
            "frame": {"showTitle": True, "title": title},
        },
    }

# ---------- datasets ----------

def snapshot_union_rowcount() -> str:
    parts = [f"SELECT '{label}' AS snapshot, COUNT(*) AS rows FROM {tbl}" for label, tbl in SNAPSHOTS]
    return " UNION ALL ".join(parts) + " ORDER BY snapshot"

def snapshot_union_coverage(col: str) -> str:
    parts = [
        f"SELECT '{label}' AS snapshot, ROUND(100.0 * COUNT({col}) / COUNT(*), 2) AS coverage_pct FROM {tbl}"
        for label, tbl in SNAPSHOTS
    ]
    return " UNION ALL ".join(parts) + " ORDER BY snapshot"

# Overview datasets
ds_total_policies = uid(); ds_gwp = uid(); ds_claims = uid(); ds_lr = uid()
ds_by_region = uid(); ds_by_tier = uid(); ds_by_construction = uid(); ds_by_flood = uid()
ds_top_postcodes = uid()
# Trends datasets
ds_trend_rowcount  = uid()
ds_trend_flood     = uid()
ds_trend_credit    = uid()
ds_trend_turnover  = uid()
ds_refresh_history = uid()

datasets = [
    # Overview
    {"name": ds_total_policies, "displayName": "Total policies",
     "queryLines": [f"SELECT COUNT(*) AS policies FROM {TABLE}"]},
    {"name": ds_gwp, "displayName": "Total gross written premium",
     "queryLines": [f"SELECT SUM(current_premium) / 1e6 AS gwp_millions FROM {TABLE}"]},
    {"name": ds_claims, "displayName": "Total incurred claims",
     "queryLines": [f"SELECT SUM(total_incurred_5y) / 1e6 AS claims_millions FROM {TABLE}"]},
    {"name": ds_lr, "displayName": "Portfolio 5-yr loss ratio",
     "queryLines": [f"SELECT ROUND(SUM(total_incurred_5y) * 100.0 / NULLIF(SUM(current_premium), 0), 1) AS loss_ratio_pct FROM {TABLE}"]},
    {"name": ds_by_region, "displayName": "Policies by region",
     "queryLines": [f"SELECT region, COUNT(*) AS policies FROM {TABLE} GROUP BY region ORDER BY policies DESC"]},
    {"name": ds_by_tier, "displayName": "GWP by industry tier",
     "queryLines": [f"SELECT industry_risk_tier AS tier, ROUND(SUM(current_premium) / 1e6, 1) AS gwp_m FROM {TABLE} GROUP BY industry_risk_tier ORDER BY gwp_m DESC"]},
    {"name": ds_by_construction, "displayName": "Policies by construction",
     "queryLines": [f"SELECT construction_type, COUNT(*) AS n FROM {TABLE} GROUP BY construction_type ORDER BY n DESC"]},
    {"name": ds_by_flood, "displayName": "Flood zone distribution",
     "queryLines": [f"SELECT flood_zone_rating AS zone, COUNT(*) AS n FROM {TABLE} GROUP BY flood_zone_rating ORDER BY zone"]},
    {"name": ds_top_postcodes, "displayName": "Top 15 postcodes by GWP",
     "queryLines": [f"""SELECT postcode_sector,
                               COUNT(*) AS policies,
                               ROUND(SUM(current_premium) / 1e6, 2) AS gwp_m,
                               ROUND(SUM(total_incurred_5y) / 1e6, 2) AS claims_m_5y
                       FROM {TABLE}
                       GROUP BY postcode_sector
                       ORDER BY gwp_m DESC
                       LIMIT 15"""]},
    # Trends
    {"name": ds_trend_rowcount, "displayName": "Row count per snapshot",
     "queryLines": [snapshot_union_rowcount()]},
    {"name": ds_trend_flood, "displayName": "Flood zone coverage by snapshot",
     "queryLines": [snapshot_union_coverage("flood_zone_rating")]},
    {"name": ds_trend_credit, "displayName": "Credit score coverage by snapshot",
     "queryLines": [snapshot_union_coverage("credit_score")]},
    {"name": ds_trend_turnover, "displayName": "Annual turnover coverage by snapshot",
     "queryLines": [snapshot_union_coverage("annual_turnover")]},
    {"name": ds_refresh_history, "displayName": "Delta refresh history",
     "queryLines": [f"""SELECT version, timestamp, operation,
                               COALESCE(operationMetrics.numOutputRows, 0) AS rows_written,
                               userName AS refreshed_by
                       FROM (DESCRIBE HISTORY {TABLE})
                       WHERE operation IN ('CREATE OR REPLACE TABLE AS SELECT', 'WRITE', 'MERGE', 'STREAMING UPDATE')
                       ORDER BY version DESC
                       LIMIT 25"""]},
]

# ---------- Overview page ----------
overview_layout = [
    {"widget": counter(ds_total_policies, "policies", "Total policies"),
     "position": {"x": 0, "y": 0, "width": 3, "height": 3}},
    {"widget": counter(ds_gwp, "gwp_millions", "Gross Written Premium (£m)", number_format="0.0"),
     "position": {"x": 3, "y": 0, "width": 3, "height": 3}},
    {"widget": counter(ds_claims, "claims_millions", "Total Incurred Claims — 5yr (£m)", number_format="0.0"),
     "position": {"x": 0, "y": 3, "width": 3, "height": 3}},
    {"widget": counter(ds_lr, "loss_ratio_pct", "Portfolio 5-yr Loss Ratio (%)", number_format="0.0"),
     "position": {"x": 3, "y": 3, "width": 3, "height": 3}},

    # Each bar chart gets a PALETTE passed so Lakeview colours each bar
    # within the chart differently (the 'color' encoding binds to x so
    # categorical values cycle through the palette).
    {"widget": bar(ds_by_region, "region", "policies", "Region", "Policies",
                   "Policies by region",
                   [PALETTE["green"], PALETTE["mint"], PALETTE["sky"], PALETTE["amber"],
                    PALETTE["coral"], PALETTE["rose"], PALETTE["red"]]),
     "position": {"x": 0, "y": 6, "width": 3, "height": 6}},
    {"widget": bar(ds_by_tier, "tier", "gwp_m", "Industry risk tier", "GWP (£m)",
                   "GWP by industry risk tier",
                   [PALETTE["red"], PALETTE["amber"], PALETTE["green"]]),
     "position": {"x": 3, "y": 6, "width": 3, "height": 6}},

    {"widget": bar(ds_by_construction, "construction_type", "n", "Construction", "Policies",
                   "Policies by construction type",
                   [PALETTE["sky"], PALETTE["green"], PALETTE["amber"], PALETTE["red"],
                    PALETTE["rose"], PALETTE["coral"]]),
     "position": {"x": 0, "y": 12, "width": 3, "height": 6}},
    {"widget": bar(ds_by_flood, "zone", "n", "Flood zone rating", "Policies",
                   "Flood zone distribution",
                   [PALETTE["green"], PALETTE["mint"], PALETTE["sky"], PALETTE["amber"],
                    PALETTE["coral"], PALETTE["rose"], PALETTE["red"]],
                   sort_by="x"),
     "position": {"x": 3, "y": 12, "width": 3, "height": 6}},

    {"widget": table(ds_top_postcodes, [
        ("postcode_sector", "Postcode", "string"),
        ("policies",        "Policies", "number"),
        ("gwp_m",           "GWP (£m)", "number"),
        ("claims_m_5y",     "Claims 5yr (£m)", "number"),
     ], "Top 15 postcode sectors by GWP"),
     "position": {"x": 0, "y": 18, "width": 6, "height": 6}},
]

# ---------- Trends page ----------
trends_layout = [
    # Row 1: row count trend — full width
    {"widget": line(ds_trend_rowcount, "snapshot", "rows", "Snapshot", "Rows",
                    "Row count per monthly snapshot", PALETTE["green"]),
     "position": {"x": 0, "y": 0, "width": 6, "height": 5}},

    # Row 2: three coverage line charts side-by-side (key factors — completeness over time)
    {"widget": line(ds_trend_flood, "snapshot", "coverage_pct", "Snapshot", "% non-null",
                    "flood_zone_rating coverage", PALETTE["sky"]),
     "position": {"x": 0, "y": 5, "width": 2, "height": 5}},
    {"widget": line(ds_trend_credit, "snapshot", "coverage_pct", "Snapshot", "% non-null",
                    "credit_score coverage", PALETTE["rose"]),
     "position": {"x": 2, "y": 5, "width": 2, "height": 5}},
    {"widget": line(ds_trend_turnover, "snapshot", "coverage_pct", "Snapshot", "% non-null",
                    "annual_turnover coverage", PALETTE["amber"]),
     "position": {"x": 4, "y": 5, "width": 2, "height": 5}},

    # Row 3: refresh history table full width
    {"widget": table(ds_refresh_history, [
        ("version",       "Version",   "string"),
        ("timestamp",     "Timestamp", "string"),
        ("operation",     "Operation", "string"),
        ("rows_written",  "Rows written", "number"),
        ("refreshed_by",  "Refreshed by", "string"),
     ], "Recent rebuild history (Delta)"),
     "position": {"x": 0, "y": 10, "width": 6, "height": 7}},
]

serialized = {
    "datasets": datasets,
    "pages": [
        {
            "name": uid(),
            "displayName": "Overview",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": overview_layout,
        },
        {
            "name": uid(),
            "displayName": "Trends",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": trends_layout,
        },
    ],
    "uiSettings": {
        "theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"},
        "applyModeEnabled": False,
    },
}

payload = {
    "display_name":        "Modelling Mart — Overview",
    "warehouse_id":        WAREHOUSE,
    "serialized_dashboard": json.dumps(serialized),
}

print("PATCHing dashboard …")
r = subprocess.run(
    ["databricks", "api", "patch", f"/api/2.0/lakeview/dashboards/{DASH_ID}", "--json", json.dumps(payload)],
    capture_output=True, text=True,
)
if r.returncode != 0:
    print("ERROR:", r.stderr or r.stdout)
    raise SystemExit(1)
print("  ok")

print("Re-publishing …")
pub = subprocess.run(
    ["databricks", "api", "post", f"/api/2.0/lakeview/dashboards/{DASH_ID}/published"],
    capture_output=True, text=True,
)
print(f"  exit: {pub.returncode}")
if pub.returncode != 0:
    print("  stderr:", pub.stderr[:300])

print(f"\ndashboard_id unchanged: {DASH_ID}")
print(f"open: https://fevm-lr-serverless-aws-us.cloud.databricks.com/dashboardsv3/{DASH_ID}")

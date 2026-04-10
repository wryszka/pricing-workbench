"""Feature Store status and online store monitoring routes."""

import logging

from fastapi import APIRouter

from server.config import fqn, get_workspace_client, get_workspace_host
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/features", tags=["features"])


@router.get("/status")
async def feature_store_status():
    """Get the status of the online feature store and UPT."""

    upt_table = fqn("unified_pricing_table_live")
    host = get_workspace_host()

    # UPT stats
    try:
        upt_stats = await execute_query(f"""
            SELECT count(*) as row_count,
                   count(DISTINCT policy_id) as unique_policies
            FROM {upt_table}
        """)
        upt_row_count = int(upt_stats[0]["row_count"]) if upt_stats else 0
        upt_policies = int(upt_stats[0]["unique_policies"]) if upt_stats else 0
    except Exception:
        upt_row_count = 0
        upt_policies = 0

    # Delta version
    try:
        history = await execute_query(f"DESCRIBE HISTORY {upt_table} LIMIT 1")
        delta_version = history[0]["version"] if history else "?"
        last_modified = history[0]["timestamp"] if history else "?"
    except Exception:
        delta_version = "?"
        last_modified = "?"

    # Column count
    try:
        cols = await execute_query(f"""
            SELECT count(*) as cnt FROM information_schema.columns
            WHERE table_catalog = '{upt_table.split('.')[0]}'
              AND table_schema = '{upt_table.split('.')[1]}'
              AND table_name = '{upt_table.split('.')[2]}'
        """)
        col_count = int(cols[0]["cnt"]) if cols else 0
    except Exception:
        col_count = 0

    # Online store status
    online_store = None
    try:
        w = get_workspace_client()
        store = w.feature_store.get_online_store("pricing-upt-online-store")
        online_store = {
            "name": store.name,
            "state": str(store.state).split(".")[-1] if store.state else "UNKNOWN",
            "capacity": store.capacity,
            "created": store.creation_time,
        }
    except Exception as e:
        online_store = {
            "name": "pricing-upt-online-store",
            "state": "NOT_CREATED",
            "message": str(e)[:100],
        }

    # Latency results (from test notebook)
    latency = {}
    try:
        lat_results = await execute_query(
            f"SELECT metric, value FROM {fqn('online_store_latency')}"
        )
        for r in lat_results:
            latency[r["metric"]] = float(r["value"])
    except Exception:
        pass

    # Tags
    tags = {}
    try:
        tag_results = await execute_query(f"""
            SELECT tag_name, tag_value
            FROM {upt_table.split('.')[0]}.information_schema.table_tags
            WHERE schema_name = '{upt_table.split('.')[1]}'
              AND table_name = '{upt_table.split('.')[2]}'
        """)
        for r in tag_results:
            tags[r["tag_name"]] = r["tag_value"]
    except Exception:
        pass

    return {
        "upt": {
            "table": upt_table,
            "row_count": upt_row_count,
            "unique_policies": upt_policies,
            "column_count": col_count,
            "delta_version": delta_version,
            "last_modified": last_modified,
            "primary_key": "policy_id",
            "tags": tags,
            "catalog_url": f"{host}/explore/data/{upt_table.replace('.', '/')}",
        },
        "online_store": online_store,
        "latency": latency,
    }

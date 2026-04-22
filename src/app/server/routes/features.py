"""Feature Store status, catalog, and online-store lifecycle routes."""

import logging

from fastapi import APIRouter, HTTPException

from server.config import fqn, get_catalog, get_schema, get_workspace_client, get_workspace_host
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/features", tags=["features"])

ONLINE_STORE_NAME = "pricing-upt-online-store"
UPT_TABLE_KEY = "unified_pricing_table_live"


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


# ---------------------------------------------------------------------------
# Feature catalog — metadata for every feature in the training feature store
# ---------------------------------------------------------------------------

@router.get("/catalog")
async def feature_catalog():
    """Return the feature_catalog table — one row per feature with full provenance.
    Foundation for feature-lineage and audit bolt-ons."""
    try:
        rows = await execute_query(f"""
            SELECT
                feature_name, feature_group, data_type, description,
                source_tables, source_columns, transformation, owner,
                regulatory_sensitive, pii
            FROM {fqn('feature_catalog')}
            ORDER BY feature_group, feature_name
        """)
        groups: dict = {}
        for r in rows:
            g = r.get("feature_group") or "other"
            groups[g] = groups.get(g, 0) + 1
        return {
            "features":    rows,
            "counts_by_group": groups,
            "total":       len(rows),
        }
    except Exception as e:
        logger.warning("feature_catalog query failed: %s", e)
        return {
            "features": [], "counts_by_group": {}, "total": 0,
            "error": f"feature_catalog table missing — run build_feature_catalog. ({str(e)[:120]})",
        }


# ---------------------------------------------------------------------------
# Online store lifecycle — promote (create) / pause (delete)
# ---------------------------------------------------------------------------

@router.post("/online/promote")
async def promote_online():
    """Promote the UPT to the online feature store (Lakebase key-value).
    Creates the online store if it doesn't exist and kicks off a SNAPSHOT publish
    of the UPT. Idempotent."""
    from databricks.sdk.service.ml import OnlineStore, PublishSpec, PublishSpecPublishMode

    upt_table = fqn(UPT_TABLE_KEY)
    steps = []

    try:
        w = get_workspace_client()

        # --- Step 1: ensure store exists ---
        try:
            store = w.feature_store.get_online_store(ONLINE_STORE_NAME)
            state = str(store.state).split(".")[-1] if store.state else "UNKNOWN"
            steps.append(f"Store exists (state: {state}).")
        except Exception:
            store = w.feature_store.create_online_store(
                online_store=OnlineStore(name=ONLINE_STORE_NAME, capacity="CU_1")
            )
            state = str(store.state).split(".")[-1] if store.state else "PROVISIONING"
            steps.append(f"Created online store ({state}) — CU_1 capacity.")

        # --- Step 2: publish UPT to online store (SNAPSHOT) ---
        try:
            result = w.feature_store.publish_table(
                source_table_name=upt_table,
                publish_spec=PublishSpec(
                    online_store=ONLINE_STORE_NAME,
                    online_table_name=upt_table,
                    publish_mode=PublishSpecPublishMode.SNAPSHOT,
                ),
            )
            steps.append(f"Published {upt_table} to {ONLINE_STORE_NAME} (SNAPSHOT).")
        except Exception as pub_err:
            err_s = str(pub_err).lower()
            if "already" in err_s:
                steps.append("UPT was already published to the online store.")
            else:
                steps.append(f"Publish failed: {str(pub_err)[:200]}")

        return {
            "status":       "ok",
            "online_store": ONLINE_STORE_NAME,
            "state":        state,
            "steps":        steps,
            "message":      "Online serving enabled — lookups by policy_id will hit Lakebase.",
        }
    except Exception as e:
        logger.exception("promote_online failed")
        raise HTTPException(500, f"Promote failed: {str(e)[:300]}")


@router.post("/online/pause")
async def pause_online():
    """Delete the online feature store to stop cost. The offline UPT is untouched —
    the online copy can be re-promoted later."""
    try:
        w = get_workspace_client()
        w.feature_store.delete_online_store(ONLINE_STORE_NAME)
        return {
            "status":       "deleted",
            "online_store": ONLINE_STORE_NAME,
            "message":      "Online store deleted. Offline UPT unchanged. Promote again to re-enable low-latency serving.",
        }
    except Exception as e:
        logger.warning("pause_online — assuming already absent: %s", e)
        return {
            "status":       "not_present",
            "online_store": ONLINE_STORE_NAME,
            "message":      "Online store was not provisioned; nothing to pause.",
            "error":        str(e)[:200],
        }

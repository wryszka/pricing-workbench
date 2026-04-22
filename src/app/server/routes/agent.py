"""AI Agent routes (OPTIONAL).

Three agents, all optional, all fully logged and auditable:
1. Model Selection — recommends which models to train
2. DQ Monitor — detects data quality anomalies beyond rule-based checks
3. Explainability — explains pricing shifts in plain English for actuaries
"""

import json
import logging
import os
from datetime import datetime, timezone

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.audit import log_audit_event
from server.config import fqn, get_workspace_client, get_current_user, get_workspace_host
from server.sql import execute_query


def _call_llm(endpoint: str, system_prompt: str, user_prompt: str, max_tokens: int = 4000) -> tuple[bool, str, dict]:
    """Call a Foundation Model API endpoint. Returns (success, response_text, token_usage)."""
    try:
        w = get_workspace_client()
        host = w.config.host.rstrip("/")
        token = w.config._header_factory()  # Gets auth headers

        resp = requests.post(
            f"{host}/serving-endpoints/{endpoint}/invocations",
            headers={**token, "Content-Type": "application/json"},
            json={
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": max_tokens,
                "temperature": 0.1,
            },
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {})
        return True, text, usage
    except Exception as e:
        return False, f"LLM call failed: {e}", {}

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/agent", tags=["agent"])

# Default endpoint — configurable via env var
DEFAULT_MODEL_ENDPOINT = "databricks-claude-sonnet-4-6"

SYSTEM_PROMPT = """You are an expert actuarial pricing AI advisor for a P&C insurance company.
You are analysing a Unified Pricing Table (wide denormalized feature table) to recommend
which pricing models should be trained.

CONTEXT: Commercial property & casualty insurance. The table contains policy data, claims
history, market benchmarks, geospatial risk scores, credit bureau data, and derived features.

IMPORTANT: Keep your response concise. List max 10 features per model. Keep descriptions
to 1-2 sentences each. The entire response must fit in one JSON block.

RESPONSE FORMAT: Return valid JSON with this exact structure:
{
  "recommendations": [
    {
      "model_name": "string - descriptive name",
      "model_type": "GLM_Poisson | GLM_Gamma | GBM_Classifier | GBM_Regressor",
      "target_variable": "column name",
      "purpose": "1-2 sentence description",
      "recommended_features": ["max 10 column names"],
      "feature_rationale": "brief rationale",
      "regulatory_notes": "regulatory considerations",
      "priority": "high | medium | low"
    }
  ],
  "data_quality_observations": ["list of observations"],
  "overall_strategy": "plain English modelling strategy"
}"""


@router.get("/status")
async def agent_status():
    """Check if the AI agent is available."""
    endpoint = os.getenv("AGENT_MODEL_ENDPOINT", DEFAULT_MODEL_ENDPOINT)
    try:
        w = get_workspace_client()
        ep = w.serving_endpoints.get(endpoint)
        ready = ep.state and "READY" in str(ep.state.ready)
        return {
            "available": ready,
            "endpoint": endpoint,
            "message": "AI assistant is ready" if ready else "Endpoint not ready",
        }
    except Exception as e:
        return {
            "available": False,
            "endpoint": endpoint,
            "message": f"AI assistant unavailable: {str(e)[:100]}",
        }


@router.post("/analyze")
async def run_analysis():
    """Run the AI model selection analysis against the current UPT."""
    endpoint = os.getenv("AGENT_MODEL_ENDPOINT", DEFAULT_MODEL_ENDPOINT)
    upt_table = fqn("unified_pricing_table_live")

    # Step 1: Profile the UPT
    try:
        profile = await execute_query(f"""
            SELECT count(*) as row_count,
                   count(DISTINCT policy_id) as unique_policies
            FROM {upt_table}
        """)
        row_count = int(profile[0]["row_count"]) if profile else 0
    except Exception as e:
        raise HTTPException(500, f"Cannot profile UPT: {e}")

    # Get column stats
    cols_info = await execute_query(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = '{upt_table.split('.')[0]}'
          AND table_schema = '{upt_table.split('.')[1]}'
          AND table_name = '{upt_table.split('.')[2]}'
        ORDER BY ordinal_position
    """)

    numeric_cols = [c["column_name"] for c in cols_info
                    if c.get("data_type") in ("DOUBLE", "BIGINT", "INT", "LONG", "FLOAT", "DECIMAL")][:25]

    # Get basic stats for numeric columns
    stats_parts = ", ".join(
        f"ROUND(AVG(CAST({c} AS DOUBLE)), 2) AS `{c}_mean`, "
        f"ROUND(STDDEV(CAST({c} AS DOUBLE)), 2) AS `{c}_std`"
        for c in numeric_cols[:15]
    )
    if stats_parts:
        stats = await execute_query(f"SELECT {stats_parts} FROM {upt_table}")
        stats_dict = stats[0] if stats else {}
    else:
        stats_dict = {}

    # Build profile text
    profile_text = f"Table: {upt_table}\nRows: {row_count:,} | Columns: {len(cols_info)}\n\n"
    profile_text += "Key numeric columns (name | mean | std):\n"
    for c in numeric_cols[:15]:
        mean = stats_dict.get(f"{c}_mean", "?")
        std = stats_dict.get(f"{c}_std", "?")
        profile_text += f"  {c} | mean={mean} | std={std}\n"

    string_cols = [c["column_name"] for c in cols_info if c.get("data_type") == "STRING"][:8]
    for c in string_cols:
        profile_text += f"  {c} | string\n"

    user_prompt = f"""Analyse this pricing feature table and recommend models to train.

{profile_text}

Target variables:
- claim_count_5y: Claims count — FREQUENCY modelling
- total_incurred_5y: Total claims cost — SEVERITY modelling
- loss_ratio_5y: Loss ratio — alternative severity
- (quote history available separately for DEMAND modelling)

Requirements:
1. At least one frequency model (GLM preferred for regulatory)
2. At least one severity model
3. At least one demand/conversion model
4. Consider GBM uplift on GLM residuals
5. Max 25 features per model
6. Suitable for UK/European Solvency II regulatory submission"""

    # Step 2: Call the LLM
    llm_response_text = ""
    llm_success = False
    token_usage = {}

    llm_success, llm_response_text, token_usage = _call_llm(endpoint, SYSTEM_PROMPT, user_prompt, max_tokens=8000)

    # Step 3: Parse recommendations
    recommendations = None
    if llm_success:
        json_text = llm_response_text
        if "```json" in json_text:
            json_text = json_text.split("```json")[1].split("```")[0]
        elif "```" in json_text:
            json_text = json_text.split("```")[1].split("```")[0]
        try:
            recommendations = json.loads(json_text.strip())
        except json.JSONDecodeError:
            pass

    # Step 4: Log to audit trail
    reviewer = get_current_user()
    await log_audit_event(
        event_type="agent_recommendation",
        entity_type="model",
        entity_id="agent_model_selector",
        user_id=reviewer,
        details={
            "model_endpoint": endpoint,
            "llm_success": llm_success,
            "token_usage": token_usage,
            "recommendations_count": len(recommendations.get("recommendations", [])) if recommendations else 0,
            "upt_rows": row_count,
            "upt_columns": len(cols_info),
        },
    )

    return {
        "success": llm_success and recommendations is not None,
        "endpoint": endpoint,
        "token_usage": token_usage,
        "recommendations": recommendations,
        "raw_response_preview": llm_response_text[:2000] if not recommendations else None,
        "profile": {
            "table": upt_table,
            "row_count": row_count,
            "column_count": len(cols_info),
        },
        "transparency": {
            "system_prompt": SYSTEM_PROMPT,
            "user_prompt": user_prompt,
            "raw_response": llm_response_text,
        },
    }


# ---------------------------------------------------------------------------
# DQ Monitor Agent
# ---------------------------------------------------------------------------

@router.post("/dq-monitor")
async def run_dq_monitor():
    """Run the DQ monitoring agent against all external datasets."""
    endpoint = os.getenv("AGENT_MODEL_ENDPOINT", DEFAULT_MODEL_ENDPOINT)

    # Profile each dataset
    datasets_config = [
        ("market_pricing_benchmark", "raw_market_pricing_benchmark", "silver_market_pricing_benchmark",
         ["market_median_rate", "competitor_a_min_premium", "price_index_trend"]),
        ("geospatial_hazard_enrichment", "raw_geospatial_hazard_enrichment", "silver_geospatial_hazard_enrichment",
         ["flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index", "subsidence_risk"]),
        ("credit_bureau_summary", "raw_credit_bureau_summary", "silver_credit_bureau_summary",
         ["credit_score", "ccj_count", "years_trading", "director_changes"]),
    ]

    profile_text = ""
    for ds_name, raw_t, silver_t, cols in datasets_config:
        try:
            raw_q = await execute_query(f"SELECT count(*) as cnt FROM {fqn(raw_t)}")
            silver_q = await execute_query(f"SELECT count(*) as cnt FROM {fqn(silver_t)}")
            raw_cnt = int(raw_q[0]["cnt"]) if raw_q else 0
            silver_cnt = int(silver_q[0]["cnt"]) if silver_q else 0
            drop_rate = round((raw_cnt - silver_cnt) / raw_cnt * 100, 1) if raw_cnt else 0

            stats_parts = ", ".join(
                f"ROUND(AVG(CAST({c} AS DOUBLE)), 3) AS {c}_mean, "
                f"ROUND(STDDEV(CAST({c} AS DOUBLE)), 3) AS {c}_std, "
                f"ROUND(SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS {c}_null"
                for c in cols
            )
            stats = await execute_query(f"SELECT {stats_parts} FROM {fqn(raw_t)}")
            s = stats[0] if stats else {}

            profile_text += f"Dataset: {ds_name} (raw={raw_cnt}, silver={silver_cnt}, drop={drop_rate}%)\n"
            for c in cols:
                profile_text += f"  {c}: mean={s.get(f'{c}_mean','?')}, std={s.get(f'{c}_std','?')}, null={s.get(f'{c}_null','?')}%\n"
            profile_text += "\n"
        except Exception as e:
            profile_text += f"Dataset: {ds_name} — error profiling: {str(e)[:80]}\n\n"

    system_prompt = """You are a data quality monitoring agent for P&C insurance pricing.
Analyse data profiles and detect anomalies. Classify each as CRITICAL, WARNING, or INFO.
Respond with valid JSON:
{"findings": [{"dataset": "name", "column": "name", "severity": "CRITICAL|WARNING|INFO",
  "finding": "description", "evidence": "numbers", "suggested_action": "action",
  "pricing_impact": "how this affects pricing"}],
 "overall_assessment": "summary", "recommended_priority": "what to fix first"}"""

    user_prompt = f"Analyse these data quality profiles:\n\n{profile_text}"

    llm_success, llm_response_text, _ = _call_llm(endpoint, system_prompt, user_prompt, max_tokens=3000)

    findings = None
    if llm_success:
        jt = llm_response_text
        if "```json" in jt: jt = jt.split("```json")[1].split("```")[0]
        elif "```" in jt: jt = jt.split("```")[1].split("```")[0]
        try:
            findings = json.loads(jt.strip())
        except json.JSONDecodeError:
            pass

    await log_audit_event(
        event_type="agent_recommendation", entity_type="dataset",
        entity_id="dq_monitor",
        details={"agent_type": "dq_monitor", "llm_success": llm_success,
                 "findings_count": len((findings or {}).get("findings", []))},
    )

    return {
        "success": llm_success, "endpoint": endpoint, "findings": findings,
        "transparency": {"system_prompt": system_prompt, "user_prompt": user_prompt,
                         "raw_response": llm_response_text},
    }


# ---------------------------------------------------------------------------
# Explainability Agent
# ---------------------------------------------------------------------------

class ExplainRequest(BaseModel):
    question: str = "Why did premiums change in the latest data update?"


@router.post("/explain")
async def run_explainability(req: ExplainRequest):
    """Explain pricing shifts in plain English for actuarial use."""
    endpoint = os.getenv("AGENT_MODEL_ENDPOINT", DEFAULT_MODEL_ENDPOINT)
    upt_table = fqn("unified_pricing_table_live")

    # Gather context
    try:
        portfolio = await execute_query(f"""
            SELECT count(*) as total_policies, round(sum(current_premium)) as total_gwp,
                   round(avg(current_premium)) as avg_premium,
                   round(avg(combined_risk_score), 2) as avg_risk
            FROM {upt_table}
        """)
    except Exception:
        portfolio = [{}]

    shadow_context = ""
    try:
        shadow = await execute_query(f"""
            SELECT count(*) as affected, round(sum(premium_delta)) as total_delta,
                   round(avg(premium_delta_pct), 1) as avg_pct,
                   sum(case when churn_risk = 'HIGH' then 1 else 0 end) as high_churn
            FROM {fqn('shadow_pricing_impact')}
        """)
        s = shadow[0] if shadow else {}
        shadow_context = f"Shadow pricing: {s.get('affected',0)} affected, delta=£{s.get('total_delta',0)}, avg={s.get('avg_pct',0)}%"

        by_ind = await execute_query(f"""
            SELECT industry_risk_tier, count(*) as policies, round(sum(premium_delta)) as delta
            FROM {fqn('shadow_pricing_impact')} GROUP BY industry_risk_tier
        """)
        for r in by_ind:
            shadow_context += f"\n  {r.get('industry_risk_tier','?')}: {r.get('policies',0)} policies, £{r.get('delta',0)}"
    except Exception:
        shadow_context = "No shadow pricing data available"

    p = portfolio[0] if portfolio else {}
    context = f"""Portfolio: {p.get('total_policies',0)} policies, £{p.get('total_gwp',0)} GWP, avg premium £{p.get('avg_premium',0)}
{shadow_context}"""

    system_prompt = """You are an actuarial explainability agent. Explain pricing changes in plain English
suitable for regulatory filings. Ground claims in data. Respond with valid JSON:
{"headline": "one sentence", "explanation": "2-3 paragraphs",
 "key_drivers": [{"factor": "name", "contribution": "amount", "detail": "explanation"}],
 "affected_segments": [{"segment": "name", "policies": N, "premium_impact": "£X"}],
 "regulatory_statement": "paragraph for regulator",
 "recommended_actions": ["action1", "action2"]}"""

    user_prompt = f"Question: {req.question}\n\nContext:\n{context}"

    llm_success, llm_response_text, _ = _call_llm(endpoint, system_prompt, user_prompt, max_tokens=3000)

    explanation = None
    if llm_success:
        jt = llm_response_text
        if "```json" in jt: jt = jt.split("```json")[1].split("```")[0]
        elif "```" in jt: jt = jt.split("```")[1].split("```")[0]
        try:
            explanation = json.loads(jt.strip())
        except json.JSONDecodeError:
            pass

    await log_audit_event(
        event_type="agent_recommendation", entity_type="model",
        entity_id="explainability_agent",
        details={"agent_type": "explainability", "question": req.question,
                 "llm_success": llm_success,
                 "headline": (explanation or {}).get("headline", "")},
    )

    return {
        "success": llm_success, "endpoint": endpoint, "explanation": explanation,
        "transparency": {"system_prompt": system_prompt, "user_prompt": user_prompt,
                         "raw_response": llm_response_text},
    }


# ===========================================================================
# Model Factory — Agentic planner
# ===========================================================================

async def _feature_catalog_summary() -> dict:
    """Pull a compact summary of the feature catalog + UPT stats for the agent."""
    features: list[dict] = []
    counts_by_group: dict[str, int] = {}
    try:
        rows = await execute_query(f"""
            SELECT feature_name, feature_group, data_type,
                   source_tables, owner, regulatory_sensitive, pii, description
            FROM {fqn('feature_catalog')}
            ORDER BY feature_group, feature_name
        """)
        for r in rows:
            g = r.get("feature_group") or "other"
            counts_by_group[g] = counts_by_group.get(g, 0) + 1
            features.append(r)
    except Exception as e:
        logger.warning("feature_catalog query failed: %s", e)

    upt_stats = {}
    try:
        s = await execute_query(f"""
            SELECT
                count(*) AS rows,
                count(DISTINCT policy_id) AS policies,
                round(avg(claim_count_5y), 4) AS avg_claim_count,
                round(sum(claim_count_5y) / count(*), 4) AS claim_rate,
                round(avg(total_incurred_5y), 2) AS avg_incurred
            FROM {fqn('unified_pricing_table_live')}
        """)
        upt_stats = s[0] if s else {}
    except Exception as e:
        logger.warning("upt stats query failed: %s", e)

    factory_history = {}
    try:
        h = await execute_query(f"""
            SELECT count(DISTINCT factory_run_id) AS runs,
                   count(*) AS configs_trained,
                   count(DISTINCT model_config_id) AS unique_configs
            FROM {fqn('mf_training_log')}
        """)
        factory_history = h[0] if h else {}
    except Exception:
        pass

    return {
        "features":         features,
        "counts_by_group":  counts_by_group,
        "upt_stats":        upt_stats,
        "factory_history":  factory_history,
    }


@router.get("/analyse-features")
async def analyse_features():
    """Claude reads the feature catalog + UPT state + factory history and returns
    a narrative analysis for the actuary — which targets make sense, what's
    missing, sensitive features to watch."""
    endpoint = os.getenv("AGENT_MODEL_ENDPOINT", DEFAULT_MODEL_ENDPOINT)
    summary = await _feature_catalog_summary()

    if not summary.get("features"):
        return {
            "success": False,
            "analysis": None,
            "error": "feature_catalog is empty — run build_feature_catalog first.",
        }

    system_prompt = """You are an expert actuarial pricing advisor analysing a commercial P&C insurance feature store.

Your audience is a pricing actuary using a web app. They see your analysis above a dropdown-driven "what to train" panel.

Keep the response concise and scannable (max ~200 words of prose + bullets).

RESPONSE FORMAT: Return JSON with this structure:
{
  "headline":        "one sentence — the state of the feature store",
  "strengths":       ["bullet 1", "bullet 2"],
  "gaps":            ["features or targets that would add value but are missing"],
  "sensitive":       ["brief note about regulatory-sensitive features they should be aware of"],
  "recommended_next": [
    {"target": "claim_count_5y", "why": "one sentence"},
    {"target": "total_incurred_5y", "why": "one sentence"}
  ]
}"""

    feature_brief = "\n".join(
        f"- {f['feature_name']} [{f['feature_group']}]: {f.get('description') or ''}"
        f"{' (regulatory)' if f.get('regulatory_sensitive') in (True, 'true', 'True') else ''}"
        for f in summary["features"][:60]
    )
    user_prompt = f"""UPT stats:
- {summary['upt_stats'].get('rows', 'unknown')} rows (policies), avg claim count 5y: {summary['upt_stats'].get('avg_claim_count')}
- claim rate: {summary['upt_stats'].get('claim_rate')}, avg incurred: {summary['upt_stats'].get('avg_incurred')}

Feature groups: {summary['counts_by_group']}

Factory history: {summary['factory_history']}

Features (first 60):
{feature_brief}
"""

    success, text, _usage = _call_llm(endpoint, system_prompt, user_prompt, max_tokens=2000)
    analysis = None
    if success:
        try:
            t = text
            if "```json" in t: t = t.split("```json")[1].split("```")[0]
            elif "```" in t:    t = t.split("```")[1].split("```")[0]
            analysis = json.loads(t.strip())
        except Exception:
            pass

    await log_audit_event(
        event_type="agent_recommendation", entity_type="feature_store",
        entity_id="analyse_features",
        details={"agent_type": "analyse_features", "llm_success": success,
                 "counts_by_group": summary["counts_by_group"]},
    )

    return {
        "success":  success,
        "endpoint": endpoint,
        "analysis": analysis,
        "raw":      text if not analysis else None,
        "context":  {
            "counts_by_group": summary["counts_by_group"],
            "upt_stats":       summary["upt_stats"],
            "factory_history": summary["factory_history"],
        },
    }


class ProposePlanRequest(BaseModel):
    target:        str                  # e.g. "claim_count_5y"
    model_family:  str                  # e.g. "GLM_Poisson"
    feature_scope: str                  # e.g. "all", "baseline_only", "plus_real_uk", "exclude_regulatory"
    sweep_size:    int = 10             # number of configs to generate
    focus:         str = "exploration"  # "interaction_terms", "hyperparam_sweep", "feature_ablation", "exploration"
    note:          str | None = None    # optional free-form from user


@router.post("/propose-plan")
async def propose_plan(req: ProposePlanRequest):
    """Given a structured intent from the user dropdowns, ask Claude to propose
    a concrete list of training configs with per-config rationale."""
    endpoint = os.getenv("AGENT_MODEL_ENDPOINT", DEFAULT_MODEL_ENDPOINT)
    summary = await _feature_catalog_summary()

    if not summary.get("features"):
        return {"success": False, "error": "feature_catalog not populated"}

    # Pre-filter features according to scope so the LLM sees a tight pool
    all_feats = summary["features"]
    rating  = [f for f in all_feats if f["feature_group"] == "rating_factor"]
    enrich  = [f for f in all_feats if f["feature_group"] == "enrichment"]
    derived = [f for f in all_feats if f["feature_group"] in ("derived", "claim_derived", "quote_derived")]
    regulatory_names = [f["feature_name"] for f in all_feats
                        if f.get("regulatory_sensitive") in (True, "true", "True")]

    if req.feature_scope == "baseline_only":
        feature_pool = [f["feature_name"] for f in rating + derived
                        if f["feature_name"] not in regulatory_names]
    elif req.feature_scope == "plus_real_uk":
        feature_pool = [f["feature_name"] for f in rating + derived + enrich]
    elif req.feature_scope == "exclude_regulatory":
        feature_pool = [f["feature_name"] for f in rating + derived + enrich
                        if f["feature_name"] not in regulatory_names]
    else:  # "all"
        feature_pool = [f["feature_name"] for f in rating + derived + enrich]

    # Drop the primary key + audit columns — never meaningful as model inputs
    feature_pool = [f for f in feature_pool if f not in ("policy_id", "last_updated_by",
                                                          "approval_timestamp", "upt_build_timestamp",
                                                          "source_version")]

    system_prompt = f"""You are an expert actuarial model factory planner for commercial P&C pricing.

The user has chosen:
- Target variable:  {req.target}
- Model family:     {req.model_family}
- Feature scope:    {req.feature_scope} (resulting pool of {len(feature_pool)} features)
- Sweep size:       {req.sweep_size}
- Focus:            {req.focus}

Your job is to generate exactly {req.sweep_size} distinct, sensible training configurations that
match the focus. Each config chooses a subset of features + hyperparameters. Rationale matters —
the actuary needs to justify each config if a regulator asks.

Available feature pool:
{", ".join(feature_pool[:80])}

RESPONSE FORMAT: Return JSON exactly like:
{{
  "plan_summary": "2-3 sentence explanation of the sweep strategy",
  "configs": [
    {{
      "config_id":   "cfg_001",
      "target":      "{req.target}",
      "model_type":  "{req.model_family}",
      "features":    ["max 12 feature names from the pool"],
      "hyperparams": {{"learning_rate": 0.05, "n_estimators": 200}},
      "rationale":   "one sentence — why this config"
    }}
  ]
}}

Each config_id must be unique (cfg_001 through cfg_{req.sweep_size:03d}).
Keep features per config between 5 and 12.
For GLMs, hyperparams can be {{"maxiter": 50}} or similar; for GBMs use learning_rate, n_estimators, num_leaves, min_child_samples, reg_alpha, reg_lambda.
"""

    user_note = f"User note: {req.note}" if req.note else "User did not provide a free-form note."
    user_prompt = user_note

    success, text, _ = _call_llm(endpoint, system_prompt, user_prompt, max_tokens=4000)
    plan = None
    if success:
        try:
            t = text
            if "```json" in t: t = t.split("```json")[1].split("```")[0]
            elif "```" in t:    t = t.split("```")[1].split("```")[0]
            plan = json.loads(t.strip())
        except Exception as e:
            logger.warning("propose-plan JSON parse failed: %s", e)

    await log_audit_event(
        event_type="agent_recommendation", entity_type="model",
        entity_id="model_factory_planner",
        details={"agent_type": "propose_plan",
                 "intent": req.model_dump(),
                 "llm_success": success,
                 "n_configs": len(plan.get("configs", [])) if plan else 0},
    )

    return {
        "success":     success,
        "endpoint":    endpoint,
        "intent":      req.model_dump(),
        "feature_pool_size": len(feature_pool),
        "plan":        plan,
        "raw":         text if not plan else None,
    }


class SubmitPlanRequest(BaseModel):
    intent:          dict                # the intent dict returned by propose-plan
    plan_summary:    str | None = None
    configs:         list[dict]          # each: {config_id, target, model_type, features, hyperparams, rationale}
    feature_analysis_text: str | None = None  # narrative from analyse-features, saved for the run log


@router.post("/submit-plan")
async def submit_plan(req: SubmitPlanRequest):
    """Persist the proposed plan to the Model Factory tables + run log.
    Creates a new factory_run_id and writes one row per config to
    mf_training_plan. The actual training happens when the user runs the
    `model_factory_pipeline` bundle job."""
    from uuid import uuid4

    factory_run_id = f"fac_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:6]}"
    user = get_current_user()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # --- Ensure mf_run_log exists ---
    await execute_query(f"""
        CREATE TABLE IF NOT EXISTS {fqn('mf_run_log')} (
            factory_run_id STRING,
            created_at STRING,
            created_by STRING,
            proposal_source STRING,
            intent_target STRING,
            intent_model_family STRING,
            intent_feature_scope STRING,
            intent_sweep_size INT,
            intent_focus STRING,
            user_note STRING,
            feature_analysis STRING,
            plan_summary STRING,
            n_configs_proposed INT,
            configs_json STRING,
            submitted_at STRING,
            status STRING,
            completed_at STRING,
            summary_metrics STRING
        )
    """)
    # --- Ensure mf_training_plan exists (matches the schema used by mf_02_automated_training) ---
    await execute_query(f"""
        CREATE TABLE IF NOT EXISTS {fqn('mf_training_plan')} (
            factory_run_id        STRING,
            model_config_id       STRING,
            model_family          STRING,
            model_type            STRING,
            target_column         STRING,
            feature_subset_name   STRING,
            feature_list_json     STRING,
            hyperparams_json      STRING,
            rationale             STRING,
            plan_source           STRING
        )
    """)

    intent = req.intent or {}
    configs = req.configs or []
    configs_safe = json.dumps(configs)[:50_000].replace("'", "''")

    def esc(s) -> str:
        return str(s if s is not None else "").replace("'", "''")

    # 1. mf_run_log row
    await execute_query(f"""
        INSERT INTO {fqn('mf_run_log')} VALUES (
            '{factory_run_id}',
            '{now}',
            '{esc(user)}',
            'agent',
            '{esc(intent.get('target'))}',
            '{esc(intent.get('model_family'))}',
            '{esc(intent.get('feature_scope'))}',
            {int(intent.get('sweep_size') or 0)},
            '{esc(intent.get('focus'))}',
            '{esc(intent.get('note'))}',
            '{esc((req.feature_analysis_text or '')[:8000])}',
            '{esc((req.plan_summary or '')[:4000])}',
            {len(configs)},
            '{configs_safe}',
            '{now}',
            'PROPOSED',
            NULL,
            NULL
        )
    """)

    # 2. One row per config in mf_training_plan — match the pipeline's column layout
    feature_subset_name = f"agent_{esc(intent.get('feature_scope') or 'custom')}"
    plan_source         = "agent"
    for cfg in configs:
        cfg_id      = esc(cfg.get("config_id") or f"cfg_{uuid4().hex[:6]}")
        family      = esc(cfg.get("model_type") or intent.get("model_family"))
        # Treat family == model_type for now; the pipeline derives the specific
        # implementation from family + hyperparams.
        model_type  = family
        target      = esc(cfg.get("target") or intent.get("target"))
        feat_json   = esc(json.dumps(cfg.get("features")    or []))
        hp_json     = esc(json.dumps(cfg.get("hyperparams") or {}))
        rationale   = esc(cfg.get("rationale") or "")
        await execute_query(f"""
            INSERT INTO {fqn('mf_training_plan')} VALUES (
                '{factory_run_id}',
                '{cfg_id}',
                '{family}',
                '{model_type}',
                '{target}',
                '{feature_subset_name}',
                '{feat_json}',
                '{hp_json}',
                '{rationale}',
                '{plan_source}'
            )
        """)

    await log_audit_event(
        event_type="agent_action", entity_type="model",
        entity_id=factory_run_id,
        details={
            "agent_type":   "model_factory_planner",
            "action":       "submit_plan",
            "intent":       intent,
            "n_configs":    len(configs),
        },
    )

    return {
        "success":        True,
        "factory_run_id": factory_run_id,
        "n_configs":      len(configs),
        "next_step":      "Run `databricks bundle run model_factory_pipeline` to train the proposed configs.",
    }

"""Legacy agent entrypoint — pared down to the single endpoint still in use.

Keeps only `/agent/explain` which the Dataset detail page uses for actuarial
explanations of pricing-impact diffs. All factory-related endpoints and the
status/dq-monitor/analyze helpers have been removed — they were part of the
old Model Factory workflow and have been superseded by the new factory flow
in `factory.py`, the governance-pack chat, and the regulator-AI placeholder.
"""

import json
import logging
import os

import requests
from fastapi import APIRouter
from pydantic import BaseModel

from server.audit import log_audit_event
from server.config import fqn, get_workspace_client
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/agent", tags=["agent"])

DEFAULT_MODEL_ENDPOINT = "databricks-claude-sonnet-4-6"


def _call_llm(endpoint: str, system_prompt: str, user_prompt: str, max_tokens: int = 3000):
    """Call a Foundation Model API endpoint. Returns (success, response_text, token_usage)."""
    try:
        w = get_workspace_client()
        host = w.config.host.rstrip("/")
        token = w.config._header_factory()
        resp = requests.post(
            f"{host}/serving-endpoints/{endpoint}/invocations",
            headers={**token, "Content-Type": "application/json"},
            json={
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                "max_tokens": max_tokens,
                "temperature": 0.1,
            },
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        return True, data["choices"][0]["message"]["content"], data.get("usage", {})
    except Exception as e:
        return False, f"LLM call failed: {e}", {}


class ExplainRequest(BaseModel):
    question: str = "Why did premiums change in the latest data update?"


@router.post("/explain")
async def run_explainability(req: ExplainRequest):
    """Explain pricing shifts in plain English for actuarial use. Called from
    the Ingestion → dataset detail page when an actuary wants a narrative for a
    shadow-pricing impact."""
    endpoint = os.getenv("AGENT_MODEL_ENDPOINT", DEFAULT_MODEL_ENDPOINT)

    try:
        portfolio = await execute_query(f"""
            SELECT count(*)                          AS total_policies,
                   round(sum(current_premium))       AS total_gwp,
                   round(avg(current_premium))       AS avg_premium,
                   round(avg(combined_risk_score),2) AS avg_risk
            FROM {fqn('unified_pricing_table_live')}
        """)
    except Exception:
        portfolio = [{}]
    p = portfolio[0] if portfolio else {}

    shadow_context = ""
    try:
        shadow = await execute_query(f"""
            SELECT count(*) AS affected, round(sum(premium_delta)) AS total_delta,
                   round(avg(premium_delta_pct),1) AS avg_pct,
                   sum(CASE WHEN churn_risk = 'HIGH' THEN 1 ELSE 0 END) AS high_churn
            FROM {fqn('shadow_pricing_impact')}
        """)
        s = shadow[0] if shadow else {}
        shadow_context = (
            f"Shadow pricing: {s.get('affected',0)} affected, "
            f"delta=£{s.get('total_delta',0)}, avg={s.get('avg_pct',0)}%"
        )
    except Exception:
        shadow_context = "No shadow pricing data available"

    context = (
        f"Portfolio: {p.get('total_policies',0)} policies, "
        f"£{p.get('total_gwp',0)} GWP, avg premium £{p.get('avg_premium',0)}\n"
        f"{shadow_context}"
    )

    system_prompt = (
        "You are an actuarial explainability agent. Explain pricing changes in plain English "
        "suitable for regulatory filings. Ground claims in the provided context. Respond with "
        "valid JSON: {\"headline\": \"one sentence\", \"explanation\": \"2-3 paragraphs\", "
        "\"key_drivers\": [{\"factor\": \"name\", \"contribution\": \"amount\", "
        "\"detail\": \"explanation\"}], \"affected_segments\": [{\"segment\": \"name\", "
        "\"policies\": N, \"premium_impact\": \"£X\"}], \"regulatory_statement\": \"paragraph\", "
        "\"recommended_actions\": [\"action1\"]}"
    )
    user_prompt = f"Question: {req.question}\n\nContext:\n{context}"

    ok, raw, _ = _call_llm(endpoint, system_prompt, user_prompt, max_tokens=3000)

    explanation = None
    if ok:
        jt = raw
        if "```json" in jt:
            jt = jt.split("```json")[1].split("```")[0]
        elif "```" in jt:
            jt = jt.split("```")[1].split("```")[0]
        try:
            explanation = json.loads(jt.strip())
        except json.JSONDecodeError:
            pass

    await log_audit_event(
        event_type="agent_recommendation",
        entity_type="model",
        entity_id="explainability_agent",
        details={"question": req.question, "llm_success": ok,
                 "headline": (explanation or {}).get("headline", "")},
    )
    return {
        "success": ok,
        "endpoint": endpoint,
        "explanation": explanation,
        "transparency": {"system_prompt": system_prompt, "user_prompt": user_prompt,
                         "raw_response": raw},
    }

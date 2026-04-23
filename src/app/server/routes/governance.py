"""Governance routes.

Two groups:

1. Summary endpoint (preserved from earlier) — `/api/governance/summary` —
   powering the dashboard-style aggregated view.
2. Model Governance tab endpoints (new) — packs catalog, PDF viewing,
   agent chat against a pack, synthetic policy scoring story.

The Model Governance tab is the flagship post-promotion view. Agent chat
calls Databricks Foundation Model API (Claude Sonnet 4.6) directly —
Agent Framework endpoint can slot in later as a one-line swap.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from server.audit import log_audit_event
from server.config import (
    fqn, get_catalog, get_current_user, get_schema,
    get_workspace_client, get_workspace_host,
)
from server.sql import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/governance", tags=["governance"])

FAMILIES = [
    {"key": "freq_glm",   "label": "Frequency (GLM)"},
    {"key": "sev_glm",    "label": "Severity (GLM)"},
    {"key": "demand_gbm", "label": "Demand (GBM)"},
    {"key": "fraud_gbm",  "label": "Fraud (GBM)"},
]
# Real Databricks Agent Framework endpoint — deployed from governance_agent.py
# via the governance_agent_deploy bundle job. The agent has 3 tools it calls
# on-demand (query_pack_index, read_pack_artefact, query_audit_log) and
# returns a tool-use trace for the UI "Show full LLM interaction" panel.
AGENT_ENDPOINT = "pricing_governance_agent"
# Direct FM call as fallback if the agent endpoint is unavailable (e.g.
# during first deploy / cold start). Preserves the chat UX.
FM_ENDPOINT = "databricks-claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# Summary (preserved)
# ---------------------------------------------------------------------------

@router.get("/summary")
async def governance_summary():
    """Aggregate governance data across all systems."""
    host = get_workspace_host()

    events_by_type = []
    try:
        events_by_type = await execute_query(f"""
            SELECT event_type, entity_type,
                   COUNT(*) AS event_count,
                   MAX(timestamp) AS last_occurrence,
                   COUNT(DISTINCT user_id) AS unique_users
            FROM {fqn('audit_log')}
            GROUP BY event_type, entity_type
            ORDER BY event_count DESC
        """)
    except Exception:
        pass

    recent = []
    try:
        recent = await execute_query(f"""
            SELECT event_id, event_type, entity_type, entity_id,
                   user_id, timestamp, source
            FROM {fqn('audit_log')}
            ORDER BY timestamp DESC LIMIT 20
        """)
    except Exception:
        pass

    dq = []
    try:
        for ds, raw, silver in [
            ("Market Pricing", "raw_market_pricing_benchmark", "silver_market_pricing_benchmark"),
            ("Geospatial Hazard", "raw_geospatial_hazard_enrichment", "silver_geospatial_hazard_enrichment"),
            ("Credit Bureau", "raw_credit_bureau_summary", "silver_credit_bureau_summary"),
        ]:
            r = await execute_query(f"SELECT count(*) as cnt FROM {fqn(raw)}")
            s = await execute_query(f"SELECT count(*) as cnt FROM {fqn(silver)}")
            raw_cnt = int(r[0]["cnt"]) if r else 0
            silver_cnt = int(s[0]["cnt"]) if s else 0
            dq.append({
                "dataset": ds, "raw_rows": raw_cnt, "silver_rows": silver_cnt,
                "dropped": raw_cnt - silver_cnt,
                "pass_rate": round(silver_cnt / raw_cnt * 100, 1) if raw_cnt else 0,
            })
    except Exception:
        pass

    lineage = []
    try:
        lineage = await execute_query(f"""
            SELECT version, timestamp, operation, userName
            FROM (DESCRIBE HISTORY {fqn('unified_pricing_table_live')} LIMIT 10)
            ORDER BY version DESC
        """)
    except Exception:
        pass

    return {
        "events_by_type": events_by_type,
        "recent_activity": recent,
        "data_quality": dq,
        "delta_lineage": lineage,
        "workspace_host": host,
    }


# ---------------------------------------------------------------------------
# Packs catalog
# ---------------------------------------------------------------------------

@router.get("/packs")
async def list_packs() -> dict:
    """Return every pack in the index, grouped by family."""
    try:
        rows = await execute_query(f"""
            SELECT pack_id, model_family, model_version, model_uc_name,
                   mlflow_run_id, story, simulated, primary_metric, primary_value,
                   pdf_path, size_bytes, generated_by, generated_at
            FROM {fqn('governance_packs_index')}
            ORDER BY generated_at DESC
        """)
    except Exception as e:
        logger.info("governance_packs_index not queryable yet: %s", e)
        return {"families": [{"key": f["key"], "label": f["label"], "packs": []} for f in FAMILIES]}

    by_family: dict[str, list] = {f["key"]: [] for f in FAMILIES}
    for r in rows:
        fam = r.get("model_family")
        if fam not in by_family:
            continue
        by_family[fam].append({
            "pack_id":       r["pack_id"],
            "model_family":  fam,
            "model_version": r["model_version"],
            "story":         r.get("story"),
            "simulated":     r.get("simulated"),
            "primary_metric":r.get("primary_metric"),
            "primary_value": r.get("primary_value"),
            "pdf_path":      r.get("pdf_path"),
            "size_bytes":    r.get("size_bytes"),
            "generated_by":  r.get("generated_by"),
            "generated_at":  str(r.get("generated_at", "")),
        })
    return {
        "families": [
            {"key": f["key"], "label": f["label"], "packs": by_family.get(f["key"], [])}
            for f in FAMILIES
        ]
    }


@router.get("/packs/by-date")
async def packs_on_date(date: str) -> dict:
    """Return the pack that was the most-recent-at-or-before `date` for each
    family. Used by the By-date entry point to show what was in force on a
    historical day."""
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except Exception:
        raise HTTPException(400, "date must be YYYY-MM-DD")

    try:
        rows = await execute_query(f"""
            SELECT model_family, pack_id, model_version, story, primary_metric,
                   primary_value, pdf_path, generated_by, generated_at
            FROM (
                SELECT *, row_number() OVER (PARTITION BY model_family
                                             ORDER BY generated_at DESC) AS rn
                FROM {fqn('governance_packs_index')}
                WHERE CAST(generated_at AS DATE) <= DATE('{date}')
            )
            WHERE rn = 1
            ORDER BY model_family
        """)
    except Exception as e:
        logger.warning("by-date query failed: %s", e)
        return {"date": date, "packs": []}
    return {
        "date": date,
        "packs": [{
            "model_family":  r["model_family"],
            "pack_id":       r["pack_id"],
            "model_version": r["model_version"],
            "story":         r.get("story"),
            "primary_metric":r.get("primary_metric"),
            "primary_value": r.get("primary_value"),
            "generated_by":  r.get("generated_by"),
            "generated_at":  str(r.get("generated_at", "")),
        } for r in rows],
    }


@router.get("/packs/{pack_id}")
async def pack_detail(pack_id: str) -> dict:
    rows = await execute_query(f"""
        SELECT pack_id, model_family, model_version, model_uc_name,
               mlflow_run_id, story, simulated, primary_metric, primary_value,
               pdf_path, size_bytes, generated_by, generated_at
        FROM {fqn('governance_packs_index')}
        WHERE pack_id = '{pack_id}'
        LIMIT 1
    """)
    if not rows:
        raise HTTPException(404, f"pack {pack_id} not found")

    await log_audit_event(
        event_type="governance_pack_viewed",
        entity_type="model",
        entity_id=rows[0]["model_family"],
        entity_version=str(rows[0]["model_version"]),
        details={"pack_id": pack_id},
    )
    r = rows[0]
    return {
        "pack_id": r["pack_id"], "model_family": r["model_family"],
        "model_version": r["model_version"], "model_uc_name": r["model_uc_name"],
        "mlflow_run_id": r["mlflow_run_id"], "story": r.get("story"),
        "simulated": r.get("simulated"),
        "primary_metric": r.get("primary_metric"),
        "primary_value":  r.get("primary_value"),
        "pdf_path": r.get("pdf_path"),
        "size_bytes": r.get("size_bytes"),
        "generated_by": r.get("generated_by"),
        "generated_at": str(r.get("generated_at", "")),
        "pdf_url": f"/api/governance/packs/{pack_id}/pdf",
    }


@router.get("/packs/{pack_id}/pdf")
async def pack_pdf(pack_id: str):
    """Stream the PDF from the UC volume so the frontend can display it
    inline (iframe or <object>)."""
    rows = await execute_query(f"""
        SELECT pdf_path, model_family, model_version
        FROM {fqn('governance_packs_index')}
        WHERE pack_id = '{pack_id}' LIMIT 1
    """)
    if not rows:
        raise HTTPException(404, f"pack {pack_id} not found")
    path = rows[0]["pdf_path"]
    try:
        resp = get_workspace_client().files.download(file_path=path)
        data = resp.contents.read() if hasattr(resp.contents, "read") else resp.contents
    except Exception as e:
        raise HTTPException(500, f"Could not download PDF: {e}")
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename={pack_id}.pdf"},
    )


# ---------------------------------------------------------------------------
# PDF text cache — extract once, reuse for the chat
# ---------------------------------------------------------------------------

_pdf_text_cache: dict[str, str] = {}


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Pull plain text from the pack PDF for use as agent context."""
    try:
        from pypdf import PdfReader
    except Exception as e:
        logger.warning("pypdf not available: %s", e)
        return ""
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for i, p in enumerate(reader.pages):
            try:
                t = p.extract_text() or ""
            except Exception:
                t = ""
            if t.strip():
                pages.append(f"--- page {i+1} ---\n{t}")
        return "\n".join(pages)
    except Exception as e:
        logger.warning("PDF text extract failed: %s", e)
        return ""


async def _pack_text(pack_id: str) -> tuple[dict, str]:
    """Return (pack metadata row, extracted text). Caches the text per pack."""
    rows = await execute_query(f"""
        SELECT pack_id, model_family, model_version, pdf_path
        FROM {fqn('governance_packs_index')}
        WHERE pack_id = '{pack_id}' LIMIT 1
    """)
    if not rows:
        raise HTTPException(404, f"pack {pack_id} not found")
    r = rows[0]
    if pack_id in _pdf_text_cache:
        return r, _pdf_text_cache[pack_id]
    try:
        resp = get_workspace_client().files.download(file_path=r["pdf_path"])
        data = resp.contents.read() if hasattr(resp.contents, "read") else resp.contents
    except Exception as e:
        logger.warning("Pack PDF download failed: %s", e)
        return r, ""
    text = _extract_pdf_text(data)
    _pdf_text_cache[pack_id] = text
    return r, text


@router.get("/packs/{pack_id}/text")
async def pack_text(pack_id: str) -> dict:
    r, text = await _pack_text(pack_id)
    return {
        "pack_id": pack_id,
        "model_family": r["model_family"],
        "model_version": r["model_version"],
        "text_length": len(text),
        "preview": text[:3000],
    }


# ---------------------------------------------------------------------------
# Agent chat — Foundation Model API (Claude Sonnet 4.6)
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    pack_id: str
    question: str
    policy_id: str | None = None   # only populated on by-policy flow


SYSTEM_PROMPT = """You are a model-governance assistant for Bricksurance SE's pricing committee.
You help compliance officers, senior actuaries, and regulators understand a specific model version
by answering questions strictly from that model's governance pack.

Rules you MUST follow:
 * Answer ONLY using information contained in the pack text provided in the user message.
 * Cite the pack section whenever you quote a fact (e.g., "see Section 4 — Model specification").
 * If the pack does not contain the information needed, reply exactly: "The pack does not document this — further investigation required." Do not guess.
 * Never speculate about fairness, bias, or model behaviour beyond what is documented.
 * Keep answers concise (4-8 sentences unless the user asks for more detail).
 * When drafting regulator/customer responses, phrase them carefully and stay grounded in the pack.
"""


@router.post("/chat")
async def chat(req: ChatRequest) -> dict:
    """Delegate the question to the governance Agent Framework endpoint.

    The agent uses tools to look up pack metadata, read sidecar artefacts, and
    query the audit log. If the agent endpoint is unavailable (cold start,
    first deploy) we fall back to the direct Foundation Model API call over
    the pack's PDF text so the chat panel still works.
    """
    if not req.question.strip():
        raise HTTPException(400, "question is required")

    pack_row = (await execute_query(f"""
        SELECT pack_id, model_family, model_version, pdf_path
        FROM {fqn('governance_packs_index')}
        WHERE pack_id = '{req.pack_id}' LIMIT 1
    """) or [{}])[0]
    if not pack_row:
        raise HTTPException(404, f"pack {req.pack_id} not found")

    # Try the real agent endpoint first
    agent_result = _query_agent_endpoint(req.pack_id, req.question, req.policy_id)

    if agent_result.get("ok"):
        answer = agent_result["answer"]
        trace  = agent_result.get("trace", [])
        model  = agent_result.get("model", AGENT_ENDPOINT)
        usage  = agent_result.get("usage", {})
        sections = sorted(set(re.findall(r"[Ss]ection\s+(\d+)", answer or "")))

        # Audit: capture every tool call for governance continuity
        await log_audit_event(
            event_type="governance_pack_chat",
            entity_type="model",
            entity_id=pack_row["model_family"],
            entity_version=str(pack_row["model_version"]),
            details={
                "pack_id": req.pack_id,
                "question": req.question[:500],
                "answer_length": len(answer or ""),
                "policy_id": req.policy_id,
                "cited_sections": sections,
                "model": model,
                "endpoint": AGENT_ENDPOINT,
                "tool_trace": [{"tool": t.get("tool"),
                                 "args": t.get("arguments"),
                                 "result_summary": t.get("result_summary")}
                                for t in trace],
                "usage": usage,
            },
        )
        return {
            "pack_id":        req.pack_id,
            "model_family":   pack_row["model_family"],
            "model_version":  pack_row["model_version"],
            "question":       req.question,
            "answer":         answer,
            "cited_sections": sections,
            "tool_trace":     trace,
            "model":          model,
            "endpoint":       AGENT_ENDPOINT,
            "usage":          usage,
            "source":         "agent_framework",
        }

    # Fallback — FM API direct over PDF text
    logger.warning("Agent endpoint unavailable (%s) — falling back to FM API",
                   agent_result.get("error", "unknown"))
    _, text = await _pack_text(req.pack_id)
    truncated = (text or "")[:40000] or "(pack text could not be extracted)"
    user_content = (
        f"Pack:\n  family:  {pack_row['model_family']}\n"
        f"  version: {pack_row['model_version']}\n  pack_id: {pack_row['pack_id']}\n\n"
        f"Pack contents (plain text extracted from PDF):\n===\n{truncated}\n===\n\n"
        f"User question: {req.question}"
    )
    if req.policy_id:
        user_content += f"\n\nContext: this question concerns policy_id={req.policy_id}."

    try:
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        fm_resp = get_workspace_client().serving_endpoints.query(
            name=FM_ENDPOINT,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT),
                ChatMessage(role=ChatMessageRole.USER,   content=user_content),
            ],
            max_tokens=800, temperature=0.2,
        )
    except Exception as e:
        logger.exception("FM API fallback also failed")
        return {
            "pack_id":  req.pack_id,
            "model":    FM_ENDPOINT,
            "answer":   f"Chat temporarily unavailable ({e}).",
            "error":    str(e)[:300],
            "source":   "unavailable",
        }

    answer = ""
    try:
        choices = getattr(fm_resp, "choices", None) or fm_resp.get("choices", [])
        if choices:
            m = choices[0].message if hasattr(choices[0], "message") else choices[0].get("message", {})
            answer = getattr(m, "content", None) or (m.get("content") if isinstance(m, dict) else "")
    except Exception:
        answer = str(fm_resp)[:2000]

    sections = sorted(set(re.findall(r"[Ss]ection\s+(\d+)", answer or "")))
    usage = {}
    try:
        u = getattr(fm_resp, "usage", None) or (fm_resp.get("usage") if isinstance(fm_resp, dict) else None)
        if u:
            usage = {
                "prompt_tokens":     getattr(u, "prompt_tokens", None) or u.get("prompt_tokens"),
                "completion_tokens": getattr(u, "completion_tokens", None) or u.get("completion_tokens"),
                "total_tokens":      getattr(u, "total_tokens", None) or u.get("total_tokens"),
            }
    except Exception:
        pass

    await log_audit_event(
        event_type="governance_pack_chat",
        entity_type="model",
        entity_id=pack_row["model_family"],
        entity_version=str(pack_row["model_version"]),
        details={
            "pack_id": req.pack_id,
            "question": req.question[:500],
            "answer_length": len(answer or ""),
            "policy_id": req.policy_id,
            "cited_sections": sections,
            "model": FM_ENDPOINT,
            "endpoint": FM_ENDPOINT,
            "source": "fm_api_fallback",
            "fallback_reason": agent_result.get("error", "unknown")[:200],
            "usage": usage,
        },
    )
    return {
        "pack_id":        req.pack_id,
        "model_family":   pack_row["model_family"],
        "model_version":  pack_row["model_version"],
        "question":       req.question,
        "answer":         answer,
        "cited_sections": sections,
        "tool_trace":     [],
        "model":          FM_ENDPOINT,
        "endpoint":       FM_ENDPOINT,
        "usage":          usage,
        "source":         "fm_api_fallback",
        "fallback_reason": agent_result.get("error", "agent unavailable"),
    }


def _query_agent_endpoint(pack_id: str, question: str, policy_id: str | None) -> dict:
    """Call the Databricks Agent Framework serving endpoint. Returns a result
    dict with `ok` flag plus answer/trace/model/usage or error info."""
    import requests as _rq
    try:
        w = get_workspace_client()
        # Confirm the endpoint is ready before invoking
        try:
            ep = w.serving_endpoints.get(AGENT_ENDPOINT)
            state = ep.state.ready if ep.state and ep.state.ready else None
            if state and "READY" not in str(state):
                return {"ok": False, "error": f"endpoint not ready (state={state})"}
        except Exception as e:
            return {"ok": False, "error": f"endpoint lookup failed: {e}"}

        host  = w.config.host.rstrip("/")
        token = w.config._header_factory()
        # The deployed agent's input signature only declares the pack_id field
        # in custom_inputs — additional fields are rejected. We squeeze the
        # policy_id context into the question text when present.
        q = question if not policy_id else f"[policy_id={policy_id}] {question}"
        body = {
            "dataframe_records": [{
                "messages": [{"role": "user", "content": q}],
                "custom_inputs": {"pack_id": pack_id},
            }],
        }
        # Long timeout — the agent may make several tool calls (SQL + volume
        # reads) before returning. First invocation after cold start is the
        # slowest.
        resp = _rq.post(
            f"{host}/serving-endpoints/{AGENT_ENDPOINT}/invocations",
            headers={**token, "Content-Type": "application/json"},
            json=body, timeout=240,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Agent endpoint call failed: %s", e)
        return {"ok": False, "error": str(e)[:300]}

    # MLflow serving wraps pyfunc output — usually under "predictions"
    preds = data.get("predictions") or data.get("outputs") or data
    if isinstance(preds, list):
        preds = preds[0] if preds else {}
    if not isinstance(preds, dict):
        return {"ok": False, "error": f"unexpected response shape: {type(preds).__name__}"}

    messages = preds.get("messages") or []
    answer = ""
    if messages:
        msg = messages[0] if isinstance(messages[0], dict) else {}
        answer = msg.get("content") or ""

    return {
        "ok":     True,
        "answer": answer,
        "trace":  preds.get("trace", []),
        "model":  preds.get("model", AGENT_ENDPOINT),
        "usage":  preds.get("usage", {}),
    }


# ---------------------------------------------------------------------------
# By-policy scoring story (synthetic, deterministic per policy_id)
# ---------------------------------------------------------------------------

def _seeded_float(seed: str, lo: float, hi: float) -> float:
    h = int(hashlib.sha256(seed.encode()).hexdigest()[:8], 16)
    r = (h % 100000) / 100000.0
    return lo + r * (hi - lo)


@router.get("/policy/{policy_id}/scoring")
async def policy_scoring(policy_id: str) -> dict:
    """Return a synthetic-but-plausible scoring story for a policy: the real
    features from the Modelling Mart plus deterministic predictions from each
    of the 4 production models. Flagged as simulated — no real inference log."""
    policy_id = policy_id.strip().upper()
    rows = await execute_query(f"""
        SELECT policy_id, current_premium, sum_insured, annual_turnover,
               industry_risk_tier, construction_type, region, postcode_sector,
               flood_zone_rating, credit_score, claim_count_5y, total_incurred_5y,
               is_coastal, urban_score
        FROM {fqn('unified_pricing_table_live')}
        WHERE policy_id = '{policy_id}' LIMIT 1
    """)
    if not rows:
        raise HTTPException(404, f"policy {policy_id} not found in Modelling Mart")
    row = rows[0]

    # Deterministic predictions seeded by policy_id
    freq_pred  = round(_seeded_float(f"{policy_id}:freq",  0.05, 0.45), 4)
    sev_pred   = round(_seeded_float(f"{policy_id}:sev",   2_500.0, 18_000.0), 0)
    demand_p   = round(_seeded_float(f"{policy_id}:demand", 0.20, 0.85), 3)
    fraud_p    = round(_seeded_float(f"{policy_id}:fraud",  0.01, 0.45), 3)

    base_premium = round(freq_pred * sev_pred, 2)
    fraud_loading = round(base_premium * (0.05 if fraud_p > 0.25 else 0.0), 2)
    demand_adj    = round(base_premium * (0.02 if demand_p < 0.4 else -0.02), 2)
    technical_premium = round(base_premium + fraud_loading + demand_adj, 2)

    # Find the current champion pack for each family (most recent)
    try:
        pack_rows = await execute_query(f"""
            SELECT model_family, pack_id, model_version, pdf_path, generated_at
            FROM (
              SELECT *, row_number() OVER (PARTITION BY model_family ORDER BY generated_at DESC) AS rn
              FROM {fqn('governance_packs_index')}
            )
            WHERE rn = 1
        """)
    except Exception:
        pack_rows = []
    packs_by_fam = {r["model_family"]: r for r in pack_rows}

    def _fam(key, label, pred, unit):
        p = packs_by_fam.get(key) or {}
        return {
            "family": key, "label": label, "prediction": pred, "unit": unit,
            "pack_id": p.get("pack_id"), "model_version": p.get("model_version"),
            "pack_generated_at": str(p.get("generated_at", "")),
        }

    await log_audit_event(
        event_type="governance_policy_lookup",
        entity_type="policy",
        entity_id=policy_id,
        details={"scoring_simulated": True},
    )

    return {
        "policy_id": policy_id,
        "simulated": True,
        "policy":    row,
        "models": [
            _fam("freq_glm",   "Frequency (GLM)",      freq_pred,  "claims/yr"),
            _fam("sev_glm",    "Severity (GLM)",       sev_pred,   "GBP"),
            _fam("demand_gbm", "Demand (GBM)",         demand_p,   "conversion p"),
            _fam("fraud_gbm",  "Fraud (GBM)",          fraud_p,    "fraud p"),
        ],
        "price_build_up": [
            {"label": "Base technical premium (freq × severity)", "amount": base_premium},
            {"label": "Fraud-risk loading",                        "amount": fraud_loading},
            {"label": "Demand-elasticity adjustment",              "amount": demand_adj},
            {"label": "Technical premium",                         "amount": technical_premium, "emphasis": True},
        ],
        "quote_timestamp": datetime.now(timezone.utc).isoformat(),
        "note": "Scoring story simulated from the policy's real features — inference-log wiring is Phase 2.",
    }

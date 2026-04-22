"""Regulatory-grade PDF model report generator.

Produces a professional PDF document for any pricing model, suitable for
regulatory submission and actuarial sign-off. Sections:
1. Model Identity
2. Data Lineage
3. Performance Metrics
4. Explainability (feature importance, relativities)
5. Regulatory Assessment
6. Approval Chain & Audit Trail
"""

import io
import json
import logging
import tempfile
from datetime import datetime
from typing import Any

from fpdf import FPDF

logger = logging.getLogger(__name__)

# Corporate color palette
NAVY = (30, 41, 59)       # #1e293b
BLUE = (59, 130, 246)     # #3b82f6
GREEN = (34, 197, 94)     # #22c55e
RED = (239, 68, 68)       # #ef4444
AMBER = (245, 158, 11)    # #f59e0b
GRAY = (107, 114, 128)    # #6b7280
WHITE = (255, 255, 255)
LIGHT_GRAY = (243, 244, 246)


class ModelReport(FPDF):
    """FPDF subclass with branded headers/footers for Bricksurance SE."""

    def __init__(self, model_name: str, **kwargs):
        super().__init__(**kwargs)
        self.model_name = model_name
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*NAVY)
        self.cell(0, 5, "Bricksurance SE  |  Pricing Model Validation Report", align="L")
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*GRAY)
        self.cell(0, 5, f"Generated: {datetime.utcnow().strftime('%d %B %Y %H:%M UTC')}", align="R", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*BLUE)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(*GRAY)
        self.cell(0, 5, "CONFIDENTIAL -For regulatory and internal use only", align="L")
        self.cell(0, 5, f"Page {self.page_no()}/{{nb}}", align="R")

    def section_title(self, num: int, title: str):
        """Render a numbered section header."""
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(*NAVY)
        self.cell(0, 10, f"{num}. {title}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*BLUE)
        self.line(10, self.get_y(), 80, self.get_y())
        self.ln(3)

    def context_box(self, text: str):
        """Render a light-gray context box explaining the section."""
        self.set_fill_color(*LIGHT_GRAY)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(*GRAY)
        self.multi_cell(0, 4, text, fill=True)
        self.ln(2)

    def key_value(self, key: str, value: str):
        """Render a key: value pair."""
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*NAVY)
        self.cell(55, 5, key)
        self.set_font("Helvetica", "", 9)
        self.set_text_color(0, 0, 0)
        self.cell(0, 5, str(value), new_x="LMARGIN", new_y="NEXT")

    def metric_table(self, rows: list[tuple[str, str, str]], header: tuple = ("Metric", "Value", "Benchmark")):
        """Render a 3-column metrics table."""
        col_w = [70, 50, 70]
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(*NAVY)
        self.set_text_color(*WHITE)
        for i, h in enumerate(header):
            self.cell(col_w[i], 6, h, border=1, fill=True, align="C")
        self.ln()
        self.set_text_color(0, 0, 0)
        for j, (metric, value, bench) in enumerate(rows):
            fill = j % 2 == 0
            if fill:
                self.set_fill_color(248, 250, 252)
            self.set_font("Helvetica", "", 8)
            self.cell(col_w[0], 5, metric, border=1, fill=fill)
            self.set_font("Helvetica", "B", 8)
            self.cell(col_w[1], 5, str(value), border=1, fill=fill, align="C")
            self.set_font("Helvetica", "", 8)
            self.cell(col_w[2], 5, str(bench), border=1, fill=fill, align="C")
            self.ln()
        self.ln(2)


def _safe(val, fmt=None) -> str:
    """Safely format a value for display."""
    if val is None:
        return "-"
    try:
        if fmt == "pct":
            return f"{float(val):.1f}%"
        if fmt == "f2":
            return f"{float(val):.2f}"
        if fmt == "f4":
            return f"{float(val):.4f}"
        if fmt == "int":
            return f"{int(float(val)):,}"
        if fmt == "gbp":
            return f"£{float(val):,.0f}"
        return str(val)
    except (ValueError, TypeError):
        return str(val)


def generate_chart_image(data: dict, chart_type: str) -> bytes | None:
    """Generate a matplotlib chart and return PNG bytes."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(6, 3), dpi=120)

        if chart_type == "metrics_bar":
            metrics = data.get("metrics", {})
            names = list(metrics.keys())
            values = [float(v) if v else 0 for v in metrics.values()]
            colors = ["#3b82f6" if v >= 0 else "#ef4444" for v in values]
            ax.barh(names, values, color=colors, height=0.5)
            ax.set_title("Model Performance Metrics", fontsize=10, fontweight="bold")
            ax.tick_params(labelsize=7)

        elif chart_type == "feature_importance":
            features = data.get("features", [])[:15]
            if not features:
                plt.close()
                return None
            names = [f["name"] for f in features]
            values = [float(f["importance"]) for f in features]
            ax.barh(names[::-1], values[::-1], color="#3b82f6", height=0.6)
            ax.set_title("Top Feature Importances", fontsize=10, fontweight="bold")
            ax.tick_params(labelsize=7)

        elif chart_type == "regulatory_gauge":
            score = float(data.get("score", 0))
            colors_gauge = ["#ef4444", "#f59e0b", "#22c55e"]
            wedges = [33.3, 33.3, 33.4]
            ax.pie(wedges, colors=colors_gauge, startangle=180,
                   wedgeprops=dict(width=0.3))
            ax.text(0, -0.1, f"{score:.0f}/100", fontsize=20, fontweight="bold",
                    ha="center", va="center")
            ax.set_title("Regulatory Suitability", fontsize=10, fontweight="bold")

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=120)
        plt.close()
        buf.seek(0)
        return buf.read()
    except Exception as e:
        logger.warning("Chart generation failed: %s", e)
        return None


def build_model_report(
    model: dict[str, Any],
    decision: dict[str, Any] | None,
    audit_events: list[dict],
    features: list[dict],
) -> bytes:
    """Build a complete model validation PDF report. Returns PDF bytes."""

    pdf = ModelReport(model.get("model_config_id", "Unknown"), orientation="P", unit="mm", format="A4")
    pdf.alias_nb_pages()
    pdf.add_page()

    # ═══════════════════════════════════════════════════════════════
    # TITLE PAGE
    # ═══════════════════════════════════════════════════════════════
    pdf.ln(15)
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(*NAVY)
    pdf.cell(0, 12, "Model Validation Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 8, model.get("model_config_id", ""), align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(*GRAY)
    model_type = model.get("model_type") or model.get("model_family", "")
    target = model.get("target_column", "")
    pdf.cell(0, 6, f"{model_type}  |  Target: {target}", align="C", new_x="LMARGIN", new_y="NEXT")

    # Status badge
    pdf.ln(8)
    status = "PENDING REVIEW"
    badge_color = AMBER
    if decision:
        status = decision.get("decision", "PENDING")
        if status == "APPROVED":
            badge_color = GREEN
        elif status == "REJECTED":
            badge_color = RED
    pdf.set_fill_color(*badge_color)
    pdf.set_text_color(*WHITE)
    pdf.set_font("Helvetica", "B", 12)
    w = pdf.get_string_width(f"  {status}  ") + 10
    pdf.cell(w, 8, f"  {status}  ", fill=True, align="C",
             new_x="LMARGIN", new_y="NEXT")
    pdf.set_x((210 - w) / 2)  # This doesn't work retroactively; status is left-aligned which is fine

    # ═══════════════════════════════════════════════════════════════
    # 1. MODEL IDENTITY
    # ═══════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title(1, "Model Identity")
    pdf.context_box(
        "Core identification details for this model version. The MLflow Run ID "
        "provides a unique, immutable reference for reproducibility."
    )

    pdf.key_value("Model Name", model.get("model_config_id", "-"))
    pdf.key_value("Model Family", model.get("model_family", "-"))
    pdf.key_value("Model Type", model.get("model_type", "-"))
    pdf.key_value("Target Variable", model.get("target_column", "-"))
    pdf.key_value("Feature Count", _safe(model.get("feature_count"), "int"))
    pdf.key_value("Rank (in cohort)", _safe(model.get("rank")))
    pdf.key_value("MLflow Run ID", model.get("mlflow_run_id", "-"))
    pdf.key_value("Evaluated At", model.get("evaluated_at", "-"))
    pdf.key_value("Recommended Action", model.get("recommended_action", "-"))
    pdf.ln(3)

    # ═══════════════════════════════════════════════════════════════
    # 2. DATA LINEAGE
    # ═══════════════════════════════════════════════════════════════
    pdf.section_title(2, "Data Lineage")
    pdf.context_box(
        "Training data provenance tracked automatically by Unity Catalog. "
        "Delta Time Travel ensures any historical state can be reconstructed."
    )

    pdf.key_value("Training Table", model.get("upt_table", "unified_pricing_table_live"))
    pdf.key_value("Delta Version", _safe(model.get("upt_delta_version")))
    pdf.key_value("Feature Count", _safe(model.get("feature_count"), "int"))
    pdf.ln(2)

    # Feature list
    if features:
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(0, 5, "Feature List:", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 7)
        col_w = [60, 50, 30, 50]
        pdf.set_fill_color(*NAVY)
        pdf.set_text_color(*WHITE)
        for i, h in enumerate(["Feature Name", "Group", "Type", "Description"]):
            pdf.cell(col_w[i], 5, h, border=1, fill=True, align="C")
        pdf.ln()
        pdf.set_text_color(0, 0, 0)
        for j, f in enumerate(features[:30]):
            fill = j % 2 == 0
            if fill:
                pdf.set_fill_color(248, 250, 252)
            pdf.cell(col_w[0], 4, str(f.get("feature_name", ""))[:30], border=1, fill=fill)
            pdf.cell(col_w[1], 4, str(f.get("feature_group", ""))[:25], border=1, fill=fill)
            pdf.cell(col_w[2], 4, str(f.get("data_type", ""))[:15], border=1, fill=fill)
            pdf.cell(col_w[3], 4, str(f.get("description", ""))[:25], border=1, fill=fill)
            pdf.ln()
        pdf.ln(2)

    # ═══════════════════════════════════════════════════════════════
    # 3. PERFORMANCE METRICS
    # ═══════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title(3, "Performance Metrics")
    pdf.context_box(
        "All metrics are computed on a held-out test set not seen during training. "
        "Industry benchmarks are indicative for commercial P&C pricing models."
    )

    # Build metrics table
    metric_rows = []
    if model.get("gini"):
        metric_rows.append(("Gini Coefficient", _safe(model["gini"], "f4"), "> 0.30 (good discrimination)"))
    if model.get("rmse"):
        metric_rows.append(("RMSE", _safe(model["rmse"], "f4"), "Lower is better"))
    if model.get("mae"):
        metric_rows.append(("MAE", _safe(model["mae"], "f4"), "Lower is better"))
    if model.get("r2"):
        metric_rows.append(("R-Squared", _safe(model["r2"], "f4"), "> 0.0 (explains variance)"))
    if model.get("aic"):
        metric_rows.append(("AIC", _safe(model["aic"], "f2"), "Lower is better (parsimony)"))
    if model.get("bic"):
        metric_rows.append(("BIC", _safe(model["bic"], "f2"), "Lower is better (parsimony)"))
    if model.get("roc_auc"):
        metric_rows.append(("ROC AUC", _safe(model["roc_auc"], "f4"), "> 0.70 (good discrimination)"))
    if model.get("lift_decile1"):
        metric_rows.append(("Lift (Top Decile)", _safe(model["lift_decile1"], "f2"), "> 2.0 (good separation)"))
    if model.get("psi"):
        metric_rows.append(("PSI (Stability)", _safe(model["psi"], "f4"), "< 0.10 (stable)"))

    if metric_rows:
        pdf.metric_table(metric_rows)

    # Metrics bar chart
    chart_metrics = {}
    for name in ["gini", "r2", "roc_auc"]:
        if model.get(name):
            chart_metrics[name.upper()] = model[name]
    if chart_metrics:
        img_bytes = generate_chart_image({"metrics": chart_metrics}, "metrics_bar")
        if img_bytes:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(img_bytes)
                tmp.flush()
                pdf.image(tmp.name, x=15, w=100)
            pdf.ln(3)

    # ═══════════════════════════════════════════════════════════════
    # 4. REGULATORY ASSESSMENT
    # ═══════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title(4, "Regulatory Assessment")
    pdf.context_box(
        "Composite score reflecting model suitability for regulatory submission. "
        "Weights: discrimination (30%), accuracy (20%), regulatory fit (20%), "
        "stability (15%), separation (15%)."
    )

    reg_score = float(model.get("regulatory_suitability_score") or 0)
    composite = float(model.get("composite_score") or 0)

    pdf.key_value("Regulatory Suitability Score", f"{reg_score:.1f} / 100")
    pdf.key_value("Composite Score", f"{composite:.4f}")
    pdf.key_value("Recommended Action", model.get("recommended_action", "-"))
    pdf.ln(2)

    # Regulatory score breakdown
    if reg_score > 0:
        level = "HIGH" if reg_score >= 70 else ("MEDIUM" if reg_score >= 50 else "LOW")
        color = GREEN if reg_score >= 70 else (AMBER if reg_score >= 50 else RED)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*color)
        pdf.cell(0, 6, f"Regulatory Readiness: {level} ({reg_score:.0f}/100)",
                 new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
        pdf.ln(1)

        # Score bar
        pdf.set_draw_color(*GRAY)
        pdf.rect(15, pdf.get_y(), 120, 6)
        pdf.set_fill_color(*color)
        pdf.rect(15, pdf.get_y(), 120 * reg_score / 100, 6, "F")
        pdf.ln(10)

    # GLM interpretability note
    family = model.get("model_family", "")
    if family and "glm" in family.lower():
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(*GRAY)
        pdf.multi_cell(0, 4,
            "GLM Interpretability: Generalised Linear Models provide transparent, "
            "additive factor relativities that are standard for regulatory pricing "
            "submissions. Each coefficient can be directly examined and justified.")
        pdf.ln(2)

    # ═══════════════════════════════════════════════════════════════
    # 5. APPROVAL CHAIN
    # ═══════════════════════════════════════════════════════════════
    pdf.section_title(5, "Approval & Sign-Off")
    pdf.context_box(
        "Human-in-the-loop governance. Every decision is recorded with the reviewer's "
        "identity, timestamp, notes, and any conditions attached to the approval."
    )

    if decision:
        pdf.key_value("Decision", decision.get("decision", "-"))
        pdf.key_value("Reviewer", decision.get("reviewer", "-"))
        pdf.key_value("Date", decision.get("decided_at", "-"))
        pdf.key_value("Notes", decision.get("reviewer_notes", "-") or "-")
        pdf.key_value("Conditions", decision.get("conditions", "-") or "-")
        pdf.key_value("Regulatory Sign-Off",
                      "Yes" if decision.get("regulatory_sign_off") in ("true", True) else "No")
    else:
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(*AMBER)
        pdf.cell(0, 6, "Pending actuarial review -no decision recorded yet.",
                 new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # ═══════════════════════════════════════════════════════════════
    # 6. AUDIT TRAIL
    # ═══════════════════════════════════════════════════════════════
    if audit_events:
        pdf.add_page()
        pdf.section_title(6, "Audit Trail")
        pdf.context_box(
            "Complete event log for this model's lifecycle. Every action -from "
            "training initiation to approval -is immutably recorded."
        )

        col_w = [35, 35, 30, 90]
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_fill_color(*NAVY)
        pdf.set_text_color(*WHITE)
        for i, h in enumerate(["Timestamp", "Event Type", "Actor", "Details"]):
            pdf.cell(col_w[i], 5, h, border=1, fill=True, align="C")
        pdf.ln()
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 6)
        for j, evt in enumerate(audit_events[:30]):
            fill = j % 2 == 0
            if fill:
                pdf.set_fill_color(248, 250, 252)
            ts = str(evt.get("event_timestamp") or evt.get("timestamp", ""))[:19]
            etype = str(evt.get("event_type", ""))[:20]
            actor = str(evt.get("actor") or evt.get("user_id", ""))[:20]
            details = str(evt.get("details_json") or evt.get("details", ""))[:50]
            pdf.cell(col_w[0], 4, ts, border=1, fill=fill)
            pdf.cell(col_w[1], 4, etype, border=1, fill=fill)
            pdf.cell(col_w[2], 4, actor, border=1, fill=fill)
            pdf.cell(col_w[3], 4, details, border=1, fill=fill)
            pdf.ln()

    # ═══════════════════════════════════════════════════════════════
    # DISCLAIMER
    # ═══════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.ln(20)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*NAVY)
    pdf.cell(0, 8, "About This Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(*GRAY)
    pdf.multi_cell(0, 4,
        "This report was generated automatically by the Bricksurance SE Pricing "
        "Governance Platform, powered by Databricks. It is intended for internal "
        "actuarial review and regulatory submission purposes.\n\n"
        "All metrics are computed on held-out test data not seen during training. "
        "The regulatory suitability score is an internal assessment and does not "
        "constitute regulatory approval.\n\n"
        "Data lineage is tracked automatically by Unity Catalog. The approval "
        "chain records are immutable and timestamped.\n\n"
        "DEMO DISCLAIMER: This is a synthetic demonstration dataset. Company names, "
        "policy data, and financial figures are entirely fictional and generated "
        "for illustrative purposes only. No real customer data is used.",
        align="C",
    )

    # Output
    return pdf.output()


# ═══════════════════════════════════════════════════════════════════════════
# Factory Run Report — full log for a model factory run
# ═══════════════════════════════════════════════════════════════════════════

class FactoryRunReport(FPDF):
    """FPDF subclass branded as a Factory Run Report."""

    def __init__(self, factory_run_id: str, **kwargs):
        super().__init__(**kwargs)
        self.factory_run_id = factory_run_id
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*NAVY)
        self.cell(0, 5, "Bricksurance SE  |  Model Factory Run Report", align="L")
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*GRAY)
        self.cell(0, 5, f"Generated: {datetime.utcnow().strftime('%d %B %Y %H:%M UTC')}", align="R", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*BLUE)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(*GRAY)
        self.cell(0, 5, "CONFIDENTIAL -For regulatory and internal use only", align="L")
        self.cell(0, 5, f"Page {self.page_no()}/{{nb}}", align="R")

    def section_title(self, num: int, title: str):
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(*NAVY)
        self.cell(0, 10, f"{num}. {title}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*BLUE)
        self.line(10, self.get_y(), 80, self.get_y())
        self.ln(3)

    def context_box(self, text: str):
        self.set_fill_color(*LIGHT_GRAY)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(*GRAY)
        self.multi_cell(0, 4, text, fill=True)
        self.ln(2)

    def key_value(self, key: str, value: str):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*NAVY)
        self.cell(55, 5, key)
        self.set_font("Helvetica", "", 9)
        self.set_text_color(0, 0, 0)
        self.cell(0, 5, str(value)[:120], new_x="LMARGIN", new_y="NEXT")


def _ascii(s: str | None) -> str:
    """fpdf2's default font is latin-1. Replace em dashes and other unsupported chars."""
    if s is None:
        return "-"
    return (
        str(s)
        .replace("—", "-").replace("–", "-")
        .replace("“", '"').replace("”", '"')
        .replace("‘", "'").replace("’", "'")
        .replace("·", "-")
    )


def build_factory_run_report(
    run_log: dict,
    configs: list[dict],
    leaderboard: list[dict],
    decisions: list[dict],
    audit_events: list[dict],
) -> bytes:
    """Produce a full-run PDF report covering:
       1. Run metadata
       2. Feature-store analysis (what Claude said)
       3. Intent + proposal summary
       4. Proposed configs table
       5. Final leaderboard
       6. Actuary decisions
       7. Audit trail
       8. Governance sign-off template
    """
    factory_run_id = str(run_log.get("factory_run_id") or "UNKNOWN")
    pdf = FactoryRunReport(factory_run_id)
    pdf.add_page()

    # ──────────────────────────────────────────────────────────────────────
    # Cover
    # ──────────────────────────────────────────────────────────────────────
    pdf.ln(8)
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(*NAVY)
    pdf.cell(0, 12, "Factory Run Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 6, f"Run ID: {factory_run_id}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # ──────────────────────────────────────────────────────────────────────
    # 1. Run metadata
    # ──────────────────────────────────────────────────────────────────────
    pdf.section_title(1, "Run Metadata")
    pdf.key_value("Created at:",   _ascii(run_log.get("created_at")))
    pdf.key_value("Created by:",   _ascii(run_log.get("created_by")))
    pdf.key_value("Source:",       _ascii(run_log.get("proposal_source")))
    pdf.key_value("Target:",       _ascii(run_log.get("intent_target")))
    pdf.key_value("Model family:", _ascii(run_log.get("intent_model_family")))
    pdf.key_value("Feature scope:",_ascii(run_log.get("intent_feature_scope")))
    pdf.key_value("Sweep size:",   _ascii(run_log.get("intent_sweep_size")))
    pdf.key_value("Focus:",        _ascii(run_log.get("intent_focus")))
    pdf.key_value("Status:",       _ascii(run_log.get("status")))
    pdf.key_value("Completed at:", _ascii(run_log.get("completed_at")))
    pdf.ln(2)

    if run_log.get("user_note"):
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*NAVY)
        pdf.cell(0, 5, "User note:", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(0, 0, 0)
        pdf.multi_cell(0, 4, _ascii(run_log.get("user_note")))
        pdf.ln(2)

    # Summary metrics, if populated
    sm = run_log.get("summary_metrics")
    if sm:
        try:
            sm_d = json.loads(sm) if isinstance(sm, str) else sm
            pdf.ln(3)
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(*NAVY)
            pdf.cell(0, 6, "Summary metrics", new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(0, 0, 0)
            for k, v in sm_d.items():
                pdf.key_value(f"{k}:", _ascii(str(v)))
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────────────────
    # 2. Feature-store analysis
    # ──────────────────────────────────────────────────────────────────────
    if run_log.get("feature_analysis"):
        pdf.add_page()
        pdf.section_title(2, "Feature-store Analysis (at the time of the run)")
        pdf.context_box(
            "Claude-Sonnet analysis of the feature catalog and UPT state at the moment this "
            "run was proposed. Captured for regulatory traceability."
        )
        fa = run_log.get("feature_analysis")
        try:
            fa_d = json.loads(fa) if isinstance(fa, str) else fa
            if isinstance(fa_d, dict):
                if fa_d.get("headline"):
                    pdf.set_font("Helvetica", "B", 10)
                    pdf.set_text_color(*NAVY)
                    pdf.multi_cell(0, 5, _ascii(fa_d["headline"]))
                    pdf.ln(1)
                for label, key in [("Strengths", "strengths"), ("Gaps", "gaps"), ("Sensitive", "sensitive")]:
                    items = fa_d.get(key) or []
                    if items:
                        pdf.set_font("Helvetica", "B", 9)
                        pdf.set_text_color(*NAVY)
                        pdf.cell(0, 5, label + ":", new_x="LMARGIN", new_y="NEXT")
                        pdf.set_font("Helvetica", "", 9)
                        pdf.set_text_color(0, 0, 0)
                        for item in items:
                            pdf.multi_cell(0, 4, "- " + _ascii(str(item)))
                        pdf.ln(1)
                rec = fa_d.get("recommended_next") or []
                if rec:
                    pdf.set_font("Helvetica", "B", 9)
                    pdf.set_text_color(*NAVY)
                    pdf.cell(0, 5, "Recommended next targets:", new_x="LMARGIN", new_y="NEXT")
                    pdf.set_font("Helvetica", "", 9)
                    pdf.set_text_color(0, 0, 0)
                    for r in rec:
                        pdf.multi_cell(0, 4, f"- {_ascii(r.get('target'))}: {_ascii(r.get('why'))}")
            else:
                pdf.multi_cell(0, 4, _ascii(str(fa_d))[:3000])
        except Exception:
            pdf.multi_cell(0, 4, _ascii(str(fa))[:3000])

    # ──────────────────────────────────────────────────────────────────────
    # 3. Proposed plan
    # ──────────────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title(3, "Proposed Plan")
    if run_log.get("plan_summary"):
        pdf.multi_cell(0, 5, _ascii(run_log.get("plan_summary")))
        pdf.ln(2)
    pdf.key_value("Configs proposed:", _ascii(run_log.get("n_configs_proposed")))

    if configs:
        pdf.ln(3)
        col_w = [22, 40, 68, 60]
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_fill_color(*NAVY)
        pdf.set_text_color(*WHITE)
        for i, h in enumerate(["Config", "Model type", "Features", "Rationale"]):
            pdf.cell(col_w[i], 5, h, border=1, fill=True, align="C")
        pdf.ln()
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 6)
        for j, cfg in enumerate(configs[:40]):
            fill = j % 2 == 0
            if fill:
                pdf.set_fill_color(248, 250, 252)
            pdf.cell(col_w[0], 4, _ascii(cfg.get("config_id"))[:22], border=1, fill=fill)
            pdf.cell(col_w[1], 4, _ascii(cfg.get("model_type"))[:22], border=1, fill=fill)
            feats = ", ".join(cfg.get("features") or [])
            pdf.cell(col_w[2], 4, _ascii(feats)[:60], border=1, fill=fill)
            pdf.cell(col_w[3], 4, _ascii(cfg.get("rationale"))[:55], border=1, fill=fill)
            pdf.ln()

    # ──────────────────────────────────────────────────────────────────────
    # 4. Final leaderboard
    # ──────────────────────────────────────────────────────────────────────
    if leaderboard:
        pdf.add_page()
        pdf.section_title(4, "Final Leaderboard")
        pdf.context_box(
            "Models ranked within each target variable by composite score (Gini-weighted). "
            "The recommended action is an internal assessment, not a regulatory approval."
        )
        col_w = [40, 22, 18, 18, 18, 38, 28]
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_fill_color(*NAVY)
        pdf.set_text_color(*WHITE)
        for i, h in enumerate(["Config", "Target", "Rank", "Gini", "AIC", "Model type", "Action"]):
            pdf.cell(col_w[i], 5, h, border=1, fill=True, align="C")
        pdf.ln()
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 7)
        for j, r in enumerate(leaderboard[:60]):
            fill = j % 2 == 0
            if fill:
                pdf.set_fill_color(248, 250, 252)
            pdf.cell(col_w[0], 4, _ascii(r.get("model_config_id"))[:30], border=1, fill=fill)
            pdf.cell(col_w[1], 4, _ascii(r.get("target_column"))[:16], border=1, fill=fill)
            pdf.cell(col_w[2], 4, _ascii(r.get("rank_within_target")), border=1, fill=fill, align="C")
            pdf.cell(col_w[3], 4, _safe(r.get("gini"), "f4"),          border=1, fill=fill, align="R")
            pdf.cell(col_w[4], 4, _safe(r.get("aic"), "f2"),           border=1, fill=fill, align="R")
            pdf.cell(col_w[5], 4, _ascii(r.get("model_type"))[:28],    border=1, fill=fill)
            pdf.cell(col_w[6], 4, _ascii(r.get("recommended_action"))[:18], border=1, fill=fill)
            pdf.ln()

    # ──────────────────────────────────────────────────────────────────────
    # 5. Actuary decisions
    # ──────────────────────────────────────────────────────────────────────
    if decisions:
        pdf.add_page()
        pdf.section_title(5, "Actuary Decisions")
        col_w = [40, 25, 30, 85]
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_fill_color(*NAVY)
        pdf.set_text_color(*WHITE)
        for i, h in enumerate(["Config", "Decision", "Reviewer", "Notes"]):
            pdf.cell(col_w[i], 5, h, border=1, fill=True, align="C")
        pdf.ln()
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 7)
        for j, d in enumerate(decisions[:30]):
            fill = j % 2 == 0
            if fill:
                pdf.set_fill_color(248, 250, 252)
            pdf.cell(col_w[0], 4, _ascii(d.get("model_config_id"))[:30], border=1, fill=fill)
            pdf.cell(col_w[1], 4, _ascii(d.get("decision"))[:20], border=1, fill=fill)
            pdf.cell(col_w[2], 4, _ascii(d.get("reviewer"))[:22], border=1, fill=fill)
            pdf.cell(col_w[3], 4, _ascii(d.get("reviewer_notes"))[:70], border=1, fill=fill)
            pdf.ln()

    # ──────────────────────────────────────────────────────────────────────
    # 6. Audit trail
    # ──────────────────────────────────────────────────────────────────────
    if audit_events:
        pdf.add_page()
        pdf.section_title(6, "Audit Trail")
        col_w = [35, 35, 30, 90]
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_fill_color(*NAVY)
        pdf.set_text_color(*WHITE)
        for i, h in enumerate(["Timestamp", "Event Type", "Actor", "Details"]):
            pdf.cell(col_w[i], 5, h, border=1, fill=True, align="C")
        pdf.ln()
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "", 6)
        for j, evt in enumerate(audit_events[:40]):
            fill = j % 2 == 0
            if fill:
                pdf.set_fill_color(248, 250, 252)
            ts = _ascii(str(evt.get("event_timestamp") or evt.get("timestamp", ""))[:19])
            etype = _ascii(str(evt.get("event_type", ""))[:20])
            actor = _ascii(str(evt.get("actor") or evt.get("user_id", ""))[:20])
            details = _ascii(str(evt.get("details_json") or evt.get("details", ""))[:55])
            pdf.cell(col_w[0], 4, ts, border=1, fill=fill)
            pdf.cell(col_w[1], 4, etype, border=1, fill=fill)
            pdf.cell(col_w[2], 4, actor, border=1, fill=fill)
            pdf.cell(col_w[3], 4, details, border=1, fill=fill)
            pdf.ln()

    # ──────────────────────────────────────────────────────────────────────
    # 7. Sign-off
    # ──────────────────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title(7, "Governance Sign-off")
    pdf.context_box(
        "Template for final actuarial and governance sign-off. Signatures recorded in "
        "the audit log; the PDF is stored in the UC reports volume."
    )
    roles = [
        ("Lead actuary",        "................................"),
        ("Head of Pricing",     "................................"),
        ("Model governance",    "................................"),
        ("Chief actuary",       "................................"),
    ]
    for role, sig in roles:
        pdf.ln(6)
        pdf.set_font("Helvetica", "B", 10); pdf.set_text_color(*NAVY)
        pdf.cell(55, 5, role + ":")
        pdf.set_font("Helvetica", "", 9); pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 5, sig, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)
        pdf.set_font("Helvetica", "I", 8); pdf.set_text_color(*GRAY)
        pdf.cell(55, 4, "Date:"); pdf.cell(0, 4, "................................", new_x="LMARGIN", new_y="NEXT")

    pdf.ln(8)
    pdf.set_font("Helvetica", "", 8); pdf.set_text_color(*GRAY)
    pdf.multi_cell(0, 4,
        "DEMO DISCLAIMER: synthetic demonstration dataset. Company names, policy data, and financial "
        "figures are fictional and generated for illustrative purposes. No real customer data is used.")

    return pdf.output()

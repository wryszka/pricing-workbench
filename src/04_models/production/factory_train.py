# Databricks notebook source
# MAGIC %md
# MAGIC # Factory Training — Real
# MAGIC
# MAGIC Fits every variant in a factory run's plan. Unlike the demo path (which
# MAGIC synthesises metrics from variant configs), this notebook:
# MAGIC
# MAGIC  * Pulls the Modelling Mart
# MAGIC  * Applies per-variant feature subset / interaction / banding
# MAGIC  * Fits a Poisson GLM (family chosen per variant)
# MAGIC  * Runs 5-fold CV for shortlist stability
# MAGIC  * Logs every run to MLflow
# MAGIC  * Registers each variant as its own UC model `factory_freq_glm_{variant_id}`
# MAGIC  * Writes real metrics back into `factory_variants`
# MAGIC
# MAGIC Factory candidates are **not** production champions. They never claim the
# MAGIC `champion` alias. They're discoverable in UC as `factory_freq_glm_*` and
# MAGIC only go live if an actuary re-trains them through the production pipeline.

# COMMAND ----------

dbutils.widgets.text("catalog_name",   "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",    "pricing_upt")
dbutils.widgets.text("factory_run_id", "")
dbutils.widgets.text("max_variants",   "15")

# COMMAND ----------

# MAGIC %pip install mlflow statsmodels scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

catalog        = dbutils.widgets.get("catalog_name")
schema         = dbutils.widgets.get("schema_name")
factory_run_id = dbutils.widgets.get("factory_run_id")
max_variants   = int(dbutils.widgets.get("max_variants") or 15)

if not factory_run_id:
    raise ValueError("factory_run_id is required")

fqn = f"{catalog}.{schema}"

import json, time
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.model_selection import KFold
from sklearn.base import BaseEstimator, RegressorMixin
import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_workbench_factory")
client = MlflowClient()

print(f"Factory run: {factory_run_id}  |  max_variants={max_variants}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load plan + training frame

# COMMAND ----------

plan_json = spark.sql(f"""
    SELECT plan_json FROM {fqn}.factory_runs WHERE run_id = '{factory_run_id}' LIMIT 1
""").collect()
if not plan_json:
    raise RuntimeError(f"No factory_runs row for {factory_run_id}")
plan = json.loads(plan_json[0]["plan_json"])[:max_variants]
print(f"Training {len(plan)} variants")

TARGET = "claim_count_5y"
KEY    = "policy_id"

mart = spark.table(f"{fqn}.unified_pricing_table_live")
pdf = mart.select(
    KEY, TARGET,
    *sorted({feat for v in plan for feat in v.get("features", [])} - {KEY, TARGET})
).toPandas()

# Drop rows with missing target
pdf = pdf.dropna(subset=[TARGET]).reset_index(drop=True)
print(f"Training frame: {len(pdf):,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — banding, interactions, Gini

# COMMAND ----------

def _apply_banding(df: pd.DataFrame, cols_numeric: list[str], strategy: str) -> pd.DataFrame:
    """Transform numeric columns per the variant's banding strategy."""
    out = df.copy()
    if strategy == "raw_linear":
        return out
    for c in cols_numeric:
        if c not in out.columns:
            continue
        s = pd.to_numeric(out[c], errors="coerce")
        if strategy == "log_then_linear":
            out[c] = np.log1p(s.clip(lower=0).fillna(0))
        elif strategy == "quantile_5_bands":
            out[c] = pd.qcut(s.rank(method="first"), q=5, labels=False, duplicates="drop").fillna(0)
        elif strategy == "quantile_10_bands":
            out[c] = pd.qcut(s.rank(method="first"), q=10, labels=False, duplicates="drop").fillna(0)
    return out


def _add_interactions(X: pd.DataFrame, interactions: list) -> pd.DataFrame:
    """Add pairwise interaction columns (simple multiplication) for each pair."""
    out = X.copy()
    for pair in interactions or []:
        a, b = pair[0], pair[1]
        # Interactions work on the encoded numeric columns — find any column
        # whose name *starts with* the raw feature name (handles one-hots).
        a_cols = [c for c in out.columns if c == a or c.startswith(a + "_")]
        b_cols = [c for c in out.columns if c == b or c.startswith(b + "_")]
        for ac in a_cols[:1]:          # keep it simple — first col of each side
            for bc in b_cols[:1]:
                if ac == bc:
                    continue
                out[f"{ac}__x__{bc}"] = out[ac] * out[bc]
    return out


def _gini(y_true, y_score):
    order = np.argsort(-np.asarray(y_score))
    y_sorted = np.asarray(y_true)[order]
    cum = np.cumsum(y_sorted) / max(1e-9, y_sorted.sum())
    n = np.arange(1, len(y_sorted) + 1) / len(y_sorted)
    return float(2 * np.trapz(cum, n) - 1)


def _pick_family(name: str):
    if name == "Negative Binomial":  return sm.families.NegativeBinomial()
    if name == "Tweedie (p=1.5)":    return sm.families.Tweedie(var_power=1.5)
    # Poisson + Quasi-Poisson both use Poisson family in statsmodels
    return sm.families.Poisson()


class PoissonGLMWrapper(BaseEstimator, RegressorMixin):
    """Minimal sklearn-compatible wrapper for a fitted statsmodels GLM result.
    Plain class so mlflow.sklearn logs it without FE wrapping / schema enforcement."""
    def __init__(self, result=None, feature_names=None):
        self.result = result
        self.feature_names = feature_names or []
    def fit(self, X, y=None):
        return self
    def predict(self, X):
        if hasattr(X, "columns"):
            cols = [c for c in self.feature_names if c in X.columns]
            X = X[cols].copy()
            for c in self.feature_names:
                if c not in X.columns:
                    X[c] = 0.0
            X = X[self.feature_names]
        arr = np.asarray(X).astype(float)
        return self.result.predict(sm.add_constant(arr, has_constant="add"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train each variant, log, register

# COMMAND ----------

# Build the full set of base categorical + numeric columns we might touch
NUMERIC_CORE = {
    "sum_insured", "annual_turnover", "current_premium",
    "credit_score", "ccj_count", "years_trading",
    "flood_zone_rating", "proximity_to_fire_station_km",
    "crime_theft_index", "subsidence_risk", "composite_location_risk",
    "urban_score", "population_density_per_km2", "elevation_metres",
    "annual_rainfall_mm", "director_stability_score",
    "employee_count_est", "distance_to_coast_km",
    "neighbourhood_claim_frequency", "is_coastal",
}

t_overall = time.time()
rows_to_upsert = []
registered_names = []

for v in plan:
    variant_id = v["variant_id"]
    features   = v.get("features", [])
    interactions = v.get("interactions", [])
    banding    = v.get("banding", "raw_linear")
    glm_family_name = (v.get("glm") or {}).get("family", "Poisson")

    present_features = [f for f in features if f in pdf.columns]
    if len(present_features) < 2:
        print(f"  {variant_id}: skipped — insufficient features")
        continue

    frame = pdf[[TARGET] + present_features].copy()
    # Banding for numeric core features only
    num_cols = [c for c in present_features if c in NUMERIC_CORE]
    frame = _apply_banding(frame, num_cols, banding)

    # One-hot for categoricals; all numerics remain numeric
    X_raw = pd.get_dummies(frame[present_features], drop_first=True, dtype=float).fillna(0.0)
    X = _add_interactions(X_raw, interactions)

    y = pd.to_numeric(frame[TARGET], errors="coerce").fillna(0).astype(float).values

    # Deterministic 80/20 split
    rng = np.random.default_rng(hash(variant_id) & 0xFFFFFFFF)
    mask = rng.integers(0, 100, len(X)) < 80
    X_tr, X_te = X.loc[mask].values, X.loc[~mask].values
    y_tr, y_te = y[mask], y[~mask]

    t0 = time.time()
    try:
        fam = _pick_family(glm_family_name)
        glm = sm.GLM(y_tr, sm.add_constant(X_tr, has_constant="add"), family=fam)
        res = glm.fit(maxiter=100)
        preds_te = res.predict(sm.add_constant(X_te, has_constant="add"))
    except Exception as e:
        print(f"  {variant_id}: FIT FAILED — {type(e).__name__}: {e}")
        continue

    # Metrics
    gini = _gini(y_te, preds_te) if y_te.sum() > 0 else 0.0
    mae  = float(np.mean(np.abs(preds_te - y_te)))
    aic  = float(res.aic)
    bic  = float(res.bic)
    # Deviance explained = 1 - (deviance / null_deviance)
    dev_exp = 0.0
    try:
        dev_exp = float(1.0 - (res.deviance / max(1e-9, res.null_deviance)))
    except Exception:
        pass

    # Quick 5-fold CV Gini
    cv_ginis = []
    try:
        kf = KFold(n_splits=5, shuffle=True, random_state=42)
        Xv, yv = X.values, y
        for tr_idx, te_idx in kf.split(Xv):
            g = sm.GLM(yv[tr_idx], sm.add_constant(Xv[tr_idx], has_constant="add"), family=fam)
            r = g.fit(maxiter=60)
            p = r.predict(sm.add_constant(Xv[te_idx], has_constant="add"))
            if yv[te_idx].sum() > 0:
                cv_ginis.append(_gini(yv[te_idx], p))
    except Exception as e:
        print(f"  {variant_id}: CV skipped ({e})")
    cv_gini_mean = float(np.mean(cv_ginis)) if cv_ginis else 0.0
    cv_gini_std  = float(np.std(cv_ginis))  if cv_ginis else 0.0

    # Register model
    feature_names = list(X.columns)
    uc_name = f"{fqn}.factory_freq_glm_{variant_id}"
    with mlflow.start_run(run_name=f"factory_{factory_run_id}_{variant_id}") as run:
        mlflow.log_params({
            "factory_run_id": factory_run_id, "variant_id": variant_id,
            "n_features": len(feature_names),
            "n_interactions": len(interactions),
            "banding": banding, "glm_family": glm_family_name,
            "train_rows": int(len(X_tr)), "test_rows": int(len(X_te)),
        })
        mlflow.log_metrics({
            "gini": gini, "aic": aic, "bic": bic,
            "deviance_explained": dev_exp, "mae": mae,
            "cv_gini_mean": cv_gini_mean, "cv_gini_std": cv_gini_std,
        })
        mlflow.set_tags({
            "factory_run_id": factory_run_id, "variant_id": variant_id,
            "family": "factory_freq_glm", "simulated": "false",
            "story": "factory_candidate",
        })
        wrapper = PoissonGLMWrapper(res, feature_names)
        from mlflow.models.signature import infer_signature
        sample_X = X.head(5)
        sample_pred = wrapper.predict(sample_X)
        signature = infer_signature(sample_X, sample_pred)
        mlflow.sklearn.log_model(
            sk_model=wrapper,
            artifact_path="model",
            registered_model_name=uc_name,
            signature=signature,
        )
        # Log the relativities CSV for pack gen
        rel_df = pd.DataFrame({
            "feature":     ["intercept"] + feature_names,
            "coefficient": res.params.tolist(),
            "relativity":  np.exp(res.params).tolist(),
            "p_value":     res.pvalues.tolist(),
        })
        rel_path = f"/tmp/{variant_id}_relativities.csv"
        rel_df.to_csv(rel_path, index=False)
        mlflow.log_artifact(rel_path)

    registered_names.append(uc_name)
    rows_to_upsert.append({
        "variant_id": variant_id,
        "metrics": {
            "gini": round(gini, 4), "aic": round(aic, 2), "bic": round(bic, 2),
            "deviance_explained": round(dev_exp, 4), "mae": round(mae, 4),
            "cv_gini_mean": round(cv_gini_mean, 4), "cv_gini_std": round(cv_gini_std, 4),
        },
        "uc_name": uc_name, "mlflow_run_id": run.info.run_id,
        "n_features": len(feature_names),
    })

    print(f"  {variant_id}: gini={gini:.4f} aic={aic:.0f} cv_gini={cv_gini_mean:.4f}±{cv_gini_std:.4f}  ({time.time()-t0:.1f}s)")

print(f"\nTotal training time: {time.time()-t_overall:.1f}s  |  {len(rows_to_upsert)}/{len(plan)} variants")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert real metrics into factory_variants

# COMMAND ----------

# Replace the synthetic rows (written by /api/factory-real/approve in the app)
# with real metrics. We delete-and-insert rather than MERGE for simplicity.
spark.sql(f"DELETE FROM {fqn}.factory_variants WHERE run_id = '{factory_run_id}'")

if rows_to_upsert:
    # Re-insert by joining the plan (for name + category + config) with the real metrics
    plan_by_id = {v["variant_id"]: v for v in plan}
    value_rows = []
    for row in rows_to_upsert:
        v = plan_by_id[row["variant_id"]]
        full_cfg = {**v, "metrics": row["metrics"], "uc_name": row["uc_name"],
                    "mlflow_run_id": row["mlflow_run_id"]}
        def _q(s):
            return str(s).replace("'", "''")
        value_rows.append(
            "SELECT "
            f"'{factory_run_id}' AS run_id, "
            f"'{row['variant_id']}' AS variant_id, "
            f"'{_q(v['name'])}' AS name, "
            f"'{v['category']}' AS category, "
            f"'{_q(json.dumps(full_cfg))}' AS config_json, "
            f"'{_q(json.dumps(row['metrics']))}' AS metrics_json, "
            f"{row['n_features']} AS n_features, "
            f"current_timestamp() AS created_at "
        )
    spark.sql(f"""
        INSERT INTO {fqn}.factory_variants
        {' UNION ALL '.join(value_rows)}
    """)

# Mark the factory run complete
spark.sql(f"""
    UPDATE {fqn}.factory_runs
    SET status = 'COMPLETED', duration_seconds = {time.time()-t_overall:.1f}
    WHERE run_id = '{factory_run_id}'
""")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "factory_run_id": factory_run_id,
    "trained": len(rows_to_upsert),
    "planned": len(plan),
    "uc_models": registered_names,
    "elapsed_seconds": round(time.time()-t_overall, 1),
}))

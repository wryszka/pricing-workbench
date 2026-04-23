# Databricks notebook source
# MAGIC %md
# MAGIC # Compare & Test — batch scoring for candidate vs champion
# MAGIC
# MAGIC Given a model family, a list of UC versions (2-5) and an optional what-if
# MAGIC scenario, this notebook:
# MAGIC  1. Loads each version via `mlflow.pyfunc.load_model`
# MAGIC  2. Pulls a stratified 5000-policy sample from the Modelling Mart (or
# MAGIC     a quote sample for demand_gbm)
# MAGIC  3. Applies the scenario as a feature perturbation in memory
# MAGIC  4. Scores each version on the same rows → apples-to-apples
# MAGIC  5. Computes: score distribution, A-vs-B shift, segment breakdown,
# MAGIC     outlier list, fresh holdout metric per version
# MAGIC  6. Writes a compact summary + heavy result to `{fqn}.compare_results`
# MAGIC     keyed on a cache hash so the app can poll for it
# MAGIC  7. Returns the cache_key + headline numbers via `dbutils.notebook.exit`

# COMMAND ----------

dbutils.widgets.text("catalog_name",   "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name",    "pricing_upt")
dbutils.widgets.text("model_family",   "freq_glm")
dbutils.widgets.text("versions",       "")              # csv e.g. "30,31"
dbutils.widgets.text("portfolio_size", "5000")
dbutils.widgets.text("scenario_id",    "none")
dbutils.widgets.text("requested_by",   "app")

# COMMAND ----------

# MAGIC %pip install mlflow statsmodels lightgbm scikit-learn --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

catalog    = dbutils.widgets.get("catalog_name")
schema     = dbutils.widgets.get("schema_name")
family     = dbutils.widgets.get("model_family")
versions_s = dbutils.widgets.get("versions")
port_size  = int(dbutils.widgets.get("portfolio_size") or 5000)
scenario   = dbutils.widgets.get("scenario_id") or "none"
user       = dbutils.widgets.get("requested_by") or "app"

fqn      = f"{catalog}.{schema}"
uc_name  = f"{fqn}.{family}"
VALID    = {"freq_glm", "sev_glm", "demand_gbm", "fraud_gbm"}
if family not in VALID:
    raise ValueError(f"family must be one of {VALID}, got '{family}'")

versions = [v.strip() for v in versions_s.split(",") if v.strip()]
if not 2 <= len(versions) <= 5:
    raise ValueError(f"versions must list 2-5 entries, got {versions}")

import json, hashlib, time, traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient

# MLflow pyfunc's schema enforcement rejects float64 columns for "integer"
# slots when the df contains NaN (which is how pandas represents null integers).
# For batch comparison scoring we don't need schema enforcement — the models'
# own predict() paths handle the types. Disable both common enforcement hooks.
def _noop_enforce(data, *args, **kwargs):
    return data
try:
    from mlflow.models import utils as _mmu
    for _name in ("_enforce_schema", "_enforce_mlflow_datatype", "_enforce_tensor_spec", "_enforce_col_spec_type"):
        if hasattr(_mmu, _name):
            setattr(_mmu, _name, _noop_enforce)
except Exception as _e:
    print(f"  schema-enforcement monkeypatch skipped: {_e}")
try:
    import mlflow.pyfunc as _pyfunc
    if hasattr(_pyfunc, "_enforce_schema"):
        _pyfunc._enforce_schema = _noop_enforce
except Exception as _e:
    print(f"  pyfunc monkeypatch skipped: {_e}")

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

run_started = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive a cache key — so re-runs of the same compare are free

# COMMAND ----------

cache_key_raw = json.dumps({
    "family": family, "versions": sorted(versions, key=int),
    "portfolio_size": port_size, "scenario": scenario,
}, sort_keys=True)
cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()[:16]
print(f"cache_key={cache_key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull the portfolio sample, stratified for a representative mix

# COMMAND ----------

from pyspark.sql import functions as F

if family == "demand_gbm":
    source_table = f"{fqn}.quotes"
    strat_cols   = ["channel", "region"]
    key_col      = "transaction_id"
else:
    source_table = f"{fqn}.unified_pricing_table_live"
    strat_cols   = ["region", "industry_risk_tier"]
    key_col      = "policy_id"

src = spark.table(source_table)
total_rows = src.count()
sample_frac = min(1.0, (port_size * 3) / max(1, total_rows))    # pull more than needed to then stratify
cand = src.sample(withReplacement=False, fraction=sample_frac, seed=42).toPandas()

# Stratified trim: roughly equal groups × strat_cols
def _stratified_pick(df: pd.DataFrame, cols, n):
    if df.empty:
        return df
    grouped = df.groupby(cols, dropna=False)
    per_group = max(1, n // max(1, len(grouped)))
    parts = []
    for _, g in grouped:
        parts.append(g.sample(n=min(len(g), per_group), random_state=42))
    out = pd.concat(parts, ignore_index=True)
    if len(out) > n:
        out = out.sample(n=n, random_state=42).reset_index(drop=True)
    return out

pdf_portfolio = _stratified_pick(cand, strat_cols, port_size)
print(f"Portfolio sample: {len(pdf_portfolio):,} rows from {source_table}")

# Pull the full set of categorical values from the source table so the dummy
# encoding step produces the same column set the model was trained on, even
# when a category is absent from the small portfolio sample.
_CAT_COLS_TO_PIN = [c for c in ("industry_risk_tier", "construction_type", "region",
                                  "channel", "flood_zone_rating", "postcode_sector")
                    if c in src.columns]
category_vocab: dict[str, list] = {}
for c in _CAT_COLS_TO_PIN:
    try:
        vals = [r[c] for r in src.select(c).distinct().toPandas().to_dict(orient="records")]
        vals = sorted([str(v) if v is not None else "(null)" for v in vals])
        category_vocab[c] = vals
    except Exception as e:
        print(f"  vocab fetch {c}: {e}")
print(f"Category vocab sizes: { {k: len(v) for k, v in category_vocab.items()} }")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply what-if scenario as a feature perturbation (pure in-memory)

# COMMAND ----------

def apply_scenario(df: pd.DataFrame, scenario_id: str, family: str) -> pd.DataFrame:
    """Return a perturbed copy of the portfolio features — scenario semantics
    defined per model family. All perturbations are bounded so the result stays
    inside the model's training range."""
    out = df.copy()
    if scenario_id == "none":
        return out

    if scenario_id == "flood_plus_1":
        # Flood risk data updates: coastal or near-coastal postcodes climb
        # one zone. Affects freq / sev / fraud.
        if "flood_zone_rating" in out.columns:
            mask = out.get("is_coastal", pd.Series(False, index=out.index)).fillna(False).astype(bool)
            out.loc[mask, "flood_zone_rating"] = (out.loc[mask, "flood_zone_rating"].fillna(3).astype(int) + 1).clip(upper=10)
            if "composite_location_risk" in out.columns:
                out.loc[mask, "composite_location_risk"] = out.loc[mask, "composite_location_risk"].fillna(50).astype(float) * 1.10
        return out

    if scenario_id == "london_claims_surge_20pct":
        # London E postcodes see a 20% frequency uplift. Feature proxy:
        # bump claim_count_5y on matching rows so fraud/demand features shift.
        if "postcode_sector" in out.columns and "claim_count_5y" in out.columns:
            london_mask = out["postcode_sector"].astype(str).str.upper().str.startswith(("E", "SE", "EC"))
            out.loc[london_mask, "claim_count_5y"] = (out.loc[london_mask, "claim_count_5y"].fillna(0).astype(float) * 1.20)
        return out

    if scenario_id == "industry_mix_up":
        # Portfolio mix shifts toward higher-risk industries — bump industry
        # tier by 1 on a random 30% of the book.
        if "industry_risk_tier" in out.columns:
            rng = np.random.default_rng(42)
            pick = rng.random(len(out)) < 0.30
            out.loc[pick, "industry_risk_tier"] = (out.loc[pick, "industry_risk_tier"].fillna(3).astype(int) + 1).clip(upper=10)
        return out

    if scenario_id == "competitor_a_minus_5pct":
        # Competitor A drops rates 5% — only affects demand_gbm.
        if "competitor_a_min_rate" in out.columns:
            out["competitor_a_min_rate"] = out["competitor_a_min_rate"].astype(float) * 0.95
        if "market_median_rate" in out.columns:
            out["market_median_rate"] = out["market_median_rate"].astype(float) * 0.975
        return out

    print(f"  (unknown scenario '{scenario_id}' — pass-through)")
    return out

pdf_scenario = apply_scenario(pdf_portfolio, scenario, family)
print(f"Scenario applied: {scenario}  (rows={len(pdf_scenario):,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load each UC version (in parallel) via mlflow.pyfunc

# COMMAND ----------

def _load_version(v):
    uri = f"models:/{uc_name}/{v}"
    mv = client.get_model_version(uc_name, v)
    try:
        r = client.get_run(mv.run_id)
        tags  = dict(r.data.tags or {})
        metrics = dict(r.data.metrics or {})
    except Exception as e:
        tags, metrics = {}, {}
        print(f"  v{v}: run fetch failed ({e})")
    model = mlflow.pyfunc.load_model(uri)
    return {
        "version":  int(v),
        "uri":      uri,
        "mv":       mv,
        "run_id":   mv.run_id,
        "tags":     tags,
        "metrics":  metrics,
        "model":    model,
    }

t0 = time.time()
with ThreadPoolExecutor(max_workers=min(5, len(versions))) as ex:
    loaded = list(ex.map(_load_version, versions))
loaded.sort(key=lambda x: x["version"])
print(f"Loaded {len(loaded)} versions in {time.time()-t0:.1f}s: {[l['version'] for l in loaded]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score each version on the perturbed portfolio

# COMMAND ----------

# freq_glm's wrapper expects already-one-hot-encoded features (drop_first=True,
# dtype=float). Other families either handle encoding inside the wrapper
# (sev_glm) or accept raw pandas categoricals (LightGBM). To keep the compare
# flow uniform we pre-encode only when the family requires it.
FREQ_GLM_FEATURES = [
    "sum_insured", "annual_turnover", "current_premium",
    "industry_risk_tier", "construction_type",
    "credit_score", "ccj_count", "years_trading",
    "flood_zone_rating", "proximity_to_fire_station_km",
    "crime_theft_index", "subsidence_risk", "composite_location_risk",
    "urban_score", "is_coastal", "population_density_per_km2",
    "elevation_metres", "annual_rainfall_mm",
    "director_stability_score", "employee_count_est",
    "distance_to_coast_km", "neighbourhood_claim_frequency",
]

def _prep_for_family(df: pd.DataFrame, family: str, model, key_col: str,
                     cat_vocab: dict | None = None) -> pd.DataFrame:
    """Apply family-specific pre-encoding + keep the FE lookup key so the FE
    pyfunc wrapper is satisfied."""
    if family == "freq_glm":
        cols_present = [c for c in FREQ_GLM_FEATURES if c in df.columns]
        sub = df[cols_present].copy()
        for c in cols_present:
            if sub[c].dtype == "object" or str(sub[c].dtype).startswith("category"):
                s = sub[c].astype(str).fillna("(null)")
                if cat_vocab and c in cat_vocab:
                    # pin the category set so get_dummies always produces the
                    # same columns regardless of which values appear in the sample
                    s = pd.Categorical(s, categories=cat_vocab[c])
                sub[c] = s
            else:
                sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(0.0)
        encoded = pd.get_dummies(sub, drop_first=True, dtype=float).fillna(0.0)
        # FE wrapper requires the lookup key even though we pass features directly.
        if key_col in df.columns:
            encoded.insert(0, key_col, df[key_col].values)
        return encoded
    # sev_glm's wrapper handles the transform itself
    if family == "sev_glm":
        out = df.copy()
        for c in out.columns:
            if out[c].dtype == "object":
                out[c] = out[c].fillna("(null)").astype(str)
        return out
    # LightGBM models (demand_gbm, fraud_gbm): cast object cols to category
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == "object":
            out[c] = out[c].astype("category")
    return out

def _predict_bypass_schema(pyfunc_model, df: pd.DataFrame) -> np.ndarray:
    """MLflow pyfunc enforces the input signature at log time, which fails when
    integer columns contain NaNs in a batch-scoring context. Two-layer bypass:
    1) dig into the pyfunc's private `_model_impl` to find the underlying
       sklearn / lightgbm / python_function predict method;
    2) as a safety net, coerce NaN-bearing int columns to nullable Int64 so
       the outer pyfunc.predict path can also succeed.
    """
    impl = getattr(pyfunc_model, "_model_impl", None)
    print(f"    impl={type(impl).__name__ if impl else None}  attrs={dir(impl)[:5] if impl else '—'}")

    # sklearn flavor — _SklearnModelWrapper.sklearn_model
    for attr in ("sklearn_model", "model"):
        if impl is not None and hasattr(impl, attr):
            inner = getattr(impl, attr)
            if hasattr(inner, "predict") and not isinstance(inner, dict):
                try:
                    print(f"    using impl.{attr} ({type(inner).__name__})")
                    return inner.predict(df)
                except Exception as ex:
                    print(f"    impl.{attr} failed: {ex}")

    # lightgbm flavor
    if impl is not None and hasattr(impl, "lgb_model"):
        print("    using impl.lgb_model")
        return impl.lgb_model.predict(df)

    # python function wrapper
    try:
        inner = pyfunc_model.unwrap_python_model()
        if hasattr(inner, "predict"):
            print(f"    using unwrap_python_model ({type(inner).__name__})")
            return inner.predict(df)
    except Exception as ex:
        print(f"    unwrap failed: {ex}")

    # Last resort — coerce types then call pyfunc
    print("    falling back to pyfunc.predict with coerced dtypes")
    schema_df = df.copy()
    try:
        schema = pyfunc_model.metadata.get_input_schema()
        for spec in (schema.inputs if schema else []):
            n = spec.name
            if n not in schema_df.columns:
                continue
            ttype = str(spec.type).lower()
            if ttype == "integer":
                schema_df[n] = pd.to_numeric(schema_df[n], errors="coerce").fillna(0).astype("int32")
            elif ttype == "long":
                schema_df[n] = pd.to_numeric(schema_df[n], errors="coerce").fillna(0).astype("int64")
            elif ttype in ("double", "float"):
                schema_df[n] = pd.to_numeric(schema_df[n], errors="coerce").astype("float64")
    except Exception as ex:
        print(f"    schema coerce failed: {ex}")
    return pyfunc_model.predict(schema_df)

predictions: dict[int, np.ndarray] = {}
score_errors: dict[int, str] = {}

for l in loaded:
    v = l["version"]
    try:
        prepped = _prep_for_family(pdf_scenario, family, l["model"], key_col, cat_vocab=category_vocab)
        preds = _predict_bypass_schema(l["model"], prepped)
        predictions[v] = np.asarray(preds, dtype=float).ravel()
        print(f"  v{v}: scored  min={predictions[v].min():.4f}  mean={predictions[v].mean():.4f}  max={predictions[v].max():.4f}")
    except Exception as e:
        score_errors[v] = f"{type(e).__name__}: {e}"
        print(f"  v{v}: SCORING FAILED — {score_errors[v]}")
        print(traceback.format_exc()[:800])

if len(predictions) < 2:
    raise RuntimeError(f"Need at least 2 successful scorings to compare. "
                       f"Loaded={len(loaded)} scored={len(predictions)} errors={score_errors}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary statistics

# COMMAND ----------

# Primary-metric nomenclature per family — used for A-vs-B framing.
FAMILY_UNIT = {
    "freq_glm":   {"label": "predicted frequency (claims/yr)", "pounds_factor": 5000,  "score_fmt": "{:.4f}"},
    "sev_glm":    {"label": "predicted severity (GBP)",        "pounds_factor": 0.08,  "score_fmt": "{:,.0f}"},
    "demand_gbm": {"label": "predicted conversion probability","pounds_factor": None,  "score_fmt": "{:.3f}"},
    "fraud_gbm":  {"label": "predicted fraud propensity",      "pounds_factor": None,  "score_fmt": "{:.3f}"},
}[family]

# Score distribution per version (quantiles + mean)
def _dist(arr):
    qs = np.quantile(arr, [0, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0])
    return {
        "mean": float(arr.mean()), "std": float(arr.std()),
        "p0": float(qs[0]), "p25": float(qs[1]), "p50": float(qs[2]),
        "p75": float(qs[3]), "p95": float(qs[4]), "p99": float(qs[5]),
        "p100": float(qs[6]),
    }

score_summary = []
for v in sorted(predictions.keys()):
    p = predictions[v]
    champion_mv = next(x for x in loaded if x["version"] == v)
    score_summary.append({
        "version": v,
        "story":   champion_mv["tags"].get("story"),
        "story_text": champion_mv["tags"].get("story_text"),
        "simulated": champion_mv["tags"].get("simulated", "false") == "true",
        "simulation_date": champion_mv["tags"].get("simulation_date"),
        "mlflow_run_id": champion_mv["run_id"],
        "training_metrics": champion_mv["metrics"],
        **_dist(p),
    })

# Pair-wise A-vs-B shift (A = first version in the sorted list)
a_version = sorted(predictions.keys())[0]
a_preds   = predictions[a_version]

pair_shifts = []
for b_version in sorted(predictions.keys()):
    if b_version == a_version:
        continue
    b_preds = predictions[b_version]
    diff    = b_preds - a_preds
    rel     = diff / np.where(np.abs(a_preds) < 1e-9, 1e-9, a_preds)
    # Buckets for quick histogram
    buckets = [(-np.inf, -0.25), (-0.25, -0.10), (-0.10, -0.02),
               (-0.02, 0.02),  (0.02, 0.10),  (0.10, 0.25), (0.25, np.inf)]
    bucket_counts = []
    for lo, hi in buckets:
        mask = (rel > lo) & (rel <= hi)
        bucket_counts.append({"lo": None if lo == -np.inf else float(lo),
                              "hi": None if hi ==  np.inf else float(hi),
                              "count": int(mask.sum())})
    pair_shifts.append({
        "a_version": a_version, "b_version": b_version,
        "mean_abs_shift":   float(np.abs(diff).mean()),
        "mean_rel_shift":   float(np.mean(rel)),
        "n_shift_gt_10pct": int((np.abs(rel) > 0.10).sum()),
        "n_shift_gt_25pct": int((np.abs(rel) > 0.25).sum()),
        "total_score_shift": float(diff.sum()),
        "total_pounds_shift": (float(diff.sum()) * FAMILY_UNIT["pounds_factor"])
                               if FAMILY_UNIT["pounds_factor"] else None,
        "histogram_buckets": bucket_counts,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segment breakdown — where is the shift concentrated?

# COMMAND ----------

def _sum_insured_band(x):
    try:
        x = float(x)
    except Exception:
        return "unknown"
    if x < 100_000:       return "< £100k"
    if x < 500_000:       return "£100-500k"
    if x < 1_000_000:     return "£500k-1m"
    if x < 5_000_000:     return "£1-5m"
    return "> £5m"

seg_df = pdf_portfolio.copy()
if family != "demand_gbm":
    seg_df["_si_band"] = seg_df.get("sum_insured", pd.Series(np.nan, index=seg_df.index)).apply(_sum_insured_band)
    segments = [("region",),
                ("industry_risk_tier",),
                ("flood_zone_rating",),
                ("_si_band",)]
else:
    segments = [("region",), ("channel",), ("industry_risk_tier",)]

segment_rows = []
for seg in segments:
    col = seg[0]
    if col not in seg_df.columns:
        continue
    g = pd.DataFrame({
        "a": a_preds,
        "b": predictions[sorted(predictions.keys())[-1]],     # compare A vs the last (= B or latest)
        "col": seg_df[col].fillna("(null)").astype(str),
    })
    grp = g.groupby("col")
    for name, rows in grp:
        if len(rows) < 5:
            continue
        a_mean = float(rows["a"].mean())
        b_mean = float(rows["b"].mean())
        rel = (b_mean - a_mean) / (a_mean if abs(a_mean) > 1e-9 else 1e-9)
        segment_rows.append({
            "segment_type": col,
            "segment":      str(name),
            "n":            int(len(rows)),
            "a_mean":       a_mean,
            "b_mean":       b_mean,
            "rel_shift":    float(rel),
        })

segment_rows.sort(key=lambda r: abs(r["rel_shift"]), reverse=True)
segment_rows = segment_rows[:30]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outlier sample — rows whose score changed most between A and latest B

# COMMAND ----------

latest_b = sorted(predictions.keys())[-1]
diff_abs = predictions[latest_b] - a_preds
rel_for_outliers = diff_abs / np.where(np.abs(a_preds) < 1e-9, 1e-9, a_preds)
outlier_idx = np.argsort(-np.abs(rel_for_outliers))[:20]

outlier_rows = []
cols_to_show = [key_col] + [c for c in ("region", "industry_risk_tier",
                                         "flood_zone_rating", "sum_insured",
                                         "credit_score", "current_premium", "channel")
                             if c in pdf_portfolio.columns]
for i in outlier_idx:
    row = pdf_portfolio.iloc[int(i)][cols_to_show].to_dict()
    row.update({
        "a_score":   float(a_preds[int(i)]),
        "b_score":   float(predictions[latest_b][int(i)]),
        "rel_shift": float(rel_for_outliers[int(i)]),
    })
    outlier_rows.append(row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fresh holdout metric per version (apples to apples)

# COMMAND ----------

from sklearn.metrics import roc_auc_score, mean_absolute_error

def _gini_sorted(y_true, y_score):
    order = np.argsort(-np.asarray(y_score))
    y_sorted = np.asarray(y_true)[order]
    cum = np.cumsum(y_sorted) / max(1e-9, y_sorted.sum())
    n = np.arange(1, len(y_sorted) + 1) / len(y_sorted)
    return float(2 * np.trapz(cum, n) - 1)

holdout_metrics = []
try:
    # Reuse the deterministic 20% split by hashing the key column — this gives
    # the SAME held-out rows regardless of how the version was originally trained.
    holdout_mask = (pdf_portfolio[key_col].astype(str).apply(lambda s: abs(hash(s)) % 100) >= 80).values

    if family == "freq_glm" and "claim_count_5y" in pdf_portfolio.columns:
        y = pdf_portfolio.loc[holdout_mask, "claim_count_5y"].fillna(0).astype(float).values
        for v in sorted(predictions.keys()):
            yp = predictions[v][holdout_mask]
            if y.sum() > 0:
                holdout_metrics.append({"version": v, "metric": "gini", "value": _gini_sorted(y, yp), "n": int(holdout_mask.sum())})
    elif family == "sev_glm" and {"claim_count_5y", "total_incurred_5y"}.issubset(pdf_portfolio.columns):
        mask = holdout_mask & (pdf_portfolio["claim_count_5y"].fillna(0) > 0).values & (pdf_portfolio["total_incurred_5y"].fillna(0) > 0).values
        if mask.sum() > 0:
            y = (pdf_portfolio.loc[mask, "total_incurred_5y"].astype(float)
                 / pdf_portfolio.loc[mask, "claim_count_5y"].astype(float)).values
            for v in sorted(predictions.keys()):
                yp = predictions[v][mask]
                holdout_metrics.append({"version": v, "metric": "gini", "value": _gini_sorted(y, yp), "n": int(mask.sum())})
                holdout_metrics.append({"version": v, "metric": "mae_gbp", "value": float(mean_absolute_error(y, yp)), "n": int(mask.sum())})
    elif family == "demand_gbm" and "converted" in pdf_portfolio.columns:
        y_raw = pdf_portfolio.loc[holdout_mask, "converted"]
        y = y_raw.astype(str).str.upper().isin({"Y", "1", "TRUE"}).astype(int).values
        if y.sum() > 0 and y.sum() < len(y):
            for v in sorted(predictions.keys()):
                yp = predictions[v][holdout_mask]
                holdout_metrics.append({"version": v, "metric": "auc", "value": float(roc_auc_score(y, yp)), "n": int(holdout_mask.sum())})
    elif family == "fraud_gbm":
        # Synthetic fraud label — deterministic hash, same formula as training notebook
        def _synth_fraud(df):
            z = (-3.5
                 + df.get("ccj_count", pd.Series(0, index=df.index)).fillna(0).astype(float) * 0.4
                 + (600 - df.get("credit_score", pd.Series(600, index=df.index)).fillna(600).astype(float)) * 0.003
                 + df.get("claim_count_5y", pd.Series(0, index=df.index)).fillna(0).astype(float) * 0.20
                 + df.get("loss_ratio_5y", pd.Series(0, index=df.index)).fillna(0).astype(float) * 0.05)
            p = 1.0 / (1.0 + np.exp(-z))
            r = df[key_col].astype(str).apply(lambda s: (abs(hash(s)) % 1_000_000) / 1_000_000.0).values
            return (r < p.values).astype(int)
        y = _synth_fraud(pdf_portfolio.loc[holdout_mask])
        if 0 < y.sum() < len(y):
            for v in sorted(predictions.keys()):
                yp = predictions[v][holdout_mask]
                holdout_metrics.append({"version": v, "metric": "auc", "value": float(roc_auc_score(y, yp)), "n": int(holdout_mask.sum())})
except Exception as e:
    print(f"holdout metric calc failed: {e}")
    print(traceback.format_exc()[:600])

print(f"Holdout rows: {holdout_metrics}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coefficient / importance diff from MLflow artefacts (top-moved features)

# COMMAND ----------

import tempfile, csv

def _download_csv(run_id, suffix):
    try:
        arts = client.list_artifacts(run_id)
    except Exception:
        return None
    target = next((a.path for a in arts if a.path.endswith(suffix)), None)
    if not target:
        return None
    with tempfile.TemporaryDirectory() as tmp:
        try:
            local = client.download_artifacts(run_id, target, dst_path=tmp)
            with open(local, newline="") as fh:
                return list(csv.DictReader(fh))
        except Exception:
            return None

explain_diff = {"type": "glm" if family.endswith("_glm") else "gbm", "rows": []}
if family.endswith("_glm"):
    # relativities.csv has feature, coefficient, relativity, p_value
    per_version = {}
    for l in loaded:
        data = _download_csv(l["run_id"], "relativities.csv") or []
        per_version[l["version"]] = {r["feature"]: float(r.get("coefficient", 0) or 0) for r in data}
    features = set().union(*per_version.values())
    a_v = sorted(per_version.keys())[0]
    b_v = sorted(per_version.keys())[-1]
    for f in features:
        a_c = per_version[a_v].get(f, 0.0)
        b_c = per_version[b_v].get(f, 0.0)
        explain_diff["rows"].append({"feature": f, "a_coef": a_c, "b_coef": b_c,
                                      "delta_coef": b_c - a_c,
                                      "a_relativity": float(np.exp(a_c)),
                                      "b_relativity": float(np.exp(b_c))})
    explain_diff["rows"].sort(key=lambda r: abs(r["delta_coef"]), reverse=True)
    explain_diff["rows"] = explain_diff["rows"][:20]
    explain_diff["a_version"] = a_v
    explain_diff["b_version"] = b_v
else:
    per_version = {}
    for l in loaded:
        data = _download_csv(l["run_id"], "importance.csv") or []
        per_version[l["version"]] = {r["feature"]: float(r.get("gain", 0) or 0) for r in data}
    features = set().union(*per_version.values())
    a_v = sorted(per_version.keys())[0]
    b_v = sorted(per_version.keys())[-1]
    for f in features:
        a_g = per_version[a_v].get(f, 0.0)
        b_g = per_version[b_v].get(f, 0.0)
        explain_diff["rows"].append({"feature": f, "a_gain": a_g, "b_gain": b_g,
                                      "delta_gain": b_g - a_g})
    explain_diff["rows"].sort(key=lambda r: abs(r["delta_gain"]), reverse=True)
    explain_diff["rows"] = explain_diff["rows"][:20]
    explain_diff["a_version"] = a_v
    explain_diff["b_version"] = b_v

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule-based reviewer (stub — real Model Serving agent wires in later)

# COMMAND ----------

def rule_based_review(score_summary, pair_shifts, holdout_metrics, segment_rows, scenario):
    findings = []
    recommendation = "INVESTIGATE"

    primary_b = next((h for h in holdout_metrics if h["metric"] in ("gini", "auc") and h["version"] == latest_b), None)
    primary_a = next((h for h in holdout_metrics if h["metric"] in ("gini", "auc") and h["version"] == a_version), None)
    if primary_a and primary_b:
        delta = primary_b["value"] - primary_a["value"]
        pct = delta / max(1e-9, primary_a["value"]) * 100
        findings.append(f"Fresh-holdout {primary_a['metric']}: A={primary_a['value']:.4f} vs B={primary_b['value']:.4f} ({pct:+.1f}%).")
        if delta >= 0.01 and pct >= 3.0:
            recommendation = "PROMOTE"
        elif delta < -0.02:
            recommendation = "REJECT"

    big_shifts = [r for r in segment_rows if abs(r["rel_shift"]) >= 0.25]
    if big_shifts:
        top3 = ", ".join(f"{r['segment_type']}={r['segment']} ({r['rel_shift']*100:+.0f}%)"
                         for r in big_shifts[:3])
        findings.append(f"{len(big_shifts)} segments shifted > 25%. Biggest: {top3}")
        if recommendation == "PROMOTE":
            recommendation = "INVESTIGATE"

    extreme = pair_shifts[-1] if pair_shifts else None
    if extreme and extreme["n_shift_gt_25pct"] / max(1, port_size) > 0.05:
        findings.append(f"{extreme['n_shift_gt_25pct']:,} policies (>5% of book) shifted > 25% — investigate before promoting.")
        if recommendation == "PROMOTE":
            recommendation = "INVESTIGATE"

    if scenario != "none":
        findings.append(f"Scenario '{scenario}' applied — interpret shifts as what-if, not steady-state model drift.")

    if not findings:
        findings.append("No material differences detected between the selected versions.")
        if recommendation == "INVESTIGATE":
            recommendation = "PROMOTE"

    return {
        "agent_type":    "rule-based (stub)",
        "recommendation": recommendation,
        "findings":       findings,
    }

review = rule_based_review(score_summary, pair_shifts, holdout_metrics, segment_rows, scenario)
print(json.dumps(review, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist to cache table + audit

# COMMAND ----------

cache_tbl = f"{fqn}.compare_results"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cache_tbl} (
        cache_key       STRING,
        family          STRING,
        versions        STRING,
        scenario        STRING,
        portfolio_size  INT,
        score_summary   STRING,
        pair_shifts     STRING,
        segment_rows    STRING,
        outlier_rows    STRING,
        holdout_metrics STRING,
        explain_diff    STRING,
        review          STRING,
        requested_by    STRING,
        generated_at    TIMESTAMP
    )
""")

payload = {
    "cache_key":      cache_key,
    "family":         family,
    "versions":       versions,
    "scenario":       scenario,
    "portfolio_size": port_size,
    "score_summary":  score_summary,
    "pair_shifts":    pair_shifts,
    "segment_rows":   segment_rows,
    "outlier_rows":   outlier_rows,
    "holdout_metrics":holdout_metrics,
    "explain_diff":   explain_diff,
    "review":         review,
    "notes": {
        "feature_snapshot":     "current Modelling Mart — time-travel disabled in this demo because simulated replays share bytes with the champion",
        "family_unit":          FAMILY_UNIT,
        "portfolio_source":     source_table,
        "score_errors":         score_errors,
        "holdout_note":         "fresh deterministic 20% hash-based holdout — same rows for every version",
    },
}

def _esc(v): return json.dumps(v).replace("'", "''")
spark.sql(f"""
    INSERT INTO {cache_tbl}
    SELECT
      '{cache_key}', '{family}',
      '{",".join(versions)}', '{scenario}', {port_size},
      '{_esc(score_summary)}', '{_esc(pair_shifts)}',
      '{_esc(segment_rows)}', '{_esc(outlier_rows)}',
      '{_esc(holdout_metrics)}', '{_esc(explain_diff)}',
      '{_esc(review)}', '{user}', current_timestamp()
""")
print(f"cached → {cache_tbl}")

det = json.dumps({"cache_key": cache_key, "family": family, "versions": versions,
                  "scenario": scenario, "portfolio_size": port_size,
                  "recommendation": review["recommendation"],
                  "n_shift_gt_25pct": sum(p["n_shift_gt_25pct"] for p in pair_shifts)}).replace("'", "''")
spark.sql(f"""
    INSERT INTO {fqn}.audit_log
      (event_id, event_type, entity_type, entity_id, entity_version, user_id, timestamp, details, source)
    SELECT uuid(), 'compare_run', 'model', '{family}', '{",".join(versions)}',
           '{user}', current_timestamp(), '{det}', 'notebook'
""")

# COMMAND ----------

# Return a tight summary payload for the app to consume
dbutils.notebook.exit(json.dumps({
    "cache_key": cache_key,
    "family": family,
    "versions": versions,
    "scenario": scenario,
    "recommendation": review["recommendation"],
    "pair_shifts_count": len(pair_shifts),
    "segments_shifted_25pct": sum(1 for r in segment_rows if abs(r["rel_shift"]) >= 0.25),
    "sample_size": int(len(pdf_portfolio)),
    "elapsed_seconds": (datetime.now(timezone.utc) - run_started).total_seconds(),
}))

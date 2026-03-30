# Databricks notebook source
# MAGIC %md
# MAGIC # Model 1: GLM Frequency — Claim Count Prediction
# MAGIC
# MAGIC Trains a Poisson GLM to predict **claim frequency** (number of claims per policy).
# MAGIC Transparent, additive relativities for regulatory justification.
# MAGIC
# MAGIC **Target:** `claim_count_5y` (count)
# MAGIC **Distribution:** Poisson (log link)

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_upt")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

mlflow.set_registry_uri("databricks-uc")
try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_upt_frequency_glm")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and prepare training data

# COMMAND ----------

upt = spark.table(f"{fqn}.unified_pricing_table_live")

feature_cols = [
    "annual_turnover", "sum_insured", "building_age_years", "current_premium",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index", "subsidence_risk",
    "composite_location_risk", "credit_score", "ccj_count", "years_trading",
    "business_stability_score", "market_median_rate",
    "credit_default_probability", "director_stability_score",
    "employee_count_est", "distance_to_coast_km", "population_density_per_km2",
    "elevation_metres", "annual_rainfall_mm",
]

select_cols = ["policy_id", "claim_count_5y"] + feature_cols
pdf = upt.select(*select_cols).toPandas()

# Fill target nulls with 0 (no claims)
pdf["claim_frequency"] = pdf["claim_count_5y"].fillna(0).astype(float)
pdf[feature_cols] = pdf[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

print(f"Training data: {len(pdf)} rows, target mean: {pdf['claim_frequency'].mean():.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train/test split by policy_id hash

# COMMAND ----------

pdf["split_hash"] = pdf["policy_id"].apply(lambda x: abs(hash(x)) % 100)
train_pdf = pdf[pdf["split_hash"] < 80].copy()
test_pdf = pdf[pdf["split_hash"] >= 80].copy()

print(f"Train: {len(train_pdf)}, Test: {len(test_pdf)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Poisson GLM

# COMMAND ----------

X_train = sm.add_constant(train_pdf[feature_cols].values)
X_test = sm.add_constant(test_pdf[feature_cols].values)
y_train = train_pdf["claim_frequency"].values
y_test = test_pdf["claim_frequency"].values

with mlflow.start_run(run_name="glm_frequency_poisson") as run:
    mlflow.log_param("model_type", "GLM_Poisson")
    mlflow.log_param("family", "Poisson")
    mlflow.log_param("link", "log")
    mlflow.log_param("features", len(feature_cols))
    mlflow.log_param("upt_table", f"{fqn}.unified_pricing_table_live")
    mlflow.log_param("train_rows", len(train_pdf))
    mlflow.log_param("test_rows", len(test_pdf))

    glm = sm.GLM(y_train, X_train, family=sm.families.Poisson(link=sm.families.links.Log()))
    result = glm.fit()

    # Predict
    y_pred = result.predict(X_test)

    # Metrics
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("aic", result.aic)
    mlflow.log_metric("bic", result.bic)

    # Log model summary as artifact
    summary_text = str(result.summary())
    with open("/tmp/glm_frequency_summary.txt", "w") as f:
        f.write(summary_text)
    mlflow.log_artifact("/tmp/glm_frequency_summary.txt")

    print(f"GLM Frequency Results:")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE:  {mae:.4f}")
    print(f"  R2:   {r2:.4f}")
    print(f"  AIC:  {result.aic:.1f}")
    print(f"  MLflow Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GLM Relativities
# MAGIC Exponentiated coefficients — multiplicative factors for rating tables.

# COMMAND ----------

import math

coef_names = ["intercept"] + feature_cols
coefficients = result.params
pvalues = result.pvalues

relativities = []
for i, name in enumerate(coef_names):
    coef = coefficients[i]
    pval = pvalues[i]
    relativity = math.exp(coef)
    relativities.append({
        "feature": name,
        "coefficient": round(float(coef), 6),
        "relativity": round(relativity, 4),
        "p_value": round(float(pval), 4),
        "significant": "Yes" if pval < 0.05 else "No",
    })

rel_df = spark.createDataFrame(relativities)
display(rel_df.orderBy("p_value"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predictions distribution

# COMMAND ----------

test_pdf["predicted_frequency"] = y_pred
result_df = spark.createDataFrame(test_pdf[["policy_id", "claim_frequency", "predicted_frequency"]].head(100))
display(result_df)

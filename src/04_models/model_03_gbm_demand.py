# Databricks notebook source
# MAGIC %md
# MAGIC # Model 3: GBM Demand — Conversion Propensity
# MAGIC
# MAGIC Trains a gradient boosted model to predict **conversion probability**
# MAGIC (will a quote turn into a bound policy?). This drives the commercial pricing
# MAGIC overlay — understanding price elasticity and demand curves.
# MAGIC
# MAGIC **Target:** `converted` (binary: 1=bound, 0=declined)
# MAGIC **Method:** Databricks AutoML (selects best from XGBoost/LightGBM)
# MAGIC **Data source:** Quote history joined with UPT features

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_upt")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

import mlflow
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, lit

mlflow.set_registry_uri("databricks-uc")
try:
    mlflow.set_experiment(f"/Workspace/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pricing_upt_demand_gbm")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load quote data enriched with UPT features
# MAGIC We join quote history with the UPT to get the full feature set at the time of quoting.

# COMMAND ----------

quotes = spark.table(f"{fqn}.internal_quote_history")
upt = spark.table(f"{fqn}.unified_pricing_table_live")

# Create binary target
quotes_labeled = (quotes
    .withColumn("converted_flag", when(col("converted") == "Y", 1.0).otherwise(0.0))
    .withColumn("competitor_flag", when(col("competitor_quoted") == "Y", 1.0).otherwise(0.0))
)

# Enrich quotes with UPT features via SIC code + postcode
# For converted quotes with policy_id, join directly
# For unconverted, join on SIC + postcode to get representative features
upt_features = upt.select(
    "sic_code", "postcode_sector",
    "flood_zone_rating", "crime_theft_index", "subsidence_risk",
    "composite_location_risk", "location_risk_tier",
    "market_median_rate", "competitor_a_min_premium", "price_index_trend",
    "credit_default_probability", "business_stability_score",
    "population_density_per_km2", "distance_to_coast_km",
).dropDuplicates(["sic_code", "postcode_sector"])

df = (quotes_labeled
    .join(upt_features, ["sic_code", "postcode_sector"], "left")
    .withColumn("quote_to_market_ratio",
        when(col("market_median_rate").isNotNull() & (col("market_median_rate") > 0),
             (col("quoted_premium") / (col("sum_insured") / 1000)) / col("market_median_rate"))
        .otherwise(None))
    .withColumn("log_quoted_premium", F.log1p(col("quoted_premium")))
    .withColumn("log_sum_insured", F.log1p(col("sum_insured")))
    .withColumn("log_turnover", F.log1p(col("annual_turnover")))
)

conversion_rate = df.agg(F.avg("converted_flag")).collect()[0][0]
print(f"Total quotes: {df.count()}")
print(f"Conversion rate: {conversion_rate:.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare features for AutoML

# COMMAND ----------

feature_cols = [
    "log_quoted_premium", "log_sum_insured", "log_turnover",
    "competitor_flag", "quote_to_market_ratio",
    "flood_zone_rating", "crime_theft_index", "subsidence_risk",
    "composite_location_risk",
    "market_median_rate", "competitor_a_min_premium", "price_index_trend",
    "credit_default_probability", "business_stability_score",
    "population_density_per_km2", "distance_to_coast_km",
]

# Fill nulls
for c in feature_cols:
    df = df.withColumn(c, F.coalesce(col(c).cast("double"), lit(0.0)))

# Add categorical
df_model = df.select(
    "quote_id", "converted_flag",
    *feature_cols,
    "sic_code", "location_risk_tier",
)

# Split
df_model = df_model.withColumn("split_hash", F.abs(F.hash(col("quote_id"))) % 100)
train_df = df_model.filter(col("split_hash") < 80)
test_df = df_model.filter(col("split_hash") >= 80)

print(f"Train: {train_df.count()}, Test: {test_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train with Databricks AutoML

# COMMAND ----------

from databricks import automl

summary = automl.classify(
    dataset=train_df,
    target_col="converted_flag",
    exclude_cols=["quote_id", "split_hash"],
    primary_metric="roc_auc",
    timeout_minutes=15,
    max_trials=20,
)

print(f"Best model: {summary.best_trial.model_description}")
print(f"Best ROC AUC: {summary.best_trial.metrics['test_roc_auc']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate best model on held-out test set

# COMMAND ----------

import mlflow.pyfunc

best_model = mlflow.pyfunc.load_model(f"runs:/{summary.best_trial.mlflow_run_id}/model")

# Convert test to pandas for sklearn-based model
test_pdf = test_df.toPandas()
feature_cols_for_pred = [c for c in test_pdf.columns if c not in ["quote_id", "converted_flag", "split_hash"]]
test_pdf["predicted_conversion"] = best_model.predict(test_pdf[feature_cols_for_pred])

# Calculate test metrics
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score

y_true = test_pdf["converted_flag"]
y_pred = test_pdf["predicted_conversion"]

roc_auc = roc_auc_score(y_true, y_pred)
accuracy = accuracy_score(y_true, y_pred.round())
precision = precision_score(y_true, y_pred.round(), zero_division=0)
recall = recall_score(y_true, y_pred.round(), zero_division=0)

print(f"Test Set Results:")
print(f"  ROC AUC:   {roc_auc:.4f}")
print(f"  Accuracy:  {accuracy:.4f}")
print(f"  Precision: {precision:.4f}")
print(f"  Recall:    {recall:.4f}")

# Log test metrics to the best run
with mlflow.start_run(run_id=summary.best_trial.mlflow_run_id):
    mlflow.log_metric("holdout_roc_auc", roc_auc)
    mlflow.log_metric("holdout_accuracy", accuracy)
    mlflow.log_metric("holdout_precision", precision)
    mlflow.log_metric("holdout_recall", recall)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance (SHAP)

# COMMAND ----------

try:
    import shap
    import matplotlib.pyplot as plt

    # Get the underlying sklearn model
    sklearn_model = best_model._model_impl.python_model
    if hasattr(sklearn_model, "predict_proba"):
        explainer = shap.TreeExplainer(sklearn_model)
        sample = test_pdf[feature_cols_for_pred].sample(min(500, len(test_pdf)), random_state=42)
        shap_values = explainer.shap_values(sample)

        fig, ax = plt.subplots(figsize=(10, 8))
        shap.summary_plot(shap_values, sample, show=False, max_display=15)
        plt.tight_layout()

        with mlflow.start_run(run_id=summary.best_trial.mlflow_run_id):
            mlflow.log_figure(fig, "shap_summary.png")
        plt.show()
        print("SHAP values logged to MLflow")
    else:
        print("Model doesn't support SHAP TreeExplainer — skipping")
except Exception as e:
    print(f"SHAP analysis skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demand Curve: Conversion vs Price Ratio
# MAGIC Shows how conversion probability changes with our price relative to market.

# COMMAND ----------

display(
    spark.createDataFrame(test_pdf[["quote_to_market_ratio", "converted_flag", "predicted_conversion"]])
    .withColumn("price_bucket", F.round(col("quote_to_market_ratio"), 1))
    .filter(col("price_bucket").between(0.3, 3.0))
    .groupBy("price_bucket")
    .agg(
        F.avg("converted_flag").alias("actual_conversion_rate"),
        F.avg("predicted_conversion").alias("predicted_conversion_rate"),
        F.count("*").alias("quote_count"),
    )
    .filter(col("quote_count") >= 10)
    .orderBy("price_bucket")
)

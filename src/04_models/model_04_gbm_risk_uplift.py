# Databricks notebook source
# MAGIC %md
# MAGIC # Model 4: GBM Risk Uplift — GLM Residual Learner
# MAGIC
# MAGIC Trains a gradient boosted model on the **residuals of the frequency GLM**
# MAGIC to capture non-linear interactions the GLM missed. This is the standard
# MAGIC "GLM + ML" approach used by sophisticated pricing teams.
# MAGIC
# MAGIC **Target:** `glm_residual` (actual frequency - GLM predicted frequency)
# MAGIC **Method:** LightGBM regression
# MAGIC **Purpose:** Identifies segments where the GLM systematically over/under-predicts

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lr_serverless_aws_us_catalog")
dbutils.widgets.text("schema_name", "pricing_upt")

catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")
fqn = f"{catalog}.{schema}"

# COMMAND ----------

import mlflow
import mlflow.spark
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import GeneralizedLinearRegression, GBTRegressor
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

mlflow.set_registry_uri("databricks-uc")
try:
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_upt_risk_uplift_gbm")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Rebuild the frequency GLM predictions
# MAGIC We retrain a quick GLM on the same data to get the base predictions,
# MAGIC then compute residuals for the GBM to learn from.

# COMMAND ----------

upt = spark.table(f"{fqn}.unified_pricing_table_live")

df = upt.withColumn("claim_frequency", F.coalesce(col("claim_count_5y"), lit(0)).cast("double"))

feature_cols_numeric = [
    "annual_turnover", "sum_insured", "building_age_years", "current_premium",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index", "subsidence_risk",
    "composite_location_risk", "credit_score", "ccj_count", "years_trading",
    "business_stability_score", "market_median_rate",
    "credit_default_probability", "director_stability_score",
    "employee_count_est", "distance_to_coast_km", "population_density_per_km2",
    "elevation_metres", "annual_rainfall_mm",
]

feature_cols_categorical = [
    "construction_type", "industry_risk_tier", "location_risk_tier", "credit_risk_tier",
]

# Additional features for GBM that GLM can't use well (interactions, non-linear)
gbm_extra_features = [
    "traffic_density_index", "air_quality_index", "average_property_value_k",
    "commercial_density_score", "historic_flood_events_10y",
    "distance_to_hospital_km", "distance_to_motorway_km",
    "broadband_speed_mbps", "average_wind_speed_mph",
    "soil_clay_content_pct", "radon_risk_level",
    "debt_to_equity_ratio", "working_capital_ratio", "revenue_growth_3y_pct",
    "supplier_concentration_score", "invoice_dispute_rate_pct",
    "profit_margin_est_pct", "asset_tangibility_ratio",
    "company_age_months", "management_experience_score",
]

all_numeric = feature_cols_numeric + gbm_extra_features

for c in all_numeric:
    df = df.withColumn(c, F.coalesce(col(c).cast("double"), lit(0.0)))

df = df.filter(col("claim_frequency").isNotNull())
df = df.withColumn("split_hash", F.abs(F.hash(col("policy_id"))) % 100)
train_df = df.filter(col("split_hash") < 80)
test_df = df.filter(col("split_hash") >= 80)

print(f"Train: {train_df.count()}, Test: {test_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Train base GLM and compute residuals

# COMMAND ----------

# Build GLM pipeline (same as Model 1)
glm_stages = []
encoded_cols = []

for cat_col in feature_cols_categorical:
    indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol=f"{cat_col}_idx", outputCol=f"{cat_col}_vec")
    glm_stages.extend([indexer, encoder])
    encoded_cols.append(f"{cat_col}_vec")

glm_assembler = VectorAssembler(
    inputCols=feature_cols_numeric + encoded_cols,
    outputCol="glm_features",
    handleInvalid="skip",
)
glm_stages.append(glm_assembler)

glm = GeneralizedLinearRegression(
    featuresCol="glm_features",
    labelCol="claim_frequency",
    predictionCol="glm_prediction",
    family="poisson",
    link="log",
    maxIter=50,
    regParam=0.01,
)
glm_stages.append(glm)

glm_pipeline = Pipeline(stages=glm_stages)
glm_model = glm_pipeline.fit(train_df)

# Score both train and test
train_scored = glm_model.transform(train_df)
test_scored = glm_model.transform(test_df)

# Compute residuals
train_scored = train_scored.withColumn("glm_residual", col("claim_frequency") - col("glm_prediction"))
test_scored = test_scored.withColumn("glm_residual", col("claim_frequency") - col("glm_prediction"))

print(f"GLM train RMSE: {RegressionEvaluator(labelCol='claim_frequency', predictionCol='glm_prediction', metricName='rmse').evaluate(train_scored):.4f}")
print(f"Mean residual: {train_scored.agg(F.avg('glm_residual')).collect()[0][0]:.6f}")
print(f"Std residual:  {train_scored.agg(F.stddev('glm_residual')).collect()[0][0]:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train GBM on residuals with expanded feature set
# MAGIC The GBM gets ALL features — including ones the GLM can't use effectively.

# COMMAND ----------

# Assemble expanded feature set for GBM
gbm_assembler = VectorAssembler(
    inputCols=all_numeric + encoded_cols,
    outputCol="gbm_features",
    handleInvalid="skip",
)

gbt = GBTRegressor(
    featuresCol="gbm_features",
    labelCol="glm_residual",
    predictionCol="uplift_prediction",
    maxDepth=5,
    maxIter=100,
    stepSize=0.1,
    subsamplingRate=0.8,
)

gbm_pipeline = Pipeline(stages=[gbm_assembler, gbt])

with mlflow.start_run(run_name="gbm_risk_uplift") as run:
    mlflow.log_param("model_type", "GBM_Risk_Uplift")
    mlflow.log_param("approach", "GLM_residual_learning")
    mlflow.log_param("base_model", "Poisson_GLM")
    mlflow.log_param("gbm_max_depth", 5)
    mlflow.log_param("gbm_max_iter", 100)
    mlflow.log_param("gbm_step_size", 0.1)
    mlflow.log_param("features_glm", len(feature_cols_numeric))
    mlflow.log_param("features_gbm_total", len(all_numeric))
    mlflow.log_param("features_gbm_extra", len(gbm_extra_features))
    mlflow.log_param("upt_table", f"{fqn}.unified_pricing_table_live")

    gbm_model = gbm_pipeline.fit(train_scored)

    # Score test set
    test_with_uplift = gbm_model.transform(test_scored)

    # Combined prediction = GLM + uplift
    test_with_uplift = test_with_uplift.withColumn(
        "combined_prediction",
        F.greatest(lit(0.0), col("glm_prediction") + col("uplift_prediction"))
    )

    # Evaluate all three: GLM only, GBM uplift, Combined
    glm_rmse = RegressionEvaluator(labelCol="claim_frequency", predictionCol="glm_prediction", metricName="rmse").evaluate(test_with_uplift)
    glm_r2 = RegressionEvaluator(labelCol="claim_frequency", predictionCol="glm_prediction", metricName="r2").evaluate(test_with_uplift)

    combined_rmse = RegressionEvaluator(labelCol="claim_frequency", predictionCol="combined_prediction", metricName="rmse").evaluate(test_with_uplift)
    combined_r2 = RegressionEvaluator(labelCol="claim_frequency", predictionCol="combined_prediction", metricName="r2").evaluate(test_with_uplift)

    uplift_rmse = RegressionEvaluator(labelCol="glm_residual", predictionCol="uplift_prediction", metricName="rmse").evaluate(test_with_uplift)
    uplift_r2 = RegressionEvaluator(labelCol="glm_residual", predictionCol="uplift_prediction", metricName="r2").evaluate(test_with_uplift)

    mlflow.log_metric("glm_only_rmse", glm_rmse)
    mlflow.log_metric("glm_only_r2", glm_r2)
    mlflow.log_metric("combined_rmse", combined_rmse)
    mlflow.log_metric("combined_r2", combined_r2)
    mlflow.log_metric("uplift_rmse", uplift_rmse)
    mlflow.log_metric("uplift_r2", uplift_r2)
    mlflow.log_metric("rmse_improvement_pct", round((glm_rmse - combined_rmse) / glm_rmse * 100, 2))

    mlflow.spark.log_model(gbm_model, "gbm_uplift_model")

    print(f"Model Comparison:")
    print(f"  {'Model':<20} {'RMSE':>10} {'R2':>10}")
    print(f"  {'-'*40}")
    print(f"  {'GLM Only':<20} {glm_rmse:>10.4f} {glm_r2:>10.4f}")
    print(f"  {'GLM + GBM Uplift':<20} {combined_rmse:>10.4f} {combined_r2:>10.4f}")
    print(f"  {'Improvement':<20} {(glm_rmse - combined_rmse) / glm_rmse * 100:>9.1f}%")
    print(f"\n  MLflow Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance: What did the GLM miss?
# MAGIC These are the features the GBM found most useful for correcting the GLM's errors.

# COMMAND ----------

gbt_model = gbm_model.stages[-1]
importances = gbt_model.featureImportances.toArray()
feature_names = all_numeric + [f"{c}_vec" for c in feature_cols_categorical]

importance_data = []
for i, imp in enumerate(importances):
    if i < len(feature_names) and imp > 0.005:
        importance_data.append({"feature": feature_names[i], "importance": round(float(imp), 4)})

imp_df = spark.createDataFrame(importance_data)
display(imp_df.orderBy(col("importance").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segments where GLM underperforms
# MAGIC Identify risk segments where the uplift model makes the biggest corrections.

# COMMAND ----------

display(
    test_with_uplift
    .withColumn("abs_uplift", F.abs(col("uplift_prediction")))
    .groupBy("industry_risk_tier", "location_risk_tier")
    .agg(
        F.count("*").alias("policies"),
        F.round(F.avg("glm_prediction"), 4).alias("avg_glm_pred"),
        F.round(F.avg("combined_prediction"), 4).alias("avg_combined_pred"),
        F.round(F.avg("claim_frequency"), 4).alias("avg_actual"),
        F.round(F.avg("abs_uplift"), 4).alias("avg_abs_uplift"),
        F.round(F.avg("uplift_prediction"), 4).alias("avg_uplift_direction"),
    )
    .orderBy(col("avg_abs_uplift").desc())
)

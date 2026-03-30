# Databricks notebook source
# MAGIC %md
# MAGIC # Model 2: GLM Severity — Average Claim Cost Prediction
# MAGIC
# MAGIC Trains a Gamma GLM to predict **claim severity** (average claim cost given a claim occurs).
# MAGIC Combined with the frequency model, this gives the **burning cost** (pure premium):
# MAGIC `Technical Price = Frequency × Severity`
# MAGIC
# MAGIC **Target:** `avg_claim_severity` (average incurred per claim)
# MAGIC **Distribution:** Gamma (log link) — standard for positive-valued cost data
# MAGIC **Filter:** Only policies with at least 1 claim

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
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

mlflow.set_registry_uri("databricks-uc")
try:
    mlflow.set_experiment(f"/Workspace/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pricing_upt_severity_glm")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and prepare training data
# MAGIC Only policies with claims — severity is conditional on a claim occurring.

# COMMAND ----------

upt = spark.table(f"{fqn}.unified_pricing_table_live")

# Target: average claim severity (total incurred / claim count)
df = (upt
    .filter(col("claim_count_5y") > 0)
    .filter(col("total_incurred_5y") > 0)
    .withColumn("avg_claim_severity",
                (col("total_incurred_5y") / col("claim_count_5y")).cast("double"))
)

feature_cols_numeric = [
    "annual_turnover", "sum_insured", "building_age_years",
    "flood_zone_rating", "proximity_to_fire_station_km", "crime_theft_index", "subsidence_risk",
    "composite_location_risk", "credit_score", "ccj_count", "years_trading",
    "business_stability_score",
    "credit_default_probability", "employee_count_est",
    "distance_to_coast_km", "population_density_per_km2",
    "elevation_metres", "annual_rainfall_mm", "soil_clay_content_pct",
]

feature_cols_categorical = [
    "construction_type", "industry_risk_tier", "location_risk_tier", "credit_risk_tier",
]

for c in feature_cols_numeric:
    df = df.withColumn(c, F.coalesce(col(c).cast("double"), lit(0.0)))

print(f"Training data: {df.count()} rows (policies with claims)")
print(f"Target mean severity: £{df.agg(F.avg('avg_claim_severity')).collect()[0][0]:,.0f}")
print(f"Target median severity: £{df.approxQuantile('avg_claim_severity', [0.5], 0.01)[0]:,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train/test split

# COMMAND ----------

df = df.withColumn("split_hash", F.abs(F.hash(col("policy_id"))) % 100)
train_df = df.filter(col("split_hash") < 80)
test_df = df.filter(col("split_hash") >= 80)

print(f"Train: {train_df.count()}, Test: {test_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build pipeline: Gamma GLM (log link)

# COMMAND ----------

stages = []
encoded_cols = []

for cat_col in feature_cols_categorical:
    indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol=f"{cat_col}_idx", outputCol=f"{cat_col}_vec")
    stages.extend([indexer, encoder])
    encoded_cols.append(f"{cat_col}_vec")

assembler = VectorAssembler(
    inputCols=feature_cols_numeric + encoded_cols,
    outputCol="features",
    handleInvalid="skip",
)
stages.append(assembler)

glm = GeneralizedLinearRegression(
    featuresCol="features",
    labelCol="avg_claim_severity",
    predictionCol="predicted_severity",
    family="gamma",
    link="log",
    maxIter=50,
    regParam=0.01,
)
stages.append(glm)

pipeline = Pipeline(stages=stages)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train and evaluate

# COMMAND ----------

with mlflow.start_run(run_name="glm_severity_gamma") as run:
    mlflow.log_param("model_type", "GLM_Gamma")
    mlflow.log_param("family", "gamma")
    mlflow.log_param("link", "log")
    mlflow.log_param("features_numeric", len(feature_cols_numeric))
    mlflow.log_param("features_categorical", len(feature_cols_categorical))
    mlflow.log_param("reg_param", 0.01)
    mlflow.log_param("upt_table", f"{fqn}.unified_pricing_table_live")
    mlflow.log_param("train_rows", train_df.count())
    mlflow.log_param("test_rows", test_df.count())
    mlflow.log_param("target_filter", "claim_count_5y > 0 AND total_incurred_5y > 0")

    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    rmse = RegressionEvaluator(labelCol="avg_claim_severity", predictionCol="predicted_severity", metricName="rmse").evaluate(predictions)
    mae = RegressionEvaluator(labelCol="avg_claim_severity", predictionCol="predicted_severity", metricName="mae").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol="avg_claim_severity", predictionCol="predicted_severity", metricName="r2").evaluate(predictions)

    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2", r2)

    glm_model = model.stages[-1]
    coefficients = glm_model.coefficients.toArray().tolist()
    intercept = glm_model.intercept

    mlflow.log_param("intercept", round(intercept, 6))
    mlflow.log_param("num_coefficients", len(coefficients))

    mlflow.spark.log_model(model, "glm_severity_model")

    print(f"GLM Severity Results:")
    print(f"  RMSE: £{rmse:,.0f}")
    print(f"  MAE:  £{mae:,.0f}")
    print(f"  R2:   {r2:.4f}")
    print(f"  Intercept: {intercept:.6f}")
    print(f"  MLflow Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Severity Relativities

# COMMAND ----------

import math

relativities = []
for i, name in enumerate(feature_cols_numeric):
    if i < len(coefficients):
        coef = coefficients[i]
        relativity = math.exp(coef)
        relativities.append({"feature": name, "coefficient": round(coef, 6), "relativity": round(relativity, 4)})

rel_df = spark.createDataFrame(relativities)
display(rel_df.orderBy(F.abs(col("coefficient")).desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Burning Cost: Frequency × Severity
# MAGIC Combine both GLMs to compute the technical price per policy.

# COMMAND ----------

# MAGIC %md
# MAGIC *Note: The burning cost combination will be done in a separate scoring notebook
# MAGIC once both GLMs are registered in Unity Catalog.*

display(predictions.select("policy_id", "avg_claim_severity", "predicted_severity",
                           "sum_insured", "construction_type", "industry_risk_tier")
        .withColumn("severity_ratio", F.round(col("predicted_severity") / col("avg_claim_severity"), 3))
        .orderBy(col("predicted_severity").desc())
        .limit(50))

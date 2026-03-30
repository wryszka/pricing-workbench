# Databricks notebook source
# MAGIC %md
# MAGIC # Model 1: GLM Frequency — Claim Count Prediction
# MAGIC
# MAGIC Trains a Poisson GLM to predict **claim frequency** (number of claims per policy).
# MAGIC This is the standard actuarial approach for technical pricing — transparent,
# MAGIC additive relativities that regulators and underwriters can inspect.
# MAGIC
# MAGIC **Target:** `claim_count_5y` (count)
# MAGIC **Distribution:** Poisson (log link)
# MAGIC **Split:** By `policy_id` hash to prevent leakage

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
from pyspark.sql.functions import col, when, lit, log as spark_log
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

mlflow.set_registry_uri("databricks-uc")
experiment_name = f"/Users/{spark.conf.get('spark.databricks.clusterUsageTags.clusterOwnerOrgId', 'default')}/pricing_upt_frequency_glm"

try:
    mlflow.set_experiment(experiment_name)
except Exception:
    mlflow.set_experiment(f"/Workspace/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pricing_upt_frequency_glm")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and prepare training data

# COMMAND ----------

upt = spark.table(f"{fqn}.unified_pricing_table_live")

# Target: claim_count_5y (fill nulls with 0 — no claims = 0 frequency)
df = upt.withColumn("claim_frequency", F.coalesce(col("claim_count_5y"), lit(0)).cast("double"))

# Select features relevant for frequency modelling
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

# Drop rows with null target or key features
df = df.filter(col("claim_frequency").isNotNull())
for c in feature_cols_numeric:
    df = df.withColumn(c, F.coalesce(col(c).cast("double"), lit(0.0)))

print(f"Training data: {df.count()} rows, target mean: {df.agg(F.avg('claim_frequency')).collect()[0][0]:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train/test split by policy_id hash

# COMMAND ----------

df = df.withColumn("split_hash", F.abs(F.hash(col("policy_id"))) % 100)
train_df = df.filter(col("split_hash") < 80)
test_df = df.filter(col("split_hash") >= 80)

print(f"Train: {train_df.count()}, Test: {test_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build pipeline: indexers + assembler + Poisson GLM

# COMMAND ----------

# String indexing and one-hot encoding for categorical features
stages = []
encoded_cols = []

for cat_col in feature_cols_categorical:
    indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol=f"{cat_col}_idx", outputCol=f"{cat_col}_vec")
    stages.extend([indexer, encoder])
    encoded_cols.append(f"{cat_col}_vec")

# Assemble all features
assembler = VectorAssembler(
    inputCols=feature_cols_numeric + encoded_cols,
    outputCol="features",
    handleInvalid="skip",
)
stages.append(assembler)

# Poisson GLM with log link
glm = GeneralizedLinearRegression(
    featuresCol="features",
    labelCol="claim_frequency",
    predictionCol="predicted_frequency",
    family="poisson",
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

with mlflow.start_run(run_name="glm_frequency_poisson") as run:
    # Log parameters
    mlflow.log_param("model_type", "GLM_Poisson")
    mlflow.log_param("family", "poisson")
    mlflow.log_param("link", "log")
    mlflow.log_param("features_numeric", len(feature_cols_numeric))
    mlflow.log_param("features_categorical", len(feature_cols_categorical))
    mlflow.log_param("reg_param", 0.01)
    mlflow.log_param("upt_table", f"{fqn}.unified_pricing_table_live")
    mlflow.log_param("train_rows", train_df.count())
    mlflow.log_param("test_rows", test_df.count())

    # Fit
    model = pipeline.fit(train_df)

    # Predict on test
    predictions = model.transform(test_df)

    # Evaluate
    rmse_eval = RegressionEvaluator(labelCol="claim_frequency", predictionCol="predicted_frequency", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="claim_frequency", predictionCol="predicted_frequency", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="claim_frequency", predictionCol="predicted_frequency", metricName="r2")

    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)

    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2", r2)

    # Log GLM coefficients
    glm_model = model.stages[-1]
    coefficients = glm_model.coefficients.toArray().tolist()
    intercept = glm_model.intercept

    mlflow.log_param("intercept", round(intercept, 6))
    mlflow.log_param("num_coefficients", len(coefficients))

    # Log the model
    mlflow.spark.log_model(model, "glm_frequency_model")

    print(f"GLM Frequency Results:")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE:  {mae:.4f}")
    print(f"  R2:   {r2:.4f}")
    print(f"  Intercept: {intercept:.6f}")
    print(f"  Coefficients: {len(coefficients)}")
    print(f"  MLflow Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GLM Relativities
# MAGIC Show the exponentiated coefficients — these are the multiplicative relativities
# MAGIC that actuaries use in rating tables.

# COMMAND ----------

import math

feature_names = feature_cols_numeric + [f"{c}_vec" for c in feature_cols_categorical]

# For numeric features, show the relativity per unit
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
# MAGIC ## Predictions distribution

# COMMAND ----------

display(predictions.select("policy_id", "claim_frequency", "predicted_frequency",
                           "construction_type", "industry_risk_tier", "location_risk_tier")
        .orderBy(col("predicted_frequency").desc())
        .limit(50))

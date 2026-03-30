# Databricks notebook source
# MAGIC %md
# MAGIC # Model 3: GBM Demand — Conversion Propensity
# MAGIC
# MAGIC Trains a gradient boosted model to predict **conversion probability**
# MAGIC (will a quote turn into a bound policy?). This drives the commercial pricing
# MAGIC overlay — understanding price elasticity and demand curves.
# MAGIC
# MAGIC **Target:** `converted` (binary: 1=bound, 0=declined)
# MAGIC **Method:** SparkML GBTClassifier
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
    user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    mlflow.set_experiment(f"/Workspace/Users/{user}/pricing_upt_demand_gbm")
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
# MAGIC ## Build pipeline: StringIndexer + Assembler + GBTClassifier

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Encode categoricals
stages = []
encoded_cols = []
for cat_col in ["sic_code", "location_risk_tier"]:
    indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol=f"{cat_col}_idx", outputCol=f"{cat_col}_vec")
    stages.extend([indexer, encoder])
    encoded_cols.append(f"{cat_col}_vec")

assembler = VectorAssembler(
    inputCols=feature_cols + encoded_cols,
    outputCol="features",
    handleInvalid="skip",
)
stages.append(assembler)

gbt = GBTClassifier(
    featuresCol="features",
    labelCol="converted_flag",
    predictionCol="predicted_conversion",
    maxDepth=5,
    maxIter=80,
    stepSize=0.1,
    subsamplingRate=0.8,
)
stages.append(gbt)

pipeline = Pipeline(stages=stages)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train and evaluate

# COMMAND ----------

with mlflow.start_run(run_name="gbt_demand_conversion") as run:
    mlflow.log_param("model_type", "GBTClassifier")
    mlflow.log_param("max_depth", 5)
    mlflow.log_param("max_iter", 80)
    mlflow.log_param("step_size", 0.1)
    mlflow.log_param("features_numeric", len(feature_cols))
    mlflow.log_param("features_categorical", 2)
    mlflow.log_param("upt_table", f"{fqn}.unified_pricing_table_live")
    mlflow.log_param("train_rows", train_df.count())
    mlflow.log_param("test_rows", test_df.count())

    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    # Metrics
    auc_eval = BinaryClassificationEvaluator(labelCol="converted_flag", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    acc_eval = MulticlassClassificationEvaluator(labelCol="converted_flag", predictionCol="predicted_conversion", metricName="accuracy")
    prec_eval = MulticlassClassificationEvaluator(labelCol="converted_flag", predictionCol="predicted_conversion", metricName="weightedPrecision")
    recall_eval = MulticlassClassificationEvaluator(labelCol="converted_flag", predictionCol="predicted_conversion", metricName="weightedRecall")

    roc_auc = auc_eval.evaluate(predictions)
    accuracy = acc_eval.evaluate(predictions)
    precision = prec_eval.evaluate(predictions)
    recall = recall_eval.evaluate(predictions)

    mlflow.log_metric("roc_auc", roc_auc)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)

    mlflow.spark.log_model(model, "gbt_demand_model")

    print(f"GBT Demand/Conversion Results:")
    print(f"  ROC AUC:   {roc_auc:.4f}")
    print(f"  Accuracy:  {accuracy:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    print(f"  MLflow Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance

# COMMAND ----------

gbt_model = model.stages[-1]
importances = gbt_model.featureImportances.toArray()
all_feature_names = feature_cols + [f"{c}_vec" for c in ["sic_code", "location_risk_tier"]]

importance_data = []
for i, imp in enumerate(importances):
    if i < len(all_feature_names) and imp > 0.005:
        importance_data.append({"feature": all_feature_names[i], "importance": round(float(imp), 4)})

imp_df = spark.createDataFrame(importance_data)
display(imp_df.orderBy(col("importance").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demand Curve: Conversion vs Price Ratio
# MAGIC Shows how conversion probability changes with our price relative to market.

# COMMAND ----------

display(
    predictions
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

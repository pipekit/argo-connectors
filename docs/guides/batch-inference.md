# Batch Inference Pipeline

Learn how to build a production batch inference pipeline using Argo Connectors to score large datasets with trained ML models.

## Overview

Batch inference (also called batch prediction or batch scoring) involves applying a trained machine learning model to large volumes of data in batches, rather than real-time. This guide shows you how to build an end-to-end batch inference pipeline using Databricks and Spark connectors.

## Use Cases

- **Daily/weekly scoring**: Score all customers or products on a schedule
- **Historical analysis**: Apply new models to historical data
- **A/B testing**: Generate predictions from multiple model versions
- **Feature generation**: Create ML features for downstream models

## Architecture

```mermaid
graph LR
    A[Load Model] --> B[Load Data]
    B --> C[Preprocess]
    C --> D[Score in Batches]
    D --> E[Post-process]
    E --> F[Save Results]
    
    style D fill:#e1f5ff
```

## Basic Batch Inference Workflow

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="batch-inference-",
    namespace="default",
    arguments=[
        Parameter(name="model-uri", value="models:/production-model/latest"),
        Parameter(name="input-path", value="s3://data-bucket/input/2024-01-01"),
        Parameter(name="output-path", value="s3://data-bucket/predictions/2024-01-01"),
    ]
) as w:
    with Steps(name="main") as s:
        # Step 1: Load and score data
        WorkflowTemplateRef(
            name="batch-score",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/ml-team/batch-inference"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="New"),

                # Use memory-optimized instances for large datasets
                Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
                Parameter(name="new-cluster-node-type", value="r5.4xlarge"),
                Parameter(name="scaling-type", value="autoscale"),
                Parameter(name="min-workers", value="5"),
                Parameter(name="max-workers", value="20"),

                # Pass workflow parameters to notebook
                Parameter(
                    name="args",
                    value="{{workflow.parameters.model-uri}},{{workflow.parameters.input-path}},{{workflow.parameters.output-path}}"
                ),
            ]
        )

        # Step 2: Validate results
        WorkflowTemplateRef(
            name="validate-output",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/ml-team/validate-predictions"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="Serverless"),
                Parameter(name="args", value="{{workflow.parameters.output-path}}"),
            ]
        )

# Submit to cluster
w.create()
```
{% endtab %}

{% tab title="YAML" %}
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: batch-inference-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: model-uri
        value: "models:/production-model/latest"
      - name: input-path
        value: "s3://data-bucket/input/2024-01-01"
      - name: output-path
        value: "s3://data-bucket/predictions/2024-01-01"
  
  templates:
    - name: main
      steps:
        # Step 1: Load and score data
        - - name: batch-score
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/batch-inference"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                
                # Use memory-optimized instances for large datasets
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "r5.4xlarge"
                - name: scaling-type
                  value: "autoscale"
                - name: min-workers
                  value: "5"
                - name: max-workers
                  value: "20"
                
                # Pass workflow parameters to notebook
                - name: args
                  value: "{{workflow.parameters.model-uri}},{{workflow.parameters.input-path}},{{workflow.parameters.output-path}}"
        
        # Step 2: Validate results
        - - name: validate-output
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/validate-predictions"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: args
                  value: "{{workflow.parameters.output-path}}"
```
{% endtab %}
{% endtabs %}

## Databricks Notebook: Batch Inference

Here's the companion Databricks notebook (`/Users/ml-team/batch-inference`):

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Inference Pipeline

# COMMAND ----------
# Get parameters from workflow
dbutils.widgets.text("model_uri", "models:/production-model/latest")
dbutils.widgets.text("input_path", "s3://data-bucket/input")
dbutils.widgets.text("output_path", "s3://data-bucket/predictions")
dbutils.widgets.text("batch_size", "10000")

model_uri = dbutils.widgets.get("model_uri")
input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"Model: {model_uri}")
print(f"Input: {input_path}")
print(f"Output: {output_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Model

# COMMAND ----------
import mlflow
from pyspark.sql.functions import struct, col

# Load model as UDF
model_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=model_uri,
    result_type="double"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------
# Load input data
df = spark.read.parquet(input_path)

print(f"Total records: {df.count():,}")
print(f"Schema:")
df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Preprocess

# COMMAND ----------
# Feature engineering
from pyspark.sql.functions import when, col, datediff, current_date

df_features = df.select(
    "customer_id",
    "age",
    "income",
    "tenure_days",
    "purchase_count",
    "total_spend",
    # Computed features
    (col("total_spend") / col("purchase_count")).alias("avg_order_value"),
    (datediff(current_date(), col("last_purchase_date"))).alias("days_since_purchase")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Score in Batches

# COMMAND ----------
# Get feature column names (excluding customer_id)
feature_cols = [c for c in df_features.columns if c != "customer_id"]

# Apply model
predictions = df_features.withColumn(
    "prediction",
    model_udf(struct(*feature_cols))
)

# Add prediction metadata
from pyspark.sql.functions import current_timestamp, lit

predictions_final = predictions.select(
    "customer_id",
    "prediction",
    current_timestamp().alias("scored_at"),
    lit(model_uri).alias("model_version")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------
# Write predictions partitioned by date
(predictions_final
    .repartition(200)  # Optimize file sizes
    .write
    .mode("overwrite")
    .parquet(output_path))

print(f"Saved {predictions_final.count():,} predictions to {output_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------
from pyspark.sql.functions import mean, stddev, min as spark_min, max as spark_max

predictions_final.select(
    spark_min("prediction").alias("min_score"),
    spark_max("prediction").alias("max_score"),
    mean("prediction").alias("mean_score"),
    stddev("prediction").alias("stddev_score")
).show()

# Distribution of predictions
predictions_final.groupBy(
    (col("prediction") * 10).cast("int").alias("score_bucket")
).count().orderBy("score_bucket").show()
```

## Advanced: Multi-Model Scoring

Score data with multiple model versions for A/B testing or ensemble predictions:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="multi-model-inference-",
    namespace="default",
    arguments=[
        Parameter(name="input-path", value="s3://data-bucket/input/2024-01-01"),
        Parameter(name="output-base-path", value="s3://data-bucket/predictions/2024-01-01"),
    ]
) as w:
    with Steps(name="main") as s:
        # Score with multiple models in parallel
        with s.parallel():
            WorkflowTemplateRef(
                name="score-model-v1",
                template_ref="databricks-connector",
                template="run-job",
                arguments=[
                    Parameter(name="code-path", value="/Users/ml-team/batch-inference"),
                    Parameter(name="task-type", value="notebook"),
                    Parameter(name="cluster-mode", value="New"),
                    Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
                    Parameter(name="new-cluster-node-type", value="r5.2xlarge"),
                    Parameter(name="scaling-type", value="autoscale"),
                    Parameter(name="min-workers", value="5"),
                    Parameter(name="max-workers", value="15"),
                    Parameter(
                        name="args",
                        value="models:/churn-model/1,{{workflow.parameters.input-path}},{{workflow.parameters.output-base-path}}/v1"
                    ),
                ]
            )

            WorkflowTemplateRef(
                name="score-model-v2",
                template_ref="databricks-connector",
                template="run-job",
                arguments=[
                    Parameter(name="code-path", value="/Users/ml-team/batch-inference"),
                    Parameter(name="task-type", value="notebook"),
                    Parameter(name="cluster-mode", value="New"),
                    Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
                    Parameter(name="new-cluster-node-type", value="r5.2xlarge"),
                    Parameter(name="scaling-type", value="autoscale"),
                    Parameter(name="min-workers", value="5"),
                    Parameter(name="max-workers", value="15"),
                    Parameter(
                        name="args",
                        value="models:/churn-model/2,{{workflow.parameters.input-path}},{{workflow.parameters.output-base-path}}/v2"
                    ),
                ]
            )

        # Compare predictions
        WorkflowTemplateRef(
            name="compare-models",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/ml-team/compare-predictions"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="Serverless"),
                Parameter(
                    name="args",
                    value="{{workflow.parameters.output-base-path}}/v1,{{workflow.parameters.output-base-path}}/v2"
                ),
            ]
        )

w.create()
```
{% endtab %}

{% tab title="YAML" %}
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: multi-model-inference-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: input-path
        value: "s3://data-bucket/input/2024-01-01"
      - name: output-base-path
        value: "s3://data-bucket/predictions/2024-01-01"
  
  templates:
    - name: main
      steps:
        # Score with multiple models in parallel
        - - name: score-model-v1
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/batch-inference"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "r5.2xlarge"
                - name: scaling-type
                  value: "autoscale"
                - name: min-workers
                  value: "5"
                - name: max-workers
                  value: "15"
                - name: args
                  value: "models:/churn-model/1,{{workflow.parameters.input-path}},{{workflow.parameters.output-base-path}}/v1"
          
          - name: score-model-v2
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/batch-inference"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "r5.2xlarge"
                - name: scaling-type
                  value: "autoscale"
                - name: min-workers
                  value: "5"
                - name: max-workers
                  value: "15"
                - name: args
                  value: "models:/churn-model/2,{{workflow.parameters.input-path}},{{workflow.parameters.output-base-path}}/v2"
        
        # Compare predictions
        - - name: compare-models
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/compare-predictions"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: args
                  value: "{{workflow.parameters.output-base-path}}/v1,{{workflow.parameters.output-base-path}}/v2"
```
{% endtab %}
{% endtabs %}

## Best Practices

### 1. Optimize Cluster Configuration

**For Large Datasets** (100GB+):
- Use memory-optimized instances (`r5` family)
- Enable autoscaling for cost efficiency
- Use spot instances for non-critical workloads

{% tabs %}
{% tab title="Python (Hera)" %}
```python
arguments=[
    Parameter(name="new-cluster-node-type", value="r5.4xlarge"),
    Parameter(name="scaling-type", value="autoscale"),
    Parameter(name="min-workers", value="10"),
    Parameter(name="max-workers", value="50"),
    Parameter(name="availability", value="SPOT"),
]
```
{% endtab %}

{% tab title="YAML" %}
```yaml
- name: new-cluster-node-type
  value: "r5.4xlarge"
- name: scaling-type
  value: "autoscale"
- name: min-workers
  value: "10"
- name: max-workers
  value: "50"
- name: availability
  value: "SPOT"
```
{% endtab %}
{% endtabs %}

### 2. Handle Failures Gracefully

Use workflow retry policies and error handling:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, RetryStrategy

with Workflow(
    generate_name="batch-inference-",
    namespace="default"
) as w:
    # Add retry strategy
    retry_strategy = RetryStrategy(
        limit=3,
        retry_policy="Always",
        backoff={
            "duration": "1m",
            "factor": 2,
            "max_duration": "10m"
        }
    )
    
    with Steps(name="main") as s:
        WorkflowTemplateRef(
            name="batch-score",
            template_ref="databricks-connector",
            template="run-job",
            retry_strategy=retry_strategy,
            arguments=[
                # ... parameters
            ]
        )
```
{% endtab %}

{% tab title="YAML" %}
```yaml
templates:
  - name: main
    steps:
      - - name: batch-score
          templateRef:
            name: databricks-connector
            template: run-job
          retryStrategy:
            limit: 3
            retryPolicy: Always
            backoff:
              duration: "1m"
              factor: 2
              maxDuration: "10m"
```
{% endtab %}
{% endtabs %}

### 3. Monitor Progress

Track scoring progress and performance:

```python
# In your Databricks notebook
from pyspark.sql.functions import current_timestamp

# Log progress
print(f"Processing started: {current_timestamp()}")
print(f"Total records to score: {df.count():,}")

# Track batch progress
checkpoint_dir = f"{output_path}/_checkpoints"
df.write.mode("overwrite").parquet(checkpoint_dir)

print(f"Checkpoint saved at: {checkpoint_dir}")
```

## Next Steps

- [Feature Engineering Guide](feature-engineering.md) - Prepare features for scoring
- [Model Training Guide](model-training.md) - Train models for batch inference
- [Multi-Step Pipelines](multi-step-pipelines.md) - Build end-to-end ML workflows
- [Databricks Examples](../connectors/databricks/hera-examples.md) - More Databricks patterns

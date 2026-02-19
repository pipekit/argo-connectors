# Data Ingestion Pipeline

Learn how to build production data ingestion pipelines to extract, validate, and load data using Argo Connectors.

## Overview

Data ingestion is the first step in most ML workflows - loading raw data from various sources, validating quality, and preparing it for downstream processing.

## Use Cases

- **Batch data loads**: Daily/hourly ingestion from databases, APIs, or file systems
- **Data validation**: Check schema, completeness, and data quality
- **Format conversion**: Convert between formats (CSV to Parquet, JSON to Delta, etc.)
- **Incremental loads**: Load only new or changed data

## Architecture

```mermaid
graph LR
    A[Data Source] --> B[Extract]
    B --> C[Validate]
    C --> D[Transform]
    D --> E[Load to Storage]
    E --> F[Data Catalog]
    
    style B fill:#e1f5ff
    style C fill:#fff4e1
```

## Basic Data Ingestion

### CSV to Parquet Conversion

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Steps, Step, TemplateRef, Parameter

with Workflow(
    generate_name="data-ingestion-",
    namespace="default",
    entrypoint="main",
    arguments=[
        Parameter(name="source-path", value="s3://raw-data/2024-01-01/data.csv"),
        Parameter(name="target-path", value="s3://processed-data/2024-01-01/"),
    ]
) as w:
    with Steps(name="main"):
        Step(
            name="ingest-and-convert",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/data-team/csv-to-parquet",
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "i3.xlarge",
                "new-cluster-num-workers": "3",
                "args": "{{workflow.parameters.source-path}},{{workflow.parameters.target-path}}",
            }
        )

w.create()
```
{% endtab %}

{% tab title="YAML" %}
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: data-ingestion-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: source-path
        value: "s3://raw-data/2024-01-01/data.csv"
      - name: target-path
        value: "s3://processed-data/2024-01-01/"
  
  templates:
    - name: main
      steps:
        - - name: ingest-and-convert
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/csv-to-parquet"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "i3.xlarge"
                - name: new-cluster-num-workers
                  value: "3"
                - name: args
                  value: "{{workflow.parameters.source-path}},{{workflow.parameters.target-path}}"
```
{% endtab %}
{% endtabs %}

### Databricks Notebook: CSV to Parquet

```python
# Databricks notebook: /Users/data-team/csv-to-parquet

# COMMAND ----------
# Get parameters
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_path", "")

source_path = dbutils.widgets.get("source_path")
target_path = dbutils.widgets.get("target_path")

print(f"Source: {source_path}")
print(f"Target: {target_path}")

# COMMAND ----------
# Read CSV data
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(source_path))

print(f"Loaded {df.count():,} records")
df.printSchema()

# COMMAND ----------
# Write as Parquet
(df.write
   .mode("overwrite")
   .parquet(target_path))

print(f"Saved to {target_path}")
```

## Data Validation Pipeline

Validate data quality before loading:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Steps, Step, TemplateRef, Parameter

with Workflow(
    generate_name="data-ingestion-with-validation-",
    namespace="default",
    entrypoint="main",
    arguments=[
        Parameter(name="source-path", value="s3://raw-data/2024-01-01/"),
        Parameter(name="target-path", value="s3://processed-data/2024-01-01/"),
    ]
) as w:
    with Steps(name="main"):
        # Step 1: Load and validate
        Step(
            name="validate-data",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/data-team/validate-schema",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
                "args": "{{workflow.parameters.source-path}}",
            }
        )
        
        # Step 2: Transform and load
        Step(
            name="transform-load",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/data-team/transform-load",
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "r5.xlarge",
                "scaling-type": "autoscale",
                "min-workers": "2",
                "max-workers": "8",
                "args": "{{workflow.parameters.source-path}},{{workflow.parameters.target-path}}",
            }
        )

w.create()
```
{% endtab %}

{% tab title="YAML" %}
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: data-ingestion-with-validation-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: source-path
        value: "s3://raw-data/2024-01-01/"
      - name: target-path
        value: "s3://processed-data/2024-01-01/"
  templates:
    - name: main
      steps:
        # Step 1: Load and validate
        - - name: validate-data
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/validate-schema"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: args
                  value: "{{workflow.parameters.source-path}}"
        
        # Step 2: Transform and load
        - - name: transform-load
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/transform-load"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "r5.xlarge"
                - name: scaling-type
                  value: "autoscale"
                - name: min-workers
                  value: "2"
                - name: max-workers
                  value: "8"
                - name: args
                  value: "{{workflow.parameters.source-path}},{{workflow.parameters.target-path}}"
```
{% endtab %}
{% endtabs %}

## Incremental Data Loading

Load only new or changed data:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Steps, Step, TemplateRef, Parameter
from datetime import datetime, timedelta

# Get yesterday's date
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

with Workflow(
    generate_name="incremental-load-",
    namespace="default",
    entrypoint="main",
    arguments=[
        Parameter(name="load-date", value=yesterday),
    ]
) as w:
    with Steps(name="main"):
        Step(
            name="incremental-load",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/data-team/incremental-load",
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "i3.xlarge",
                "scaling-type": "autoscale",
                "min-workers": "2",
                "max-workers": "10",
                "args": "{{workflow.parameters.load-date}}",
            }
        )

w.create()
```
{% endtab %}

{% tab title="YAML" %}
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: incremental-load-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: load-date
        value: "2024-01-01"
  
  templates:
    - name: main
      steps:
        - - name: incremental-load
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/incremental-load"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "i3.xlarge"
                - name: scaling-type
                  value: "autoscale"
                - name: min-workers
                  value: "2"
                - name: max-workers
                  value: "10"
                - name: args
                  value: "{{workflow.parameters.load-date}}"
```
{% endtab %}
{% endtabs %}

### Databricks Notebook: Incremental Load

```python
# Databricks notebook: /Users/data-team/incremental-load

# COMMAND ----------
dbutils.widgets.text("load_date", "")
load_date = dbutils.widgets.get("load_date")

print(f"Loading data for: {load_date}")

# COMMAND ----------
# Read incremental data
source_path = f"s3://raw-data/date={load_date}/"
df_new = spark.read.parquet(source_path)

print(f"New records: {df_new.count():,}")

# COMMAND ----------
# Read existing data
target_path = "s3://processed-data/daily/"
try:
    df_existing = spark.read.parquet(target_path)
    print(f"Existing records: {df_existing.count():,}")
except:
    df_existing = None
    print("No existing data found")

# COMMAND ----------
# Merge and deduplicate
if df_existing:
    df_merged = df_new.union(df_existing).dropDuplicates(["id"])
else:
    df_merged = df_new

print(f"Total records after merge: {df_merged.count():,}")

# COMMAND ----------
# Write back
(df_merged.write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(target_path))

print(f"Data loaded successfully")
```

## Multi-Source Ingestion

Ingest from multiple sources in parallel:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Steps, Step, TemplateRef, Parameter

with Workflow(
    generate_name="multi-source-ingestion-",
    namespace="default",
    entrypoint="main",
    arguments=[
        Parameter(name="date", value="2024-01-01"),
    ]
) as w:
    with Steps(name="main") as s:
        # Ingest from multiple sources in parallel
        with s.parallel():
            Step(
                name="ingest-database-a",
                template_ref=TemplateRef(
                    name="databricks-connector",
                    template="run-job",
                    cluster_scope=False,
                ),
                arguments={
                    "code-path": "/Users/data-team/ingest-database-a",
                    "task-type": "notebook",
                    "cluster-mode": "Serverless",
                    "args": "{{workflow.parameters.date}}",
                }
            )
            
            Step(
                name="ingest-database-b",
                template_ref=TemplateRef(
                    name="databricks-connector",
                    template="run-job",
                    cluster_scope=False,
                ),
                arguments={
                    "code-path": "/Users/data-team/ingest-database-b",
                    "task-type": "notebook",
                    "cluster-mode": "Serverless",
                    "args": "{{workflow.parameters.date}}",
                }
            )
            
            Step(
                name="ingest-api-data",
                template_ref=TemplateRef(
                    name="databricks-connector",
                    template="run-job",
                    cluster_scope=False,
                ),
                arguments={
                    "code-path": "/Users/data-team/ingest-api",
                    "task-type": "notebook",
                    "cluster-mode": "Serverless",
                    "args": "{{workflow.parameters.date}}",
                }
            )
        
        # Merge all sources
        Step(
            name="merge-sources",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/data-team/merge-all-sources",
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "r5.2xlarge",
                "new-cluster-num-workers": "4",
                "args": "{{workflow.parameters.date}}",
            }
        )

w.create()
```
{% endtab %}

{% tab title="YAML" %}
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: multi-source-ingestion-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: date
        value: "2024-01-01"
  
  templates:
    - name: main
      steps:
        # Ingest from multiple sources in parallel
        - - name: ingest-database-a
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/ingest-database-a"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: args
                  value: "{{workflow.parameters.date}}"
          
          - name: ingest-database-b
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/ingest-database-b"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: args
                  value: "{{workflow.parameters.date}}"
          
          - name: ingest-api-data
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/ingest-api"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: args
                  value: "{{workflow.parameters.date}}"
        
        # Merge all sources
        - - name: merge-sources
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/merge-all-sources"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "r5.2xlarge"
                - name: new-cluster-num-workers
                  value: "4"
                - name: args
                  value: "{{workflow.parameters.date}}"
```
{% endtab %}
{% endtabs %}

## Best Practices

### 1. Use Serverless for Small Files
For files < 10GB, serverless is fastest:
```python
arguments={
    "cluster-mode": "Serverless",
}
```

### 2. Partition Large Datasets
Write data partitioned for efficient querying:
```python
# In your Databricks notebook
(df.write
   .mode("overwrite")
   .partitionBy("date", "region")
   .parquet(target_path))
```

### 3. Validate Before Processing
Check schema and data quality first:
```python
# Validate expected columns exist
expected_cols = ["id", "timestamp", "value"]
actual_cols = df.columns
missing = set(expected_cols) - set(actual_cols)

if missing:
    raise ValueError(f"Missing columns: {missing}")
```

### 4. Handle Schema Evolution
Use merge schema for evolving data:
```python
spark.conf.set("spark.sql.parquet.mergeSchema", "true")
```

## Next Steps

- [Feature Engineering](feature-engineering.md) - Process ingested data
- [Model Training](model-training.md) - Train models on your data
- [Multi-Step Pipelines](multi-step-pipelines.md) - Build complete pipelines
- [Passing Data Between Steps](passing-data-between-steps.md) - Connect workflow steps

# Databricks Connector: Hera Python SDK Examples

This page provides complete Python examples using the [Hera SDK](https://github.com/argoproj-labs/hera) for common Databricks workflows.

## Setup

Install Hera:

```bash
pip install hera
```

Configure Hera to connect to your Argo server:

```python
from hera.workflows import GlobalConfig

GlobalConfig.host = "https://argo-server.example.com"
GlobalConfig.token = "your-argo-token"  # or use kubeconfig
GlobalConfig.namespace = "default"
```

## Running a Notebook

### Basic Notebook Execution (Serverless)

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="notebook-serverless-",
    namespace="default"
) as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="run-notebook",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/data-team/etl-pipeline"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="Serverless"),
            ]
        )

# Submit to cluster
w.create()
```

### Notebook with New Job Cluster

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="notebook-new-cluster-",
    namespace="default"
) as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="feature-engineering",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                # Code configuration
                Parameter(name="code-path", value="/Users/ml-team/feature-engineering"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="New"),
                
                # Cluster configuration
                Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
                Parameter(name="new-cluster-node-type", value="i3.xlarge"),
                Parameter(name="scaling-type", value="autoscale"),
                Parameter(name="min-workers", value="2"),
                Parameter(name="max-workers", value="8"),
                
                # Cloud configuration
                Parameter(name="cloud-provider", value="AWS"),
                Parameter(name="availability", value="SPOT"),
                
                # Job metadata
                Parameter(name="run-name", value="feature-engineering-job"),
                Parameter(name="email-notifications", value="ml-team@company.com"),
            ]
        )

w.create()
```

### Using Python Variables for Configuration

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

# Define configuration
NOTEBOOK_PATH = "/Users/ml-team/model-training"
SPARK_VERSION = "13.3.x-scala2.12"
NODE_TYPE = "i3.2xlarge"
MIN_WORKERS = 2
MAX_WORKERS = 10

with Workflow(
    generate_name="ml-training-",
    namespace="default"
) as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="train-model",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value=NOTEBOOK_PATH),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="New"),
                Parameter(name="new-cluster-spark-version", value=SPARK_VERSION),
                Parameter(name="new-cluster-node-type", value=NODE_TYPE),
                Parameter(name="scaling-type", value="autoscale"),
                Parameter(name="min-workers", value=str(MIN_WORKERS)),
                Parameter(name="max-workers", value=str(MAX_WORKERS)),
            ]
        )

w.create()
```

## Running Spark Python Jobs

### PySpark Script with Arguments

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="spark-python-",
    namespace="default"
) as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="data-processing",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="dbfs:/scripts/process_data.py"),
                Parameter(name="task-type", value="spark-python"),
                Parameter(name="args", value="input/path/,output/path/,--mode=production"),
                
                # Cluster configuration
                Parameter(name="cluster-mode", value="New"),
                Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
                Parameter(name="new-cluster-node-type", value="r5.2xlarge"),
                Parameter(name="new-cluster-num-workers", value="4"),
                
                # Custom Spark config
                Parameter(name="spark-conf", value="spark.sql.shuffle.partitions=200,spark.executor.memory=4g"),
            ]
        )

w.create()
```

## Running JAR Applications

### Scala JAR with Main Class

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="spark-jar-",
    namespace="default"
) as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="batch-processing",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="dbfs:/jars/batch-processor.jar"),
                Parameter(name="task-type", value="spark-jar"),
                Parameter(name="main-class-name", value="com.company.BatchProcessor"),
                Parameter(name="args", value="2024-01-01,2024-01-31,incremental"),
                
                # Cluster with instance pool
                Parameter(name="cluster-mode", value="New"),
                Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
                Parameter(name="instance-pool-id", value="pool-abc123"),
                Parameter(name="scaling-type", value="fixed"),
                Parameter(name="new-cluster-num-workers", value="10"),
            ]
        )

w.create()
```

## Multi-Step Pipeline

### Data Pipeline with Multiple Notebooks

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="ml-pipeline-",
    namespace="default"
) as w:
    with Steps(name="main") as s:
        # Step 1: Data Ingestion
        WorkflowTemplateRef(
            name="ingest-data",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/data-team/01-ingest"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="Serverless"),
                Parameter(name="run-name", value="ingest-data"),
            ]
        )

        # Step 2: Feature Engineering
        WorkflowTemplateRef(
            name="feature-engineering",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/ml-team/02-features"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="New"),
                Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
                Parameter(name="new-cluster-node-type", value="i3.2xlarge"),
                Parameter(name="scaling-type", value="autoscale"),
                Parameter(name="min-workers", value="2"),
                Parameter(name="max-workers", value="10"),
                Parameter(name="run-name", value="feature-engineering"),
            ]
        )

        # Step 3: Model Training
        WorkflowTemplateRef(
            name="train-model",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/ml-team/03-train"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="New"),
                Parameter(name="new-cluster-spark-version", value="13.3.x-gpu-ml-scala2.12"),
                Parameter(name="new-cluster-node-type", value="g4dn.xlarge"),
                Parameter(name="new-cluster-num-workers", value="1"),
                Parameter(name="run-name", value="train-model"),
            ]
        )

        # Step 4: Model Evaluation
        WorkflowTemplateRef(
            name="evaluate-model",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/ml-team/04-evaluate"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="Serverless"),
                Parameter(name="run-name", value="evaluate-model"),
            ]
        )

w.create()
```

### Using Helper Functions for Reusability

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps
from typing import List

def create_databricks_step(
    name: str,
    notebook_path: str,
    cluster_mode: str = "Serverless",
    **kwargs
) -> WorkflowTemplateRef:
    """Helper function to create Databricks workflow steps"""
    args = [
        Parameter(name="code-path", value=notebook_path),
        Parameter(name="task-type", value="notebook"),
        Parameter(name="cluster-mode", value=cluster_mode),
        Parameter(name="run-name", value=name),
    ]
    
    # Add any additional parameters
    for key, value in kwargs.items():
        args.append(Parameter(name=key, value=str(value)))
    
    return WorkflowTemplateRef(
        name=name,
        template_ref="databricks-connector",
        template="run-job",
        arguments=args
    )

# Use the helper function
with Workflow(generate_name="pipeline-", namespace="default") as w:
    with Steps(name="main") as s:
        create_databricks_step(
            name="data-prep",
            notebook_path="/Users/team/data-prep",
        )

        create_databricks_step(
            name="model-train",
            notebook_path="/Users/team/model-train",
            cluster_mode="New",
            new_cluster_spark_version="13.3.x-scala2.12",
            new_cluster_node_type="i3.xlarge",
            scaling_type="autoscale",
            min_workers="2",
            max_workers="8",
        )

w.create()
```

## Using Job Outputs

### Capturing and Using Outputs

```python
from hera.workflows import (
    Workflow,
    Parameter,
    WorkflowTemplateRef,
    Steps,
    Container,
    script
)

with Workflow(
    generate_name="pipeline-with-outputs-",
    namespace="default"
) as w:
    with Steps(name="main") as s:
        # Step 1: Run Databricks job
        databricks_step = WorkflowTemplateRef(
            name="process-data",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/data-team/processor"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="Serverless"),
            ]
        )

        # Step 2: Use outputs from previous step
        @script()
        def print_results(run_id: str, run_url: str, result: str, state: str):
            print(f"Databricks Run ID: {run_id}")
            print(f"Databricks Run URL: {run_url}")
            print(f"Result: {result}")
            print(f"State: {state}")
        
        print_results(
            arguments=[
                Parameter(name="run_id", value="{{steps.process-data.outputs.parameters.run-id}}"),
                Parameter(name="run_url", value="{{steps.process-data.outputs.parameters.run-url}}"),
                Parameter(name="result", value="{{steps.process-data.outputs.parameters.result}}"),
                Parameter(name="state", value="{{steps.process-data.outputs.parameters.state}}"),
            ]
        )

w.create()
```

## Running Existing Databricks Jobs

### Trigger Existing Job by ID

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="existing-job-",
    namespace="default"
) as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="run-scheduled-job",
            template_ref="databricks-connector",
            template="run-existing-job",
            arguments=[
                Parameter(name="job-id", value="123456"),
                Parameter(name="notebook-params", value="date=2024-01-01,env=production"),
            ]
        )

w.create()
```

### Multiple Existing Jobs with Different Parameters

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(
    generate_name="existing-jobs-",
    namespace="default"
) as w:
    with Steps(name="main") as s:
        # Notebook-based job
        WorkflowTemplateRef(
            name="run-notebook-job",
            template_ref="databricks-connector",
            template="run-existing-job",
            arguments=[
                Parameter(name="job-id", value="111111"),
                Parameter(name="notebook-params", value="start_date=2024-01-01,end_date=2024-01-31"),
            ]
        )

        # Python-based job
        WorkflowTemplateRef(
            name="run-python-job",
            template_ref="databricks-connector",
            template="run-existing-job",
            arguments=[
                Parameter(name="job-id", value="222222"),
                Parameter(name="python-params", value="arg1,arg2,arg3"),
            ]
        )

        # JAR-based job
        WorkflowTemplateRef(
            name="run-jar-job",
            template_ref="databricks-connector",
            template="run-existing-job",
            arguments=[
                Parameter(name="job-id", value="333333"),
                Parameter(name="jar-params", value="param1,param2,param3"),
            ]
        )

w.create()
```

## Advanced Patterns

### Conditional Execution Based on Environment

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps
import os

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

# Different cluster configs per environment
CLUSTER_CONFIGS = {
    "dev": {
        "cluster_mode": "Serverless",
    },
    "staging": {
        "cluster_mode": "New",
        "new_cluster_spark_version": "13.3.x-scala2.12",
        "new_cluster_node_type": "i3.xlarge",
        "new_cluster_num_workers": "2",
    },
    "prod": {
        "cluster_mode": "New",
        "new_cluster_spark_version": "13.3.x-scala2.12",
        "new_cluster_node_type": "i3.2xlarge",
        "scaling_type": "autoscale",
        "min_workers": "4",
        "max_workers": "16",
    }
}

config = CLUSTER_CONFIGS[ENVIRONMENT]

with Workflow(
    generate_name=f"pipeline-{ENVIRONMENT}-",
    namespace="default"
) as w

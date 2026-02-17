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
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="notebook-serverless-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        Step(
            name="run-notebook",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,  # Use True for ClusterWorkflowTemplate
            ),
            arguments={
                "code-path": "/Users/data-team/etl-pipeline",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
            }
        )

# Submit to cluster
w.create()
```

### Notebook with New Job Cluster

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="notebook-new-cluster-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        Step(
            name="feature-engineering",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                # Code configuration
                "code-path": "/Users/ml-team/feature-engineering",
                "task-type": "notebook",
                "cluster-mode": "New",
                
                # Cluster configuration
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "i3.xlarge",
                "scaling-type": "autoscale",
                "min-workers": "2",
                "max-workers": "8",
                
                # Cloud configuration
                "cloud-provider": "AWS",
                "availability": "SPOT",
                
                # Job metadata
                "run-name": "feature-engineering-job",
                "email-notifications": "ml-team@company.com",
            }
        )

w.create()
```

### Using Python Variables for Configuration

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

# Define configuration
NOTEBOOK_PATH = "/Users/ml-team/model-training"
SPARK_VERSION = "13.3.x-scala2.12"
NODE_TYPE = "i3.2xlarge"
MIN_WORKERS = 2
MAX_WORKERS = 10

with Workflow(
    generate_name="ml-training-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        Step(
            name="train-model",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": NOTEBOOK_PATH,
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": SPARK_VERSION,
                "new-cluster-node-type": NODE_TYPE,
                "scaling-type": "autoscale",
                "min-workers": str(MIN_WORKERS),
                "max-workers": str(MAX_WORKERS),
            }
        )

w.create()
```

## Running Spark Python Jobs

### PySpark Script with Arguments

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="spark-python-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        Step(
            name="data-processing",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "dbfs:/scripts/process_data.py",
                "task-type": "spark-python",
                "args": "input/path/,output/path/,--mode=production",
                
                # Cluster configuration
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "r5.2xlarge",
                "new-cluster-num-workers": "4",
                
                # Custom Spark config
                "spark-conf": "spark.sql.shuffle.partitions=200,spark.executor.memory=4g",
            }
        )

w.create()
```

## Running JAR Applications

### Scala JAR with Main Class

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="spark-jar-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        Step(
            name="batch-processing",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "dbfs:/jars/batch-processor.jar",
                "task-type": "spark-jar",
                "main-class-name": "com.company.BatchProcessor",
                "args": "2024-01-01,2024-01-31,incremental",
                
                # Cluster with instance pool
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "instance-pool-id": "pool-abc123",
                "scaling-type": "fixed",
                "new-cluster-num-workers": "10",
            }
        )

w.create()
```

## Multi-Step Pipeline

### Data Pipeline with Multiple Notebooks

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="ml-pipeline-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main") as s:
        # Step 1: Data Ingestion
        Step(
            name="ingest-data",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/data-team/01-ingest",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
                "run-name": "ingest-data",
            }
        )
        
        # Step 2: Feature Engineering
        Step(
            name="feature-engineering",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/ml-team/02-features",
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "i3.2xlarge",
                "scaling-type": "autoscale",
                "min-workers": "2",
                "max-workers": "10",
                "run-name": "feature-engineering",
            }
        )
        
        # Step 3: Model Training
        Step(
            name="train-model",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/ml-team/03-train",
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-gpu-ml-scala2.12",
                "new-cluster-node-type": "g4dn.xlarge",
                "new-cluster-num-workers": "1",
                "run-name": "train-model",
            }
        )
        
        # Step 4: Model Evaluation
        Step(
            name="evaluate-model",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/ml-team/04-evaluate",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
                "run-name": "evaluate-model",
            }
        )

w.create()
```

### Using Helper Functions for Reusability

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef
from typing import Dict

def databricks_step(
    name: str,
    notebook_path: str,
    cluster_mode: str = "Serverless",
    **kwargs
) -> Step:
    """Helper function to create Databricks workflow steps"""
    args = {
        "code-path": notebook_path,
        "task-type": "notebook",
        "cluster-mode": cluster_mode,
        "run-name": name,
    }
    
    # Add any additional parameters
    args.update({k.replace("_", "-"): v for k, v in kwargs.items()})
    
    return Step(
        name=name,
        template_ref=TemplateRef(
            name="databricks-connector",
            template="run-job",
            cluster_scope=False,
        ),
        arguments=args
    )

# Use the helper function
with Workflow(
    generate_name="pipeline-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        databricks_step(
            name="data-prep",
            notebook_path="/Users/team/data-prep",
        )
        
        databricks_step(
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
    Steps,
    Step,
    TemplateRef,
    script
)

with Workflow(
    generate_name="pipeline-with-outputs-",
    namespace="default",
    entrypoint="main"
) as w:
    @script()
    def print_results(run_id: str, run_url: str, result: str, state: str):
        print(f"Databricks Run ID: {run_id}")
        print(f"Databricks Run URL: {run_url}")
        print(f"Result: {result}")
        print(f"State: {state}")
    
    with Steps(name="main"):
        # Step 1: Run Databricks job
        databricks_step = Step(
            name="process-data",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/data-team/processor",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
            }
        )
        
        # Step 2: Use outputs from previous step
        print_results(
            arguments={
                "run_id": "{{steps.process-data.outputs.parameters.run-id}}",
                "run_url": "{{steps.process-data.outputs.parameters.run-url}}",
                "result": "{{steps.process-data.outputs.parameters.result}}",
                "state": "{{steps.process-data.outputs.parameters.state}}",
            }
        )

w.create()
```

## Running Existing Databricks Jobs

### Trigger Existing Job by ID

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="existing-job-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        Step(
            name="run-scheduled-job",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-existing-job",
                cluster_scope=False,
            ),
            arguments={
                "job-id": "123456",
                "notebook-params": "date=2024-01-01,env=production",
            }
        )

w.create()
```

### Multiple Existing Jobs with Different Parameters

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="existing-jobs-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        # Notebook-based job
        Step(
            name="run-notebook-job",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-existing-job",
                cluster_scope=False,
            ),
            arguments={
                "job-id": "111111",
                "notebook-params": "start_date=2024-01-01,end_date=2024-01-31",
            }
        )
        
        # Python-based job
        Step(
            name="run-python-job",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-existing-job",
                cluster_scope=False,
            ),
            arguments={
                "job-id": "222222",
                "python-params": "arg1,arg2,arg3",
            }
        )
        
        # JAR-based job
        Step(
            name="run-jar-job",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-existing-job",
                cluster_scope=False,
            ),
            arguments={
                "job-id": "333333",
                "jar-params": "param1,param2,param3",
            }
        )

w.create()
```

## Advanced Patterns

### Conditional Execution Based on Environment

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef
import os

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

# Different cluster configs per environment
CLUSTER_CONFIGS = {
    "dev": {
        "cluster-mode": "Serverless",
    },
    "staging": {
        "cluster-mode": "New",
        "new-cluster-spark-version": "13.3.x-scala2.12",
        "new-cluster-node-type": "i3.xlarge",
        "new-cluster-num-workers": "2",
    },
    "prod": {
        "cluster-mode": "New",
        "new-cluster-spark-version": "13.3.x-scala2.12",
        "new-cluster-node-type": "i3.2xlarge",
        "scaling-type": "autoscale",
        "min-workers": "4",
        "max-workers": "16",
    }
}

config = CLUSTER_CONFIGS[ENVIRONMENT]

with Workflow(
    generate_name=f"pipeline-{ENVIRONMENT}-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        base_args = {
            "code-path": "/Users/team/notebook",
            "task-type": "notebook",
        }
        base_args.update(config)
        
        Step(
            name="run-job",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments=base_args
        )

w.create()
```

### Combining Connectors with Custom Logic

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef, script

@script()
def validate_data():
    """Custom validation logic"""
    import requests
    # Check if data exists before processing
    response = requests.get("https://api.example.com/data/status")
    if response.status_code != 200:
        raise Exception("Data not ready")
    print("Data validation passed")

@script()
def send_notification(message: str):
    """Send notification after job completes"""
    print(f"Notification: {message}")
    # Add your notification logic here

with Workflow(
    generate_name="pipeline-with-custom-logic-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        # Step 1: Custom validation
        validate_data()
        
        # Step 2: Run Databricks connector
        Step(
            name="process-data",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/team/process-data",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
            }
        )
        
        # Step 3: Custom notification
        send_notification(arguments={"message": "Processing complete"})

w.create()
```

## Next Steps

- [YAML Examples](yaml-examples.md) - See these examples in YAML format
- [Parameter Reference](parameter-reference.md) - Complete parameter documentation
- [Setup Guide](setup.md) - Configure secrets and prerequisites
- [Cluster Modes](cluster-modes.md) - Deep dive into cluster options

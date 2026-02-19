# Passing Data Between Steps

Learn how to pass data, parameters, and artifacts between workflow steps when using Argo Connectors.

## Overview

Multi-step workflows often need to share information between steps. Argo Workflows provides three mechanisms for this:

1. **Parameters**: Small pieces of metadata (job IDs, URLs, metrics)
2. **Artifacts**: Large data files via S3/GCS artifact repository
3. **Volumes**: Shared storage for parallel reads/writes (fastest for large files)

## Using Output Parameters

Connectors expose output parameters that subsequent steps can reference.

### Databricks Connector Outputs

The Databricks connector provides these outputs:

| Output | Description | Example |
|--------|-------------|---------|
| `run-id` | Databricks run ID | `12345` |
| `run-url` | Link to Databricks UI | `https://workspace.databricks.com/...` |
| `result` | Job result/output | `{"status": "success"}` |
| `state` | Final job state | `SUCCESS` |
| `json` | Full job details | Complete JSON response |

### Spark Connector Outputs

The Spark connector provides:

| Output | Description | Example |
|--------|-------------|---------|
| `spark-job-name` | SparkApplication name | `spark-pi-abc123` |
| `spark-job-namespace` | Kubernetes namespace | `default` |

## Basic Parameter Passing

### Accessing Connector Outputs

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Steps, Step, TemplateRef, script

with Workflow(
    generate_name="pipeline-with-outputs-",
    namespace="default",
    entrypoint="main"
) as w:
    @script()
    def process_results(run_id: str, run_url: str, state: str):
        print(f"Databricks Run ID: {run_id}")
        print(f"Databricks UI: {run_url}")
        print(f"Job State: {state}")
        
        # Your custom logic here
        if state != "SUCCESS":
            raise Exception("Job failed!")
    
    with Steps(name="main"):
        # Step 1: Run Databricks job
        Step(
            name="databricks-job",
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
        
        # Step 2: Use outputs from Step 1
        process_results(
            arguments={
                "run_id": "{{steps.databricks-job.outputs.parameters.run-id}}",
                "run_url": "{{steps.databricks-job.outputs.parameters.run-url}}",
                "state": "{{steps.databricks-job.outputs.parameters.state}}",
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
  generateName: pipeline-with-outputs-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        # Step 1: Run Databricks job
        - - name: databricks-job
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/process-data"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
        
        # Step 2: Use outputs
        - - name: process-results
            template: print-info
            arguments:
              parameters:
                - name: run-id
                  value: "{{steps.databricks-job.outputs.parameters.run-id}}"
                - name: run-url
                  value: "{{steps.databricks-job.outputs.parameters.run-url}}"
                - name: state
                  value: "{{steps.databricks-job.outputs.parameters.state}}"
    
    - name: print-info
      inputs:
        parameters:
          - name: run-id
          - name: run-url
          - name: state
      container:
        image: alpine:latest
        command: [sh, -c]
        args:
          - |
            echo "Databricks Run ID: {{inputs.parameters.run-id}}"
            echo "Databricks UI: {{inputs.parameters.run-url}}"
            echo "Job State: {{inputs.parameters.state}}"
```
{% endtab %}
{% endtabs %}

## Passing Parameters Between Connectors

### From Databricks to Spark

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="databricks-to-spark-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        # Step 1: Process with Databricks
        Step(
            name="databricks-processing",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/team/prepare-data",
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "i3.xlarge",
                "new-cluster-num-workers": "2",
            }
        )
        
        # Step 2: Additional processing with Spark on K8s
        Step(
            name="spark-processing",
            template_ref=TemplateRef(
                name="spark-data-connector-python",
                template="spark",
                cluster_scope=False,
            ),
            arguments={
                "mainApplicationFile": "s3://scripts/process.py",
                "namespace": "default",
                "globalImage": "gcr.io/spark-operator/spark-py:v3.1.1",
                "executorInstances": "5",
                "driverMemory": "2048m",
                "executorMemory": "4096m",
            }
        )
        
        # Step 3: Cleanup Spark resources
        Step(
            name="cleanup-spark",
            template_ref=TemplateRef(
                name="spark-delete-resource",
                template="spark-delete",
                cluster_scope=False,
            ),
            arguments={
                "namespace": "{{steps.spark-processing.outputs.parameters.spark-job-namespace}}",
                "spark-job-name": "{{steps.spark-processing.outputs.parameters.spark-job-name}}",
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
  generateName: databricks-to-spark-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        # Step 1: Databricks processing
        - - name: databricks-processing
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/prepare-data"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "i3.xlarge"
                - name: new-cluster-num-workers
                  value: "2"
        
        # Step 2: Spark processing
        - - name: spark-processing
            templateRef:
              name: spark-data-connector-python
              template: spark
            arguments:
              parameters:
                - name: mainApplicationFile
                  value: "s3://scripts/process.py"
                - name: namespace
                  value: "default"
                - name: globalImage
                  value: "gcr.io/spark-operator/spark-py:v3.1.1"
                - name: executorInstances
                  value: "5"
                - name: driverMemory
                  value: "2048m"
                - name: executorMemory
                  value: "4096m"
        
        # Step 3: Cleanup
        - - name: cleanup-spark
            templateRef:
              name: spark-delete-resource
              template: spark-delete
            arguments:
              parameters:
                - name: namespace
                  value: "{{steps.spark-processing.outputs.parameters.spark-job-namespace}}"
                - name: spark-job-name
                  value: "{{steps.spark-processing.outputs.parameters.spark-job-name}}"
```
{% endtab %}
{% endtabs %}

## Using Workflow-Level Parameters

Share parameters across multiple steps:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Steps, Step, TemplateRef, Parameter

with Workflow(
    generate_name="shared-params-",
    namespace="default",
    entrypoint="main",
    arguments=[
        Parameter(name="date", value="2024-01-01"),
        Parameter(name="environment", value="production"),
        Parameter(name="region", value="us-west-2"),
    ]
) as w:
    with Steps(name="main"):
        # All steps can access workflow parameters
        Step(
            name="step1",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/team/step1",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
                "run-name": "step1-{{workflow.parameters.environment}}-{{workflow.parameters.date}}",
                "args": "{{workflow.parameters.date}},{{workflow.parameters.region}}",
            }
        )
        
        Step(
            name="step2",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/team/step2",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
                "run-name": "step2-{{workflow.parameters.environment}}-{{workflow.parameters.date}}",
                "args": "{{workflow.parameters.date}},{{workflow.parameters.region}}",
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
  generateName: shared-params-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: date
        value: "2024-01-01"
      - name: environment
        value: "production"
      - name: region
        value: "us-west-2"
  
  templates:
    - name: main
      steps:
        - - name: step1
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/step1"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: run-name
                  value: "step1-{{workflow.parameters.environment}}-{{workflow.parameters.date}}"
                - name: args
                  value: "{{workflow.parameters.date}},{{workflow.parameters.region}}"
        
        - - name: step2
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/step2"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: run-name
                  value: "step2-{{workflow.parameters.environment}}-{{workflow.parameters.date}}"
                - name: args
                  value: "{{workflow.parameters.date}},{{workflow.parameters.region}}"
```
{% endtab %}
{% endtabs %}

## Working with Artifacts

While connectors primarily use parameters, you can combine them with artifact passing for large data:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import (
    Workflow,
    Steps,
    Step,
    TemplateRef,
    script,
    Artifact
)

with Workflow(
    generate_name="pipeline-with-artifacts-",
    namespace="default",
    entrypoint="main"
) as w:
    @script(
        outputs=[
            Artifact(name="model-path", path="/tmp/model_path.txt")
        ]
    )
    def save_model_path():
        """Save model path for next step"""
        model_path = "s3://models/churn-model-v1/"
        with open("/tmp/model_path.txt", "w") as f:
            f.write(model_path)
    
    @script(
        inputs=[
            Artifact(name="model-path", path="/tmp/model_path.txt")
        ]
    )
    def use_model_path():
        """Use model path from previous step"""
        with open("/tmp/model_path.txt", "r") as f:
            model_path = f.read()
        print(f"Using model from: {model_path}")
    
    with Steps(name="main"):
        # Step 1: Train and save model path
        Step(
            name="train",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/ml-team/train",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
            }
        )
        
        save_model_path(name="save-path")
        
        # Step 2: Use the model path
        use_model_path(
            name="use-path",
            arguments={
                "model-path": "{{steps.save-path.outputs.artifacts.model-path}}"
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
  generateName: pipeline-with-artifacts-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: train
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/train"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
        
        - - name: save-path
            template: save-model-path
        
        - - name: use-path
            template: use-model-path
            arguments:
              artifacts:
                - name: model-path
                  from: "{{steps.save-path.outputs.artifacts.model-path}}"
    
    - name: save-model-path
      outputs:
        artifacts:
          - name: model-path
            path: /tmp/model_path.txt
      script:
        image: python:3.9
        command: [python]
        source: |
          model_path = "s3://models/churn-model-v1/"
          with open("/tmp/model_path.txt", "w") as f:
              f.write(model_path)
    
    - name: use-model-path
      inputs:
        artifacts:
          - name: model-path
            path: /tmp/model_path.txt
      script:
        image: python:3.9
        command: [python]
        source: |
          with open("/tmp/model_path.txt", "r") as f:
              model_path = f.read()
          print(f"Using model from: {model_path}")
```
{% endtab %}
{% endtabs %}

## Passing Data Through Storage

The recommended pattern for large data is to write to cloud storage and pass the path:

### Writing Results to S3

In your Databricks notebook:
```python
# Save results to S3
output_path = f"s3://ml-data/results/{run_id}/"
df_results.write.mode("overwrite").parquet(output_path)

# Return the path via dbutils
dbutils.notebook.exit(output_path)
```

Then reference it in the next step:
```python
arguments={
    "args": "{{steps.previous-step.outputs.parameters.result}}",  # Contains the S3 path
}
```

## Pattern: Metadata Passing

Pass metadata between steps while data stays in storage:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import Workflow, Steps, Step, TemplateRef, Parameter

with Workflow(
    generate_name="metadata-passing-",
    namespace="default",
    entrypoint="main",
    arguments=[
        Parameter(name="base-path", value="s3://ml-data/pipeline/"),
    ]
) as w:
    with Steps(name="main"):
        # Step 1: Process and save to S3
        Step(
            name="process",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/team/process",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
                "args": "{{workflow.parameters.base-path}}",
            }
        )
        
        # Step 2: Reference S3 path from output
        # The notebook should use dbutils.notebook.exit(path) to return the path
        Step(
            name="validate",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/team/validate",
                "task-type": "notebook",
                "cluster-mode": "Serverless",
                # Use the path returned from previous step
                "args": "{{steps.process.outputs.parameters.result}}",
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
  generateName: metadata-passing-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: base-path
        value: "s3://ml-data/pipeline/"
  
  templates:
    - name: main
      steps:
        - - name: process
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/process"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: args
                  value: "{{workflow.parameters.base-path}}"
        
        - - name: validate
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/validate"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: args
                  value: "{{steps.process.outputs.parameters.result}}"
```
{% endtab %}
{% endtabs %}

### Databricks Notebook Pattern

```python
# Databricks notebook: /Users/team/process

# COMMAND ----------
dbutils.widgets.text("base_path", "")
base_path = dbutils.widgets.get("base_path")

# COMMAND ----------
# Process data
df = spark.read.parquet(f"{base_path}/input/")

# ... processing logic ...

# COMMAND ----------
# Save results
output_path = f"{base_path}/processed/run-{datetime.now().strftime('%Y%m%d%H%M%S')}/"
df.write.mode("overwrite").parquet(output_path)

# COMMAND ----------
# Return the output path to Argo
dbutils.notebook.exit(output_path)
```

## Using Volumes for Parallel Data Access

For workflows that need to share large files across parallel steps, volumes provide significantly better performance than artifact repositories.

### Performance Comparison

Based on [Pipekit's ArgoCon 2023 talk](https://www.youtube.com/watch?v=QZI-LXJGWYI):

| Method | Time to Share 10GB File Across 3 Parallel Steps | Use Case |
|--------|------|----------|
| **NFS Volume** | ~20 seconds | Fast parallel reads/writes |
| **S3 Artifacts** | ~7 minutes | Simpler setup, slower |

**Use volumes when:**
- Multiple parallel steps need to read the same large file
- You need fast data sharing between steps
- Steps need to write to shared storage simultaneously

**Use artifacts (S3/GCS) when:**
- Simple sequential workflows
- Smaller files (< 1GB)
- You want managed storage without volume provisioning

### NFS Volume Pattern

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import (
    Workflow,
    Steps,
    Step,
    script,
    Parameter,
    models as m
)

with Workflow(
    generate_name="volume-sharing-",
    namespace="default",
    entrypoint="main",
    volume_claim_templates=[
        m.PersistentVolumeClaim(
            metadata=m.ObjectMeta(name="workdir"),
            spec=m.PersistentVolumeClaimSpec(
                access_modes=["ReadWriteMany"],
                storage_class_name="nfs",  # Requires NFS provisioner
                resources=m.ResourceRequirements(
                    requests={"storage": "50Gi"}
                )
            )
        )
    ]
) as w:
    @script(
        volume_mounts=[m.VolumeMount(name="workdir", mount_path="/workdir")]
    )
    def create_large_file():
        """Create a large file that parallel steps will read"""
        import subprocess
        # Create 10GB file
        subprocess.run([
            "dd", "if=/dev/zero", "of=/workdir/LARGE_FILE",
            "bs=1", "count=0", "seek=10G"
        ])
        print("Large file created")
    
    @script(
        volume_mounts=[m.VolumeMount(name="workdir", mount_path="/workdir")]
    )
    def process_file(index: str):
        """Process the shared file in parallel"""
        import subprocess
        import os
        
        print(f"Worker {index} starting")
        
        # Verify file exists
        result = subprocess.run(["ls", "-lah", "/workdir"], capture_output=True, text=True)
        print(result.stdout)
        
        # Check file size
        result = subprocess.run(["du", "-h", "/workdir/LARGE_FILE"], capture_output=True, text=True)
        print(f"File size: {result.stdout}")
        
        # Create new file (each worker creates their own)
        subprocess.run([
            "dd", "if=/dev/zero", f"of=/workdir/OUTPUT_{index}",
            "bs=1", "count=0", "seek=5G"
        ])
        print(f"Worker {index} created output file")
    
    with Steps(name="main") as s:
        # Step 1: Create large file
        create_large_file(name="setup")
        
        # Step 2: Process in parallel (all can read the same file)
        with s.parallel():
            for i in range(1, 4):
                process_file(
                    name=f"worker-{i}",
                    arguments={"index": str(i)}
                )

w.create()
```
{% endtab %}

{% tab title="YAML" %}
```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: volume-sharing-
spec:
  entrypoint: main
  
  # Define shared volume
  volumeClaimTemplates:
    - metadata:
        name: workdir
      spec:
        accessModes: ["ReadWriteMany"]
        storageClassName: nfs  # Requires NFS provisioner
        resources:
          requests:
            storage: 50Gi
  
  templates:
    - name: main
      dag:
        tasks:
          # Create large file first
          - name: setup
            template: create-file
          
          # Process in parallel
          - name: worker-1
            template: process-file
            arguments:
              parameters:
                - name: index
                  value: "1"
            depends: setup
          
          - name: worker-2
            template: process-file
            arguments:
              parameters:
                - name: index
                  value: "2"
            depends: setup
          
          - name: worker-3
            template: process-file
            arguments:
              parameters:
                - name: index
                  value: "3"
            depends: setup
    
    - name: create-file
      container:
        image: alpine
        command: [sh, -c]
        args:
          - |
            cd /workdir
            dd if=/dev/zero of=LARGE_FILE bs=1 count=0 seek=10G
            echo "Large file created"
        volumeMounts:
          - name: workdir
            mountPath: /workdir
    
    - name: process-file
      inputs:
        parameters:
          - name: index
      container:
        image: alpine
        command: [sh, -c]
        args:
          - |
            echo "Worker {{inputs.parameters.index}} starting"
            cd /workdir
            ls -lah
            du -h LARGE_FILE
            echo "Creating output file"
            dd if=/dev/zero of=OUTPUT_{{inputs.parameters.index}} bs=1 count=0 seek=5G
            echo "Worker {{inputs.parameters.index}} done"
        volumeMounts:
          - name: workdir
            mountPath: /workdir
```
{% endtab %}
{% endtabs %}

### Setting Up NFS Provisioner

To use the volume pattern, you need an NFS provisioner in your cluster:

```bash
# Install nfs-server-provisioner
helm repo add nfs-ganesha-server-and-external-provisioner \
  https://kubernetes-sigs.github.io/nfs-ganesha-server-and-external-provisioner/
helm install nfs-provisioner nfs-ganesha-server-and-external-provisioner/nfs-server-provisioner \
  --set persistence.enabled=true \
  --set persistence.size=200Gi \
  --set storageClass.name=nfs
```

### Combining Volumes with Connectors

Use volumes to share data between custom steps and connector steps:

{% tabs %}
{% tab title="Python (Hera)" %}
```python
from hera.workflows import (
    Workflow,
    Steps,
    Step,
    TemplateRef,
    script,
    Parameter,
    models as m
)

with Workflow(
    generate_name="connector-with-volume-",
    namespace="default",
    entrypoint="main",
    volume_claim_templates=[
        m.PersistentVolumeClaim(
            metadata=m.ObjectMeta(name="shared-data"),
            spec=m.PersistentVolumeClaimSpec(
                access_modes=["ReadWriteMany"],
                storage_class_name="nfs",
                resources=m.ResourceRequirements(
                    requests={"storage": "100Gi"}
                )
            )
        )
    ]
) as w:
    @script(
        volume_mounts=[m.VolumeMount(name="shared-data", mount_path="/data")]
    )
    def prepare_data():
        """Prepare training data and save to shared volume"""
        import pandas as pd
        import os
        
        # Generate or download data
        data = pd.DataFrame({
            'feature1': range(1000000),
            'feature2': range(1000000),
            'target': [i % 2 for i in range(1000000)]
        })
        
        # Save to shared volume
        data.to_parquet("/data/training_data.parquet")
        print(f"Saved {len(data):,} rows to /data/training_data.parquet")
    
    @script(
        volume_mounts=[m.VolumeMount(name="shared-data", mount_path="/data")]
    )
    def upload_to_s3():
        """Upload prepared data to S3 for Databricks to access"""
        import subprocess
        
        # Use AWS CLI or boto3 to upload
        subprocess.run([
            "aws", "s3", "cp",
            "/data/training_data.parquet",
            "s3://ml-data/prepared/training_data.parquet"
        ])
        print("Uploaded to S3")
    
    with Steps(name="main"):
        # Step 1: Prepare data locally
        prepare_data(name="prep")
        
        # Step 2: Upload to S3
        upload_to_s3(name="upload")
        
        # Step 3: Train model in Databricks using the S3 data
        Step(
            name="train",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/ml-team/train-model",
                "task-type": "notebook",
                "cluster-mode": "New",
                "new-cluster-spark-version": "13.3.x-scala2.12",
                "new-cluster-node-type": "r5.2xlarge",
                "new-cluster-num-workers": "3",
                "args": "s3://ml-data/prepared/training_data.parquet",
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
  generateName: connector-with-volume-
spec:
  entrypoint: main
  
  volumeClaimTemplates:
    - metadata:
        name: shared-data
      spec:
        accessModes: ["ReadWriteMany"]
        storageClassName: nfs
        resources:
          requests:
            storage: 100Gi
  
  templates:
    - name: main
      steps:
        - - name: prep
            template: prepare-data
        
        - - name: upload
            template: upload-to-s3
        
        - - name: train
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/train-model"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "r5.2xlarge"
                - name: new-cluster-num-workers
                  value: "3"
                - name: args
                  value: "s3://ml-data/prepared/training_data.parquet"
    
    - name: prepare-data
      script:
        image: python:3.9
        command: [python]
        source: |
          import pandas as pd
          
          data = pd.DataFrame({
              'feature1': range(1000000),
              'feature2': range(1000000),
              'target': [i % 2 for i in range(1000000)]
          })
          
          data.to_parquet("/data/training_data.parquet")
          print(f"Saved {len(data):,} rows")
        volumeMounts:
          - name: shared-data
            mountPath: /data
    
    - name: upload-to-s3
      container:
        image: amazon/aws-cli
        command: [sh, -c]
        args:
          - |
            aws s3 cp /data/training_data.parquet s3://ml-data/prepared/training_data.parquet
            echo "Uploaded to S3"
        volumeMounts:
          - name: shared-data
            mountPath: /data
```
{% endtab %}
{% endtabs %}

### When to Use Volumes vs Artifacts

**Use Volumes (NFS/EFS) when:**
- Multiple parallel steps read the same large file (10GB+)
- Performance is critical (~20 seconds vs ~7 minutes for 10GB)
- Steps need to write to shared storage simultaneously
- Working with intermediate model checkpoints

**Use Artifacts (S3/GCS) when:**
- Sequential workflows (one step at a time)
- Smaller files (< 1GB)
- Simpler setup preferred (no volume provisioner needed)
- Long-term storage required

**Use Cloud Storage (S3/GCS directly) when:**
- Working with Databricks/Spark connectors (they access S3 natively)
- Very large datasets (100GB+)
- Data needs to persist beyond workflow execution
- Multiple workflows need to access the same data

### Volume Access Modes

| Access Mode | Description | Use Case |
|-------------|-------------|----------|
| `ReadWriteOnce` (RWO) | Single node read-write | Not suitable for parallel steps |
| `ReadWriteMany` (RWX) | Multiple nodes read-write | **Required for parallel access** |
| `ReadOnlyMany` (ROX) | Multiple nodes read-only | Parallel reads of static data |

**Important**: Use `ReadWriteMany` for parallel workflows. This requires a storage class that supports RWX (like NFS, EFS, or Azure Files).

## Best Practices

### 1. Use Parameters for Metadata
- Job IDs, URLs, status codes
- File paths, S3 URIs
- Configuration values
- Small result sets

### 2. Choose the Right Data Passing Method

**Parameters** (< 1MB):
- Job IDs, URLs, status codes
- File paths, S3 URIs
- Configuration values
- Small result sets

**Volumes** (large files, parallel access):
- Intermediate datasets shared across parallel workers
- Model checkpoints during distributed training
- Large files that multiple steps need to read

**Artifacts** (sequential, moderate size):
- Model files (1-10GB)
- Datasets for sequential processing
- Results that need long-term storage

**Cloud Storage** (very large, persistent):
- Training datasets (100GB+)
- Model registries
- Production predictions
- Data lakes

### 3. Design for Failure
Make steps idempotent - they should produce the same result if re-run:
```python
# Use overwrite mode
df.write.mode("overwrite").parquet(path)
```

### 4. Document Data Contracts
Clearly define what each step outputs:
```python
# In your notebook
"""
Outputs:
- S3 Path: s3://bucket/path/
- Schema: customer_id (string), score (double), timestamp (timestamp)
"""
```

### 5. Consider Cost vs Performance

**Volumes (NFS/EFS)**:
- Faster for large files
- Incurs storage costs while workflow runs
- Cleaned up when workflow completes

**Artifacts (S3/GCS)**:
- Slower data transfer
- Cheaper long-term storage
- Persists after workflow completes

## Additional Resources

- **[Pipekit ArgoCon Talk](https://www.youtube.com/watch?v=QZI-LXJGWYI)**: Configuring Volumes for Parallel Workflow Reads and Writes
- **[Example Workflows](https://github.com/pipekit/talk-demos/tree/main/argocon-demos/2023-configuring-volumes-for-parallel-workflow-reads-and-writes)**: Complete demo code
- **[NFS CI Example](https://github.com/pipekit/argo-workflows-ci-example)**: Working example with nfs-server-provisioner

## Next Steps

- [Multi-Step Pipelines](multi-step-pipelines.md) - Build complex workflows
- [Data Ingestion](data-ingestion.md) - Start your pipeline
- [Batch Inference](batch-inference.md) - Use outputs for predictions
- [Feature Engineering](feature-engineering.md) - Process data at scale

# Databricks Connector: YAML Examples

This page provides complete YAML examples for common Databricks workflows using the Databricks connector.

## Running a Notebook

### Basic Notebook Execution (Serverless)

The fastest way to run a notebook with minimal configuration:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: notebook-serverless-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: run-notebook
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/etl-pipeline"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
```

### Notebook with New Job Cluster

Run a notebook with a dedicated job cluster:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: notebook-new-cluster-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: feature-engineering
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/feature-engineering"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                
                # Cluster Configuration
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "i3.xlarge"
                - name: scaling-type
                  value: "autoscale"
                - name: min-workers
                  value: "2"
                - name: max-workers
                  value: "8"
                
                # Cloud Configuration
                - name: cloud-provider
                  value: "AWS"
                - name: availability
                  value: "SPOT"
                
                # Job Configuration
                - name: run-name
                  value: "feature-engineering-{{workflow.name}}"
                - name: email-notifications
                  value: "ml-team@company.com,data-team@company.com"
```

### Notebook with Existing Cluster

Use an already-running interactive cluster:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: notebook-existing-cluster-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: quick-analysis
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/analyst/quick-analysis"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Existing"
                - name: existing-cluster-id
                  value: "1234-567890-abc123"
```

## Running Spark Python Jobs

### PySpark Script with Arguments

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: spark-python-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: data-processing
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "dbfs:/scripts/process_data.py"
                - name: task-type
                  value: "spark-python"
                - name: args
                  value: "input/path/,output/path/,--mode=production"
                
                # Cluster Configuration
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "r5.2xlarge"
                - name: new-cluster-num-workers
                  value: "4"
                
                # Custom Spark Configuration
                - name: spark-conf
                  value: "spark.sql.shuffle.partitions=200,spark.executor.memory=4g"
```

## Running JAR Applications

### Scala JAR with Main Class

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: spark-jar-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: batch-processing
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "dbfs:/jars/batch-processor.jar"
                - name: task-type
                  value: "spark-jar"
                - name: main-class-name
                  value: "com.company.BatchProcessor"
                - name: args
                  value: "2024-01-01,2024-01-31,incremental"
                
                # Cluster with Instance Pool
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: instance-pool-id
                  value: "pool-abc123"
                - name: scaling-type
                  value: "fixed"
                - name: new-cluster-num-workers
                  value: "10"
```

## Multi-Step Pipeline

### Data Pipeline with Multiple Notebooks

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: ml-pipeline-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        # Step 1: Data Ingestion
        - - name: ingest-data
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/01-ingest"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: run-name
                  value: "ingest-{{workflow.name}}"
        
        # Step 2: Feature Engineering
        - - name: feature-engineering
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/02-features"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "i3.2xlarge"
                - name: scaling-type
                  value: "autoscale"
                - name: min-workers
                  value: "2"
                - name: max-workers
                  value: "10"
                - name: run-name
                  value: "features-{{workflow.name}}"
        
        # Step 3: Model Training
        - - name: train-model
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/03-train"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-gpu-ml-scala2.12"
                - name: new-cluster-node-type
                  value: "g4dn.xlarge"
                - name: new-cluster-num-workers
                  value: "1"
                - name: run-name
                  value: "train-{{workflow.name}}"
                
        # Step 4: Model Evaluation
        - - name: evaluate-model
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/ml-team/04-evaluate"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: run-name
                  value: "evaluate-{{workflow.name}}"
```

## Using Job Outputs

### Passing Outputs to Next Steps

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
        # Step 1: Run Databricks job and capture outputs
        - - name: process-data
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/processor"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
        
        # Step 2: Use outputs from previous step
        - - name: print-results
            template: print-output
            arguments:
              parameters:
                - name: run-id
                  value: "{{steps.process-data.outputs.parameters.run-id}}"
                - name: run-url
                  value: "{{steps.process-data.outputs.parameters.run-url}}"
                - name: result
                  value: "{{steps.process-data.outputs.parameters.result}}"
                - name: state
                  value: "{{steps.process-data.outputs.parameters.state}}"
    
    - name: print-output
      inputs:
        parameters:
          - name: run-id
          - name: run-url
          - name: result
          - name: state
      container:
        image: alpine:latest
        command: [sh, -c]
        args:
          - |
            echo "Databricks Run ID: {{inputs.parameters.run-id}}"
            echo "Databricks Run URL: {{inputs.parameters.run-url}}"
            echo "Result: {{inputs.parameters.result}}"
            echo "State: {{inputs.parameters.state}}"
```

## Running Existing Databricks Jobs

### Trigger Existing Job by ID

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: existing-job-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: run-scheduled-job
            templateRef:
              name: databricks-connector
              template: run-existing-job
            arguments:
              parameters:
                - name: job-id
                  value: "123456"
                - name: notebook-params
                  value: "date=2024-01-01,env=production"
```

### Trigger with Different Parameter Types

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: existing-job-params-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        # For notebook-based jobs
        - - name: run-notebook-job
            templateRef:
              name: databricks-connector
              template: run-existing-job
            arguments:
              parameters:
                - name: job-id
                  value: "111111"
                - name: notebook-params
                  value: "start_date=2024-01-01,end_date=2024-01-31"
        
        # For Python-based jobs
        - - name: run-python-job
            templateRef:
              name: databricks-connector
              template: run-existing-job
            arguments:
              parameters:
                - name: job-id
                  value: "222222"
                - name: python-params
                  value: "arg1,arg2,arg3"
        
        # For JAR-based jobs
        - - name: run-jar-job
            templateRef:
              name: databricks-connector
              template: run-existing-job
            arguments:
              parameters:
                - name: job-id
                  value: "333333"
                - name: jar-params
                  value: "param1,param2,param3"
```

## Advanced Configurations

### Using Cluster Policies

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: with-policy-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: governed-job
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/governed-notebook"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: policy-id
                  value: "ABC123DEF456"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
```

### Custom Databricks Secret

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: custom-secret-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: dev-environment
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/dev-team/notebook"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
                - name: databricks-secret-name
                  value: "databricks-dev-secret"  # Custom secret for dev environment
```

## Next Steps

- **[Hera Examples](hera-examples.md)**: See these examples in Python
- **[Parameter Reference](parameter-reference.md)**: Complete parameter documentation
- **[Setup Guide](setup.md)**: Configure secrets and prerequisites
- **[Cluster Modes](cluster-modes.md)**: Deep dive into cluster options

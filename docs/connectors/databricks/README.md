# Databricks Connector

The Databricks connector enables you to run Databricks notebooks, Spark Python scripts, and JAR applications directly from Argo Workflows with full cluster configuration and monitoring support.

## Overview

The Databricks connector provides two main templates:

1. **`run-job`**: Submit and run a new Databricks job with custom configuration
2. **`run-existing-job`**: Trigger an existing Databricks job by ID

Both templates handle:
- ✅ Job submission to Databricks
- ✅ Automatic status monitoring
- ✅ Result and output retrieval
- ✅ Cluster provisioning (new, existing, or serverless)
- ✅ Error handling and logging

## Key Features

### Multiple Task Types

Run different types of Databricks workloads:

- **Notebooks**: Execute Databricks notebooks with parameters
- **Spark Python**: Run standalone Python files
- **Spark JAR**: Execute Scala/Java JARs with main class

### Flexible Cluster Options

Choose from three cluster modes:

- **New Cluster**: Provision a dedicated job cluster
- **Existing Cluster**: Use a running interactive cluster
- **Serverless**: Leverage Databricks serverless compute (fastest startup)

### Full Configuration Control

- Instance types and Spark versions
- Autoscaling and fixed worker counts
- Spot vs on-demand instances
- Custom Spark configurations
- Instance pools and cluster policies

### Rich Outputs

Access job results and metadata:

```yaml
outputs:
  parameters:
    - name: run-id         # Databricks run ID
    - name: run-url        # Link to job in Databricks UI
    - name: result         # Job result/output
    - name: state          # Final state (SUCCESS/FAILED)
    - name: json           # Full job details as JSON
```

## Quick Example

### Running a Notebook

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: databricks-notebook-
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
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "i3.xlarge"
                - name: new-cluster-num-workers
                  value: "2"
```

[→ See More Examples](yaml-examples.md)

## Installation

### WorkflowTemplate (Namespace-Scoped)

```bash
kubectl
# Apache Spark Connector

The Apache Spark connector enables you to run Spark applications (JVM and Python) on Kubernetes using the [Spark Operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator), directly from Argo Workflows.

## Overview

The Spark connector provides templates for running Spark applications with full control over cluster configuration:

1. **`spark-data-connector-jvm`**: Run Java or Scala Spark applications
2. **`spark-data-connector-python`**: Run PySpark (Python) applications
3. **`spark-delete-resource`**: Clean up Spark resources after job completion

## Key Features

### Language Support

- ✅ **Java/Scala**: Run JAR files with custom main classes
- ✅ **Python**: Execute PySpark scripts with dependencies

### Full Cluster Control

- Driver and executor resource configuration (CPU, memory)
- Custom service accounts for fine-grained RBAC
- Image pull secrets for private registries
- Environment variable injection via ConfigMaps
- Custom JVM options for driver and executor

### Dependency Management

- JAR dependencies (local, HTTP, HDFS, S3, GCS)
- Python file dependencies (`.py`, `.zip`, `.egg`)
- Maven package resolution
- Custom repositories

### Production Features

- Automatic job monitoring via Spark Operator
- Spark UI access during execution
- Workflow-scoped labeling for tracking
- Resource cleanup templates
- Success/failure condition detection

## Quick Example

### Running a Scala Spark Job

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: spark-pi-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: spark-job
            templateRef:
              name: spark-data-connector-jvm
              template: spark
            arguments:
              parameters:
                - name: type
                  value: "Scala"
                - name: mainClass
                  value: "org.apache.spark.examples.SparkPi"
                - name: namespace
                  value: "default"
                - name: globalImage
                  value: "gcr.io/spark-operator/spark:v3.1.1"
                - name: mainApplicationFile
                  value: "local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar"
                - name: executorInstances
                  value: "2"
```

[→ See More Examples](yaml-examples.md)

## Prerequisites

### 1. Spark Operator

The Spark Operator must be installed in your cluster. Install it with:

```bash
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true
```

### 2. RBAC Configuration

Argo needs permission to create and manage SparkApplication resources:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: argo-spark-clusterrole
rules:
  - apiGroups:
      - sparkoperator.k8s.io
    resources:
      - sparkapplications
      - sparkapplications/status
      - scheduledsparkapplications
      - scheduledsparkapplications/status
    verbs:
      - '*'
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: argo-spark-cluster-role-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: argo-spark-clusterrole
subjects:
- kind: ServiceAccount
  name: default  # or your Argo service account
  namespace: argo
```

[→ Detailed Setup Guide](setup.md)

## Installation

### WorkflowTemplate (Namespace-Scoped)

For JVM (Java/Scala):
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/workflowtemplate-jvm.yaml
```

For Python:
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/workflowtemplate-python.yaml
```

For resource cleanup:
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/workflowtemplate-spark-resource-cleanup.yaml
```

### ClusterWorkflowTemplate (Cluster-Scoped)

For JVM (Java/Scala):
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/clusterworkflowtemplate-jvm.yaml
```

For Python:
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/clusterworkflowtemplate-python.yaml
```

For resource cleanup:
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/clusterworkflowtemplate-spark-resource-cleanup.yaml
```

### Verify Installation

```bash
# For WorkflowTemplates
kubectl get workflowtemplates | grep spark

# For ClusterWorkflowTemplates
kubectl get clusterworkflowtemplates | grep spark
```

## Common Parameters

### JVM Jobs (Java/Scala)

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `type` | ✅ | Language type | `Java`, `Scala` |
| `mainClass` | ✅ | Main class with full package path | `org.apache.spark.examples.SparkPi` |
| `mainApplicationFile` | ✅ | Path to JAR file | `local:///app.jar` |
| `namespace` | ✅ | Kubernetes namespace | `default` |
| `globalImage` | One of global/driver/executor | Docker image | `gcr.io/spark-operator/spark:v3.1.1` |

### Python Jobs (PySpark)

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `mainApplicationFile` | ✅ | Path to Python file | `local:///app/main.py` |
| `namespace` | ✅ | Kubernetes namespace | `default` |
| `globalImage` | One of global/driver/executor | Docker image | `gcr.io/spark-operator/spark-py:v3.1.1` |
| `pythonVersion` | | Python major version | `3` (default) |

[→ Full Parameter Reference](parameter-reference.md)

## Output Parameters

The Spark connector provides these output parameters:

| Output | Description | Example |
|--------|-------------|---------|
| `spark-job-name` | Name of the SparkApplication | `spark-pi-abc123` |
| `spark-job-namespace` | Namespace where job runs | `default` |

Use these outputs in subsequent workflow steps:

```yaml
- - name: cleanup
    templateRef:
      name: spark-delete-resource
       template: spark-delete
    arguments:
      parameters:
        - name: namespace
          value: "{{steps.spark-job.outputs.parameters.spark-job-namespace}}"
        - name: spark-job-name
          value: "{{steps.spark-job.outputs.parameters.spark-job-name}}"
```

## Accessing Spark UI

During execution, the Spark UI is available via a Kubernetes service:

```bash
# Port-forward to access Spark UI
kubectl port-forward svc/<spark-job-name>-ui-svc 4040:4040

# Access at http://localhost:4040
```

## Resource Cleanup

By default, Spark driver pods and services remain after job completion. Use the cleanup template to remove them:

```yaml
steps:
  - - name: run-spark-job
      templateRef:
        name: spark-data-connector-jvm
        template: spark
      arguments:
        parameters:
          - name: type
            value: "Scala"
          # ... other parameters
  
  - - name: cleanup
      templateRef:
        name: spark-delete-resource
        template: spark-delete
      arguments:
        parameters:
          - name: namespace
            value: "{{steps.run-spark-job.outputs.parameters.spark-job-namespace}}"
          - name: spark-job-name
            value: "{{steps.run-spark-job.outputs.parameters.spark-job-name}}"
```

[→ Resource Cleanup Guide](resource-cleanup.md)

## Next Steps

- **[Setup Guide](setup.md)**: Configure RBAC and prerequisites
- **[JVM Jobs](java-scala-jobs.md)**: Running Java/Scala applications
- **[Python Jobs](python-jobs.md)**: Running PySpark applications
- **[YAML Examples](yaml-examples.md)**: Complete workflow examples
- **[Hera Examples](hera-examples.md)**: Python SDK examples
- **[Parameter Reference](parameter-reference.md)**: Complete parameter documentation

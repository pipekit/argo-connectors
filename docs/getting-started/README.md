# Quick Start

Get up and running with community-maintained Argo Connectors in less than 10 minutes.

> **New to Argo Connectors?** This hub provides production-ready integrations maintained by the community. Start here to use existing connectors, or [contribute your own](../CONTRIBUTING.md)!

## Prerequisites

Before you begin, ensure you have:

- **Kubernetes cluster** (v1.21+) with kubectl access
- **Argo Workflows** (v3.5+) installed and running
- **kubectl** configured to communicate with your cluster

For platform-specific connectors:
- **Databricks Connector**: A Databricks workspace and access token
- **Spark Connector**: Spark Operator (v1beta2+) installed

[→ Detailed Prerequisites](prerequisites.md)

## Step 1: Install a Connector

Choose your connector and install it to your cluster:

### Databricks Connector

**WorkflowTemplate** (namespace-scoped):
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/databricks/workflow-template.yaml
```

**ClusterWorkflowTemplate** (cluster-scoped):
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/databricks/cluster-template.yaml
```

### Apache Spark Connector

**WorkflowTemplate** (namespace-scoped):
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/workflowtemplate-jvm.yaml
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/workflowtemplate-python.yaml
```

**ClusterWorkflowTemplate** (cluster-scoped):
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/clusterworkflowtemplate-jvm.yaml
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/apache/spark/clusterworkflowtemplate-python.yaml
```

### Verify Installation

```bash
# For WorkflowTemplates
kubectl get workflowtemplates

# For ClusterWorkflowTemplates
kubectl get clusterworkflowtemplates
```

## Step 2: Configure Credentials (Databricks)

If you're using the Databricks connector, create a secret with your credentials:

```bash
kubectl create secret generic databricks-secret \
  --from-literal=host='https://your-workspace.cloud.databricks.com' \
  --from-literal=token='your-databricks-token'
```

[→ Detailed Databricks Setup](../connectors/databricks/setup.md)

## Step 3: Run Your First Workflow

### Option A: Using YAML

Create a file named `my-first-workflow.yaml`:

<details>
<summary><strong>Databricks Notebook Example</strong></summary>

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
        - - name: run-notebook
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/your-email@company.com/HelloWorld"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "Serverless"
```
</details>

<details>
<summary><strong>Spark Job Example</strong></summary>

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
```
</details>

Submit the workflow:
```bash
kubectl create -f my-first-workflow.yaml
```

### Option B: Using Hera (Python SDK)

Install Hera:
```bash
pip install hera
```

Create a file named `my_first_workflow.py`:

<details>
<summary><strong>Databricks Notebook Example</strong></summary>

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="databricks-notebook-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        Step(
            name="run-notebook",
            template_ref=TemplateRef(
                name="databricks-connector",
                template="run-job",
                cluster_scope=False,
            ),
            arguments={
                "code-path": "/Users/your-email@company.com/HelloWorld",
                "task-type": "notebook",
                "cluster-mode": "Serverless"
            }
        )

# Submit to cluster
w.create()
```
</details>

<details>
<summary><strong>Spark Job Example</strong></summary>

```python
from hera.workflows import Workflow, Steps, Step, TemplateRef

with Workflow(
    generate_name="spark-pi-",
    namespace="default",
    entrypoint="main"
) as w:
    with Steps(name="main"):
        Step(
            name="spark-job",
            template_ref=TemplateRef(
                name="spark-data-connector-jvm",
                template="spark",
                cluster_scope=False,
            ),
            arguments={
                "type": "Scala",
                "mainClass": "org.apache.spark.examples.SparkPi",
                "namespace": "default",
                "globalImage": "gcr.io/spark-operator/spark:v3.1.1",
                "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar"
            }
        )

w.create()
```
</details>

Run the workflow:
```bash
python my_first_workflow.py
```

## Step 4: Monitor Workflow Execution

### Using kubectl

```bash
# List workflows
kubectl get workflows

# Watch workflow in real-time
kubectl get workflows -w

# Get workflow details
kubectl describe workflow <workflow-name>

# View logs
kubectl logs -l workflows.argoproj.io/workflow=<workflow-name>
```

### Using Argo UI

Access the Argo Workflows UI:
```bash
kubectl -n argo port-forward deployment/argo-server 2746:2746
```

Then visit: http://localhost:2746

## Step 5: Access Workflow Outputs

Connectors provide rich output parameters that you can use in subsequent steps:

```yaml
- - name: process-results
    template: my-template
    arguments:
      parameters:
        - name: notebook-url
          value: "{{steps.run-notebook.outputs.parameters.run-url}}"
        - name: result
          value: "{{steps.run-notebook.outputs.parameters.result}}"
```

## Common Parameters

### Databricks Connector

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `code-path` | ✅ | Path to notebook or code | `/Users/email/notebook` |
| `task-type` | ✅ | Type of task | `notebook`, `spark-python`, `spark-jar` |
| `cluster-mode` | ✅ | Cluster type | `New`, `Existing`, `Serverless` |
| `databricks-secret-name` | | Secret name | `databricks-secret` (default) |

[→ Full Parameter Reference](../connectors/databricks/parameter-reference.md)

### Spark Connector

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `namespace` | ✅ | Kubernetes namespace | `default` |
| `type` | ✅ | Language type | `Java`, `Scala`, `Python` |
| `mainApplicationFile` | ✅ | Path to application | `local:///app.jar` |
| `globalImage` | ✅ | Docker image | `gcr.io/spark-operator/spark:v3.1.1` |

[→ Full Parameter Reference](../connectors/apache-spark/parameter-reference.md)

## Troubleshooting

### Workflow Failed

Check the workflow logs:
```bash
kubectl logs -l workflows.argoproj.io/workflow=<workflow-name>
```

### Connection Issues

Verify credentials:
```bash
kubectl get secret databricks-secret -o yaml
```

### Timeout Issues

Increase timeout in your workflow spec:
```yaml
spec:
  activeDeadlineSeconds: 3600  # 1 hour
```

[→ Full Troubleshooting Guide](../troubleshooting/README.md)

## Next Steps

Now that you've run your first connector workflow, explore:

- **[Core Concepts](../core-concepts/README.md)**: Understand WorkflowTemplates and parameters
- **[Databricks Connector](../connectors/databricks/README.md)**: Learn all Databricks capabilities
- **[Apache Spark Connector](../connectors/apache-spark/README.md)**: Explore Spark job options
- **[Guides](../guides/README.md)**: Build real-world ML pipelines
- **[Hera SDK](../hera-sdk/README.md)**: Master the Python SDK

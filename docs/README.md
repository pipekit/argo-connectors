# Argo Connectors

**Reusable, production-ready connectors for orchestrating data workloads with Argo Workflows**

Argo Connectors is a library of pre-built [WorkflowTemplates](https://argo-workflows.readthedocs.io/en/latest/workflow-templates/) and [ClusterWorkflowTemplates](https://argo-workflows.readthedocs.io/en/latest/cluster-workflow-templates/) that enable you to seamlessly integrate third-party data tools into your Argo Workflows. These connectors are designed to be composable, maintainable, and ready for production use.

## What are Argo Connectors?

Argo Connectors provide standardized interfaces to popular data processing platforms, allowing you to:

- **Run Databricks notebooks and Spark jobs** directly from Argo Workflows
- **Execute Apache Spark applications** (JVM and Python) on Kubernetes
- **Build complex ML pipelines** by chaining multiple connectors together
- **Manage data workflows** using both YAML and Python (via [Hera](https://github.com/argoproj-labs/hera))

## Quick Start

Get up and running in 5 minutes:

```bash
# Install a connector to your cluster
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/databricks/workflow-template.yaml

# Run your first Databricks notebook
kubectl create -f examples/databricks-notebook.yaml
```

[→ Full Getting Started Guide](getting-started/README.md)

## Available Connectors

### Databricks Connector
Run Databricks notebooks, Spark Python jobs, and JAR applications with full cluster configuration support.

**Key features:**
- Execute notebooks, Python files, or JAR applications
- Support for new job clusters, existing clusters, or serverless compute
- Automatic job monitoring and result retrieval
- Configurable cluster scaling and resource allocation

[→ Databricks Connector Documentation](connectors/databricks/README.md)

### Apache Spark Connector
Submit and monitor Spark applications on Kubernetes using the Spark Operator.

**Key features:**
- Run JVM (Java/Scala) and Python Spark applications
- Full control over driver and executor configuration
- Automatic resource cleanup
- Support for dependencies (JARs, files, packages)

[→ Apache Spark Connector Documentation](connectors/apache-spark/README.md)

## Why Argo Connectors?

### 🔌 **Drop-in Integration**
Install connectors as WorkflowTemplates and use them like any other Argo Workflow step. No custom controllers or operators needed.

### 🐍 **Python-Friendly**
Use the [Hera SDK](https://github.com/argoproj-labs/hera) to define workflows in Python while leveraging the same battle-tested connectors.

### 🔧 **Production-Ready**
Built with best practices for error handling, resource management, and monitoring in production environments.

### 📦 **Composable**
Chain multiple connectors together to build complex data pipelines with clear dependencies and data flow.

## Example: Databricks Notebook Pipeline

**Using YAML:**
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
        - - name: feature-engineering
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/data-team/feature-engineering"
                - name: task-type
                  value: "notebook"
                - name: cluster-mode
                  value: "New"
                - name: new-cluster-spark-version
                  value: "13.3.x-scala2.12"
                - name: new-cluster-node-type
                  value: "i3.xlarge"
```

**Using Hera (Python SDK):**
```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(generate_name="ml-pipeline-", namespace="default") as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="feature-engineering",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/data-team/feature-engineering"),
                Parameter(name="task-type", value="notebook"),
                Parameter(name="cluster-mode", value="New"),
                Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
                Parameter(name="new-cluster-node-type", value="i3.xlarge"),
            ]
        )
```

[→ See More Examples](guides/README.md)

## Documentation Structure

- **[Getting Started](getting-started/README.md)** - Install and run your first connector
- **[Core Concepts](core-concepts/README.md)** - Understand WorkflowTemplates and parameters
- **[Connectors](connectors/README.md)** - Detailed documentation for each connector
- **[Guides](guides/README.md)** - Task-oriented tutorials for common ML workflows
- **[Hera SDK](hera-sdk/README.md)** - Using connectors with Python
- **[Best Practices](best-practices/README.md)** - Production deployment patterns
- **[Troubleshooting](troubleshooting/README.md)** - Common issues and solutions
- **[Reference](reference/README.md)** - Complete API documentation

## Community & Support

- **GitHub Repository**: [pipekit/argo-connectors](https://github.com/pipekit/argo-connectors)
- **Issues**: [Report bugs or request features](https://github.com/pipekit/argo-connectors/issues)
- **Discussions**: [Ask questions and share ideas](https://github.com/pipekit/argo-connectors/discussions)

## Contributing

We welcome contributions! See our [Contributing Guide](CONTRIBUTING.md) for details on how to:
- Report bugs
- Suggest new connectors
- Submit pull requests
- Improve documentation

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

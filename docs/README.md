# Argo Connectors

**A community-driven hub of reusable, production-ready connectors for orchestrating data workloads with Argo Workflows**

Argo Connectors is an open-source, community-maintained library of pre-built [WorkflowTemplates](https://argo-workflows.readthedocs.io/en/latest/workflow-templates/) and [ClusterWorkflowTemplates](https://argo-workflows.readthedocs.io/en/latest/cluster-workflow-templates/) that enable you to seamlessly integrate third-party data tools into your Argo Workflows.

**Think of it as a "Docker Hub for Argo Workflow Connectors"** - a central repository where the community shares, discovers, and maintains standardized integrations to popular data platforms. These connectors are designed to be composable, maintainable, and ready for production use.

> **Founded by [Pipekit](https://pipekit.io)** - We're seeding this initiative with production-tested connectors for Databricks and Apache Spark. Our vision is to grow this into the go-to marketplace for Argo Workflow integrations, maintained by and for the community.

## What is Argo Connectors?

Argo Connectors is a community hub where you'll find:

- **Production-Ready Integrations**: Standardized, tested connectors to popular data platforms
- **Community Contributions**: Connectors built and maintained by the community
- **Best Practices**: Learn from real-world implementations
- **Shared Standards**: Consistent interfaces across all connectors

### What You Can Do

Use community-maintained connectors to:

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

> **Want to contribute a connector?** Check out our [Contributing Guide](CONTRIBUTING.md) to add integrations for platforms like Snowflake, dbt, Airflow, Fivetran, and more!

### Databricks Connector
**Maintained by: Pipekit**
Run Databricks notebooks, Spark Python jobs, and JAR applications with full cluster configuration support.

**Key features:**
- Execute notebooks, Python files, or JAR applications
- Support for new job clusters, existing clusters, or serverless compute
- Automatic job monitoring and result retrieval
- Configurable cluster scaling and resource allocation

[→ Databricks Connector Documentation](connectors/databricks/README.md)

### Apache Spark Connector
**Maintained by: Pipekit**
Submit and monitor Spark applications on Kubernetes using the Spark Operator.

**Key features:**
- Run JVM (Java/Scala) and Python Spark applications
- Full control over driver and executor configuration
- Automatic resource cleanup
- Support for dependencies (JARs, files, packages)

[→ Apache Spark Connector Documentation](connectors/apache-spark/README.md)

## Why Use Argo Connectors?

### 🌍 **Community-Driven**
Built by practitioners, for practitioners. Benefit from collective experience and contribute your own learnings back to the community.

### 🔌 **Drop-in Integration**
Install connectors as WorkflowTemplates and use them like any other Argo Workflow step. No custom controllers or operators needed.

### 🤝 **Shared Standards**
Consistent parameter interfaces and patterns across all connectors, reducing the learning curve as you adopt new integrations.

### 🐍 **Python-Friendly**
All connectors work seamlessly with the [Hera SDK](https://github.com/argoproj-labs/hera) - define workflows in Python while leveraging community-maintained integrations.

### 🔧 **Production-Ready**
Connectors are battle-tested in production environments with best practices for error handling, resource management, and monitoring.

### 📦 **Composable**
Chain multiple connectors together to build complex data pipelines with clear dependencies and data flow.

### 🚀 **Growing Ecosystem**
As the community expands, discover new connectors for emerging tools and platforms - or contribute your own!

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

## Join the Community

Argo Connectors thrives on community participation. Here's how you can get involved:

### 🐛 Report Issues & Request Features
Found a bug or want a new connector? [Open an issue](https://github.com/pipekit/argo-connectors/issues)

### 💬 Join Discussions
Ask questions, share your workflows, and help others: [GitHub Discussions](https://github.com/pipekit/argo-connectors/discussions)

### 🤝 Contribute
We welcome all contributions! See our [Contributing Guide](CONTRIBUTING.md) for:
- **Adding New Connectors**: Share your integration with the community
- **Improving Existing Connectors**: Bug fixes, new features, better documentation
- **Documentation**: Help others learn and adopt connectors
- **Testing**: Validate connectors in different environments

### 🎯 Connector Wishlist
Help us prioritize! Vote on or suggest connectors for:
- ❄️ Snowflake
- 🔧 dbt (Data Build Tool)
- 🌊 Airflow
- 📊 Fivetran
- 🔄 Great Expectations
- 📈 Looker/Mode/Tableau
- And more!

[→ See Full Wishlist & Vote](https://github.com/pipekit/argo-connectors/discussions/categories/connector-requests)

## Maintainers

### Founding Contributors
- **[Pipekit](https://pipekit.io)** - We built and maintain the initial Databricks and Spark connectors, and provide stewardship for the project

### Community Maintainers
Interested in becoming a maintainer? [Learn more](CONTRIBUTING.md#becoming-a-maintainer)

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

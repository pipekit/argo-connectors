# Connectors Overview

Welcome to the Argo Connectors hub! This is a community-driven collection of production-ready integrations for Argo Workflows.

## Available Connectors

### Data & Compute Platforms

#### [Databricks](databricks/README.md)
**Maintained by: Pipekit** | Status: Production-ready

Run Databricks notebooks, Spark Python jobs, and JAR applications with full cluster configuration support.

Features:
- Notebook execution
- Spark Python & JAR jobs
- New, existing, and serverless clusters
- Autoscaling and spot instances

[View Documentation](databricks/README.md)

#### [Apache Spark](apache-spark/README.md)
**Maintained by: Pipekit** | Status: Production-ready

Run Spark applications on Kubernetes using the Spark Operator.

Features:
- JVM (Java/Scala) applications
- PySpark (Python) applications
- Full cluster configuration
- Dependency management

[View Documentation](apache-spark/README.md)

## Priority Connectors

We're actively building these connectors. Contributions welcome!

### Distributed Computing
- **Dask** - Parallel computing in Python
- **Ray** - Distributed computing framework for ML workloads

### Machine Learning Frameworks
- **PyTorch** - Training and inference jobs with PyTorch
- **TensorFlow** - TensorFlow training and serving
- **Stable Diffusion** - Image generation pipelines

### ML Operations
- **MLflow** - Experiment tracking, model registry, and deployment
- **Weights & Biases** - Experiment tracking and model management
- **HuggingFace** - Model training and inference with Transformers

### CI/CD
- **Hera Image Build CI** - Build and push Docker images for Hera workflows

## Future Connectors

Community-requested integrations we'd love to see:

**Data Platforms**
- Snowflake - Execute SQL, stored procedures, and Snowpark jobs
- dbt (Data Build Tool) - Run dbt models, tests, and docs

**Data Quality**
- Great Expectations - Run validation suites
- Soda - Data quality testing

**Workflow Orchestration**
- Airflow - Trigger Airflow DAGs from Argo

**Data Movement**
- Fivetran - Sync data connectors programmatically
- Apache Kafka - Message queue operations

## Want a New Connector?

**Request a connector**: [Open an issue](https://github.com/pipekit/argo-connectors/issues/new) describing the platform and your use case

**Contribute a connector**: [Submit a pull request](https://github.com/pipekit/argo-connectors/pulls) - see our [Contributing Guide](../CONTRIBUTING.md) for details

### What Makes a Good Connector?

**Clear Use Case**: Solves a real integration need

**Well Documented**: Complete examples and parameter reference

**Production-Tested**: Battle-tested in real workflows

**Follows Standards**: Consistent with existing connectors

**Community Value**: Benefits multiple users/organizations

## Connector Quality Levels

Connectors in the hub display quality indicators:

**Production-Ready**: Tested in production environments

**Beta**: Functional but needs more testing

**Alpha**: Experimental, may have breaking changes

**Well-Documented**: Complete docs and examples

**Tested**: Has automated tests

**Community-Maintained**: Multiple active maintainers


## Finding the Right Connector

### By Platform Category
- [Data & Compute Platforms](#data--compute-platforms)
- [Distributed Computing](#distributed-computing)
- [Machine Learning Frameworks](#machine-learning-frameworks)
- [ML Operations](#ml-operations)

### By Use Case
- **ETL/ELT Pipelines**: Databricks, Spark, dbt, Fivetran
- **Data Quality**: Great Expectations, Soda
- **ML Workflows**: Databricks, Spark, PyTorch, TensorFlow, MLflow
- **Experiment Tracking**: MLflow, Weights & Biases
- **Model Serving**: PyTorch, TensorFlow, HuggingFace

### By Programming Language
- **Python**: Databricks (PySpark), Spark (Python), Dask, PyTorch, TensorFlow
- **JVM**: Spark (Scala/Java), Databricks (JARs)

## Community Support

Questions or issues?

- [Report Issues](https://github.com/pipekit/argo-connectors/issues) - Bug reports and feature requests
- [Submit Pull Requests](https://github.com/pipekit/argo-connectors/pulls) - Contribute improvements
- [Contributing Guide](../CONTRIBUTING.md) - Learn how to contribute

## Next Steps

- [Get Started](../getting-started/README.md) - Install and use your first connector
- [Contributing Guide](../CONTRIBUTING.md) - Add your own connector
- [Core Concepts](../core-concepts/README.md) - Learn how connectors work
- [Guides](../guides/README.md) - Build complete data pipelines

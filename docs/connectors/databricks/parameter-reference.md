# Databricks Connector: Parameter Reference

Complete reference for all parameters supported by the Databricks connector.

## Template: `run-job`

Submit and run a new Databricks job with custom configuration.

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `code-path` | string | Path to the code (notebook, Python file, or JAR URI) | `/Users/team/notebook` |

### Task Configuration

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `task-type` | enum | `notebook` | Type of task to run | `notebook`, `spark-python`, `spark-jar` |
| `main-class-name` | string | `""` | Main class name (required for `spark-jar`) | `com.company.MainClass` |
| `args` | string | `""` | Comma-separated positional arguments | `arg1,arg2,arg3` |

### Run Metadata

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `run-name` | string | `argo-workflow-run` | Name of the job run | `feature-engineering-job` |
| `task-key` | string | `main_task` | Key for the task | `main_task` |
| `email-notifications` | string | `""` | Comma-separated email addresses for notifications | `team@company.com,admin@company.com` |

### Cluster Mode

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `cluster-mode` | enum | `New` | Cluster mode | `New`, `Existing`, `Serverless` |

### Existing Cluster Configuration

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `existing-cluster-id` | string | `""` | ID of existing cluster (required if `cluster-mode=Existing`) | `1234-567890-abc123` |

### New Cluster Configuration

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `new-cluster-node-type` | string | `""` | Node type (required for `New` cluster unless using instance pool) | `i3.xlarge`, `r5.2xlarge` |
| `new-cluster-spark-version` | string | `""` | Spark version (required for `New` cluster) | `13.3.x-scala2.12` |
| `instance-pool-id` | string | `""` | Instance pool ID for new cluster | `pool-abc123` |
| `policy-id` | string | `""` | Policy ID for new cluster | `ABC123DEF456` |
| `spark-conf` | string | `""` | Spark configuration (comma-separated key=value pairs) | `spark.executor.memory=4g,spark.sql.shuffle.partitions=200` |

### Scaling Configuration

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `scaling-type` | enum | `fixed` | Scaling strategy | `fixed`, `autoscale` |
| `new-cluster-num-workers` | string | `1` | Number of workers (if `scaling-type=fixed`) | `4` |
| `min-workers` | string | `1` | Minimum workers (if `scaling-type=autoscale`) | `2` |
| `max-workers` | string | `2` | Maximum workers (if `scaling-type=autoscale`) | `10` |

### Cloud Provider Configuration

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `cloud-provider` | enum | `""` | Cloud provider | `AWS`, `AZURE`, `GCP` |
| `availability` | enum | `""` | Instance availability | `SPOT`, `ON_DEMAND` |

### Authentication

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `databricks-secret-name` | string | `databricks-secret` | Name of K8s Secret containing credentials | `databricks-prod-secret` |
| `connector-image` | string | `ghcr.io/pipekit/databricks-connector:0.0.2` | Container image for connector | `ghcr.io/pipekit/databricks-connector:0.0.3` |

### Output Parameters

The `run-job` template provides the following output parameters:

| Output | Type | Description | Example |
|--------|------|-------------|---------|
| `run-id` | string | Databricks run ID | `12345` |
| `run-url` | string | Link to job in Databricks UI | `https://workspace.cloud.databricks.com/...` |
| `result` | string | Job result/output | `{"status": "success"}` |
| `state` | string | Final job state | `SUCCESS`, `FAILED` |
| `json` | string | Full job details as JSON | `{"run_id": 12345, ...}` |

## Template: `run-existing-job`

Trigger an existing Databricks job by ID.

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `job-id` | string | ID of the existing Databricks job | `123456` |

### Optional Parameters

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `job-parameters` | string | `""` | Comma-separated parameters (legacy, key=value format) | `key1=value1,key2=value2` |
| `notebook-params` | string | `""` | Notebook parameters (key=value format) | `date=2024-01-01,env=prod` |
| `python-params` | string | `""` | Python parameters (positional, comma-separated) | `arg1,arg2,arg3` |
| `jar-params` | string | `""` | JAR parameters (positional, comma-separated) | `param1,param2,param3` |
| `spark-submit-params` | string | `""` | Spark submit parameters (positional) | `--conf spark.executor.memory=4g` |

### Authentication

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `databricks-secret-name` | string | `databricks-secret` | Name of K8s Secret containing credentials | `databricks-prod-secret` |
| `connector-image` | string | `ghcr.io/pipekit/databricks-connector:0.0.2` | Container image for connector | `ghcr.io/pipekit/databricks-connector:0.0.3` |

### Output Parameters

Same as `run-job` template.

## Parameter Value Formats

### Task Types

- `notebook`: Execute a Databricks notebook
- `spark-python`: Run a Python Spark application
- `spark-jar`: Execute a JAR file (requires `main-class-name`)

### Cluster Modes

- `New`: Create a new job cluster (destroyed after job completion)
- `Existing`: Use an already-running interactive cluster
- `Serverless`: Use Databricks serverless compute (fastest startup, no cluster configuration needed)

### Scaling Types

- `fixed`: Fixed number of workers specified by `new-cluster-num-workers`
- `autoscale`: Dynamic scaling between `min-workers` and `max-workers`

### Cloud Providers

- `AWS`: Amazon Web Services
- `AZURE`: Microsoft Azure
- `GCP`: Google Cloud Platform

### Availability Options

- `ON_DEMAND`: On-demand instances (most reliable)
- `SPOT`: Spot/preemptible instances (cost-effective but may be interrupted)

## Common Node Types by Cloud

### AWS

| Node Type | vCPUs | Memory | Use Case |
|-----------|-------|--------|----------|
| `i3.xlarge` | 4 | 30.5 GB | General purpose, local SSD |
| `i3.2xlarge` | 8 | 61 GB | General purpose, larger workloads |
| `r5.xlarge` | 4 | 32 GB | Memory-optimized |
| `r5.2xlarge` | 8 | 64 GB | Memory-optimized, larger |
| `g4dn.xlarge` | 4 | 16 GB | GPU workloads (1x T4 GPU) |
| `g4dn.12xlarge` | 48 | 192 GB | Large GPU workloads (4x T4 GPUs) |

### Azure

| Node Type | vCPUs | Memory | Use Case |
|-----------|-------|--------|----------|
| `Standard_DS3_v2` | 4 | 14 GB | General purpose |
| `Standard_DS4_v2` | 8 | 28 GB | General purpose, larger |
| `Standard_E4s_v3` | 4 | 32 GB | Memory-optimized |
| `Standard_NC6s_v3` | 6 | 112 GB | GPU workloads (1x V100 GPU) |

### GCP

| Node Type | vCPUs | Memory | Use Case |
|-----------|-------|--------|----------|
| `n1-standard-4` | 4 | 15 GB | General purpose |
| `n1-standard-8` | 8 | 30 GB | General purpose, larger |
| `n1-highmem-4` | 4 | 26 GB | Memory-optimized |
| `n1-highmem-8` | 8 | 52 GB | Memory-optimized, larger |

## Spark Versions

Common Spark versions (as of 2024):

- `13.3.x-scala2.12`: Spark 3.4.x, Scala 2.12
- `13.3.x-gpu-ml-scala2.12`: Spark 3.4.x with GPU support and ML libraries
- `12.2.x-scala2.12`: Spark 3.3.x, Scala 2.12 (LTS)
- `11.3.x-scala2.12`: Spark 3.3.x, Scala 2.12 (older LTS)

For the latest versions, check: [Databricks Runtime Release Notes](https://docs.databricks.com/release-notes/runtime/releases.html)

## Examples by Use Case

### Development/Testing
```yaml
cluster-mode: Serverless
# No cluster configuration needed
```

### Production ETL (Cost-Optimized)
```yaml
cluster-mode: New
new-cluster-spark-version: "13.3.x-scala2.12"
new-cluster-node-type: "i3.xlarge"
scaling-type: autoscale
min-workers: "2"
max-workers: "10"
cloud-provider: "AWS"
availability: "SPOT"
```

### Production ML Training (Performance-Optimized)
```yaml
cluster-mode: New
new-cluster-spark-version: "13.3.x-gpu-ml-scala2.12"
new-cluster-node-type: "g4dn.xlarge"
new-cluster-num-workers: "4"
cloud-provider: "AWS"
availability: "ON_DEMAND"
```

### Using Existing Interactive Cluster
```yaml
cluster-mode: Existing
existing-cluster-id: "1234-567890-abc123"
```

## Next Steps

- **[YAML Examples](yaml-examples.md)**: See parameters in action
- **[Hera Examples](hera-examples.md)**: Python SDK examples
- **[Cluster Modes](cluster-modes.md)**: Deep dive into cluster options
- **[Setup Guide](setup.md)**: Configure secrets and credentials

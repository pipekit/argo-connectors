# Understanding Parameters

Parameters are the primary way to configure and customize WorkflowTemplate and ClusterWorkflowTemplate behavior in Argo Workflows. This guide explains how parameters work with Argo Connectors.

## What are Parameters?

Parameters are key-value pairs that you pass to a workflow template to configure its behavior. Think of them as function arguments for your templates.

```yaml
# Template defines parameters it accepts
inputs:
  parameters:
    - name: code-path        # Parameter name
      description: "Path to notebook"  # Optional description
    - name: cluster-mode
      default: "Serverless"  # Optional default value
```

```yaml
# Workflow provides parameter values
arguments:
  parameters:
    - name: code-path
      value: "/Users/team/notebook"  # Actual value
    - name: cluster-mode
      value: "New"  # Override default
```

## Parameter Types

### Required Parameters

Parameters without default values must be provided:

```yaml
inputs:
  parameters:
    - name: namespace      # REQUIRED - no default
    - name: mainClass      # REQUIRED - no default
```

If you don't provide these, the workflow will fail.

### Optional Parameters

Parameters with default values are optional:

```yaml
inputs:
  parameters:
    - name: cluster-mode
      default: "Serverless"  # Will use this if not provided
    - name: executorInstances
      default: "1"           # Will use this if not provided
```

You can override defaults by providing a value:

```yaml
arguments:
  parameters:
    - name: cluster-mode
      value: "New"  # Overrides "Serverless" default
```

## Passing Parameters in YAML

### Basic Parameter Passing

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: example-
spec:
  entrypoint: main
  templates:
    - name: main
      steps:
        - - name: run-job
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/notebook"
                - name: cluster-mode
                  value: "Serverless"
```

### Using Workflow Parameters

Define parameters at the workflow level to reuse them:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: example-
spec:
  entrypoint: main
  
  # Define workflow-level parameters
  arguments:
    parameters:
      - name: environment
        value: "production"
      - name: date
        value: "2024-01-01"
  
  templates:
    - name: main
      steps:
        - - name: run-job
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/notebook"
                # Reference workflow parameters
                - name: run-name
                  value: "job-{{workflow.parameters.environment}}-{{workflow.parameters.date}}"
```

### Using Step Outputs as Parameters

Pass outputs from one step as inputs to another:

```yaml
steps:
  # Step 1: Run first job
  - - name: process-data
      templateRef:
        name: databricks-connector
        template: run-job
      arguments:
        parameters:
          - name: code-path
            value: "/Users/team/process"
          - name: cluster-mode
            value: "Serverless"
  
  # Step 2: Use outputs from step 1
  - - name: print-results
      template: print-info
      arguments:
        parameters:
          - name: run-id
            value: "{{steps.process-data.outputs.parameters.run-id}}"
          - name: run-url
            value: "{{steps.process-data.outputs.parameters.run-url}}"
```

## Passing Parameters with Hera

### Basic Parameter Passing

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

with Workflow(generate_name="example-", namespace="default") as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="run-job",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value="/Users/team/notebook"),
                Parameter(name="cluster-mode", value="Serverless"),
            ]
        )

w.create()
```

### Using Python Variables

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

# Define configuration
NOTEBOOK_PATH = "/Users/team/notebook"
CLUSTER_MODE = "New"
SPARK_VERSION = "13.3.x-scala2.12"

with Workflow(generate_name="example-", namespace="default") as w:
    with Steps(name="main"):
        WorkflowTemplateRef(
            name="run-job",
            template_ref="databricks-connector",
            template="run-job",
            arguments=[
                Parameter(name="code-path", value=NOTEBOOK_PATH),
                Parameter(name="cluster-mode", value=CLUSTER_MODE),
                Parameter(name="new-cluster-spark-version", value=SPARK_VERSION),
            ]
        )

w.create()
```

### Dynamic Parameter Generation

```python
from hera.workflows import Workflow, Parameter, WorkflowTemplateRef, Steps

def generate_cluster_params(env: str) -> list:
    """Generate cluster parameters based on environment"""
    if env == "dev":
        return [
            Parameter(name="cluster-mode", value="Serverless"),
        ]
    elif env == "prod":
        return [
            Parameter(name="cluster-mode", value="New"),
            Parameter(name="new-cluster-spark-version", value="13.3.x-scala2.12"),
            Parameter(name="new-cluster-node-type", value="i3.2xlarge"),
            Parameter(name="scaling-type", value="autoscale"),
            Parameter(name="min-workers", value="4"),
            Parameter(name="max-workers", value="16"),
        ]
    return []

environment = "prod"

with Workflow(generate_name=f"{environment}-job-", namespace="default") as w:
    with Steps(name="main"):
        base_params = [
            Parameter(name="code-path", value="/Users/team/notebook"),
        ]
        
        WorkflowTemplateRef(
            name="run-job",
            template_ref="databricks-connector",
            template="run-job",
            arguments=base_params + generate_cluster_params(environment)
        )

w.create()
```

## Parameter Value Formats

### String Values

Most parameters accept string values:

```yaml
- name: code-path
  value: "/Users/team/notebook"
```

### Numeric Values as Strings

Numeric parameters must be provided as strings:

```yaml
- name: new-cluster-num-workers
  value: "4"  # NOT: value: 4
  
- name: min-workers
  value: "2"  # NOT: value: 2
```

### Comma-Separated Lists

Some parameters accept comma-separated values:

```yaml
- name: args
  value: "arg1,arg2,arg3"

- name: email-notifications
  value: "team@company.com,admin@company.com"

- name: spark-conf
  value: "spark.executor.memory=4g,spark.sql.shuffle.partitions=200"
```

### Key-Value Pairs

Some parameters use key=value format:

```yaml
- name: notebook-params
  value: "date=2024-01-01,env=production"

- name: spark-conf
  value: "spark.executor.memory=4g,spark.driver.memory=2g"
```

## Common Parameter Patterns

### Environment-Specific Configuration

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: job-
spec:
  entrypoint: main
  arguments:
    parameters:
      - name: env
        value: "production"  # or "dev", "staging"
  
  templates:
    - name: main
      steps:
        - - name: run-job
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "/Users/team/notebook-{{workflow.parameters.env}}"
                - name: run-name
                  value: "job-{{workflow.parameters.env}}-{{workflow.name}}"
                # Conditional cluster config based on env
                - name: cluster-mode
                  value: "{{= workflow.parameters.env == 'production' ? 'New' : 'Serverless' }}"
```

### Date-Based Processing

```yaml
arguments:
  parameters:
    - name: process-date
      value: "2024-01-01"
    - name: code-path
      value: "/Users/team/daily-job"
    - name: args
      value: "{{workflow.parameters.process-date}}"
    - name: run-name
      value: "daily-job-{{workflow.parameters.process-date}}"
```

### Reusable Cluster Configuration

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: pipeline-
spec:
  entrypoint: main
  arguments:
    parameters:
      # Cluster config as workflow parameters
      - name: spark-version
        value: "13.3.x-scala2.12"
      - name: node-type
        value: "i3.xlarge"
      - name: min-workers
        value: "2"
      - name: max-workers
        value: "10"
  
  templates:
    - name: main
      steps:
        - - name: job1
            template: databricks-job
            arguments:
              parameters:
                - name: notebook-path
                  value: "/Users/team/job1"
        
        - - name: job2
            template: databricks-job
            arguments:
              parameters:
                - name: notebook-path
                  value: "/Users/team/job2"
    
    - name: databricks-job
      inputs:
        parameters:
          - name: notebook-path
      steps:
        - - name: run
            templateRef:
              name: databricks-connector
              template: run-job
            arguments:
              parameters:
                - name: code-path
                  value: "{{inputs.parameters.notebook-path}}"
                - name: cluster-mode
                  value: "New"
                # Reuse workflow-level cluster config
                - name: new-cluster-spark-version
                  value: "{{workflow.parameters.spark-version}}"
                - name: new-cluster-node-type
                  value: "{{workflow.parameters.node-type}}"
                - name: scaling-type
                  value: "autoscale"
                - name: min-workers
                  value: "{{workflow.parameters.min-workers}}"
                - name: max-workers
                  value: "{{workflow.parameters.max-workers}}"
```

## Parameter Validation

Argo Workflows validates parameters at submission time:

### Required Parameters
If you omit a required parameter, you'll get an error:
```
Error: parameter 'code-path' is required but not provided
```

### Type Validation
While all parameters are strings, some templates validate format:
```yaml
# This might fail validation
- name: cluster-mode
  value: "Invalid"  # Must be "New", "Existing", or "Serverless"
```

## Best Practices

### 1. Use Descriptive Names
```yaml
# Good
- name: new-cluster-num-workers
  value: "4"

# Bad
- name: workers
  value: "4"
```

### 2. Provide Defaults for Optional Parameters
```yaml
inputs:
  parameters:
    - name: cluster-mode
      default: "Serverless"  # Sensible default
    - name: email-notifications
      default: ""  # Empty default for optional notifications
```

### 3. Document Parameters
Use descriptions in your custom templates:
```yaml
inputs:
  parameters:
    - name: code-path
      description: "Path to Databricks notebook or script"
    - name: cluster-mode
      description: "Cluster mode: New, Existing, or Serverless"
      default: "Serverless"
```

### 4. Use Workflow Parameters for Reusability
```yaml
# Define once at workflow level
arguments:
  parameters:
    - name: environment
      value: "production"

# Reuse multiple times
- name: run-name
  value: "job-{{workflow.parameters.environment}}"
- name: email-notifications
  value: "{{workflow.parameters.environment}}-team@company.com"
```

## Next Steps

- **[Invoking Templates](invoking-templates.md)**: Learn how to call WorkflowTemplates
- **[Working with Outputs](outputs.md)**: Use template outputs in your workflows
- **[Parameter References](../connectors/databricks/parameter-reference.md)**: Complete Databricks parameter list
- **[YAML Examples](../connectors/databricks/yaml-examples.md)**: See parameters in action

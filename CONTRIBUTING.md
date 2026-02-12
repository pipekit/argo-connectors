# Contributing to Argo Connectors

Thank you for your interest in contributing to Argo Connectors! This project is community-driven, and we welcome contributions from everyone.

## Ways to Contribute

### Report Bugs
Found a bug? [Open an issue](https://github.com/pipekit/argo-connectors/issues/new) with:
- Clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Your environment (Argo version, Kubernetes version, etc.)

### Suggest New Connectors
Want a connector for a specific platform? [Open an issue](https://github.com/pipekit/argo-connectors/issues/new) describing:
- The platform/tool you want to integrate
- Your use case
- Why it would benefit the community

### Contribute a Connector
Ready to build a connector? Follow these steps:

## Building a Connector

### 1. Check Existing Connectors
Browse the [existing connectors](docs/connectors/README.md) to see if something similar exists.

### 2. Design Your Connector
A good connector includes:
- **WorkflowTemplate or ClusterWorkflowTemplate** - The main integration
- **Clear parameters** - Well-documented inputs with sensible defaults
- **Error handling** - Proper success and failure conditions
- **Examples** - Both YAML and Hera SDK examples
- **Documentation** - Complete guide with parameter reference

### 3. Follow the Pattern
Use the [Databricks Connector](databricks/workflow-template.yaml) as your reference template.

Key elements:
```yaml
apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate  # or ClusterWorkflowTemplate
metadata:
  name: your-connector-name
  annotations:
    description: "Brief description of what this connector does"
spec:
  entrypoint: main-template
  templates:
    - name: main-template
      inputs:
        parameters:
          # Define your parameters
          - name: required-param
          - name: optional-param
            default: "default-value"
          
          # IMPORTANT: Docker images MUST be parameters with defaults
          - name: connector-image
            default: "ghcr.io/your-org/your-connector:version"
            description: "Container image for the connector"
      
      outputs:
        parameters:
          # Define outputs for chaining
          - name: result
            valueFrom:
              path: /tmp/result
      
      container:
        # Use the parameter, never hard-code the image
        image: "{{inputs.parameters.connector-image}}"
        # Your connector implementation
```

**Critical Rule**: Docker images must ALWAYS be parameterized with a default value. Never hard-code image names in the template. This allows users to:
- Use their own registry mirrors
- Pin to specific versions
- Test with custom builds

### 4. Create Documentation
Each connector needs:

**README.md** - Overview and quick start
```markdown
# Your Connector Name

Brief description of what your connector does.

## Features
- Feature 1
- Feature 2

## Quick Example
[YAML example here]

## Installation
[Installation instructions]
```

**yaml-examples.md** - Complete YAML examples

**hera-examples.md** - Python SDK examples

**parameter-reference.md** - Complete parameter documentation

See [Databricks docs](docs/connectors/databricks/) as a template.

### 5. Test Your Connector
Before submitting:
- Test with different parameter combinations
- Verify error handling
- Confirm outputs work correctly
- Test in both namespace and cluster scope (if applicable)

### 6. Submit a Pull Request
1. Fork the repository
2. Create a branch: `git checkout -b connector/your-connector-name`
3. Add your connector files:
   ```
   your-platform/
   ├── workflow-template.yaml
   ├── cluster-template.yaml (optional)
   ├── README.md
   └── examples/
       └── basic-example.yaml
   ```
4. Add documentation:
   ```
   docs/connectors/your-platform/
   ├── README.md
   ├── yaml-examples.md
   ├── hera-examples.md
   └── parameter-reference.md
   ```
5. Update `docs/connectors/README.md` to list your connector
6. Commit: `git commit -m "Add [Platform] connector"`
7. Push: `git push origin connector/your-connector-name`
8. Open a Pull Request with:
   - Description of the connector
   - Use cases it enables
   - Testing you've done

## Improving Existing Connectors

### Bug Fixes
1. [Open an issue](https://github.com/pipekit/argo-connectors/issues/new) describing the bug
2. Reference the issue in your PR
3. Include tests if possible

### New Features
1. [Open an issue](https://github.com/pipekit/argo-connectors/issues/new) to discuss the feature
2. Get feedback from maintainers
3. Implement and submit a PR

### Documentation Improvements
Documentation PRs are always welcome! Fix typos, clarify examples, or add missing information.

## Connector Standards

### Parameter Naming
Use clear, descriptive names:
- `cluster-mode` not `mode`
- `new-cluster-spark-version` not `spark-ver`

### Defaults
Provide sensible defaults for all optional parameters:
```yaml
- name: timeout
  default: "3600"  # 1 hour

- name: connector-image
  default: "ghcr.io/your-org/connector:v1.0.0"  # REQUIRED for all connectors
```

### Docker Images Must Be Parameters
**This is a strict requirement**: All Docker images must be defined as parameters with default values.

**Correct**:
```yaml
inputs:
  parameters:
    - name: connector-image
      default: "ghcr.io/pipekit/databricks-connector:0.0.2"
      description: "Container image for the Databricks connector"

container:
  image: "{{inputs.parameters.connector-image}}"
```

**Incorrect (DO NOT DO THIS)**:
```yaml
container:
  image: "ghcr.io/pipekit/databricks-connector:0.0.2"  # Hard-coded - NOT ALLOWED
```

See the [Databricks connector](databricks/workflow-template.yaml) for the correct implementation pattern.

### Documentation
Every parameter needs:
- Description
- Type (string, number, etc.)
- Default value (if applicable)
- Example

### Examples
Provide both:
- Basic example (minimal parameters)
- Advanced example (all features)
- Real-world use case

## Code of Conduct

### Be Respectful
Treat everyone with respect. We're all here to build great tools together.

### Be Constructive
Provide helpful feedback. If you disagree, explain why and suggest alternatives.

### Be Patient
Maintainers are volunteers. PRs may take time to review.

## Questions?

- **General questions**: [Open an issue](https://github.com/pipekit/argo-connectors/issues)
- **Bug reports**: [Open an issue](https://github.com/pipekit/argo-connectors/issues)
- **Feature requests**: [Open an issue](https://github.com/pipekit/argo-connectors/issues)

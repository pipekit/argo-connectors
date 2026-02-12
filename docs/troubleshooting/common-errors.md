# Common Errors and Solutions

This guide covers common errors you may encounter when using Argo Connectors and how to resolve them.

## Databricks Connector Errors

### Error: "databricks-secret not found"

**Full Error:**
```
Error: secrets "databricks-secret" not found
```

**Cause:** The Kubernetes secret containing Databricks credentials doesn't exist or is in the wrong namespace.

**Solution:**

1. Create the secret:
```bash
kubectl create secret generic databricks-secret \
  --from-literal=host='https://your-workspace.cloud.databricks.com' \
  --from-literal=token='your-databricks-token'
```

2. Verify the secret exists:
```bash
kubectl get secret databricks-secret -o yaml
```

3. If using a different namespace, create it there:
```bash
kubectl create secret generic databricks-secret \
  --from-literal=host='https://your-workspace.cloud.databricks.com' \
  --from-literal=token='your-databricks-token' \
  --namespace your-namespace
```

### Error: "Cluster does not have Databricks Runtime installed"

**Cause:** You specified `cluster-mode: Existing` with an invalid cluster ID.

**Solution:**

1. Verify the cluster ID in Databricks UI
2. Ensure the cluster is running
3. Use the correct cluster ID format:
```yaml
- name: existing-cluster-id
  value: "1234-567890-abc123"  # Correct format
```

### Error: "RESOURCE_DOES_NOT_EXIST: Notebook not found"

**Cause:** The notebook path is incorrect or the notebook doesn't exist.

**Solution:**

1. Verify the notebook path in Databricks
2. Use the full path including `/Users/` or `/Repos/`:
```yaml
- name: code-path
  value: "/Users/your-email@company.com/notebook-name"
```

3. Ensure you have permission to access the notebook

### Error: "Cloud is not configured"

**Full Error:**
```
Error: Cloud is not configured for account
```

**Cause:** Using serverless compute without proper account configuration.

**Solution:**

1. Use a new cluster instead:
```yaml
- name: cluster-mode
  value: "New"
- name: new-cluster-spark-version
  value: "13.3.x-scala2.12"
- name: new-cluster-node-type
  value: "i3.xlarge"
```

2. Or contact your Databricks admin to enable serverless

### Error: "Invalid parameter value"

**Cause:** Parameter format is incorrect.

**Solution:**

Ensure numeric values are strings:
```yaml
# Correct
- name: new-cluster-num-workers
  value: "4"

# Wrong
- name: new-cluster-num-workers
  value: 4
```

## Spark Connector Errors

### Error: "SparkApplication CRD not found"

**Full Error:**
```
Error: the server doesn't have a resource type "sparkapplications"
```

**Cause:** Spark Operator is not installed.

**Solution:**

Install Spark Operator:
```bash
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true
```

### Error: "Forbidden: User cannot create resource"

**Full Error:**
```
Error: sparkapplications.sparkoperator.k8s.io is forbidden: User "system:serviceaccount:argo:default" cannot create resource
```

**Cause:** Argo service account lacks permissions to create SparkApplications.

**Solution:**

Create RBAC rules:
```bash
kubectl apply -f - <<EOF
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
    verbs:
      - '*'
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: argo-spark-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: argo-spark-clusterrole
subjects:
- kind: ServiceAccount
  name: default
  namespace: argo
EOF
```

### Error: "ImagePullBackOff"

**Cause:** Cannot pull the Spark image.

**Solution:**

1. Verify image name and tag:
```yaml
- name: globalImage
  value: "gcr.io/spark-operator/spark:v3.1.1"  # Check this exists
```

2. For private registries, add image pull secret:
```yaml
- name: imagePullSecret
  value: "my-registry-secret"
```

3. Create the secret if needed:
```bash
kubectl create secret docker-registry my-registry-secret \
  --docker-server=gcr.io \
  --docker-username=_json_key \
  --docker-password="$(cat key.json)"
```

### Error: "Application state: FAILED"

**Cause:** Spark application failed during execution.

**Solution:**

1. Check Spark driver logs:
```bash
kubectl logs <spark-job-name>-driver
```

2. Common issues:
   - **ClassNotFoundException**: Wrong main class
   - **FileNotFoundException**: Wrong mainApplicationFile path
   - **OutOfMemoryError**: Increase driver/executor memory

## Workflow Template Errors

### Error: "WorkflowTemplate not found"

**Full Error:**
```
Error: workflowtemplates.argoproj.io "databricks-connector" not found
```

**Cause:** Template not installed or in wrong namespace.

**Solution:**

1. Install the template:
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/databricks/workflow-template.yaml
```

2. For namespace-scoped templates, install in the same namespace as your workflow

3. Or use ClusterWorkflowTemplate for cluster-wide access:
```bash
kubectl apply -f https://raw.githubusercontent.com/pipekit/argo-connectors/main/databricks/cluster-template.yaml
```

### Error: "Required parameter not provided"

**Full Error:**
```
Error: parameter 'code-path' is required but not provided
```

**Cause:** Missing required parameter.

**Solution:**

Add the required parameter:
```yaml
arguments:
  parameters:
    - name: code-path
      value: "/Users/team/notebook"
```

## Hera SDK Errors

### Error: "ModuleNotFoundError: No module named 'hera'"

**Cause:** Hera not installed.

**Solution:**
```bash
pip install hera
```

### Error: "Connection refused"

**Cause:** Cannot connect to Argo server.

**Solution:**

1. Configure Hera:
```python
from hera.workflows import GlobalConfig

GlobalConfig.host = "https://argo-server.example.com"
GlobalConfig.token = "your-token"
```

2. Or use kubeconfig:
```python
GlobalConfig.host = "http://localhost:2746"
# Ensure port-forward is running:
# kubectl -n argo port-forward deployment/argo-server 2746:2746
```

### Error: "Parameter type error"

**Cause:** Passing non-string values to Parameter.

**Solution:**

Convert all values to strings:
```python
# Wrong
Parameter(name="num-workers", value=4)

# Correct
Parameter(name="num-workers", value="4")

# Or use str()
num_workers = 4
Parameter(name="num-workers", value=str(num_workers))
```

## Timeout Errors

### Error: "Workflow deadline exceeded"

**Cause:** Workflow took longer than activeDeadlineSeconds.

**Solution:**

Increase timeout:
```yaml
spec:
  activeDeadlineSeconds: 7200  # 2 hours
```

### Error: "Step timeout"

**Cause:** Individual step exceeded timeout.

**Solution:**

1. For Databricks, jobs automatically poll until complete
2. Check Databricks job logs for actual failure
3. For Spark, increase Spark-specific timeouts

## Network Errors

### Error: "Connection timeout to Databricks"

**Cause:** Cannot reach Databricks API.

**Solution:**

1. Verify network connectivity from cluster
2. Check firewall rules
3. Verify Databricks workspace URL:
```bash
kubectl get secret databricks-secret -o jsonpath='{.data.host}' | base64 -d
```

## Debug Tips

### Enable Verbose Logging

For Databricks connector:
```yaml
# Add to connector-image
- name: connector-image
  value: "ghcr.io/pipekit/databricks-connector:latest-debug"
```

### Check Pod Logs

```bash
# Get pod name
kubectl get pods | grep <workflow-name>

# View logs
kubectl logs <pod-name>

# Follow logs in real-time
kubectl logs -f <pod-name>
```

### Inspect Workflow Status

```bash
# Get workflow details
kubectl get workflow <workflow-name> -o yaml

# Check node status
kubectl get workflow <workflow-name> -o jsonpath='{.status.nodes}'
```

### Describe Resources

```bash
# Workflow
kubectl describe workflow <workflow-name>

# WorkflowTemplate
kubectl describe workflowtemplate databricks-connector

# SparkApplication (for Spark connector)
kubectl describe sparkapplication <spark-job-name>
```

## Getting Help

If you're still stuck:

1. **Check Logs**: Always start with pod and workflow logs
2. **GitHub Issues**: Search [existing issues](https://github.com/pipekit/argo-connectors/issues)
3. **Community**: Ask in [Discussions](https://github.com/pipekit/argo-connectors/discussions)
4. **Argo Slack**: Join [Argo Workflows Slack](https://argoproj.github.io/community/join-slack)

## Next Steps

- **[Debugging Guide](debugging.md)**: Advanced debugging techniques
- **[FAQ](faq.md)**: Frequently asked questions
- **[Troubleshooting Overview](README.md)**: Back to troubleshooting main page

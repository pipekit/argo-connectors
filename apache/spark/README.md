# Apache Spark data connector for Argo Workflows

Apache Spark data connector allow users to submit Spark jobs from Argo Workflows. This data connector is implemented using Argo `WorkflowTemplate` and  `ClusterWorkflowTemplate` feature. Implementing it this way allows a user to simply reference these templates inside larger workflow like any other Argo Workflow step/task. Internally, it is responsible for submitting and waiting for the execution to finish, which allows Argo to be aware of the execution result. Spark data connector relies on `Spark Operator` for Spark job scheduling, so it can only run Spark jobs in Kubernetes.

## Table of Contents
- [Apache Spark data connector for Argo Workflows](#apache-spark-data-connector-for-argo-workflows)
    * [Table of Contents](#table-of-contents)
    * [Requirements](#requirements)
    * [How to use Apache Spark data connector](#how-to-use-apache-spark-data-connector)
    * [Configuration](#configuration)
        + [Java/Scala Configuration](#java-scala-configuration)
        + [Python Configuration](#python-configuration)
        + [General Configuration](#general-configuration)
        + [Spark UI](#spark-ui)


## Requirements
1. Spark on k8s operator ([link](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)) - admission webhook must be enabled
2. Argo service account must be able to manipulate with SparkApplication CRD. If Argo and Spark jobs are running in different namespaces you must create `ClusterRole` and `ClusterRoleBinding` otherwise `Role` and `RoleBinding` is enough. Example:

``` yaml
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
```


This example creates cluster role binding for `default` service account in `argo` namespace. This service account is later used for Argo Workflow and Spark job scheduling:
```
kubectl create clusterrolebinding argo-spark-cluster-role-binding \
--clusterrole=argo-spark-clusterrole \
--serviceaccount=argo:default
```


## How to use Apache Spark data connector

You first need to install all prerequisites (like Spark operator and Argo - Spark service account role/role binding). After that, you need to add `WorkflowTemplate` /`ClusterWorkflowTemplate` to Argo Workflows. Now, templates are in Argo and you can use them inside your workflows. Check `example` folder for examples (note: you may need to change namespace and service account).


## Configuration

There are four templates, two for JVM languages (Java/Scala) and two for Python. Templates for Java/Scala have some JVM specific configuration and Python templates have some configuration specific for Python.

### Java/Scala Configuration

| Config Name  |  Mandatory |  Default Value |  Description |
|---|---|---|---|
| type  | Yes  |   | type tells the type of the Spark application. Can be Java or Scala  |
| mainApplicationFile  |  Yes |   | mainApplicationFile is the path to a bundled JAR with Spark code. Example: `local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar`   |
| mainClass  |  Yes |   |  mainClass is full classpath name of main execution file inside JAR. Example: `org.apache.spark.examples.SparkPi` |
|			driverJvmOptions			| No| empty | driverJvmOptions is a string of extra JVM options to pass to the driver. For instance, GC settings or other logging.|
| jars | No | empty | jars is a list of JAR files the Spark application depends on. It must be specified as a single string where each entry is separated by comma. Jar can be inside container or can be downloaded from HTTP server, HDFS, AWS S3 or Google Cloud Storage. Example: `"local:///opt/spark-jars/one-jar.jar,gs://spark-data/other.jar" ` |
|jarsDownloadDir|No|empty|jarsDownloadDir is used for specifying the location in the driver and executor pods where jars should be downloaded to|
|repositories|No|empty|repositories is a list of additional remote repositories to search for the maven coordinate given with the “packages” option. It must be specified as a single string where each entry is separated by comma. Example: `https://repository.example.com/prod, https://repository.example2.com/prod`|
|packages|No|empty|packages is a list of maven coordinates of jars to include on the driver and executor classpath. This will search the local maven repo, then maven central and any additional remote repositories given by the `repositories` option. It must be specified as a single string where each entry is separated by comma. Each package should be in the form `groupId:artifactId:version`. Example: `groupId:artifactId1:version,groupId:artifactId2:version`|
|excludePackages|No|empty|excludePackages is a list of `groupId:artifactId`, to exclude while resolving the dependencies provided in Packages to avoid dependency conflicts. It must be specified as a single string where each entry is separated by comma. Example: `groupId1:artifactId1,groupId2:artifactId2`|



### Python Configuration
| Config Name  |  Mandatory |  Default Value |  Description |
|---|---|---|---|
| mainApplicationFile  | Yes  |  | mainApplicationFile is the path to a Python application inside container. Example: `local:///opt/spark/examples/src/main/python/pyfiles.py`  |
|pythonVersion| Yes | 3 | pythonVersion sets the major Python version of the docker image used to run the driver and executor containers. Can either be 2 or 3|
|pyFiles|No|empty|pyFiles is a list of Python files the Spark application depends on. It must be specified as a single string where each entry is separated by comma. Example: `local:///opt/spark/examples/src/main/python/py_container_checks.py,gs://spark-data/python-dep.zip`|



### General Configuration
| Config Name  |  Mandatory |  Default Value |  Description |
|---|---|---|---|
|sparkConnectorName|No|spark-job|sparkConnectorName specifies name of Spark connector name inside Kubernetes|
|namespace|Yes||namespace defines Kubernetes namespace where Spark job should run.|
|imagePullSecret|No|empty|imagePullSecret defines secret used by Kubernetes to fetch Spark container images. It must be specified as a single string where each entry is separated by comma. Example: `secret1,secret2`|
|globalImage|No|empty|globalImage defines container image used by both driver and executor. You must either specify this property or executorImage and driverImage property. In case both globalImage and executorImage/driverImage is specified, executorImage/driverImage is used.|
|driverImage|No|empty|driverImage defines container image used by Spark driver. In case same image is used by driver and executor, use globalImage property instead.|
|executorImage|No|empty|executorImage defines container image used by Spark executor. In case same image is used by driver and executor, use globalImage property instead.|
|imagePullPolicy|No|IfNotPresent|imagePullPolicy is the image pull policy for the driver and executor|
|globalServiceAccount|No|default|globalServiceAccount is the name of the custom Kubernetes service account used by the pod. You must either specify this property or driverServiceAccount and executorServiceAccount property. In case both globalServiceAccount and driverServiceAccount/executorServiceAccount is specified, driverServiceAccount/executorServiceAccount is used.|
|driverServiceAccount|No|default|driverServiceAccount defines service account used by Spark driver. In case same service account is used by driver and executor, use globalServiceAccount property instead.|
|executorServiceAccount|No|default|executorServiceAccount defines service account used by Spark executor. In case same service account is used by driver and executor, use globalServiceAccount property instead.|
|driverCores|No|1|driverCores is the physical CPU core request for the driver|
|executorCores|No|1|executorCores is the physical CPU core request for the executor|
|driverMemory|No|1024m|driverMemory is the amount of memory to request for the driver pod|
|executorMemory|No|1024m|executorMemory is the amount of memory to request for the executor pod|
|executorInstances|No|1|executorInstances is number of executor instances|
|driverEnvConfigMap|No|driver-env-configmap|driverEnvConfigMap is configmap used for populating environment variables in driver pod. Default value is only a placeholder and is not used|
|executorEnvConfigMap|No|executor-env-configmap|executorEnvConfigMap is configmap used for populating environment variables in executor pod. Default value is only a placeholder and is not used.|
|files|No|empty|files is a list of files the Spark application depends on. File can be inside container or can be downloaded from HTTP server, HDFS, AWS S3 or Google Cloud Storage. It must be specified as a single string where each file is separated by comma. Example: `gs://spark-data/data-file-1.txt,local:///opt/spark-jars/gcs-connector.jar`|
|filesDownloadDir|No|empty|filesDownloadDir is used for specifying the location in the driver and executor pods where files should be downloaded to.|


### Spark UI
Spark UI is available during Spark execution and is exposed using k8s service (\<workflow-name>-ui-svc) on port 4040. You can use `kubectl port-forward` function, or you can expose it using Ingress/NodePort.
Example:
```
kubectl port-forward svc/<workflow-name>-ui-svc 4040:4040
```

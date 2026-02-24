# Introduction 
TODO: Give a short introduction of your project. Let this section explain the objectives or the motivation behind this project. 

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references


# Install Confluent Manager for Apache Flink (CMF): 

Reference: https://docs.confluent.io/platform/current/flink/installation/helm.html

```bash
kubectl create namespace confluent
kubectl config set-context --current --namespace=confluent
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
helm upgrade --install cp-flink-kubernetes-operator confluentinc/flink-kubernetes-operator
helm upgrade --install cmf confluentinc/confluent-manager-for-apache-flink --namespace confluent --set cmf.sql.production=false
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes  --set enableCMFDay2Ops=true
```
# Infra Prerequisites

Navigate to the resources/k8s/prerequisites directory and follow the instructions in the `README.md` file to set up the necessary infrastructure components.

1. Apply the cmf-rest class using the following command:

```bash
kubectl apply -f cmf-rest-class.yaml
```
2. Apply the flink environment using the following command:

```bash
kubectl apply -f flink-env.yaml
```
3. Create a pvc using the following command:
```bash
kubectl apply -f flink-pvc.yaml
```
4. Navigate to resources/kafka directory. Setup Kraft, Kafka, Schema Registry, and Control Center using the provided command.

```bash
kubectl apply -f confluent-platform.yaml
```
5. Port-forward the Control Center service to access it from your local machine:

```bash
kubectl port-forward pod/controlcenter-0 9021
```
6. Create the following topics that will be used in the project:

    a. users
    b. pageviews
    c. filtered-pageviews
    d. enriched-pageviews
    e. aggregated-users

7. Create users and pageviews datagen connectors from the connect section of the Control Center UI. Use the files provided in resources/kafka/connectors directory.

8. Port-forward Kafka pod for local access:

```bash
kubectl port-forward pod/kafka-0 9092
```

# Quick Start (Automated Setup)

For automated cluster setup, see [QUICK_START.md](QUICK_START.md) or run:

```bash
./setup-cluster.sh
```

This script automates the deployment of:
- Flink prerequisites (CMF REST class, Flink environment, PVC)
- Both Flink applications (streaming and batch)
- Prometheus for metrics collection
- Grafana with pre-configured Flink dashboards

# Build and Run the Project
To build and run the project, follow these steps:
1. Install Java 17, Maven if you haven't already.
2. Run the following command to build the project:

```bash
mvn clean package
```
3. Then build the Docker image as described below.


# Build Docker Image

To build the Docker image using the provided `Dockerfile`, run the following command from the project root (after building your JAR with Maven):

```bash
docker build -t flink-training:1.0.0 .
```

# Monitoring

The following components are part of the Prometheus installation:
- **Prometheus Server:** The core component that scrapes and stores metrics data.
- **Alertmanager:** Manages and sends alerts based on rules defined in Prometheus.
- **PushGateway:** A metric cache for short-lived jobs that cannot be scraped directly.

### 1. Install Prometheus
Use Helm to install the Prometheus stack in the `monitoring` namespace.
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install prometheus prometheus-community/prometheus --namespace monitoring --create-namespace
```

### 2. Access Prometheus UI
The Prometheus Server is the core of the monitoring system, responsible for collecting and storing metrics. Forward the Prometheus server port to your local machine to access the UI at `http://localhost:9090`.
```bash
export POD_NAME=$(kubectl get pods --namespace monitoring -l "app.kubernetes.io/name=prometheus,app.kubernetes.io/instance=prometheus" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace monitoring port-forward $POD_NAME 9090
```

### 3. Access Alertmanager UI
The Alertmanager handles alerts sent by client applications such as the Prometheus server. Forward the Alertmanager port to your local machine to access the UI at `http://localhost:9093`.
```bash
export POD_NAME=$(kubectl get pods --namespace monitoring -l "app.kubernetes.io/name=alertmanager,app.kubernetes.io/instance=prometheus" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace monitoring port-forward $POD_NAME 9093
```

### 4. Access PushGateway UI
The PushGateway allows ephemeral and batch jobs to expose their metrics to Prometheus. Forward the PushGateway port to your local machine to access the UI at `http://localhost:9091`.
```bash
export POD_NAME=$(kubectl get pods --namespace monitoring -l "app=prometheus-pushgateway,component=pushgateway" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace monitoring port-forward $POD_NAME 9091
```

### 5. Enabling Prometheus Metrics on Flink Kubernetes Operator

To enable the Prometheus metrics reporter for the Flink operator, create a file named `prometheus-values.yaml` with the following content:

```yaml
defaultConfiguration:
  create: true
  append: true
  flink-conf.yaml: |+
    kubernetes.operator.metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
    kubernetes.operator.metrics.reporter.prom.port: 9999
metrics:
  port: 9999
operatorPod:
  annotations:
    metrics.dynatrace.com/port: "9999"
    metrics.dynatrace.com/scrape: "true"
    prometheus.io/port: "9999"
    prometheus.io/scrape: "true"
```

Then, apply the changes by running the following Helm command:

```bash
helm upgrade --install cp-flink-kubernetes-operator confluentinc/flink-kubernetes-operator --namespace confluent --values prometheus-values.yaml
```

### 6. Key Flink Metrics for Monitoring and Alerting

Here are some important Flink metrics that you can monitor in Prometheus to ensure your jobs are running smoothly. You can also use these metrics to configure alerts in Alertmanager.

| Metric                                                 | Description                                                      | Example Alert Condition                                                               |
| ------------------------------------------------------ | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| **Job Health & Status**                                |                                                                  |                                                                                       |
| `flink_jobmanager_job_uptime`                          | The time that the job has been running without interruption.     |                                                                                       |
| `flink_jobmanager_job_numberOfCompletedCheckpoints`    | The number of successfully completed checkpoints.                |                                                                                       |
| `flink_jobmanager_job_numberOfFailedCheckpoints`       | The number of failed checkpoints.                                | `increase(flink_jobmanager_job_numberOfFailedCheckpoints[5m]) > 0`                    |
| `flink_jobmanager_job_runningState`                    | Boolean (1/0) expressing if a job is currently running.          | `flink_jobmanager_job_runningState == 0` for more than 2 minutes                      |
| `flink_jobmanager_job_restartingState`                 | Boolean (1/0) expressing if a job is currently restarting.       | `flink_jobmanager_job_restartingState == 1` for more than 5 minutes                   |
| `flink_jobmanager_job_failingState`                    | Boolean (1/0) expressing if a job is currently failing.          | `flink_jobmanager_job_failingState == 1` for more than 30 seconds                     |
| `flink_jobmanager_job_deployingState`                  | 1 if job is deploying tasks, 0 otherwise.                        | `flink_jobmanager_job_deployingState == 1` for more than 5 minutes                    |
| **Restarts & Failures**                                |                                                                  |                                                                                       |
| `flink_taskmanager_job_task_numRestarts`               | Total restarts including scaling and failures.                   | `increase(flink_taskmanager_job_task_numRestarts[5m]) > 3`                            |
| **Checkpointing**                                      |                                                                  |                                                                                       |
| `flink_jobmanager_job_lastCheckpointDuration`          | Time (in ms) the last checkpoint took to complete.               |                                                                                       |
| `flink_jobmanager_job_lastCheckpointSize`              | Size (in bytes) of the last checkpoint (operator state only).    |                                                                                       |
| `flink_jobmanager_job_lastCheckpointFullSize`          | Size (in bytes) of the last full checkpoint (including metadata).|                                                                                       |
| `flink_taskmanager_job_task_checkpointAlignmentTime`   | Time taken for stream alignment during checkpointing.            |                                                                                       |
| **JVM & System**                                       |                                                                  |                                                                                       |
| `flink_jobmanager_Status_JVM_CPU_Load`                 | JVM CPU usage on the JobManager (0.0–1.0).                       | `flink_jobmanager_Status_JVM_CPU_Load > 0.7` for a sustained period                   |
| `flink_taskmanager_Status_JVM_CPU_Load`                | JVM CPU usage on the TaskManager (0.0–1.0).                      | `flink_taskmanager_Status_JVM_CPU_Load > 0.7` for a sustained period                  |
| `flink_jobmanager_Status_JVM_Memory_Heap_Used`         | Heap memory currently used by the JobManager.                    | `flink_jobmanager_Status_JVM_Memory_Heap_Used / flink_jobmanager_Status_JVM_Memory_Heap_Max > 0.7` |
| `flink_taskmanager_Status_JVM_Memory_Heap_Used`        | Heap memory currently used by the TaskManager.                   | `flink_taskmanager_Status_JVM_Memory_Heap_Used / flink_taskmanager_Status_JVM_Memory_Heap_Max > 0.7` |
| **Kubernetes Operator**                                |                                                                  |                                                                                       |
| `flink_k8soperator_namespace_JmDeploymentStatus_ERROR_Count` | Number of jobmanager deployments running into errors.            | `flink_k8soperator_namespace_JmDeploymentStatus_ERROR_Count > 1`                      |



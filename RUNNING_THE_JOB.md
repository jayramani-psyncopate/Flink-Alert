# Running the Flink Alert Trigger Job 

This document provides step-by-step instructions for building, deploying, and running the Alert Trigger Job.

## Prerequisites

1. **Kubernetes cluster** with Flink operator, CMF, CFK installed
2. **Docker** installed and configured
3. **Maven** 3.6+ installed
4. **kubectl** configured to access your cluster


## Install CMF, FKO, CFK (If needed)

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

## Complete Flow

### Optional: Skip Step 1 and Step 2 (Use Prebuilt Docker Image)

If you do not want to build the JAR and Docker image locally, pull the prebuilt image:

```bash
docker pull jrramani12/flink-alert:1.0.0
```

If you use this image, you can skip:
- **Step 1: Build the Project**
- **Step 2: Build Docker Image**

### Step 1: Build the Project

```bash
# Navigate to project root
cd Flink Demo

# Clean and build the project
mvn clean package

# Verify the jar was created
ls -lh target/flinkalert-1.0-SNAPSHOT.jar
```

**Expected Output:**
- JAR file created at `target/flinkalert-1.0-SNAPSHOT.jar`
- Main class in manifest: `org.example.jobs.AlertTriggerJob`

### Step 2: Build Docker Image

```bash
# Build the Docker image
docker build -t flink-alert:1.0.0 .

# Verify the image was created
docker images | grep flink-alert

# Optional: If using a private registry, tag and push
# docker tag flink-alert:1.0.0 <registry>/flink-alert:1.0.0
# docker push <registry>/flink-alert:1.0.0
```

**Important:** 
- Image tag `flink-alert:1.0.0` must match the `image:` field in `flink-app.yaml`
- If using a private registry, update the image name in `flink-app.yaml` accordingly

### Step 3: Load Image to Cluster (if needed)

If your Kubernetes cluster can't pull from your local Docker registry:

**Option A: Use Kind/Minikube (local cluster)**
```bash
# For Kind
kind load docker-image flink-alert:1.0.0

# For Minikube
minikube image load flink-alert:1.0.0
```

**Option B: Push to accessible registry**
```bash
# Tag for your registry
docker tag flink-alert:1.0.0 <your-registry>/flink-alert:1.0.0

# Push
docker push <your-registry>/flink-alert:1.0.0

# Update flink-app.yaml image field to: <your-registry>/flink-alert:1.0.0
```

### Step 4: Configure Alert Type

Edit `src/main/resources/FlinkCluster/flink-app.yaml`:

```yaml
job:
  jarURI: local:///opt/flink/lib/flinkalert-1.0-SNAPSHOT.jar
  state: running
  parallelism: 1
  upgradeMode: stateless
  args: ["normal"]  # <-- Use "normal" for no intentional alert trigger
```

**Available alert types:**
- `job-failing` - Simulates job failures
- `job-restarting` - Causes repeated restarts
- `checkpoint-failure` - Triggers checkpoint failures
- `checkpoint-stuck` - Simulates stuck checkpoints
- `checkpoint-duration-high` - Increases checkpoint duration
- `cpu-high` - Simulates high CPU usage
- `memory-high` - Simulates high memory usage
- `jobmanager-cpu-high` - Simulates high JobManager CPU usage
- `jobmanager-memory-high` - Simulates high JobManager heap usage
- `restart-rate-high` - Causes high restart rate


## Step 5: Run the Automated Cluster Setup Script

You can use the project setup script instead of performing the monitoring and demo resource setup manually:

```bash
./setup-cluster.sh
```

This script brings up and/or configures:
- `confluent` and `monitoring` namespaces
- Flink prerequisites from `src/main/resources/FlinkCluster` (CMF REST class, Flink environment, PVC)
- Flink application (`flink-app.yaml`)
- Prometheus in the `monitoring` namespace
- Grafana in the `monitoring` namespace (using `src/main/resources/grafana-values.yaml` when present)
- Grafana alert provisioning config map from `src/main/resources/alerting-provisioning-alerttrigger.yaml` and restarts Grafana to reload alert rules
- Flink Kubernetes Operator upgrade with Prometheus metrics configuration (when `src/main/resources/prometheus-values.yaml` is present)

### Step 6: Verify Prerequisites

Ensure these resources exist in your cluster:

```bash
# Check Flink environment
kubectl get flinkenvironment flink-env1 -n confluent

# Check PVC
kubectl get pvc flink-pvc -n confluent

# Check CMF REST class
kubectl get cmfrestclass default -n confluent
```

### Step 7: Verify flink application

```bash
# Apply the manifest
kubectl apply -f src/main/resources/FlinkCluster/flink-app.yaml

# Watch the FlinkApplication status
kubectl get flinkapplication my-app1 -n confluent -w
```

**Expected Output:**
```
NAME      STATUS    AGE
my-app1   Running   10s
```

### Step 8: Monitor the Job

**Recommended: Use the helper script to port-forward all required services**

```bash
./port-forward-services.sh
```

This starts port-forwards for:
- Flink UI (`my-app1`) on `http://localhost:8081`
- Prometheus on `http://localhost:9090`
- Grafana on `http://localhost:3000`
- Alertmanager on `http://localhost:9093`

Keep this script running while you monitor the job. Use `Ctrl+C` to stop all port-forwards.

**Manual monitoring commands (optional / fallback):**

**Check JobManager Pod:**
```bash
# Get JobManager pod name
kubectl get pods -n confluent -l app=flink,component=jobmanager

# View logs
kubectl logs -f <jobmanager-pod-name> -n confluent
```

**Check TaskManager Pods:**
```bash
# Get TaskManager pod names
kubectl get pods -n confluent -l app=flink,component=taskmanager

# View logs
kubectl logs -f <taskmanager-pod-name> -n confluent
```

**Check Job Status via Flink UI:**
```bash
# Port forward to Flink UI
kubectl port-forward -n confluent svc/<flink-rest-service> 8081:8081

# Open browser: http://localhost:8081
```

**Check Prometheus Metrics:**
```bash
# Port forward to Prometheus (if configured)
kubectl port-forward -n monitoring svc/prometheus-server 9090:9090

# Query metrics, e.g.:
# flink_jobmanager_job_failingState{job_name="AlertTriggerJob"}
```

### Step 9: Verify Alert Triggered

Based on the alert type you selected:

1. **Check Prometheus metrics** - Query the relevant metric for your alert type
2. **Check Grafana dashboards** - View Flink dashboards
3. **Check AlertManager** - Verify alerts are firing
4. **Check job logs** - Look for expected behavior (errors, slow processing, etc.)

### Step 10: Change Alert Type (Optional)

To test a different alert:

```bash
# Edit flink-app.yaml and change args
vim src/main/resources/FlinkCluster/flink-app.yaml
# Change: args: ["normal"] to args: ["backpressure"]

# Apply the updated manifest
kubectl apply -f src/main/resources/FlinkCluster/flink-app.yaml

# The operator will restart the job with new args
```

### Step 11: Cleanup

You can use the project cleanup script.

```bash
./cleanup-cluster.sh
```

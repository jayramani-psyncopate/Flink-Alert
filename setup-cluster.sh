#!/bin/bash

# Flink Demo - Kubernetes Cluster Setup Script
# This script automates the setup of Flink applications, Prometheus, and Grafana

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="confluent"
MONITORING_NAMESPACE="monitoring"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOURCES_DIR="${SCRIPT_DIR}/src/main/resources"
FLINK_CLUSTER_DIR="${RESOURCES_DIR}/FlinkCluster"

# Functions
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed. Please install kubectl first."
        exit 1
    fi
    print_success "kubectl found: $(kubectl version --client --short 2>/dev/null || echo 'installed')"
    
    # Check helm
    if ! command -v helm &> /dev/null; then
        print_error "helm is not installed. Please install helm first."
        exit 1
    fi
    print_success "helm found: $(helm version --short 2>/dev/null || echo 'installed')"
    
    # Check kubectl connection
    if ! kubectl cluster-info &> /dev/null; then
        print_error "Cannot connect to Kubernetes cluster. Please check your kubeconfig."
        exit 1
    fi
    print_success "Connected to Kubernetes cluster"
    
    # Check if confluent namespace exists
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        print_warning "Namespace '$NAMESPACE' does not exist. It will be created."
    else
        print_success "Namespace '$NAMESPACE' exists"
    fi
}

setup_namespaces() {
    print_header "Setting Up Namespaces"
    
    # Create confluent namespace if it doesn't exist
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        kubectl create namespace "$NAMESPACE"
        print_success "Created namespace: $NAMESPACE"
    else
        print_info "Namespace '$NAMESPACE' already exists"
    fi
    
    # Set context to confluent namespace
    kubectl config set-context --current --namespace="$NAMESPACE"
    print_success "Set context to namespace: $NAMESPACE"
    
    # Create monitoring namespace if it doesn't exist
    if ! kubectl get namespace "$MONITORING_NAMESPACE" &> /dev/null; then
        kubectl create namespace "$MONITORING_NAMESPACE"
        print_success "Created namespace: $MONITORING_NAMESPACE"
    else
        print_info "Namespace '$MONITORING_NAMESPACE' already exists"
    fi
}

setup_flink_prerequisites() {
    print_header "Setting Up Flink Prerequisites"
    
    cd "$FLINK_CLUSTER_DIR"
    
    # Apply CMF REST Class
    if [ -f "cmf-rest-class.yaml" ]; then
        print_info "Applying CMF REST Class..."
        kubectl apply -f cmf-rest-class.yaml
        print_success "Applied cmf-rest-class.yaml"
    else
        print_error "cmf-rest-class.yaml not found in $FLINK_CLUSTER_DIR"
        exit 1
    fi
    
    # Apply Flink Environment
    if [ -f "flink-env.yaml" ]; then
        print_info "Applying Flink Environment..."
        kubectl apply -f flink-env.yaml
        print_success "Applied flink-env.yaml"
    else
        print_error "flink-env.yaml not found in $FLINK_CLUSTER_DIR"
        exit 1
    fi
    
    # Apply PVC
    if [ -f "flink-pvc.yaml" ]; then
        print_info "Applying Persistent Volume Claim..."
        kubectl apply -f flink-pvc.yaml
        print_success "Applied flink-pvc.yaml"
        
        # Wait for PVC to be bound
        print_info "Waiting for PVC to be bound..."
        kubectl wait --for=condition=Bound pvc/flink-pvc -n "$NAMESPACE" --timeout=60s || {
            print_warning "PVC not bound within 60s, continuing anyway..."
        }
    else
        print_error "flink-pvc.yaml not found in $FLINK_CLUSTER_DIR"
        exit 1
    fi
    
    cd "$SCRIPT_DIR"
}

setup_flink_applications() {
    print_header "Setting Up Flink Application"
    
    cd "$FLINK_CLUSTER_DIR"
    
    # Apply streaming application (my-app1)
    if [ -f "flink-app.yaml" ]; then
        print_info "Applying Flink Streaming Application (my-app1)..."
        kubectl apply -f flink-app.yaml
        print_success "Applied flink-app.yaml"
    else
        print_error "flink-app.yaml not found in $FLINK_CLUSTER_DIR"
        exit 1
    fi
    
    cd "$SCRIPT_DIR"
    
    print_info "Waiting for Flink application to be ready..."
    sleep 5
}

setup_prometheus() {
    print_header "Setting Up Prometheus"
    
    # Add Prometheus Helm repo if not already added
    if ! helm repo list | grep -q prometheus-community; then
        print_info "Adding Prometheus Helm repository..."
        helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
        print_success "Added Prometheus Helm repository"
    else
        print_info "Prometheus Helm repository already exists"
    fi
    
    # Update Helm repos
    print_info "Updating Helm repositories..."
    helm repo update
    
    # Install or upgrade Prometheus
    print_info "Installing/Upgrading Prometheus..."
    helm upgrade --install prometheus prometheus-community/prometheus \
        --namespace "$MONITORING_NAMESPACE" \
        --create-namespace \
        --wait \
        --timeout 5m || {
        print_warning "Prometheus installation may still be in progress..."
    }
    
    print_success "Prometheus installation initiated"
}

setup_grafana() {
    print_header "Setting Up Grafana"
    
    # Provision Grafana alerting rules config map (used by grafana-values.yaml extraConfigmapMounts)
    GRAFANA_ALERT_RULES_FILE="${RESOURCES_DIR}/alerting-provisioning-alerttrigger.yaml"
    if [ -f "$GRAFANA_ALERT_RULES_FILE" ]; then
        print_info "Applying Grafana alerting provisioning config map..."
        kubectl create configmap grafana-alerting-provisioning -n "$MONITORING_NAMESPACE" \
            --from-file=alerting-provisioning-alerttrigger.yaml="$GRAFANA_ALERT_RULES_FILE" \
            --dry-run=client -o yaml | kubectl apply -f -
        print_success "Applied Grafana alerting provisioning config map"
    else
        print_warning "Alerting provisioning file not found: $GRAFANA_ALERT_RULES_FILE"
    fi
    
    # Add Grafana Helm repo if not already added
    if ! helm repo list | grep -q grafana; then
        print_info "Adding Grafana Helm repository..."
        helm repo add grafana https://grafana.github.io/helm-charts
        print_success "Added Grafana Helm repository"
    else
        print_info "Grafana Helm repository already exists"
    fi
    
    # Update Helm repos
    helm repo update
    
    # Check if grafana-values.yaml exists
    GRAFANA_VALUES="${RESOURCES_DIR}/grafana-values.yaml"
    if [ -f "$GRAFANA_VALUES" ]; then
        print_info "Installing/Upgrading Grafana with custom values..."
        helm upgrade --install grafana grafana/grafana \
            --namespace "$MONITORING_NAMESPACE" \
            --create-namespace \
            --values "$GRAFANA_VALUES" \
            --wait \
            --timeout 5m || {
            print_warning "Grafana installation may still be in progress..."
        }
        print_success "Grafana installation initiated with custom dashboards"
    else
        print_warning "grafana-values.yaml not found, installing Grafana with default values..."
        helm upgrade --install grafana grafana/grafana \
            --namespace "$MONITORING_NAMESPACE" \
            --create-namespace \
            --wait \
            --timeout 5m || {
            print_warning "Grafana installation may still be in progress..."
        }
        print_success "Grafana installation initiated"
    fi
    
    # Restart Grafana so updated provisioning config map is reloaded on existing installs
    if kubectl get deployment grafana -n "$MONITORING_NAMESPACE" &> /dev/null; then
        print_info "Restarting Grafana deployment to reload alert provisioning..."
        kubectl rollout restart deployment/grafana -n "$MONITORING_NAMESPACE"
        kubectl rollout status deployment/grafana -n "$MONITORING_NAMESPACE" --timeout=5m || {
            print_warning "Grafana restart may still be in progress..."
        }
        print_success "Grafana restart initiated"
    fi
}

upgrade_flink_operator() {
    print_header "Upgrading Flink Kubernetes Operator with Prometheus Metrics"
    
    PROMETHEUS_VALUES="${RESOURCES_DIR}/prometheus-values.yaml"
    if [ -f "$PROMETHEUS_VALUES" ]; then
        print_info "Upgrading Flink operator with Prometheus metrics configuration..."
        helm upgrade --install cp-flink-kubernetes-operator \
            confluentinc/flink-kubernetes-operator \
            --namespace "$NAMESPACE" \
            --values "$PROMETHEUS_VALUES" \
            --wait \
            --timeout 5m || {
            print_warning "Flink operator upgrade may still be in progress..."
        }
        print_success "Flink operator upgrade initiated"
    else
        print_warning "prometheus-values.yaml not found, skipping operator upgrade"
    fi
}

show_status() {
    print_header "Cluster Status"
    
    echo -e "\n${BLUE}Flink Applications:${NC}"
    kubectl get flinkapplications -n "$NAMESPACE" || print_warning "No Flink applications found"
    
    echo -e "\n${BLUE}Flink Pods:${NC}"
    kubectl get pods -n "$NAMESPACE" -l app=flink || print_warning "No Flink pods found"
    
    echo -e "\n${BLUE}Prometheus Pods:${NC}"
    kubectl get pods -n "$MONITORING_NAMESPACE" -l app.kubernetes.io/name=prometheus || print_warning "No Prometheus pods found"
    
    echo -e "\n${BLUE}Grafana Pods:${NC}"
    kubectl get pods -n "$MONITORING_NAMESPACE" -l app.kubernetes.io/name=grafana || print_warning "No Grafana pods found"
    
    echo -e "\n${BLUE}Services:${NC}"
    kubectl get svc -n "$MONITORING_NAMESPACE" | grep -E "prometheus|grafana" || print_warning "No monitoring services found"
}

show_port_forward_commands() {
    print_header "Port Forward Commands"
    
    echo -e "${YELLOW}To access Prometheus UI (port 9090):${NC}"
    echo "export POD_NAME=\$(kubectl get pods --namespace $MONITORING_NAMESPACE -l \"app.kubernetes.io/name=prometheus,app.kubernetes.io/instance=prometheus\" -o jsonpath=\"{.items[0].metadata.name}\")"
    echo "kubectl --namespace $MONITORING_NAMESPACE port-forward \$POD_NAME 9090"
    
    echo -e "\n${YELLOW}To access Grafana UI (port 3000):${NC}"
    echo "export POD_NAME=\$(kubectl get pods --namespace $MONITORING_NAMESPACE -l \"app.kubernetes.io/name=grafana\" -o jsonpath=\"{.items[0].metadata.name}\")"
    echo "kubectl --namespace $MONITORING_NAMESPACE port-forward \$POD_NAME 3000"
    
    echo -e "\n${YELLOW}To access Alertmanager UI (port 9093):${NC}"
    echo "export POD_NAME=\$(kubectl get pods --namespace $MONITORING_NAMESPACE -l \"app.kubernetes.io/name=alertmanager,app.kubernetes.io/instance=prometheus\" -o jsonpath=\"{.items[0].metadata.name}\")"
    echo "kubectl --namespace $MONITORING_NAMESPACE port-forward \$POD_NAME 9093"
    
    echo -e "\n${YELLOW}Grafana default credentials:${NC}"
    echo "Username: admin"
    echo "Password: admin123 (as configured in grafana-values.yaml)"
}

# Main execution
main() {
    print_header "Flink Demo - Kubernetes Cluster Setup"
    
    check_prerequisites
    setup_namespaces
    setup_flink_prerequisites
    setup_flink_applications
    setup_prometheus
    setup_grafana
    upgrade_flink_operator
    
    print_header "Setup Complete!"
    
    show_status
    show_port_forward_commands
    
    echo -e "\n${GREEN}Setup completed successfully!${NC}"
    echo -e "${YELLOW}Note: Some pods may take a few minutes to become ready.${NC}"
    echo -e "${YELLOW}Use 'kubectl get pods -n $NAMESPACE' and 'kubectl get pods -n $MONITORING_NAMESPACE' to check status.${NC}"
}

# Run main function
main

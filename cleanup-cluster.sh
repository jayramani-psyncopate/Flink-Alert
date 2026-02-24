#!/bin/bash

# Flink Demo - Kubernetes Cluster Cleanup Script
# This script removes Flink applications and port-forward processes.
# All infrastructure (prerequisites, monitoring services) is preserved for reuse.

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

# Parse command line arguments
DELETE_NAMESPACES=false
FORCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --delete-namespaces)
            DELETE_NAMESPACES=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --delete-namespaces    Also delete the confluent and monitoring namespaces"
            echo "  --force               Skip confirmation prompts"
            echo "  -h, --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

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

confirm() {
    if [ "$FORCE" = true ]; then
        return 0
    fi
    
    read -p "$(echo -e ${YELLOW}$1${NC}) (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        return 0
    else
        return 1
    fi
}

kill_port_forwards() {
    print_header "Killing Port-Forward Processes"
    
    # Find and kill kubectl port-forward processes
    PORT_FORWARD_PIDS=$(ps aux | grep "kubectl.*port-forward" | grep -v grep | awk '{print $2}' || true)
    
    if [ -z "$PORT_FORWARD_PIDS" ]; then
        print_info "No port-forward processes found"
    else
        print_info "Found port-forward processes: $PORT_FORWARD_PIDS"
        for pid in $PORT_FORWARD_PIDS; do
            print_info "Killing port-forward process $pid..."
            kill "$pid" 2>/dev/null || print_warning "Could not kill process $pid (may already be terminated)"
        done
        print_success "Port-forward processes terminated"
    fi
    
    # Clean up any PID files from port-forward-services.sh
    for pidfile in /tmp/flink-demo-*-pf.pid; do
        if [ -f "$pidfile" ]; then
            PID=$(cat "$pidfile" 2>/dev/null || echo "")
            if [ -n "$PID" ]; then
                kill "$PID" 2>/dev/null || true
            fi
            rm -f "$pidfile"
            print_info "Cleaned up PID file: $pidfile"
        fi
    done
}

delete_flink_applications() {
    print_header "Deleting Flink Applications"
    
    # Delete Flink applications
    if kubectl get flinkapplication -n "$NAMESPACE" &> /dev/null; then
        print_info "Deleting Flink applications..."
        kubectl delete flinkapplication --all -n "$NAMESPACE" --timeout=60s || {
            print_warning "Some Flink applications may still be terminating..."
        }
        print_success "Flink applications deleted"
        
        # Wait for pods to terminate
        print_info "Waiting for Flink pods to terminate..."
        sleep 5
    else
        print_info "No Flink applications found"
    fi
}

show_flink_prerequisites() {
    print_header "Flink Prerequisites (Preserved)"
    
    print_info "The following prerequisites are kept intact:"
    
    # Check PVC
    if kubectl get pvc flink-pvc -n "$NAMESPACE" &> /dev/null; then
        print_success "PVC 'flink-pvc' exists (preserved)"
    else
        print_info "PVC 'flink-pvc' not found"
    fi
    
    # Check Flink Environment
    if kubectl get flinkenvironment flink-env1 -n "$NAMESPACE" &> /dev/null; then
        print_success "Flink Environment 'flink-env1' exists (preserved)"
    else
        print_info "Flink Environment 'flink-env1' not found"
    fi
    
    # Check CMF REST Class
    if kubectl get cmfrestclass default -n "$NAMESPACE" &> /dev/null; then
        print_success "CMF REST Class 'default' exists (preserved)"
    else
        print_info "CMF REST Class 'default' not found"
    fi
    
    print_info "These prerequisites can be reused for future Flink applications."
}

show_monitoring_services() {
    print_header "Monitoring Services (Preserved)"
    
    print_info "The following monitoring services are kept intact:"
    
    # Check Prometheus
    if helm list -n "$MONITORING_NAMESPACE" | grep -q "prometheus"; then
        print_success "Prometheus Helm release exists (preserved)"
    else
        print_info "Prometheus Helm release not found"
    fi
    
    # Check Grafana
    if helm list -n "$MONITORING_NAMESPACE" | grep -q "grafana"; then
        print_success "Grafana Helm release exists (preserved)"
    else
        print_info "Grafana Helm release not found"
    fi
    
    # Check Flink Operator
    if helm list -n "$NAMESPACE" | grep -q "cp-flink-kubernetes-operator"; then
        print_success "Flink Kubernetes Operator exists (preserved)"
    else
        print_info "Flink Kubernetes Operator Helm release not found"
    fi
    
    print_info "These monitoring services can be reused for future Flink applications."
}

delete_namespaces() {
    if [ "$DELETE_NAMESPACES" = true ]; then
        print_header "Deleting Namespaces"
        
        # Delete monitoring namespace
        if kubectl get namespace "$MONITORING_NAMESPACE" &> /dev/null; then
            print_info "Deleting namespace: $MONITORING_NAMESPACE..."
            kubectl delete namespace "$MONITORING_NAMESPACE" --timeout=5m || {
                print_warning "Namespace deletion may still be in progress..."
            }
            print_success "Namespace '$MONITORING_NAMESPACE' deleted"
        else
            print_info "Namespace '$MONITORING_NAMESPACE' not found"
        fi
        
        # Note: We don't delete the confluent namespace as it may contain CMF and other Confluent components
        print_warning "Skipping deletion of '$NAMESPACE' namespace (may contain CMF and other Confluent components)"
        print_info "To delete the '$NAMESPACE' namespace manually, run: kubectl delete namespace $NAMESPACE"
    else
        print_info "Skipping namespace deletion (use --delete-namespaces to delete namespaces)"
    fi
}

show_remaining_resources() {
    print_header "Remaining Resources"
    
    echo -e "\n${BLUE}Flink Applications:${NC}"
    kubectl get flinkapplications -n "$NAMESPACE" 2>/dev/null || print_info "No Flink applications found"
    
    echo -e "\n${BLUE}Flink Pods:${NC}"
    kubectl get pods -n "$NAMESPACE" -l app=flink 2>/dev/null || print_info "No Flink pods found"
    
    echo -e "\n${BLUE}PVCs (Preserved):${NC}"
    kubectl get pvc -n "$NAMESPACE" 2>/dev/null || print_info "No PVCs found"
    
    echo -e "\n${BLUE}Flink Environments (Preserved):${NC}"
    kubectl get flinkenvironment -n "$NAMESPACE" 2>/dev/null || print_info "No Flink environments found"
    
    echo -e "\n${BLUE}CMF REST Classes (Preserved):${NC}"
    kubectl get cmfrestclass -n "$NAMESPACE" 2>/dev/null || print_info "No CMF REST classes found"
    
    echo -e "\n${BLUE}Helm Releases in $MONITORING_NAMESPACE (Preserved):${NC}"
    helm list -n "$MONITORING_NAMESPACE" 2>/dev/null || print_info "No Helm releases found"
    
    echo -e "\n${BLUE}Helm Releases in $NAMESPACE (Preserved):${NC}"
    helm list -n "$NAMESPACE" 2>/dev/null || print_info "No Helm releases found"
}

# Main execution
main() {
    print_header "Flink Demo - Kubernetes Cluster Cleanup"
    
    # Check prerequisites
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed. Please install kubectl first."
        exit 1
    fi
    
    if ! command -v helm &> /dev/null; then
        print_error "helm is not installed. Please install helm first."
        exit 1
    fi
    
    if ! kubectl cluster-info &> /dev/null; then
        print_error "Cannot connect to Kubernetes cluster. Please check your kubeconfig."
        exit 1
    fi
    
    # Confirmation
    if [ "$FORCE" != true ]; then
        echo -e "${YELLOW}This will delete:${NC}"
        echo "  - Flink streaming application (my-app1)"
        echo "  - All port-forward processes"
        if [ "$DELETE_NAMESPACES" = true ]; then
            echo "  - Monitoring namespace"
        fi
        echo ""
        echo -e "${GREEN}This will preserve:${NC}"
        echo "  - Flink prerequisites (PVC, Flink Environment, CMF REST Class)"
        echo "  - Monitoring services (Prometheus, Grafana)"
        echo "  - Flink Kubernetes Operator"
        echo ""
        if ! confirm "Are you sure you want to proceed?"; then
            print_info "Cleanup cancelled"
            exit 0
        fi
    fi
    
    # Execute cleanup steps
    kill_port_forwards
    delete_flink_applications
    show_flink_prerequisites
    show_monitoring_services
    delete_namespaces
    
    print_header "Cleanup Complete!"
    
    show_remaining_resources
    
    echo -e "\n${GREEN}Cleanup completed successfully!${NC}"
    echo -e "${YELLOW}Note: Some resources may take a few minutes to fully terminate.${NC}"
    if [ "$DELETE_NAMESPACES" != true ]; then
        echo -e "${YELLOW}To delete namespaces, run: $0 --delete-namespaces${NC}"
    fi
}

# Run main function
main


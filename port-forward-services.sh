#!/bin/bash

# Port Forward Helper Script for Flink Demo Services
# This script sets up port-forwards for Prometheus, Grafana, Alertmanager, and Flink applications

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

MONITORING_NAMESPACE="monitoring"
CONFLUENT_NAMESPACE="confluent"

echo -e "${BLUE}Setting up port-forwards for all services...${NC}\n"

# Function to port-forward a service
port_forward() {
    local service_name=$1
    local port=$2
    local label_selector=$3
    local namespace=${4:-$MONITORING_NAMESPACE}
    
    echo -e "${YELLOW}Setting up port-forward for $service_name on port $port...${NC}"
    
    POD_NAME=$(kubectl get pods --namespace "$namespace" -l "$label_selector" -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)
    
    if [ -z "$POD_NAME" ]; then
        echo -e "${YELLOW}⚠ $service_name pod not found. It may still be starting up.${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Found pod: $POD_NAME${NC}"
    echo -e "${BLUE}Port-forwarding $service_name to localhost:$port (Ctrl+C to stop)...${NC}\n"
    
    kubectl --namespace "$namespace" port-forward "$POD_NAME" "$port" &
    PF_PID=$!
    echo "$PF_PID" > "/tmp/flink-demo-$service_name-pf.pid"
    echo -e "${GREEN}Port-forward started (PID: $PF_PID)${NC}\n"
}

# Function to port-forward a Flink application by pod name pattern
port_forward_flink_app() {
    local app_name=$1
    local port=$2
    
    echo -e "${YELLOW}Setting up port-forward for Flink app $app_name on port $port...${NC}"
    
    # Find the JobManager pod for this Flink application
    # Flink JobManager pods are named like: my-app1-xxx (without "taskmanager" in the name)
    # TaskManager pods have "taskmanager" in the name, so we exclude those
    POD_NAME=$(kubectl get pods --namespace "$CONFLUENT_NAMESPACE" \
        --field-selector=status.phase=Running \
        -o jsonpath="{range .items[*]}{.metadata.name}{'\n'}{end}" 2>/dev/null | \
        grep "^${app_name}-" | grep -v taskmanager | head -1)
    
    if [ -z "$POD_NAME" ]; then
        echo -e "${YELLOW}⚠ Flink app $app_name JobManager pod not found. It may still be starting up.${NC}"
        echo -e "${YELLOW}   Available pods:${NC}"
        kubectl get pods --namespace "$CONFLUENT_NAMESPACE" | grep "$app_name" || echo "   (none found)"
        return 1
    fi
    
    echo -e "${GREEN}Found pod: $POD_NAME${NC}"
    echo -e "${BLUE}Port-forwarding $app_name to localhost:$port (Ctrl+C to stop)...${NC}\n"
    
    kubectl --namespace "$CONFLUENT_NAMESPACE" port-forward "$POD_NAME" "$port:8081" &
    PF_PID=$!
    echo "$PF_PID" > "/tmp/flink-demo-$app_name-pf.pid"
    echo -e "${GREEN}Port-forward started (PID: $PF_PID)${NC}\n"
}

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up port-forwards...${NC}"
    for pidfile in /tmp/flink-demo-*-pf.pid; do
        if [ -f "$pidfile" ]; then
            PID=$(cat "$pidfile")
            kill "$PID" 2>/dev/null || true
            rm "$pidfile"
        fi
    done
    echo -e "${GREEN}Cleanup complete${NC}"
    exit 0
}

trap cleanup SIGINT SIGTERM

# Port-forward Prometheus
port_forward "prometheus" "9090" "app.kubernetes.io/name=prometheus,app.kubernetes.io/instance=prometheus"

# Port-forward Grafana
port_forward "grafana" "3000" "app.kubernetes.io/name=grafana"

# Port-forward Alertmanager
port_forward "alertmanager" "9093" "app.kubernetes.io/name=alertmanager,app.kubernetes.io/instance=prometheus"

# Port-forward Flink applications
echo -e "\n${BLUE}Setting up Flink application port-forward...${NC}\n"
port_forward_flink_app "my-app1" "8081"

echo -e "\n${GREEN}All port-forwards are running!${NC}"
echo -e "${BLUE}Access:${NC}"
echo -e "  Prometheus: http://localhost:9090"
echo -e "  Grafana: http://localhost:3000 (admin/admin123)"
echo -e "  Alertmanager: http://localhost:9093"
echo -e "  Flink my-app1: http://localhost:8081"
echo -e "\n${YELLOW}Press Ctrl+C to stop all port-forwards${NC}"

# Wait for all background processes
wait


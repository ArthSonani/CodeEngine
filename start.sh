#!/bin/bash
set -e

echo "🚀 Starting Minikube Cluster..."
minikube start

echo "🐳 Pointing Docker to Minikube..."
eval $(minikube docker-env)

echo "🔨 Building Go Images..."
docker build -t engine-api:latest ./api
docker build -t engine-worker:latest ./worker

echo "⚙️ Installing KEDA Autoscaler..."
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm upgrade --install keda kedacore/keda --namespace keda --create-namespace

echo "🏗️ Deploying Infrastructure (Postgres & Kafka)..."
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/kafka.yaml
sleep 10 # Give stateful services a moment to boot

echo "🚀 Deploying Code Engine..."
kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/worker.yaml
kubectl apply -f k8s/keda-scaler.yaml

echo "Waiting for Kafka to initialize..."
kubectl wait --for=condition=ready pod -l app=kafka --timeout=120s

echo "Provisioning code-submissions topic with 10 partitions..."
kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic code-submissions --partitions 10 --bootstrap-server localhost:9092

echo "✅ Deployment Complete! Run 'minikube service engine-api' to access the gateway."

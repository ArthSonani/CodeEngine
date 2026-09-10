#!/bin/bash
set -e

echo "🚀 Starting Minikube Cluster..."
minikube start

# echo "🐳 Pointing Docker to Minikube..."
# eval $(minikube docker-env)

# echo "🔨 Building Go Images..."
# docker build -t engine-api:latest ./api
# docker build -t engine-worker:latest ./worker
echo "🔨 Building Go Images directly inside Minikube..."
minikube image build -t engine-api:latest ./api
minikube image build -t engine-worker:latest ./worker

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

# 1. Ensure Kafka is fully booted before executing commands
echo "⏳ Waiting for Kafka to be ready..."
kubectl wait --for=condition=ready pod -l app=kafka --timeout=120s

# 2. Force-create the topic with 10 partitions (ignores if already exists)
kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --create --topic code-submissions --partitions 10 --replication-factor 1 --bootstrap-server kafka.default.svc.cluster.local:9092 --if-not-exists

# 3. Temporarily remove KEDA so it stops fighting us
kubectl delete -f k8s/keda-scaler.yaml --ignore-not-found=true

# 4. Scale to 0 to disconnect workers from the queue
kubectl scale deploy engine-worker --replicas=0

# 5. Wait for all worker pods to physically terminate
echo "⏳ Waiting for workers to gracefully shut down..."
while [[ $(kubectl get pods -l app=engine-worker | grep -c -e "Terminating" -e "Running") -gt 0 ]]; do
    sleep 2
done

# 6. Wipe the ghost lag (Consumer group is now 'Empty' or 'Dead')
echo "🔄 Resetting consumer offsets to 0..."
kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka.default.svc.cluster.local:9092 --group isolate-workers --reset-offsets --to-latest --execute --topic code-submissions

# 7. Reactivate KEDA
echo "📈 Reactivating KEDA Autoscaler..."
kubectl apply -f k8s/keda-scaler.yaml

echo "✅ Deployment Complete! Run 'minikube service engine-api' to access the gateway."

# Distributed Code Execution Engine

A fault-tolerant, event-driven code execution platform built in Go. This system securely compiles and runs untrusted user code in isolated Linux sandboxes, utilizing asynchronous message brokering for traffic backpressure and KEDA for dynamic scale-to-zero pod orchestration.

## System Architecture


## Requirements
- Minikube
- Docker

## Quickstart
1. Start minikube: `minikube start`
2. Run the deployment script: `./start.sh`
3. The script will automatically build the Docker images, deploy the K8s resources, and provision the Kafka queue.

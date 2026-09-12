# CodeEngine (Exenova) 🚀

CodeEngine is a highly reliable, scale-to-zero, asynchronous code execution sandbox. Engineered as the backend processing engine, it securely executes untrusted payloads in isolated environments and dispatches the execution results via webhooks.

Built for production-grade resilience, the system scales seamlessly based on real-time traffic spikes, ensuring high delivery accuracy and zero wasted compute when idle.

## 🏗 Architecture & Tech Stack

*   **API Gateway (Go / Gin):** Accepts incoming code execution payloads and immediately offloads them to the message broker.
*   **Message Broker (Apache Kafka):** Buffers incoming tasks in the `code-submissions` topic, completely decoupling the API from the execution layer.
*   **Autoscaler (KEDA):** Monitors Kafka queue lag. Idles at `0` pods to save resources, and instantly provisions up to `10` pods during traffic bursts.
*   **Execution Workers (Go):** Consume tasks from Kafka, execute the code within a secure timeout threshold (handling Time Limit, Memory Limit Exceeded / Other errors), and asynchronously deliver results.
*   **Infrastructure (Kubernetes):** Orchestrates the entire lifecycle, completely automated via native Minikube builds and Helm.

---

## ⚙️ Prerequisites

Before you begin, ensure you have the following installed on your machine:

1.  **Docker:** Must be installed and running in the background.
    *   *Mac/Windows:* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
    *   *Linux:* Docker Engine
2.  **Minikube:** Local Kubernetes cluster.
3.  **Helm:** Kubernetes package manager (used for KEDA).
4.  **kubectl:** Command-line tool for Kubernetes.

### Quick Install Commands
*   **macOS (Homebrew):** `brew install minikube helm kubectl`
*   **Linux (Debian/Ubuntu):** `sudo apt install helm kubectl` (Install Minikube via official binary)
*   **Windows (Winget):** `winget install Kubernetes.minikube Helm.Helm Kubernetes.kubectl`

---

## 🚀 Local Setup Guide

Follow these steps to deploy the self-healing cluster to your local machine.

### 1. Start the Kubernetes Cluster
Boot Minikube using the Docker driver. This avoids heavy virtual machines.
```bash
minikube start --driver=docker --ports=30080:30080
```

### 2. Run the Deployment Script
Clone the repository, grant execution rights, and run the automated setup.

```bash
chmod +x start.sh
./start.sh
```

**What this script does automatically:**
*   Builds the Go API and Worker Docker images natively inside Minikube.
*   Installs the KEDA autoscaler via Helm.
*   Deploys the PostgreSQL, Kafka, API, and Worker nodes(pods).
*   Provisions exactly 10 Kafka partitions and wipes any initial "ghost lag" so the engine cleanly drops to 0 replicas.

---

## 🧪 Testing the Pipeline (End-to-End)

**Send a Payload:**
Replace `YOUR-WEBHOOK-UUID` with your actual webhook URL in test_engine.sh. 

```bash
chmod +x test_engine.sh
./test_engine.sh
```

## 📈 Verifying the Autoscaler (0-to-10 Burst Test)
To watch the engine scale dynamically under load:

**Watch the pods in a separate terminal:**

```bash
kubectl get pods -l app=engine-worker -w
```

**Fire a 50-payload burst by running test_engine.sh on separate terminal** (containing a 10-second sleep to force a queue backlog). 

Watch KEDA instantly provision 10 pods, process the Time-Limit-Exceeded (TLE) timeouts, deliver the webhooks, and cleanly scale back down to 0 after the 60-second cooldown window.
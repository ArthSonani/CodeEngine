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
minikube start --driver=docker
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
Because Docker network routing differs by operating system, follow the specific testing instructions for your machine.

### 🍎 For macOS Users
macOS runs Docker inside a hidden virtual machine, meaning your Mac cannot reach the cluster IP directly. You must open a network tunnel.

**Open a Tunnel (Terminal 1):** Leave this command running in the background.

```bash
minikube service engine-api --url
```
*(It will output a URL like `http://127.0.0.1:51234`)*

**Send a Payload (Terminal 2):** Use the URL provided by the previous step to send a test payload. Replace `YOUR_TUNNEL_PORT` and the `webhook_url`:

```bash
curl -X POST http://127.0.0.1:YOUR_TUNNEL_PORT/submit \
-H "Content-Type: application/json" \
-d '{
      "id": "mac-test-1",
      "language": "python",
      "code": "print("Execution Successful!")",
      "webhook_url": "https://webhook.site/YOUR-WEBHOOK-UUID"
    }'
```

### 🐧🪟 For Linux & Windows (WSL2) Users
Linux and WSL2 attach Docker directly to the host network, so you can hit the cluster IP instantly.

**Get the Cluster IP:**

```bash
minikube ip
```

**Send a Payload:** Replace `<MINIKUBE_IP>` with the address from the previous step.

```bash
curl -X POST http://<MINIKUBE_IP>:30080/submit \
-H "Content-Type: application/json" \
-d '{
      "id": "linux-test-1",
      "language": "python",
      "code": "print("Execution Successful!")",
      "webhook_url": "https://webhook.site/YOUR-WEBHOOK-UUID"
    }'
```

---

## 📈 Verifying the Autoscaler (0-to-10 Burst Test)
To watch the engine scale dynamically under load:

**Watch the pods in a separate terminal:**

```bash
kubectl get pods -l app=engine-worker -w
```

**Fire a 50-payload burst:** (containing a 10-second sleep to force a queue backlog). *Note: macOS users must swap the IP for their `127.0.0.1:PORT` tunnel.*

```bash
for i in {1..50}; do curl -s -X POST http://<YOUR_IP_OR_TUNNEL>/submit -H "Content-Type: application/json" -d '{"id": "burst-'$i'", "language": "python", "code": "import time; time.sleep(10); print("Burst Complete!")", "webhook_url": "https://webhook.site/YOUR-WEBHOOK-UUID"}' & done; wait
```

Watch KEDA instantly provision 10 pods, process the Time-Limit-Exceeded (TLE) timeouts, deliver the webhooks, and cleanly scale back down to 0 after the 60-second cooldown window.
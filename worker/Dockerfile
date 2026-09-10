# ---------------------------------------------------------
# STAGE 1: Build the Go Worker (Using latest Go)
# ---------------------------------------------------------
FROM golang:latest AS builder
WORKDIR /app

# Enable toolchain auto-switching
ENV GOTOOLCHAIN=auto

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build the binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o engine-worker main.go

# ---------------------------------------------------------
# STAGE 2: Build the Sandbox Environment (Ubuntu)
# ---------------------------------------------------------
FROM ubuntu:22.04

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# 1. Install Isolate dependencies and Language Runtimes
# NEW: Added 'uidmap' which is required for user namespaces in Docker
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    pkg-config \
    libcap-dev \
    libsystemd-dev \
    libseccomp-dev \
    uidmap \
    python3 \
    gcc \
    g++ \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# 2. NEW: Setup Sub-UIDs for Isolate
# Create the isolate user and allocate a block of 65,536 UIDs/GIDs for sandboxing
RUN useradd -r isolate && \
    echo "isolate:100000:65536" >> /etc/subuid && \
    echo "isolate:100000:65536" >> /etc/subgid

# 3. Compile Isolate from source
RUN git clone https://github.com/ioi/isolate.git /tmp/isolate
WORKDIR /tmp/isolate
RUN make isolate
RUN make install

# 4. Pull in the Go binary from Stage 1
WORKDIR /app
COPY --from=builder /app/engine-worker .

# 5. Start the worker
CMD ["./engine-worker"]

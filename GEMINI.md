# Project Context: Message Broker

## Project Overview
This project explores and compares various message broker systems running on Kubernetes. It includes deployment configurations (Helm values, manifests) and Java (Spring Boot) client implementations for performance testing and functional verification.

## Key Components

### Broker Deployments (Kubernetes/Helm)
- **Redpanda:** `redpanda/` (Values, Learning docs, Test pods)
- **NATS:** `nats/` (Values, Learning docs, Test pods)
- **Kafka (Strimzi):** `kafka/` (Strimzi config, Learning docs)
- **Pulsar:** `pulsar/` (Values, Learning docs)
- **RabbitMQ:** `rabbitmq/`
- **Redis:** `redis/`
- **Infrastructure:** `kind-ha-config.yaml` for local HA Kind cluster.

### Client Implementations (Java/Spring Boot)
- **NATS Demo:** `java-nats-demo/` (Producer/Consumer with OpenTelemetry)
- **Redpanda Demo:** `java-redpanda-demo/` (Producer/Consumer with OpenTelemetry)

### Observability
- **Common Configs:** `common/` contains Grafana Alloy configurations, ServiceMonitors, and Dashboards for NATS and Redpanda.

### Documentation
- `docs/` contains broker comparisons, learning plans, and test scenarios.

## Development Conventions
- **Language:** Java 17+ (Spring Boot) for clients.
- **Build Tool:** Maven.
- **Infrastructure:** Kubernetes (Kind), Helm.
- **Observability:** OpenTelemetry, Prometheus, Grafana.

## Status
- Active development of broker comparisons and client implementations.
- Focus on High Availability and Observability integration.
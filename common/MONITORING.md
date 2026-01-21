# 통합 모니터링 가이드 (Prometheus & Grafana)

## 1. 개요
K8s 환경에서 메시지 브로커를 운영할 때, 각 브로커가 노출하는 Metrics를 수집하여 대시보드로 시각화하는 것은 필수입니다.
가장 표준적인 방법인 `kube-prometheus-stack`을 사용합니다.

## 2. 설치 방법

```bash
# Helm Repo 추가
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# 네임스페이스 생성
kubectl create ns monitoring

# 설치 (기본 설정)
helm install monitoring prometheus-community/kube-prometheus-stack -n monitoring
```

## 3. 브로커별 연동 포인트

### 3.1 ServiceMonitor
Prometheus Operator는 `ServiceMonitor`라는 CRD(Custom Resource)를 통해 "어떤 파드의 어떤 포트에서 메트릭을 긁어올지" 정의합니다.
각 브로커의 Helm Chart에서 `metrics.enabled=true` 및 `serviceMonitor.enabled=true`를 설정하면 자동으로 생성됩니다.

### 3.2 Grafana Dashboard Import
Grafana Labs에는 이미 훌륭한 대시보드들이 공개되어 있습니다. ID만 알면 바로 임포트 가능합니다.

*   **Kafka (Kafka Exporter):** Dashboard ID `7589`
*   **RabbitMQ:** Dashboard ID `10991`
*   **NATS:** Dashboard ID `10640`
*   **Pulsar:** 공식 문서에서 제공하는 JSON 파일 사용
*   **Redpanda:** Redpanda 공식 대시보드 JSON 다운로드

## 4. 접속 방법
```bash
# Grafana 포트포워딩
kubectl port-forward svc/monitoring-grafana 3000:80 -n monitoring
```
브라우저에서 `localhost:3000` 접속 (admin / prom-operator)

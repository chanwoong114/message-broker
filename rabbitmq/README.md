# RabbitMQ Setup & Test Guide

## 1. Setup
```bash
# 네임스페이스 생성
kubectl create ns rabbitmq

# Helm 설치
helm install rabbitmq bitnami/rabbitmq -f values.yaml -n rabbitmq

# 테스트용 파드 생성
kubectl apply -f test-pod.yaml -n rabbitmq
```

## 2. Validation Steps
### Step 1: 웹 UI 접속 (Port-forward)
```bash
kubectl port-forward svc/rabbitmq 15672:15672 -n rabbitmq
# 브라우저에서 http://localhost:15672 접속 (user/password)
```

### Step 2: 성능 테스트 (PerfTest)
`rabbitmq-perf-test` 이미지는 Java 기반의 공식 벤치마크 툴을 포함합니다.
```bash
kubectl exec -it rabbitmq-test-client -n rabbitmq -- /bin/sh

# 내부에서
# 1000 msg/s 속도로 자동 테스트 시작
bin/runjava com.rabbitmq.perf.PerfTest --uri amqp://user:password@rabbitmq:5672 --rate 1000
```

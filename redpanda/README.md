# Redpanda Setup & Test Guide

## 1. Setup
```bash
# 네임스페이스 생성
kubectl create ns redpanda

# Helm 설치
helm install redpanda redpanda/redpanda -f values.yaml -n redpanda

# 테스트용 파드 생성
kubectl apply -f test-pod.yaml -n redpanda
```

## 2. Validation Steps
### Step 1: rpk CLI 사용 (가장 강력한 CLI)
Redpanda는 `rpk`라는 전용 CLI를 제공하며, Kafka보다 훨씬 사용하기 편합니다.
```bash
kubectl exec -it redpanda-test-client -n redpanda -- /bin/bash

# 클러스터 상태 확인
rpk cluster info
rpk cluster health

# 토픽 생성
rpk topic create my-topic -p 3 -r 3
```

### Step 2: 메시지 발행/구독
```bash
# 메시지 발행 (Interactive Mode)
rpk topic produce my-topic
> hello redpanda
> fast message
(Ctrl+C to exit)

# 메시지 구독
rpk topic consume my-topic --from-beginning
```

### Step 3: 벤치마크 (내장 툴)
Redpanda `rpk`에는 벤치마크 도구가 내장되어 있지 않으므로(별도 설치 필요), Kafka의 `perf-test` 도구를 그대로 사용하거나 `k6`를 사용하는 것이 좋습니다. 하지만 간단한 레이턴시 테스트는 가능합니다.
```bash
# (Optional) 외부 Kafka 벤치마크 툴 연결 가능
```

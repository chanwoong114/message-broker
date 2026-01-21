# Redis (Valkey) Setup & Test Guide

## 1. Setup
```bash
# 네임스페이스 생성
kubectl create ns redis

# Helm 설치
helm install redis bitnami/redis -f values.yaml -n redis

# 테스트용 파드 생성
kubectl apply -f test-pod.yaml -n redis
```

## 2. Validation Steps
### Step 1: Redis Stream 기본 사용
```bash
kubectl exec -it redis-test-client -n redis -- /bin/bash

# 내부에서
redis-cli -h redis-master -a password

# Stream에 메시지 추가 (Key: mystream)
> XADD mystream * sensor-id 1234 temperature 19.8
"1518951480106-0"

# Stream 읽기
> XRANGE mystream - +
```

### Step 2: Consumer Group 테스트
```bash
# 그룹 생성
> XGROUP CREATE mystream mygroup $

# 그룹으로 읽기
> XREADGROUP GROUP mygroup alice COUNT 1 STREAMS mystream >
```

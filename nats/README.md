# NATS JetStream Setup & Test Guide

## 1. Setup
```bash
# 네임스페이스 생성
kubectl create ns nats

# Helm 설치 (nats 디렉토리에서 실행한다고 가정)
helm install nats nats/nats -f values.yaml -n nats

# 테스트용 파드 생성
kubectl apply -f test-pod.yaml -n nats
```

## 2. Validation Steps
### Step 1: 접속 확인
```bash
kubectl exec -it nats-test-client -n nats -- /bin/sh

# 내부에서
nats context save local --server nats://nats:4222
nats pub hello "world"
```

### Step 2: JetStream 생성 및 테스트
```bash
# 스트림 생성 (이름: my-stream, 주제: orders.*)
nats stream add my-stream --subjects "orders.*" --storage file --replicas 3

# 메시지 발행
nats pub orders.new "order 1"
nats pub orders.new "order 2"

# 메시지 확인 (Consumer 없이 바로 읽기)
nats stream view my-stream

# Consumer 생성 및 구독
nats consumer add my-stream my-con --pull --deliver all
nats consumer next my-stream my-con
```

### Step 3: 벤치마크
`nats-box` 안에는 `nats-bench` 툴이 있습니다.
```bash
# 100만건 메시지 발행 테스트
nats-bench -n 1000000 -pub 1 -sub 0 orders.bench
```

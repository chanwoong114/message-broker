# Apache Pulsar Setup & Test Guide

## 1. Setup
```bash
# 네임스페이스 생성
kubectl create ns pulsar

# Helm 설치 (시간이 꽤 걸립니다)
helm install pulsar apache/pulsar -f values.yaml -n pulsar

# 테스트용 파드 생성
kubectl apply -f test-pod.yaml -n pulsar
```

## 2. Validation Steps
### Step 1: 상태 확인
```bash
kubectl exec -it pulsar-test-client -n pulsar -- /bin/bash

# 클러스터 상태
bin/pulsar-admin clusters list
bin/pulsar-admin brokers list
```

### Step 2: 메시지 발행/구독
```bash
# 구독 시작 (백그라운드 또는 별도 창)
bin/pulsar-client consume my-topic -s "my-sub" -n 0 &

# 메시지 발행
bin/pulsar-client produce my-topic --messages "hello pulsar"
```

### Step 3: 성능 테스트 (pulsar-perf)
Pulsar도 강력한 자체 벤치마크 툴을 내장하고 있습니다.
```bash
bin/pulsar-perf produce my-topic --rate 100 --size 1024
bin/pulsar-perf consume my-topic --subscriber-name perf-sub
```

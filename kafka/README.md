# Apache Kafka Setup & Test Guide

## 1. Setup
```bash
# 네임스페이스 생성
kubectl create ns kafka

# Helm 설치
helm install kafka bitnami/kafka -f values.yaml -n kafka

# 테스트용 파드 생성
kubectl apply -f test-pod.yaml -n kafka
```

## 2. Validation Steps
### Step 1: 접속 확인 및 토픽 생성
```bash
kubectl exec -it kafka-test-client -n kafka -- /bin/bash

# 내부에서
# 토픽 생성 (파티션 3, 복제본 3)
kafka-topics.sh --create --topic my-topic --partitions 3 --replication-factor 3 --bootstrap-server kafka-controller-0.kafka-controller-headless:9092
```

### Step 2: 메시지 주고 받기 (Console Producer/Consumer)
```bash
# Producer 실행
kafka-console-producer.sh --topic my-topic --bootstrap-server kafka-controller-0.kafka-controller-headless:9092
> message 1
> message 2

# (다른 터미널에서) Consumer 실행
kafka-console-consumer.sh --topic my-topic --from-beginning --bootstrap-server kafka-controller-0.kafka-controller-headless:9092
```

### Step 3: 성능 테스트 (kafka-producer-perf-test)
Kafka에는 성능 측정 도구가 내장되어 있습니다.
```bash
kafka-producer-perf-test.sh \
  --topic my-topic \
  --num-records 100000 \
  --record-size 100 \
  --throughput -1 \
  --producer-props bootstrap.servers=kafka-controller-0.kafka-controller-headless:9092
```


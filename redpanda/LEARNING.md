# Redpanda Deep Dive & Learning Guide

## 1. Redpanda 아키텍처: Kafka와의 차별점

### 1.1 Thread-per-Core Model (Seastar Framework)
Redpanda의 가장 큰 특징은 하드웨어 성능을 극한으로 끌어올리는 **Thread-per-Core** 아키텍처입니다.
*   **Kafka (JVM):** 수많은 OS 스레드를 사용하며, 컨텍스트 스위칭(Context Switching) 비용이 발생합니다.
*   **Redpanda (C++):** CPU 코어 하나당 딱 하나의 OS 스레드만 띄웁니다(Pinning). 모든 I/O는 비동기(Asynchronous)로 처리됩니다. 락(Lock)을 거의 사용하지 않아 멀티코어 확장성이 매우 뛰어납니다.

### 1.2 Storage Engine & Shadow Indexing
*   **Direct I/O:** OS의 페이지 캐시를 거치지 않고 디스크에 직접 씁니다(DMA). 데이터 유실 가능성을 최소화합니다.
*   **Shadow Indexing (Tiered Storage):** 오래된 데이터를 로컬 디스크가 아닌 S3/GCS 같은 오브젝트 스토리지로 자동으로 내립니다.
    *   **장점:** 로컬 디스크 비용 절감, "무한대"에 가까운 데이터 보관 가능.
    *   **동작:** 클라이언트가 오래된 데이터를 요청하면, Redpanda가 알아서 S3에서 가져와서 캐싱하고 서빙합니다. 클라이언트는 데이터가 어디 있는지 알 필요가 없습니다.

### 1.3 Kafka 호환성 (Drop-in Replacement)
*   Redpanda는 Kafka API를 그대로 구현했습니다.
*   기존 Kafka 클라이언트(Java, Python, Go 등)와 Kafka Connect, KSQL 등을 수정 없이 그대로 사용할 수 있습니다.
*   **Wasm(WebAssembly) Transform:** (베타 기능) 브로커 내부에서 데이터 변환 로직(Javascript/Go)을 직접 실행할 수 있습니다. 이를 "Data Ping-pong" 없이 처리하여 효율적입니다.

## 2. 운영 및 설정 가이드 (Best Practices)

### 2.1 튜닝: rpk iotune
Redpanda는 하드웨어 특성을 타므로, 설치 전 `rpk iotune`이라는 툴이 디스크와 CPU 성능을 측정하여 최적의 설정 파일(`io-config.yaml`)을 생성합니다. K8s 환경에서는 Init Container가 이를 수행하기도 합니다.

### 2.2 라이선스 (BSL vs Enterprise)
*   **Community (BSL):** 소스 코드는 볼 수 있지만, "Redpanda와 경쟁하는 서비스(예: Redpanda 관리형 서비스)"를 만들어서는 안 됩니다. 일반적인 기업 내부 사용이나 상용 제품 탑재는 대부분 허용되나 법무 검토가 필요합니다.
*   **Enterprise:** Tiered Storage, SSO, Audit Logging 등 고급 기능은 유료 라이선스가 필요할 수 있습니다.

## 3. 테스트 시나리오

### 3.1 성능 비교 (vs Kafka)
*   동일한 CPU/Memory를 할당했을 때 Redpanda가 Kafka보다 훨씬 낮은 Latency(지연 시간)를 보여주는지 확인합니다. (특히 99.9th percentile).
*   **Why?** Redpanda는 JVM GC(가비지 컬렉션) 멈춤 현상이 없기 때문입니다.

### 3.2 고속 복구
*   노드 하나를 재시작했을 때, Kafka보다 훨씬 빠르게 클러스터에 재합류(Rejoin)하는지 관찰합니다.

## 4. 납품용 솔루션 관점
"Kafka를 쓰고 싶지만 Zookeeper 관리가 싫고, JVM 튜닝이 어렵다"면 Redpanda가 답입니다. 단일 바이너리라 배포가 NATS만큼 쉽습니다. 다만 라이선스 제약 확인이 필수입니다.

# Redpanda vs RabbitMQ 비교 분석

## 1. 개요
본 문서는 Kubernetes 환경에서의 메시지 브로커 도입을 위해 **Redpanda**와 **RabbitMQ**를 비교 분석합니다.  
주요 목적은 **미들웨어로서의 통신 기능 확인**이며, 성능, 스펙, 메시지 히스토리 저장 기능을 최우선으로 검토합니다.

---

## 2. 요약 비교 (Quick View)

| 특징 | Redpanda | RabbitMQ |
| :--- | :--- | :--- |
| **기본 아키텍처** | **분산 커밋 로그 (Kafka 호환)** | **스마트 브로커 (Smart Broker, Dumb Consumer)** |
| **주요 프로토콜** | Kafka API (TCP) | AMQP 0-9-1, MQTT, STOMP |
| **메시지 히스토리** | **기본 지원** (Tiered Storage 지원으로 무제한 저장 가능) | **기본 미지원** (TTL 만료 시 삭제). 단, **Streams** 기능 사용 시 지원 가능. |
| **성능 (P99 Latency)** | **매우 안정적** (높은 부하에서도 수 ms 유지) | **변동성 큼** (부하/큐 적재량에 따라 급증 가능) |
| **권장 용도** | 대용량 이벤트 스트리밍, 로그 수집, 이벤트 소싱 | 복잡한 라우팅, 작업 큐(Task Queue), 우선순위 처리 |

---

## 3. 상세 분석 (우선순위 높음)

### 3.1. 성능 (Performance & Load Testing)

성능 비교의 핵심은 **처리량(Throughput)**과 **지연 시간(Latency)**, 특히 **P99 Latency**(하위 99% 요청의 최대 응답 시간)의 안정성입니다.

#### Redpanda
*   **아키텍처:** C++ 기반, Seastar 프레임워크 사용 (Thread-per-Core). JVM GC(Garbage Collection)가 없어 "Stop-the-world" 현상이 발생하지 않습니다.
*   **P95 / P99 Latency:**
    *   **안정성:** 부하가 증가하거나 데이터가 디스크에 쌓여도 P99 레이턴시가 매우 일정하게 유지됩니다.
    *   **수치 예시:** 최적화된 환경(NVMe)에서 수백 MB/s 처리 시에도 P99 레이턴시는 한 자릿수 ms(milliseconds) 단위를 유지하는 경향이 있습니다.
*   **처리량:** 디스크 I/O와 CPU 코어 수에 정비례하여 선형적으로 확장됩니다.

#### RabbitMQ
*   **아키텍처:** Erlang/OTP 기반. 메시지 라우팅 및 큐 관리에 강점이 있으나, 메모리 의존도가 높습니다.
*   **P95 / P99 Latency:**
    *   **저부하 시:** 큐가 비어있고 메시지가 즉시 소비될 때는 매우 빠릅니다 (1ms 미만 가능).
    *   **고부하 시:** 큐에 메시지가 적재(Backlog)되기 시작하면 메모리 압박으로 인해 P99 레이턴시가 급격히 증가(수 초 단위까지)할 수 있습니다.
*   **처리량:** 단일 큐 처리량에 한계가 있어, 수평 확장을 위해 샤딩(Sharding) 등의 추가 설정이 필요할 수 있습니다.

### 3.2. 스펙 (Requirements)

| 구분 | Redpanda | RabbitMQ |
| :--- | :--- | :--- |
| **현재 설정 (Kind)** | CPU: 1 Core<br>Mem: 1.5Gi (Max)<br>Storage: 5Gi | CPU: 100m (Req)<br>Mem: 256Mi (Req) / 1Gi (Limit) |
| **최소 사양 (Prod)** | 2 Physical Cores<br>4GB RAM | 1 vCPU<br>2GB RAM |
| **권장 사양 (Prod)** | 4+ Physical Cores<br>16GB+ RAM<br>**NVMe SSD 권장** (XFS 파일시스템 강력 권장) | 4 vCPU<br>8GB+ RAM<br>Fast SSD |
| **참고** | Redpanda는 하드웨어 성능을 끝까지 뽑아내기 위해 NVMe/XFS 환경에서 최적화되어 있습니다. 일반 SSD/EXT4에서도 동작은 가능하나 성능 저하가 발생할 수 있습니다. | RabbitMQ는 파일 시스템보다는 메모리 속도와 용량이 전체 성능에 더 큰 영향을 미칩니다. |

### 3.3. 메시지 히스토리 저장 (Message Retention)

두 시스템 모두 **"로그(Log)"** 기반의 저장 방식을 통해 메시지 히스토리를 관리합니다. RabbitMQ Streams가 도입되면서 이 부분에서 Redpanda(Kafka)와 매우 유사해졌으나, 내부 구현에는 중요한 차이가 있습니다.

#### Redpanda (Partitioned Logs)
*   **저장 구조:**
    *   `Topic` -> `Partition` -> `Segment File` (.log) 구조로 저장됩니다.
    *   데이터는 디스크에 **Append-only** 방식으로 순차 기록되며 불변(Immutable)합니다.
*   **I/O 메커니즘 (차별점):**
    *   **Direct I/O & DMA:** OS의 페이지 캐시(Page Cache)를 사용하지 않고, 디스크 컨트롤러와 직접 통신(DMA)하여 데이터를 씁니다. 이를 통해 캐시 플러시 오버헤드를 없애고 예측 가능한 레이턴시를 보장합니다.
    *   **Seastar:** 스레드 간 컨텍스트 스위칭 비용을 최소화하기 위해 CPU 코어별로 메모리와 디스크 I/O를 할당하는 구조를 가집니다.
*   **히스토리 관리:**
    *   설정된 정책(`retention.bytes`, `retention.ms`)을 초과한 오래된 **세그먼트 파일 단위**로 삭제 또는 아카이빙(Tiered Storage)됩니다.
    *   **Shadow Indexing:** 로컬 디스크 용량이 부족하면 오래된 데이터를 S3/GCS로 자동 이관하여, 클라이언트는 무한한 히스토리를 가진 것처럼 투명하게 접근할 수 있습니다.

#### RabbitMQ Streams (Single Log per Stream)
*   **저장 구조:**
    *   각 Stream은 디스크에 여러 개의 **세그먼트 파일**(`.osc`)로 구성된 하나의 거대한 로그로 저장됩니다.
    *   AMQP 큐와 달리 인덱스 파일이 별도로 존재하여 특정 오프셋으로의 빠른 탐색(Seek)을 지원합니다.
*   **I/O 메커니즘:**
    *   **OS Page Cache 의존:** Redpanda와 달리 OS의 페이지 캐시를 적극적으로 활용합니다. 커널이 자주 읽는 데이터를 메모리에 캐싱하도록 맡기는 방식입니다.
    *   **sendfile:** 데이터를 네트워크로 전송할 때 커널 영역에서 바로 소켓으로 복사하는 `sendfile` 시스템 콜을 사용하여("Zero-copy") CPU 사용량을 줄입니다.
*   **히스토리 관리:**
    *   Stream 생성 시 `x-max-age`(시간) 또는 `x-max-length-bytes`(용량) 인자를 통해 보존 정책을 설정합니다.
    *   정책에 의해 만료된 세그먼트 파일은 백그라운드에서 비동기적으로 삭제됩니다.
    *   *주의:* Stream은 일반 큐와 달리 소비자가 메시지를 읽어도 서버에서 삭제되지 않습니다 (오직 보존 정책에 의해서만 삭제됨).

#### 요약 비교
| 특징 | Redpanda | RabbitMQ Streams |
| :--- | :--- | :--- |
| **파일 시스템** | Direct I/O (OS 캐시 미사용) | OS Page Cache 적극 활용 |
| **확장성** | 파티셔닝을 통한 수평 확장 (Sharding) | 단일 Stream은 단일 노드에 종속 (복제는 가능) |
| **네트워크 전송** | Zero-copy 기술 활용 | `sendfile` (Zero-copy) 활용 |
| **장기 보존** | **Tiered Storage (S3 연동)** 강력 지원 | 로컬 디스크 용량에 제한됨 (S3 연동 기본 미지원) |

#### RabbitMQ (Classic/Quorum Queues) - *참고*
*   **기본 동작:** 소비된 메시지는 **즉시 삭제**됩니다. 보존은 주로 TTL(Time-To-Live)로 관리되며, 이는 "오래된 메시지 삭제"가 목적이지 "히스토리 저장"이 목적이 아닙니다.


---

## 4. 미들웨어 통신 적합성 및 결론

*   **히스토리 저장이 필수라면?**
    *   👉 **Redpanda**가 압도적으로 유리합니다. 아키텍처 자체가 이를 위해 설계되었습니다.
*   **복잡한 라우팅이 필수라면?**
    *   👉 **RabbitMQ**가 유리합니다. Topic Exchange 등을 통해 정교한 메시지 분배가 가능합니다.
*   **결론:**
    *   단순 파이프라인 및 고성능 버퍼링, 이벤트 소싱 목적이라면 **Redpanda**를 권장합니다.
    *   마이크로서비스 간의 복잡한 트랜잭션 처리나 작업 분배가 주 목적이라면 **RabbitMQ**를 권장합니다.

---

## 6. 참고 문헌 (References)

### Official Documentation
*   [Redpanda Documentation - Data Retention](https://docs.redpanda.com/current/manage/data-retention/)
*   [Redpanda - Tiered Storage](https://docs.redpanda.com/current/manage/tiered-storage/)
*   [RabbitMQ Documentation - Time-To-Live (TTL)](https://www.rabbitmq.com/ttl.html)
*   [RabbitMQ Streams Overview](https://www.rabbitmq.com/streams.html)

### Benchmarks & Comparisons
*   [Redpanda vs. Kafka Benchmark (Redpanda Blog)](https://redpanda.com/blog/redpanda-vs-kafka-benchmark) - *참고: 벤더사 블로그이므로 Redpanda에 유리한 결과일 수 있음*
*   [RabbitMQ Performance Benchmarks (CloudAMQP)](https://www.cloudamqp.com/blog/rabbitmq-benchmarks.html)
*   [Message Queue Performance Comparison (Medium)](https://medium.com/better-programming/message-queue-benchmark-2023-748f77f95f) - *일반적인 P99/P95 개념 설명 참조*
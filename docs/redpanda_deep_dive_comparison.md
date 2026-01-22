# Apache Kafka vs Redpanda: 아키텍처 심층 분석 및 기술 비교

**작성일:** 2026-01-20 (업데이트: 리더 선출 메커니즘 심층 분석 추가)
**목적:** 단순한 기능 비교를 넘어, 내부 동작 원리(Internals)와 아키텍처 차이를 분석하여 고성능 데이터 파이프라인 설계를 위한 기술적 근거를 마련함.

---

## 1. 아키텍처 철학의 차이 (Design Philosophy)

### 1.1 Apache Kafka: "범용성과 OS 의존"
Kafka는 2011년 LinkedIn에서 개발될 당시의 하드웨어 환경(HDD, 1G Network)과 JVM 생태계를 기반으로 설계되었습니다.
*   **Thread Pool Model:** 수많은 스레드가 요청을 처리하며, OS 스케줄러에 의해 컨텍스트 스위칭이 빈번하게 발생합니다.
*   **Page Cache Reliance:** 자체 캐싱 대신 OS의 페이지 캐시에 전적으로 의존합니다. "OS가 메모리 관리를 더 잘한다"는 철학입니다.

### 1.2 Redpanda: "하드웨어 최적화와 독립"
Redpanda는 최신 하드웨어(NVMe SSD, 100G Network, Many-core CPU)의 성능을 100% 끌어내기 위해 설계되었습니다.
*   **Thread-per-Core (TPC):** **Seastar** 프레임워크를 기반으로, CPU 코어 하나당 하나의 스레드만 할당하고 고정(Pinning)합니다.
*   **Share-nothing:** 코어끼리 메모리나 락(Lock)을 공유하지 않습니다. 데이터도 코어별로 파티셔닝되어 처리됩니다. 락 경합(Contention)이 '0'에 수렴합니다.

---

## 2. 핵심 기술 심층 비교 (Deep Dive)

### 2.1 I/O 모델 및 메모리 관리
| 구분 | Apache Kafka (JVM) | Redpanda (C++) |
| :--- | :--- | :--- |
| **I/O 방식** | **Buffered I/O.** OS Page Cache를 거쳐 디스크에 씀. | **Direct I/O (DMA).** Page Cache를 우회하고 디스크 컨트롤러와 직접 통신. |
| **장점** | 구현이 쉽고 OS 튜닝으로 성능 향상 가능. | 데이터 유실 원천 차단(OS Crash 시 안전), GC 없음. |
| **단점** | **Double Buffering:** 데이터가 JVM 힙과 OS 캐시에 중복 저장됨. | 구현 난이도가 매우 높음 (Redpanda 팀이 해결). |
| **Latency** | **GC Spike 존재.** 가비지 컬렉션 시 멈춤 현상. | **Flat Latency.** 꼬리 지연(Tail Latency, P99)이 매우 낮고 일정함. |

### 2.2 합의 알고리즘 및 리더 선출 (Consensus & Leader Election) - *중요*

Kafka와 Redpanda 모두 리더-팔로워 모델을 사용하지만, 리더를 **"누가, 어떻게 뽑느냐"**에서 결정적인 차이가 있습니다.

#### A. Apache Kafka (KRaft): "중앙집권형" (Top-down)
*   **방식:** 클러스터 전체를 관리하는 **컨트롤러(Controller)** 노드가 존재합니다.
*   **리더 선출:**
    1.  브로커들 중 하나가 Raft로 컨트롤러(반장)가 됩니다.
    2.  이 컨트롤러가 모든 파티션의 상태를 감시하다가, 리더가 죽으면 **"다음 리더는 너야!"**라고 지명(Assignment)합니다.
*   **한계:** 컨트롤러에 부하가 집중되거나 컨트롤러 자체가 장애가 날 경우, 새로운 컨트롤러가 선출될 때까지 **클러스터 전체의 파티션 관리(리더 변경 등)가 일시 정지**될 수 있습니다. (Recovery Time Objective 증가)

#### B. Redpanda: "지방자치형" (Partition-based Raft)
*   **방식:** 중앙 컨트롤러에 의존하지 않고, **각 파티션(Partition) 자체가 독립적인 Raft 그룹**을 형성합니다.
*   **리더 선출:**
    1.  파티션 0번을 가진 노드들(0, 1, 2)끼리 자체적으로 투표합니다. "내가 리더 할게!"
    2.  과반수 동의를 얻으면 즉시 리더가 됩니다. 중앙의 허락이 필요 없습니다.
*   **우위:**
    *   **병렬 복구:** 수천 개의 파티션 리더가 동시에 죽어도, 각자 알아서 투표하므로 복구 속도가 획기적으로 빠릅니다.
    *   **단순함:** 데이터 복제와 메타데이터 관리가 Raft라는 하나의 알고리즘으로 통일되어 있습니다. (Kafka는 ISR + Raft 혼용)

---

## 3. 심화 주제: Zookeeper vs KRaft vs Redpanda Raft

Kafka가 Zookeeper를 제거(KRaft)했음에도, 왜 Redpanda가 구조적으로 더 우수한가에 대한 분석입니다.

### 3.1 과거: Kafka + Zookeeper (이중 구조)
*   **역할:** Zookeeper는 브로커 상태 감시, 리더 선출, 설정 관리를 담당하는 **외부 중앙 관리소**였습니다.
*   **문제점:** 운영 복잡도 증가 및 병목 현상 발생.

### 3.2 현재: Kafka KRaft (내재화된 관리소)
*   **변화:** Zookeeper 프로세스 제거, 브로커 내장 컨트롤러 도입.
*   **한계:** 여전히 **JVM 위에서 동작**하며, 데이터 복제에는 Raft가 아닌 기존 ISR 방식을 사용하여 아키텍처가 이원화되어 있음.

### 3.3 혁신: Redpanda Raft (완전한 분산 합의)
*   **Data Plane Raft:** 메시지 데이터 자체도 Raft로 복제.
*   **우위:** 메타데이터와 데이터 평면이 동일한 알고리즘(Raft)으로 동작하여 아키텍처가 단순하고 견고함.

---

## 4. 생태계 및 호환성 (Ecosystem Compatibility)

### 4.1 Kafka API 호환성
Redpanda는 Kafka 프로토콜을 바이너리 수준에서 구현했습니다.
*   **결과:** Java, Python, Go 등 모든 Kafka 클라이언트가 Redpanda를 Kafka로 인식합니다.
*   **Kafka Connect / Streams / KSQL:** 그대로 사용 가능. `bootstrap.servers`만 바꾸면 됩니다.

### 4.2 Schema Registry & Wasm
*   **Schema Registry:** 내장(Built-in)되어 있어 별도 설치 불필요.
*   **Wasm Transforms:** 브로커 내부에서 WebAssembly 코드를 실행하여 데이터 변환 수행 (Redpanda 전용).

---

## 5. 종합 비교표 (The Ultimate Comparison)

| 비교 항목 | Apache Kafka (KRaft) | Redpanda | 승자 |
| :--- | :--- | :--- | :--- |
| **언어/런타임** | Java / Scala (JVM) | C++ (No Runtime) | **Redpanda** |
| **리더 선출** | 컨트롤러가 지명 (중앙집중) | 파티션별 자치 투표 (분산) | **Redpanda** |
| **메모리 사용** | 높음 (Heap + Page Cache) | 낮음 (자동 튜닝) | **Redpanda** |
| **Latency (P99)** | GC로 인해 튐 | 매우 낮고 일정함 | **Redpanda** |
| **처리량 (Throughput)** | 높음 | **압도적으로 높음** (NVMe 활용 시) | **Redpanda** |
| **관리 도구** | Kafka UI (별도 설치) | Redpanda Console (내장/제공) | **Redpanda** |
| **라이선스** | Apache 2.0 (완전 무료) | BSL (상용 서비스 제한) | **Kafka** |

---

## 6. 결론: 엔지니어의 선택 가이드

1.  **극한의 성능과 운영 편의성**을 원한다면 **Redpanda**가 답입니다. 하드웨어 스펙이 좋을수록 Kafka와의 격차는 벌어집니다.
2.  **레거시 호환성**과 **방대한 레퍼런스(구글링)**가 중요하다면 **Kafka**가 안전한 선택입니다.
3.  **"Kafka를 쓰고 싶은데 관리하기 싫다"**면 Redpanda가 최고의 대안입니다.

> **작성자의 한마디:** Redpanda는 단순한 'Kafka 복제품'이 아닙니다. Kafka가 10년 전에 하고 싶었지만 하드웨어 제약으로 못 했던 이상향을, 현대 기술로 실현한 **"Kafka의 완성형"**에 가깝습니다.

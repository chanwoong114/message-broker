# 단일 노드 운영 비교: Kafka(KRaft) vs Redpanda vs RabbitMQ

**작성일:** 2026-01-26  
**환경:** Single Node (Non-Clustered) Deployment  
**목적:** 개발 환경, 엣지 컴퓨팅, 또는 소규모 서비스를 위한 단일 노드 브로커 선택 가이드

---

## 1. 개요
전통적으로 Kafka는 Zookeeper라는 외부 의존성 때문에 단일 노드(Single Node)로 운영하기에 무겁고 복잡했습니다. 하지만 **KRaft(Kafka Raft Metadata) 모드**의 도입으로 Zookeeper가 제거되면서 상황이 달라졌습니다. 본 문서는 단일 노드 환경에서 각 브로커의 장단점을 분석합니다.

---

## 2. Kafka (with KRaft Mode)

### 2.1. 변화된 점
*   **Zookeeper 제거:** 이제 Kafka 프로세스 하나만 띄우면 됩니다. (`kafka-storage.sh format` 후 실행)
*   **배포 용이성:** 예전처럼 컨테이너 2개(Zookeeper + Kafka)를 띄울 필요가 없어 설정이 간소화되었습니다.

### 2.2. 단일 노드 장단점
*   **장점:**
    *   **표준의 힘:** 가장 방대한 생태계와 라이브러리 지원.
    *   **KRaft의 경량화:** Zookeeper 통신 오버헤드가 사라져 메타데이터 처리가 빨라짐.
*   **단점 (여전한 한계):**
    *   **JVM 기반:** Java 가상 머신(JVM) 위에서 돌아가므로, 아무리 가벼워져도 **초기 메모리 점유(Heap)**가 큽니다. (최소 512MB~1GB 권장)
    *   **콜드 스타트:** JVM 웜업 시간이 필요하여 프로세스 시작 속도가 상대적으로 느립니다.

---

## 3. Redpanda (C++ Kafka)

### 3.1. 특징
*   **단일 바이너리:** 의존성 없이 `rpk` 명령어 하나나 `redpanda` 실행 파일 하나로 끝납니다.
*   **Thread-per-Core:** CPU 코어 수에 맞춰 스레드를 딱 하나씩만 쓰므로 컨텍스트 스위칭 비용이 없습니다.

### 3.2. 단일 노드 장단점
*   **장점 (압도적):**
    *   **부팅 속도:** 1초 이내. (JVM이 없으므로 즉시 시작)
    *   **설치 편의성:** `rpk redpanda start` 한 줄이면 끝. 개발용 로컬 환경(Docker Desktop, Mac)에서 가장 쾌적함.
*   **단점:**
    *   Wasm(WebAssembly) 같은 고급 변환 기능을 쓰면 리소스를 좀 더 먹을 수 있음.

---

## 4. RabbitMQ (Stream)

### 4.1. 특징
*   **Erlang VM:** JVM보다는 가볍지만, 동시성 처리를 위한 Erlang 런타임이 필요합니다.
*   **하이브리드:** 단일 노드에서 큐(Queue)와 스트림(Stream)을 동시에 쓸 수 있다는 게 최대 강점입니다.

### 4.2. 단일 노드 장단점
*   **장점:**
    *   **다목적:** 복잡한 라우팅과 로그 저장을 서버 하나로 해결 가능.
*   **단점:**
    *   **메모리 관리:** 메시지가 쌓이면 인덱스 관리를 위해 메모리를 많이 씁니다. 단일 노드 리소스가 꽉 차면 전체 서비스가 멈출 수 있습니다 (Memory Alarm).

---

## 5. 종합 비교 요약

| 구분 | Kafka (KRaft) | Redpanda | RabbitMQ |
| :--- | :--- | :--- | :--- |
| **언어/런타임** | Java / JVM | **C++ / Native** | Erlang / BEAM |
| **기본 메모리 정책** | JVM Heap 설정 필요 (OS 캐시 별도) | **메모리 전체 선점 (Pre-allocation)** | 메모리 알람/워터마크 방식 |
| **최소 실행 메모리** | ~1GB 권장 (하한선 높음) | **~1GB 권장 (최하 512MB 가능\*)** | **~256MB 이하 가능** |
| **운영 권장 메모리** | 4GB+ (힙 + 페이지 캐시) | **2GB+ (코어당 할당 권장)** | 2GB+ (워크로드에 따라) |
| **부팅 속도** | 느림 (수 초~수십 초) | **매우 빠름 (< 1초)** | 빠름 (수 초) |

> **\* Redpanda 메모리 주의사항:** 기술적으로 512MB 설정으로 프로세스 기동은 가능하나, 실제 메시지 처리 시 OOM(Out of Memory) 크래시 가능성이 매우 높습니다. 개발 환경에서도 최소 **1GB** 할당이 실무적인 마지노선으로 권장됩니다.

### 🏆 최종 추천 및 메모리 요약

1.  **Redpanda의 유연성:** Redpanda는 C++ 기반으로 작성되어 JVM 같은 거대한 런타임 오버헤드가 없습니다. 따라서 매우 적은 메모리 설정으로도 프로세스를 띄우는 것이 가능합니다. 단, 프로덕션 환경에서는 성능 보장을 위해 2GB 이상(코어당) 할당을 강력히 권장합니다.

2.  **Kafka (KRaft):** Zookeeper가 빠졌어도 JVM의 무게는 여전합니다. 힙 메모리를 너무 줄이면 GC 오버헤드로 인해 단일 노드 전체가 느려질 수 있습니다. 최소 1GB는 할당해야 실습이 가능합니다.

3.  **RabbitMQ:** 세 브로커 중 가장 적은 메모리로도 "일단 돌아는 가는" 상태를 만들기 가장 쉽습니다. 소규모 단일 노드 서비스에 가장 경제적입니다.

---

### 🔗 관련 레퍼런스
*   [Redpanda 공식 문서: 시스템 요구 사양 (Prod)](https://docs.redpanda.com/current/deploy/deployment-option/self-hosted/manual/production/production-deployment/#system-requirements)
*   [Redpanda GitHub Discussion: 개발 환경 리소스 최적화 가이드 (1GB 권장)](https://github.com/redpanda-data/redpanda/discussions/23089)
*   [Redpanda GitHub Issue #9533: 저부하 환경에서의 OOM 사례 분석](https://github.com/redpanda-data/redpanda/issues/9533)
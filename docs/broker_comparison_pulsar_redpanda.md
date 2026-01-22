# Apache Pulsar vs Redpanda: 차세대 메시징 시스템 비교

**작성일:** 2026-01-20  
**목적:** "Post-Kafka" 시대를 이끄는 두 주자, Pulsar와 Redpanda의 아키텍처 및 활용 사례 심층 비교.

---

## 1. 핵심 철학의 차이 (Philosophy)

### 1.1 Redpanda: "Simplicity & Performance" (단순함과 성능)
*   **접근법:** Kafka의 복잡함(JVM, Zookeeper)을 제거하고, C++와 최신 하드웨어 기술로 **Kafka API를 완벽하게, 더 빠르게 구현**하는 것에 집중합니다.
*   **목표:** "가장 빠르고 운영하기 쉬운 Kafka"가 되는 것.

### 1.2 Apache Pulsar: "Scalability & Features" (확장성과 기능)
*   **접근법:** Kafka의 아키텍처적 한계(스토리지와 컴퓨팅의 결합)를 극복하기 위해, **컴퓨팅(Broker)과 스토리지(BookKeeper)를 완벽하게 분리**한 클라우드 네이티브 아키텍처를 제시합니다.
*   **목표:** "전 세계를 연결하는 통합 메시징 플랫폼" (스트리밍 + 큐잉 + 서버리스 함수).

---

## 2. 아키텍처 상세 비교

| 구분 | Redpanda | Apache Pulsar | 승자 |
| :--- | :--- | :--- | :--- |
| **구조** | **Monolithic (단일 계층).** 브로커가 저장소 역할도 겸함. | **Multi-tier (다중 계층).** 브로커(Stateless) + 북키(Stateful) + 주키퍼(Metadata). | **Redpanda** (운영 용이성) |
| **배포/운영** | 바이너리 하나만 실행하면 됨. 매우 간편. | 최소 3가지 컴포넌트(Broker, Bookie, ZK)를 관리해야 함. 매우 복잡. | **Redpanda** |
| **확장성** | 노드 추가 시 데이터 리밸런싱(이동) 필요. | 브로커는 즉시 확장 가능(Stateless). 스토리지는 세그먼트 단위로 자동 분산. | **Pulsar** (무중단 확장) |
| **멀티 테넌시** | 논리적 분리 지원. | **네이티브 지원.** 테넌트/네임스페이스별 완벽한 격리 및 리소스 제어. | **Pulsar** |
| **Geo-Replication** | 지원하나 설정 필요. | **Core Feature.** 글로벌 리전 간 데이터 동기화가 설정만으로 가능. | **Pulsar** |

---

## 3. 메시지 모델: 스트림 vs 큐

### 3.1 Redpanda (Kafka Model)
*   **Dumb Broker, Smart Client:** 메시지를 순차적으로 저장하고, 클라이언트가 오프셋을 관리하며 읽어갑니다.
*   **스트리밍(Streaming)** 처리에 최적화되어 있습니다.

### 3.2 Pulsar (Hybrid Model)
*   **Unified Messaging:** Kafka 같은 **스트리밍**과 RabbitMQ 같은 **큐잉(Queueing)**을 동시에 지원합니다.
*   **구독 모드:**
    *   `Exclusive/Failover`: 순서 보장 (Kafka 스타일).
    *   `Shared`: 여러 컨슈머가 라운드 로빈으로 나눠 가짐 (RabbitMQ 스타일).
    *   `Key_Shared`: 키별로 순서 보장하며 병렬 처리.

---

## 4. 저장소 아키텍처 (Storage)

### 4.1 Redpanda (Local + Tiered)
*   로컬 디스크(NVMe) 성능을 극한으로 활용합니다.
*   오래된 데이터는 S3로 내리는 Tiered Storage를 지원합니다.

### 4.2 Pulsar (Segmented Streams)
*   데이터를 잘게 쪼개서(Segment) 여러 Bookie(저장소 노드)에 분산 저장합니다.
*   **장점:** 특정 노드에 데이터가 쏠리는 Hotspot 문제가 거의 없습니다.
*   **단점:** 네트워크 홉(Network Hop)이 한 번 더 발생하여(브로커->북키), 이론상 Latency는 Redpanda보다 느릴 수 있습니다.

---

## 5. 결론: 언제 무엇을 써야 할까?

### ✅ Redpanda를 선택해야 하는 경우
1.  **"운영이 쉬워야 한다."** (가장 큰 이유) 엔지니어링 리소스가 부족하거나 소규모 팀일 때.
2.  **"Kafka 생태계(도구, 라이브러리)를 그대로 쓰고 싶다."**
3.  **"극강의 Low Latency가 필요하다."** (HFT, 실시간 광고 등)

### ✅ Apache Pulsar를 선택해야 하는 경우
1.  **"SaaS 서비스를 만들어서 수많은 고객사에게 메시징 기능을 제공해야 한다."** (멀티 테넌시 필수)
2.  **"전 세계 여러 리전에 데이터를 실시간 동기화해야 한다."** (Geo-Replication)
3.  **"스트리밍과 큐(Queue) 기능이 둘 다 필요한데, 시스템을 하나로 통일하고 싶다."**
4.  **"데이터 양이 페타바이트 급이라서 무중단 확장이 밥 먹듯이 일어나야 한다."**

### 🏁 한 줄 요약
*   **Redpanda:** "Kafka의 고성능/편의성 업그레이드 버전"
*   **Pulsar:** "Kafka가 해결하지 못한 아키텍처 문제를 해결한 엔터프라이즈 플랫폼" (단, 운영 난이도 높음)

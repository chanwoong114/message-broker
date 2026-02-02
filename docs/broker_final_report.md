# 메시지 브로커 4대장 비교 분석: Kafka vs Redpanda vs NATS vs RabbitMQ

**작성일:** 2026-01-20 (업데이트: 보상 트랜잭션 전략 추가)
**목적:** 애플리케이션 요구사항(성능, 기능, 운영)에 따른 최적의 메시지 브로커 선정 가이드.

---

## 1. 한눈에 보는 요약 (Executive Summary)

| 구분 | **Apache Kafka** | **Redpanda** | **NATS JetStream** | **RabbitMQ** |
| :--- | :--- | :--- | :--- | :--- |
| **핵심 철학** | **"대용량 로그 저장소"** | **"더 빠르고 쉬운 Kafka"** | **"초경량/초고속 연결"** | **"똑똑한 우체국장"** |
| **주 무기** | 표준 생태계, 안정성 | 성능(C++), Kafka 호환 | 가벼움(Go), 운영 편의 | **복잡한 라우팅**, 신뢰성 |
| **아키텍처** | JVM (무거움) | Thread-per-Core (가벼움) | Single Binary (초경량) | Erlang (안정적) |
| **메시지 모델** | Log (Append-only) | Log (Append-only) | Stream / Log | **Queue (Pop & Delete)** |
| **적합한 용도** | 데이터 파이프라인, 분석 | 고성능 파이프라인 | MSA 통신, IoT, 엣지 | **복잡한 업무 로직**, 레거시 |

---

## 2. 상세 비교 분석

### 2.1 메시지 처리 방식 (Message Model)
*   **Kafka / Redpanda / NATS:** **"Dumb Broker, Smart Client"**
    *   브로커는 단순히 저장만 하고, 클라이언트가 알아서 가져갑니다.
    *   데이터를 소비해도 삭제되지 않으므로(설정 기간 동안), **다시 읽기(Replay)**가 가능합니다.
*   **RabbitMQ:** **"Smart Broker, Dumb Consumer"**
    *   브로커가 메시지를 **라우팅(Exchange)**하고, 컨슈머에게 **할당(Push)**합니다.
    *   **Ack(확인)**를 받으면 큐에서 **삭제**합니다. (영구 저장이 목적이 아님)

### 2.2 성능 및 확장성 (Performance & Scalability)
*   **처리량(Throughput):** **Redpanda > NATS >= Kafka >>> RabbitMQ**
*   **확장성(Scale-out):** Kafka/Redpanda/NATS는 파티션 기반으로 수평 확장이 자연스러우나, RabbitMQ는 클러스터링 시 네트워크 부하가 큽니다.

### 2.3 운영 편의성 (Operations)
*   **NATS / Redpanda:** 단일 바이너리. 설정 파일 하나. **(최상)**
*   **RabbitMQ:** Erlang VM 위에서 돌아가며, 관리 UI가 훌륭합니다. **(중)**
*   **Kafka:** Zookeeper(또는 KRaft), JVM 튜닝 등 손이 많이 갑니다. **(하)**

---

## 3. 심화 주제: 분산 트랜잭션과 보상 처리 (Saga Pattern)

MSA 환경에서 "주문 실패 시 결제 취소"와 같은 **보상 트랜잭션**을 어떻게 구현해야 하는지에 대한 비교입니다.

### 3.1 공통 전제
모든 브로커는 비즈니스 로직(보상 처리)을 자동으로 수행해주지 않습니다. **애플리케이션이 "실패 이벤트"를 발행하고, 이를 구독하여 취소 로직을 수행하는 Saga 패턴**을 구현해야 합니다.

### 3.2 Kafka / Redpanda: "강력한 도구 지원"
*   **Atomic Transaction API:** `beginTransaction` -> `commit`을 지원하여, "이벤트 A 처리 완료"와 "이벤트 B 발행"을 **원자적(Atomic)**으로 묶을 수 있습니다. 데이터 정합성 보장에 매우 유리합니다.
*   **Kafka Streams:** 상태(State)를 저장하고 관리하는 프레임워크를 제공하여, 복잡한 Saga 로직(예: "결제 대기 중" 상태 유지)을 코드로 구현하기 쉽습니다.

### 3.3 NATS JetStream: "가볍고 직접적인 구현"
*   **Transaction 미지원:** 여러 메시지를 묶어서 커밋하는 기능은 없습니다.
*   **KV Store 활용:** JetStream 내장 Key-Value 저장소를 활용하여 트랜잭션 상태(State)를 공유하고 관리하는 방식으로 Saga 패턴을 구현합니다.
*   **구현:** 라이브러리에 의존하기보다, 개발자가 명시적으로 보상 이벤트를 발행하는 코드를 작성해야 합니다.

---

## 4. RabbitMQ 심층 분석: 언제 써야 할까?

RabbitMQ는 "느리고 구식"이 아닙니다. **Kafka가 못 하는 일**을 아주 잘합니다.

### ✅ RabbitMQ를 선택해야 하는 경우 (Killer Features)
1.  **복잡한 라우팅 (Routing):** Topic Exchange를 통한 정교한 메시지 분배.
2.  **작업 큐 (Task Queue):** 오래 걸리는 작업을 여러 워커에게 분배하고, 실패 시 재할당(Redelivery).
3.  **우선순위 큐 (Priority Queue):** VIP 메시지 우선 처리.

---

## 5. 최종 결론 및 추천

| 시나리오 | 추천 브로커 | 이유 |
| :--- | :--- | :--- |
| **"전사 데이터 허브를 구축하자"** | **Kafka / Redpanda** | 모든 데이터를 모으고, 다시 꺼내 쓰고, 분석하기 위함. |
| **"MSA끼리 빠르고 가볍게 통신하자"** | **NATS JetStream** | 운영 부담 없고, 빠르고, 쿠버네티스와 찰떡궁합. |
| **"복잡한 주문 처리/결제 시스템을 만들자"** | **RabbitMQ** | 메시지 하나하나의 라우팅과 처리 보장(Ack)이 중요함. |
| **"Kafka를 쓰고 싶은데 너무 느려/비싸/어려워"** | **Redpanda** | Kafka의 모든 장점 + 성능/운영 편의성. |

### 🏁 한 줄 요약
*   **데이터를 "흐르게(Stream)" 하려면:** Kafka, Redpanda, NATS
*   **데이터를 "분배(Route)" 하려면:** RabbitMQ
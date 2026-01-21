# 메시지 브로커 4대장 비교 분석: Kafka vs Redpanda vs NATS vs RabbitMQ

**작성일:** 2026-01-20  
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
    *   **장점:** "이 메시지는 A에게, 저 메시지는 B에게" 같은 **복잡한 분기 처리**가 가능합니다.

### 2.2 성능 및 확장성 (Performance & Scalability)
*   **처리량(Throughput):**
    *   **Redpanda > NATS >= Kafka >>> RabbitMQ**
    *   RabbitMQ는 메시지마다 라우팅/삭제 로직이 돌기 때문에 대용량 처리(초당 수십만 건 이상)에는 불리합니다.
*   **확장성(Scale-out):**
    *   **Kafka/Redpanda/NATS:** 파티션/Subject 기반으로 수평 확장이 매우 자연스럽습니다.
    *   **RabbitMQ:** 클러스터링은 되지만, 큐 미러링(Mirroring) 방식이라 네트워크 부하가 크고 확장에 한계가 있습니다.

### 2.3 운영 편의성 (Operations)
*   **NATS / Redpanda:** 단일 바이너리. 설정 파일 하나. **(최상)**
*   **RabbitMQ:** Erlang VM 위에서 돌아가므로 관리가 까다로울 수 있지만, 오래된 만큼 안정적입니다. 관리 UI(Management Plugin)가 기본 내장되어 매우 훌륭합니다. **(중)**
*   **Kafka:** Zookeeper(또는 KRaft), JVM 튜닝, 리밸런싱 관리 등 손이 많이 갑니다. **(하)**

---

## 3. RabbitMQ 심층 분석: 언제 써야 할까?

RabbitMQ는 "느리고 구식"이 아닙니다. **Kafka가 못 하는 일**을 아주 잘합니다.

### ✅ RabbitMQ를 선택해야 하는 경우 (Killer Features)
1.  **복잡한 라우팅 (Routing):**
    *   "서울 지역의 '에러' 로그는 A팀에게, '정보' 로그는 B팀에게 보내줘." (Topic Exchange)
    *   이런 로직을 Kafka에서 하려면 별도의 스트림즈 앱을 짜야 하지만, RabbitMQ는 설정만으로 됩니다.
2.  **작업 큐 (Task Queue):**
    *   오래 걸리는 작업(이미지 인코딩 등)을 여러 워커에게 골고루 나눠주고, **실패하면 다른 워커에게 다시 주는(Redelivery)** 기능이 강력합니다.
3.  **우선순위 큐 (Priority Queue):**
    *   "VIP 고객의 주문은 먼저 처리해줘." (Kafka는 기본적으로 지원 안 함)
4.  **다양한 프로토콜:**
    *   MQTT(IoT), STOMP 등 다양한 프로토콜을 플러그인으로 지원합니다.

---

## 4. 최종 결론 및 추천

| 시나리오 | 추천 브로커 | 이유 |
| :--- | :--- | :--- |
| **"전사 데이터 허브를 구축하자"** | **Kafka / Redpanda** | 모든 데이터를 모으고, 다시 꺼내 쓰고, 분석하기 위함. |
| **"MSA끼리 빠르고 가볍게 통신하자"** | **NATS JetStream** | 운영 부담 없고, 빠르고, 쿠버네티스와 찰떡궁합. |
| **"복잡한 주문 처리/결제 시스템을 만들자"** | **RabbitMQ** | 메시지 하나하나의 라우팅과 처리 보장(Ack)이 중요함. |
| **"Kafka를 쓰고 싶은데 너무 느려/비싸/어려워"** | **Redpanda** | Kafka의 모든 장점 + 성능/운영 편의성. |

### 🏁 한 줄 요약
*   **데이터를 "흐르게(Stream)" 하려면:** Kafka, Redpanda, NATS
*   **데이터를 "분배(Route)" 하려면:** RabbitMQ

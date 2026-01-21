# NATS JetStream Deep Dive & Learning Guide

## 1. NATS & JetStream 아키텍처 심층 분석

### 1.1 Core NATS vs JetStream
NATS는 원래 "Fire-and-Forget" 방식의 초고속 메시징 시스템이었습니다. 메시지를 보내면 누군가 듣고 있으면 받고, 없으면 사라집니다. 이를 **Core NATS**라고 합니다.
하지만 데이터 유실이 없어야 하는 엔터프라이즈 환경을 위해 **JetStream**이라는 Persistence(영속성) 계층이 추가되었습니다.

*   **Core NATS:** At-most-once delivery. 소켓 통신 수준의 속도. 펍/섭 패턴.
*   **JetStream:** At-least-once delivery. Kafka와 유사한 로그 기반 저장소. Stream, Consumer 개념 도입.

### 1.2 JetStream의 핵심 컴포넌트

1.  **Stream (스트림):**
    *   메시지를 저장하는 불변의 로그(Immutable Log)입니다. Kafka의 Topic과 유사하지만 더 유연합니다.
    *   하나의 Stream은 여러 개의 "Subject(주제)"를 캡처할 수 있습니다. (예: `orders.*`를 캡처하는 `ORDERS` 스트림).
    *   **Retention Policy:**
        *   `Limits`: 개수, 용량, 시간(TTL) 기반 삭제.
        *   `Interest`: 현재 구독 중인 Consumer가 다 읽으면 삭제 (Work Queue 패턴).
        *   `WorkQueue`: 메시지가 한 번 소비되면 삭제됨.

2.  **Consumer (컨슈머):**
    *   Stream에 저장된 데이터를 읽어가는 뷰(View)입니다. Kafka의 Consumer Group과 유사하지만 서버 측에 상태가 저장됩니다.
    *   **Durable Consumer:** 이름을 가지며, 연결이 끊겨도 어디까지 읽었는지 서버가 기억합니다.
    *   **Ephemeral Consumer:** 연결이 끊기면 사라집니다.
    *   **Ack Policy:**
        *   `Explicit`: 클라이언트가 명시적으로 `ack()`를 보내야 함.
        *   `All`: 받은 메시지 중 가장 마지막 것만 ack하면 이전 것도 모두 처리된 것으로 간주.
        *   `None`: 보내자마자 처리된 것으로 간주 (속도 중시).

3.  **Subject-Based Messaging:**
    *   NATS의 가장 큰 특징은 **Subject(주제)** 기반 라우팅입니다.
    *   물리적인 IP나 DNS가 아닌 논리적인 이름(`service.us.east`)으로 통신합니다.
    *   와일드카드 지원: `*` (단일 토큰), `>` (나머지 전체). 예: `time.us.>`는 `time.us.east`, `time.us.east.atlanta` 모두 매칭.

### 1.3 RAFT 알고리즘과 클러스터링
JetStream은 데이터의 안전한 복제를 위해 **RAFT 합의 알고리즘**을 사용합니다.
*   3대 이상의 노드로 클러스터를 구성하면, 하나의 리더(Leader)와 팔로워(Follower)가 선출됩니다.
*   쓰기 작업은 리더를 통해 이루어지며, 과반수 이상의 노드에 데이터가 저장되어야 성공(Ack)을 반환합니다.
*   **장점:** Zookeeper 같은 외부 코디네이터가 전혀 필요 없습니다. 단일 바이너리에 모든 것이 포함되어 있습니다.

## 2. 운영 및 설정 가이드 (Best Practices)

### 2.1 스토리지 전략
*   **File Store:** 디스크에 저장. 중요 데이터용. NVMe SSD 권장.
*   **Memory Store:** RAM에 저장. 빠른 속도, 재시작 시 사라짐. 캐시성 데이터용.
*   **Replication:** 프로덕션에서는 `R=3` (3중 복제)을 권장합니다. 노드 하나가 죽어도 데이터 유실이 없고 서비스가 지속됩니다.

### 2.2 성능 튜닝 포인트
*   **Max Pending:** 컨슈머가 Ack를 안 보내고 한 번에 가져갈 수 있는 메시지 수. 너무 크면 재전송 시 부하, 너무 작으면 처리량 저하.
*   **Batch Size:** 클라이언트 라이브러리에서 `Fetch(100)`과 같이 배치로 가져오는 것이 건건이 가져오는 것보다 훨씬 빠릅니다.

## 3. 테스트 시나리오별 상세 설명

### 3.1 장애 대응 (Chaos Testing)
*   **시나리오:** 3개의 NATS 파드 중 리더 파드를 `kubectl delete pod`로 삭제.
*   **기대 결과:** 즉시 다른 팔로워 중 하나가 리더로 승격. 클라이언트는 잠시 끊겼다가 자동으로 재접속(Reconnect)되어야 함. 데이터 유실 0.

### 3.2 뇌 분리 (Split Brain)
*   **시나리오:** 네트워크 정책으로 파드 간 통신 차단.
*   **기대 결과:** 과반수(Quorum)를 잃은 소수 파드들은 쓰기를 거부하여 데이터 정합성을 지킴.

## 4. 왜 납품용 솔루션으로 NATS인가?
1.  **가벼움:** 도커 이미지가 20MB 미만. JVM 기반(Kafka)에 비해 메모리 사용량이 극히 적음.
2.  **단순함:** Zookeeper 없음. 설정 파일 하나로 끝.
3.  **멀티 패턴:** Pub/Sub(실시간 이벤트), Stream(로그 저장), KV Store(설정 저장), Object Store(큰 파일 저장)를 모두 지원.
4.  **보안:** 계정(Account) 격리, 멀티 테넌시 지원이 강력함.

---
**학습 팁:** `nats-box` 컨테이너에 들어가서 `nats` CLI 도구를 적극 활용하세요.
`nats stream add` -> `nats pub` -> `nats sub` 과정을 손으로 쳐보는 것이 가장 빠릅니다.

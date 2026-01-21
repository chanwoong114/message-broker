# Apache Pulsar Deep Dive & Learning Guide

## 1. Pulsar 아키텍처: 분리의 미학

### 1.1 Compute (Broker) & Storage (BookKeeper) Separation
Pulsar의 가장 큰 혁신은 **Stateless Broker**와 **Stateful Storage**의 분리입니다.
*   **Broker:** 메시지를 받고 전달하고 캐싱하는 역할만 합니다. 디스크에 저장하지 않습니다. 따라서 브로커 확장은 매우 쉽고 빠릅니다.
*   **Apache BookKeeper (Bookie):** 실제 데이터를 저장하는 저장소입니다. Ledger(장부)라는 단위로 데이터를 관리합니다.

### 1.2 Multi-tenancy (멀티 테넌시)
Pulsar는 태생부터 "공유 클러스터"를 위해 설계되었습니다.
*   **Hierarchy:** `Tenant` > `Namespace` > `Topic` 구조를 가집니다.
    *   예: `google/search/logs`, `google/maps/events`.
*   각 테넌트/네임스페이스별로 리소스 쿼터(Quota), 인증, 스토리지 정책을 다르게 설정할 수 있습니다. Kafka는 이를 흉내만 낼 뿐, Pulsar만큼 완벽하게 격리되지 않습니다.

### 1.3 Geo-Replication (지리적 복제)
별도의 미러링 툴(Kafka MirrorMaker) 없이, 브로커 설정만으로 **Global Replicated Namespace**를 만들 수 있습니다. 서울 리전에 쓴 데이터를 즉시 도쿄 리전에서 읽을 수 있습니다.

## 2. 메시징 모델: Queue + Stream

### 2.1 Subscription Modes
Pulsar는 하나의 토픽에 대해 다양한 소비 방식을 지원합니다.
1.  **Exclusive:** 오직 하나의 컨슈머만 읽음 (순서 보장).
2.  **Failover:** 주(Active) 컨슈머가 죽으면 부(Standby) 컨슈머가 읽음.
3.  **Shared:** 여러 컨슈머가 라운드 로빈으로 나눠서 읽음 (RabbitMQ 스타일, 순서 보장 안 됨).
4.  **Key_Shared:** 같은 키를 가진 메시지는 같은 컨슈머에게 감 (순서 보장 + 병렬 처리).

## 3. 운영 및 설정 가이드

### 3.1 BookKeeper 이해하기 (Ensemble, Write, Ack)
데이터 저장 시 3가지 파라미터가 중요합니다. (E, Qw, Qa)
*   **Ensemble Size (E):** 데이터를 저장할 전체 북키(Bookie) 후보 수.
*   **Write Quorum (Qw):** 실제 데이터를 복제해서 쓸 북키 수.
*   **Ack Quorum (Qa):** 몇 군데에서 "저장 완료" 응답을 받아야 성공으로 칠 것인가.
*   이 설정을 통해 성능과 내구성 사이의 정밀한 조절이 가능합니다.

## 4. 납품용 솔루션 관점
*   **장점:** 하나의 클러스터로 수많은 고객사를 격리해서 수용해야 한다면(SaaS 형태) 최고의 선택입니다.
*   **단점:** 아키텍처가 너무 복잡합니다(ZK + BK + Broker + Proxy). 작은 규모의 온프레미스 납품용으로는 유지보수가 부담될 수 있습니다. (NATS나 Redpanda가 나음).

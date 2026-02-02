# Redpanda vs RabbitMQ 비교 분석

## 1. 개요
본 문서는 Kubernetes 환경에서 메시지 브로커(메시징 미들웨어) 도입을 검토하기 위해 **Redpanda**와 **RabbitMQ**를 비교 분석합니다.

주요 목적은 **미들웨어로서의 통신 기능 검증**이며, 단순 기능 나열이 아니라 **운영 환경에서의 실패 모드/병목/운영 난이도**까지 포함해 의사결정에 필요한 근거를 정리합니다. 특히 다음 항목을 최우선 검토 대상으로 삼습니다.

- **처리 성능**: Throughput 및 Tail Latency(P95/P99) 특성, 부하 변화/Backlog 증가 시 안정성
- **시스템 스펙 및 자원 사용 특성**: CPU/Memory/Storage(특히 디스크 I/O) 민감도, 오버커밋 허용 범위
- **메시지 히스토리 저장 및 재처리(Replay) 가능 여부**: “전달 중심” vs “기록 중심” 모델 차이와 설계 영향
- **프로젝트 성숙도(출시 시점) 및 운영 지원 여부**: 장기 운영 검증, K8s 배포/운영 도구(Operator/Helm) 지원

> 본 문서는 “정답”을 제시하기보다, **워크로드 특성(트래픽 패턴·재처리 요구·라우팅 복잡도·운영 제약)**에 따라 어떤 선택이 합리적인지 판단할 수 있도록 비교 축을 제공합니다.

---

## 2. 요약 비교 (Quick View)

| 항목 | Redpanda | RabbitMQ |
|---|---|---|
| **기본 아키텍처** | 분산 커밋 로그 기반 (Kafka API 호환) | 스마트 브로커 기반 (Smart Broker, Dumb Consumer) |
| **주요 프로토콜** | Kafka API (TCP) | AMQP 0-9-1, MQTT, STOMP |
| **메시지 히스토리** | 로그 기반 Retention 기본 지원<br>Tiered Storage(S3/GCS 등) 가능 | 기본 큐는 미지원(ACK 시 삭제)<br>Streams 사용 시 로그 기반 보존 가능 |
| **P99 Latency 특성** | 부하 증가 시에도 비교적 예측 가능 | 큐 적재량 증가 시 변동성 큼 |
| **자원 사용 성향** | CPU·디스크 성능 중심 (코어당 메모리 바닥선 존재) | 메모리·큐 적재량 영향 큼 |
| **최초 공개 시점** | **2021년 초** (일반 사용 가능한 첫 공개 릴리스)<br>*개발 시작: 2019년* | **2007년 2월** |
| **권장 용도** | 이벤트 스트리밍, 로그 수집, 이벤트 소싱 | 작업 큐, 복잡한 라우팅, 트랜잭션 처리 |

> 핵심 요약:  
> - **Redpanda**는 “기록(로그) 기반으로 장기 보존·재처리를 전제로” 설계된 스트리밍 계열  
> - **RabbitMQ**는 “전달(메시지 분배) 기반으로 라우팅·제어 흐름에 강한” 메시징 브로커 계열

---

## 3. 상세 분석

### 3.1 성능 특성 (Performance & Load Characteristics)

성능 비교의 핵심은 절대 수치보다 **부하 증가 및 Backlog 상황에서의 Tail Latency(P95/P99) 안정성**, 그리고 **병목이 발생하는 지점(디스크 vs 메모리/라우팅)**에 있습니다. 아래는 “어떤 조건에서 느려지는가”를 중심으로 정리합니다.

#### 3.1.1 비교 관점 정리
- **Low-latency(저지연)**: 빈 큐/즉시 소비/짧은 경로에서의 왕복 지연
- **High-throughput(고처리량)**: 지속적인 대량 이벤트 유입을 버티는 능력
- **Tail-latency 안정성**: 부하 상승·Backlog 증가·디스크 적재 증가 시 P95/P99가 얼마나 흔들리는지
- **확장 방식**: 단일 단위(큐/스트림/파티션)의 확장 한계와 샤딩/파티셔닝 전략

---

#### Redpanda
- **아키텍처**
  - C++ 기반 구현
  - Seastar 프레임워크 기반 **thread-per-core 모델**
  - JVM GC pause가 존재하지 않음(Stop-the-world 요인 제거)
- **Latency 특성**
  - 디스크 적재량 증가 및 처리량 상승 상황에서도 Tail Latency를 예측 가능하게 유지하도록 설계
  - 단, 실제 수치는 **스토리지 계층(Local NVMe vs Network PV), 복제 계수, acks 설정, 배치/압축 설정**에 따라 달라짐
- **Throughput 특성**
  - 파티션 수와 CPU 코어 수 증가에 따라 처리량이 비교적 선형적으로 증가하는 경향(파티션 설계가 곧 확장 설계)
  - 동일 파티션에 키가 쏠리면(핫 파티션) 해당 파티션이 병목이 될 수 있으므로 **키 분산/파티션 전략**이 중요
- **Backlog 증가 시**
  - “큐가 메모리에 쌓여 폭발”하기보다는 “로그가 길어짐”에 가까움
  - 병목은 **디스크 쓰기/읽기 대역폭, replication I/O**로 이동
- **요약**
  - **지속 유입 이벤트(스트리밍) + 재처리/리플레이 + 장기 보존**이 결합된 워크로드에서 강점
  - 성능 품질이 **하드웨어(특히 디스크)**에 민감하므로 스펙 산정이 필수

---

#### RabbitMQ
- **아키텍처**
  - Erlang/OTP 기반
  - 메시지 라우팅, 안정성, 장애 복구에 강점(성숙한 운영 패턴)
- **Latency 특성**
  - 저부하·즉시 소비 환경에서는 매우 낮은 지연 시간(짧은 경로)
  - 큐에 Backlog가 쌓이면 메모리 압박 및 디스크 I/O 증가로 Tail Latency가 급격히 증가할 수 있음
- **Throughput 특성**
  - 단일 큐(혹은 특정 라우팅 경로)에 부하가 집중되면 해당 지점이 병목
  - 고처리량 확보를 위해서는 **큐/익스체인지 설계(샤딩), consumer 병렬성, prefetch/ack 전략**이 핵심
- **Backlog 증가 시**
  - “소비 지연 → 큐 적재 증가 → 메모리 압박/흐름 제어 → 지연 폭증” 형태의 실패 모드가 발생 가능
  - 병목은 **메모리, 라우팅 비용, 디스크(영속 큐), consumer 처리율**로 복합적으로 발생
- **요약**
  - **작업 큐(한 번 처리), 복잡한 라우팅, 우선순위/재시도/제어 흐름**이 중요한 워크로드에 강점
  - 성능은 “브로커 자체”만큼이나 **메시지 모델링/ack/prefetch/consumer 설계 품질**에 좌우됨

---

### 3.2 자원 사용량 관점 비교 (현실적 운영 기준)

Kubernetes에서는 “기능 지원”보다 **자원 사용 패턴**이 운영 안정성을 좌우합니다. 아래는 실무에서 자주 부딪히는 관측 포인트를 정리합니다.

#### 3.2.1 공통 관측 지표(권장)
- **Producer 측**: publish latency(P95/P99), retry rate, request timeout
- **Broker 측**: CPU steal/usage, memory RSS, filesystem latency, disk throughput, network throughput
- **Lag/Backlog**:
  - Redpanda: consumer lag(오프셋 지연), partition별 핫스팟
  - RabbitMQ: queue depth, consumer utilization, unacked messages, rate in/out
- **장애 징후**:
  - 급격한 P99 상승, backlog 증가율이 소비율을 초과, 디스크 latency 급등

---

#### Redpanda
- **CPU**
  - 코어 수에 성능이 직접적으로 비례하는 성향
  - CPU limit을 과도하게 제한하거나 오버커밋 시 Tail Latency에 영향(특히 burst 트래픽)
- **메모리**
  - 코어 수 및 파티션 수에 따라 필요한 최소 메모리 바닥선 존재
  - 파티션이 많을수록 메타데이터/인덱스/캐시 성격의 메모리 요구 증가
- **디스크**
  - 성능의 핵심 병목 지점(지연 안정성에 직접 영향)
  - Local NVMe vs Network PV에 따른 체감 차이가 매우 큼
- **운영 성향 요약**
  - “CPU·디스크 중심 설계”
  - K8s에서는 **StatefulSet + 성능 보장 스토리지**가 사실상 전제
  - 성능 예측을 위해 **리소스 요청/제한을 보수적으로 산정**하는 편이 안전

---

#### RabbitMQ
- **CPU**
  - Exchange/Binding 수, 연결 수, 라우팅 복잡도에 따라 사용량 증가
  - 메시지 크기·빈도보다 **라우팅/ack/consumer 동시성**에 민감한 경우가 많음
- **메모리**
  - 큐 적재량과 소비 지연이 메모리 압박으로 직결되는 경향
  - Streams 사용 시 커널 Page Cache 영향이 커서 “컨테이너 메모리” 해석이 까다로울 수 있음
- **디스크**
  - Streams는 disk I/O heavy, 영속 큐/정책 사용 시 디스크 지연이 tail에 반영될 수 있음
- **운영 성향 요약**
  - “메모리·큐 상태 중심 설계”
  - 운영 시 **backlog/consumer 처리율/flow control**을 가장 먼저 보게 됨
  - 빠르게 시작 가능하지만, 트래픽 증가 시 설계/튜닝이 중요

---

### 3.3 시스템 스펙 및 운영 요구사항

아래 표는 “절대 기준”이 아니라,  
**사내 내부 서비스 ↔ 미들웨어 통신 용도로 사용할 때  
운영 실패(OOM, Tail Latency 급증, 불안정한 PoC)를 피하기 위한 현실적인 출발점**을 기준으로 정리한 것이다.

외부 사용자 트래픽이 없고,  
사내 솔루션의 내부 파이프라인/버퍼링 목적이라면  
반드시 대규모 스펙이 필요한 것은 아니며,  
**기능 검증 → 소규모 운영 → 필요 시 단계적 확장**을 전제로 한다.

실제 요구량은 메시지 크기, 초당 메시지 수, 소비 지연(Backlog),  
복제 계수, 보존 기간, Consumer 병렬성에 따라 크게 달라질 수 있다.

| 구분 | Redpanda | RabbitMQ |
|---|---|---|
| **경량 테스트 (기능 PoC)** | 1 vCPU / 2Gi RAM | 0.5~1 vCPU / 512Mi~1Gi RAM |
| **운영 최소 (내부 서비스용)** | 2 vCPU / 4Gi RAM | 1 vCPU / 2Gi RAM |
| **운영 권장 (성능·안정성 중시)** | 4+ vCPU(전용에 준함) / 8~16Gi RAM / NVMe(XFS) | 4 vCPU / 8Gi+ RAM / SSD |
| **주요 병목** | 디스크 성능, CPU 코어 안정성 | 메모리, 큐 적재량(Backlog) |

> ※ Redpanda의 “4Gi RAM”은 외부 트래픽 대비 과한 값이 아니라,  
> **로그 기반 스트리밍 엔진 특성상 PoC를 안정적으로 평가하기 위한 현실적인 하한선**에 가깝다.  
> 2Gi로도 구동은 가능하나, 작은 Backlog에도 지표(P99, 재처리)가 왜곡될 수 있다.

---

#### 3.3.1 스펙 산정 체크리스트 (권장)

아래 항목들은 “평균 트래픽”이 아닌  
**문제 상황(소비 지연, 장애, 버스트 트래픽)**에서  
필요 자원을 결정하는 핵심 요소들이다.

- **보존 기간 / 저장 용량**
  - Redpanda는 로그 기반 구조로 retention 기간이 길어질수록  
    디스크 사용량 및 I/O 부담이 증가
  - RabbitMQ는 기본 큐는 소비 시 삭제되며,  
    Streams 사용 시에만 로그 보존 성격을 가짐

- **복제 계수**
  - 두 시스템 모두 복제 사용 시 네트워크 및 디스크 쓰기 비용 증가
  - Redpanda는 복제가 곧 write-path latency에 영향
  - RabbitMQ는 Quorum Queue 사용 시 메모리·디스크 사용량 증가

- **메시지 크기**
  - 메시지 크기가 커질수록 네트워크·디스크 영향이 즉각적으로 증가
  - 특히 Redpanda는 큰 메시지에서 디스크 I/O 특성이 더 중요해짐

- **Burst 트래픽 / 소비 지연**
  - RabbitMQ는 버스트가 backlog로 쌓일 경우  
    메모리 압박 → Tail Latency 급증 형태로 나타날 수 있음
  - Redpanda는 backlog가 로그 길이 증가로 흡수되지만  
    디스크 성능 및 복제 설정이 병목이 될 수 있음

- **Kubernetes 스토리지 타입**
  - Redpanda는 Local SSD / NVMe 환경에서 장점이 뚜렷함
  - NFS와 같은 네트워크 스토리지에서는  
    기능 검증은 가능하나 성능 지표 해석에 주의가 필요
  - RabbitMQ는 NFS에서도 운영 가능하나,  
    Streams 또는 대량 적재 시 디스크 지연 영향이 발생할 수 있음

---

> 요약하면,  
> **사내 내부 통신용 미들웨어라면 작은 스펙으로 시작하는 것이 합리적이지만,  
> Redpanda는 로그 기반 특성상 최소한의 메모리/디스크 바닥선은 반드시 필요**하다.  
> RabbitMQ는 상대적으로 작은 자원에서도 빠르게 시작할 수 있으나,  
> backlog 관리와 메모리 관측이 운영 안정성의 핵심이 된다.
---

### 3.4 메시지 히스토리 저장 (Message Retention)

이 항목은 “기능 지원 여부”뿐 아니라, **서비스 설계 자체(재처리/감사/이벤트 소싱 가능 여부)**에 영향을 줍니다.

#### Redpanda (Partitioned Log Model)
- `Topic → Partition → Segment(.log)` 구조
- Append-only 로그 방식(기록이 기본 동작)
- `retention.ms`, `retention.bytes` 기준 세그먼트 단위 삭제
- Tiered Storage를 통해 S3/GCS/Azure Blob Storage로 오프로딩 가능
- **소비(Consume)는 삭제 조건이 아님**: 동일 데이터를 여러 consumer group이 각자 offset으로 재처리 가능
- **적합한 경우**
  - 이벤트 소싱, 감사 로그, 재처리(Replay)가 요구되는 파이프라인
  - “한 번 보내고 끝”이 아니라 “나중에 다시 읽을 수 있어야 함”이 요구되는 경우

#### RabbitMQ Streams
- Append-only 로그 구조
- 항상 persistent & replicated
- 소비자가 읽어도 메시지는 즉시 삭제되지 않음(보존 정책 기반)
- Super Stream을 통해 파티셔닝 기반 확장 가능
- 객체 스토리지 기반 장기 보존은 기본 제공되지 않음
- **적합한 경우**
  - 기존 RabbitMQ 운영 체계에서 “로그형 소비 모델”이 일부 필요한 경우
  - 단, 장기 보존/대규모 스트리밍을 핵심 요구로 둘 때는 비용·운영 특성을 별도 평가 필요

#### RabbitMQ Classic / Quorum Queue (참고)
- ACK 이후 메시지 삭제(전달이 기본 동작)
- TTL은 히스토리 저장 목적이 아니라 만료/정리 목적
- 이벤트 소싱/장기 보존에는 구조적으로 부적합(별도 저장소 필요)

---

## 4. 프로젝트 성숙도 및 출시 시점

성숙도는 단순 “출시 연도”가 아니라, **운영 레퍼런스, 생태계, 도구/가이드 성숙도**까지 포함합니다.

| 항목 | Redpanda | RabbitMQ |
|---|---|---|
| **개발 시작** | 2019년 | 2006~2007년 |
| **최초 공개(실사용 가능)** | **2021년 초** | **2007년 2월** |
| **성숙도 해석** | 비교적 최신 스트리밍 플랫폼 | 장기간 운영 검증된 메시지 브로커 |

> ※ Redpanda는 2021년에 “개발이 완료”된 것이 아니라,  
> **이 시점부터 일반 사용이 가능한 제품으로 공개되었으며 현재도 지속 개발 중**입니다.

#### 4.1 성숙도 해석 가이드(현실 기준)
- **RabbitMQ**: 운영 노하우/트러블슈팅 패턴/튜닝 레퍼런스가 풍부(인력 수급도 비교적 용이)
- **Redpanda**: Kafka 생태계 활용이 가능하나, “스토리지/코어 기반 성능” 특성 때문에 운영 스펙·스토리지 설계 역량이 중요

---

## 5. 미들웨어 통신 적합성 및 결론

### 5.1 선택 기준(업무 요구사항 기준)

**A. “메시지 전달”이 목표인가, “이벤트 기록/재처리”가 목표인가**
- 기록/재처리/감사 로그가 핵심 → Redpanda 우세
- 작업 큐/제어 흐름/라우팅이 핵심 → RabbitMQ 우세

**B. 메시지 라우팅 복잡도**
- 복잡한 라우팅(토픽/헤더/우선순위/재시도 정책 등)을 브로커에서 해결 → RabbitMQ 유리
- 단순 토픽 기반 스트리밍(consumer group이 처리 분담) → Redpanda 유리

**C. 운영 제약(K8s/스토리지)**
- 고성능 스토리지(Local/NVMe)와 안정적 리소스 할당이 가능 → Redpanda 장점 극대화
- 가벼운 시작/빠른 도입이 필요하고, 소비자가 즉시 처리하는 패턴 → RabbitMQ 유리

---

### 5.2 결론(문서 기준 요약)

- **메시지 히스토리 저장·재처리가 핵심인 경우**
  - → **Redpanda 권장**
- **복잡한 라우팅, 작업 큐, 제어 흐름이 핵심인 경우**
  - → **RabbitMQ 권장**
- **종합 결론**
  - 이벤트 스트리밍, 로그 파이프라인, 이벤트 소싱  
    → Redpanda
  - 마이크로서비스 간 작업 분배, 트랜잭션 제어  
    → RabbitMQ

> 권장: 최종 결정을 위해서는 아래 3가지 PoC 시나리오를 최소 수행하는 것이 안전합니다.  
> 1) 정상 부하 + 버스트(peak) 부하에서의 P95/P99 및 backlog 변화  
> 2) 보존 정책 적용(retention/TTL/stream max age) 시 디스크·메모리 변화  
> 3) 장애 상황(consumer 지연/노드 재시작)에서의 복구 시간과 운영 난이도

---

## 6. 참고 문헌 (References)

### 6.1 Redpanda – Architecture & Storage

- Redpanda Architecture Overview  
  https://docs.redpanda.com/current/get-started/architecture/

- Topic, Partition, Segment 구조 설명 (Glossary)  
  https://docs.redpanda.com/current/reference/glossary/#segment

- Topic Properties & Log Retention Configuration  
  https://docs.redpanda.com/current/manage/cluster-maintenance/topic-properties/

- Redpanda Tiered Storage (Object Storage Offloading)  
  https://docs.redpanda.com/current/manage/tiered-storage/

---

### 6.2 Redpanda – Performance & Resource Usage

- Hardware & Sizing Recommendations  
  https://docs.redpanda.com/current/deploy/redpanda/manual/sizing/

- Production Requirements & Best Practices  
  https://docs.redpanda.com/current/deploy/redpanda/manual/production/requirements/

- Redpanda vs Kafka – Design & Performance Rationale  
  https://www.redpanda.com/blog/redpanda-vs-kafka/

---

### 6.3 RabbitMQ – Core Messaging Model

- RabbitMQ Core Concepts (Queues, Exchanges, Routing)  
  https://www.rabbitmq.com/tutorials/amqp-concepts.html

- Message Acknowledgements & Delivery Semantics  
  https://www.rabbitmq.com/confirms.html

- Time-To-Live (TTL)  
  https://www.rabbitmq.com/ttl.html

---

### 6.4 RabbitMQ Streams

- RabbitMQ Streams – Concept & Architecture  
  https://www.rabbitmq.com/streams.html

- Streams vs Classic / Quorum Queues  
  https://www.rabbitmq.com/docs/streams#comparison

- Super Streams (Partitioned Streams)  
  https://www.rabbitmq.com/docs/streams#super-streams

---

### 6.5 RabbitMQ – Resource & Performance Considerations

- Memory Usage, Watermarks & Flow Control  
  https://www.rabbitmq.com/memory.html

- Disk I/O & Persistence Behavior  
  https://www.rabbitmq.com/persistence-conf.html

- Production Checklist & Performance Tuning  
  https://www.rabbitmq.com/production-checklist.html

---

### 6.6 Kubernetes Deployment & Operations

- Redpanda Kubernetes Operator  
  https://docs.redpanda.com/current/deploy/kubernetes/operator/

- RabbitMQ Cluster Operator for Kubernetes  
  https://www.rabbitmq.com/kubernetes/operator/operator-overview.html

---

### 6.7 Background / History

- RabbitMQ Project History  
  https://en.wikipedia.org/wiki/RabbitMQ

- Redpanda Company & Product Background  
  https://www.redpanda.com/blog/bsl-source-available-license/

---

### 비고
본 문서는 아키텍처 및 기술 검토 목적의 비교 문서이며,  
실제 도입 전에는 반드시 **사내 워크로드 기준 PoC 및 성능 측정**을 병행하는 것을 권장합니다.
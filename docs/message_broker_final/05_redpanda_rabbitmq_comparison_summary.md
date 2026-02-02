# Redpanda vs RabbitMQ 기술 비교 및 하드웨어 가이드 (Final v2.0)

> **작성일:** 2026-01-27  
> **검증:** Redpanda, RabbitMQ Classic, RabbitMQ Stream 공식 문서 기반 재작성  
> **버전:** Final v2.0 (기술적 정확도 강화)

## 1. 개요

본 문서는 메시지 브로커의 **아키텍처적 차이**에 기반하여, 실제 운영 시의 **성능 특성, 데이터 관리, 그리고 정확한 하드웨어 요구사항**을 비교합니다. 특히 RabbitMQ Stream과 Redpanda는 비슷해 보이지만 메모리 사용 방식에서 결정적인 차이가 있음을 명시합니다.

---

## 2. 아키텍처 및 동작 원리 비교

| 항목 | Redpanda | RabbitMQ Stream | RabbitMQ Classic/Quorum |
| :--- | :--- | :--- | :--- |
| **기반 언어/엔진** | **C++ (Seastar Framework)** | **Erlang (OTP)** | **Erlang (OTP)** |
| **스레딩 모델** | **Thread-per-Core**<br>(코어당 1스레드 고정, 컨텍스트 스위칭 최소화) | **Erlang Light-weight Threads**<br>(수천 개의 경량 프로세스, OS 스케줄링) | **Erlang Light-weight Threads**<br>(큐 하나가 보통 1코어 처리) |
| **메모리 관리** | **자체 관리 (Pre-allocated)**<br>시작 시 메모리를 선점하고 직접 관리함. | **OS Page Cache 위임**<br>앱 메모리는 적게 쓰고, OS 파일 캐시를 활용. | **Heap Memory 의존**<br>메시지를 메모리에 적재. 쌓이면 RAM 급증. |
| **디스크 I/O** | **Direct I/O (`O_DIRECT`)**<br>OS 캐시를 건너뛰고 디스크에 직접 기록. | **Buffered I/O**<br>OS 커널의 `sendfile` 및 캐시 활용. | **Buffered**<br>메모리 부족 시 디스크로 스왑(Paging). |

---

## 3. 운영 편의성: UI 및 데이터 조회 도구 (Observability)

운영자가 CLI(`rpk` 등)를 쓰지 않고 웹에서 데이터를 조회하거나 제어할 수 있는지 여부입니다.

| 도구 | Redpanda Console (Web UI) | RabbitMQ Classic (Web UI) | RabbitMQ Stream (Web UI) |
| :--- | :--- | :--- | :--- |
| **메시지 내용 조회** | **매우 강력함**<br>- 타임스탬프/오프셋 기반 검색<br>- JSON 필터링 및 구문 강조<br>- 실시간 데이터 스트리밍 보기 | **제한적**<br>- "Get Message" 기능 존재.<br>- `Requeue` 미설정 시 삭제 위험.<br>- 실시간 스트림 보기 불가. | **미지원 (매우 불편)**<br>- 기본 UI에서 메시지 내용 조회 **불가**.<br>- 오직 통계(입/출력 속도)만 확인 가능.<br>- 별도 CLI 도구 필수. |
| **재처리 (Offset Reset)** | **UI 지원 (클릭)**<br>- 컨슈머 그룹 선택 후 "Reset Offset" 버튼으로 특정 시점 되감기 가능. | **미지원**<br>- 소비된 메시지는 삭제되므로 재처리 개념 없음. | **미지원**<br>- UI에서 오프셋 조작 불가.<br>- 앱 코드나 외부 도구로만 가능. |
| **API 호환성** | **Kafka Admin API**<br>- 모든 Kafka 호환 도구 사용 가능. | **AMQP / HTTP API**<br>- 성숙한 생태계 보유. | **Stream Protocol**<br>- 전용 바이너리 프로토콜.<br>- 도구 생태계가 아직 빈약함. |

> **결론:** **Redpanda**는 `Redpanda Console`이라는 강력한 GUI 도구를 통해 운영자가 손쉽게 데이터를 눈으로 확인하고, 클릭만으로 장애 시점 복구(Replay)를 수행할 수 있습니다. 반면 **RabbitMQ Stream**은 성능은 좋지만, 웹 UI에서 데이터를 들여다볼 수 없어 운영 난이도가 가장 높습니다.

---

## 4. 기능 및 성능 상세 비교

| 비교 항목 | Redpanda | RabbitMQ Stream | RabbitMQ Classic |
| :--- | :--- | :--- | :--- |
| **처리량 (Throughput)** | **최상** (Batch & Zero-copy) | **최상** (Kafka급, Batch 지원) | **중간** (건별 처리) |
| **데이터 보존** | **디스크 (Log 기반)** | **디스크 (Log 기반)** | **메모리 (Queue 기반)** |
| **재처리 (Replay)** | **가능** (Offset 이동) | **가능** (Offset 이동) | **불가능** (소비 시 삭제) |
| **라우팅 (Routing)** | 단순 (Topic) | 단순 (Topic/Filter) | **강력** (Exchange 복합 라우팅) |
| **우선순위 큐** | 미지원 | 미지원 | **지원** (Classic 완벽, Quorum 제한적) |
| **NFS 호환성** | **나쁨 (튜닝 필수)**<br>Direct I/O와 충돌. Latency 민감. | **주의 (로그 지연)**<br>Page Cache 덕분에 버티지만 로그 쓰기 지연 발생 가능. | **보통 (상대적 양호)**<br>메모리 버퍼링으로 지연을 어느 정도 상쇄. |

---

## 4. 하드웨어 권장 및 최소 사양 (Resource Requirements)

**핵심:** 세 기술의 메모리 사용 패턴이 다르므로, 사양 산정 방식도 달라야 합니다.

### 4.1 Redpanda (Resource Hog type)
Redpanda는 **"코어당 메모리 2GB"**라는 공식 규칙(Rule of Thumb)이 있습니다. Seastar 엔진이 하드웨어를 장악하고 최적화합니다.

| 환경 | CPU | RAM | 스토리지 (Retention 기준) | 설정 주의사항 |
| :--- | :--- | :--- | :--- | :--- |
| **개발/검증 (Min)** | **1 Core** | **1~2 GB** | **10 GB+** | - `redpanda.developer_mode: true` (필수)<br>- 메모리가 부족하면 실행 자체가 안 될 수 있음. |
| **운영 (Prod)** | **4 Cores+** | **8 GB+**<br>*(4 Core × 2GB)* | **NVMe SSD** | - XFS 파일시스템 권장<br>- NFS 사용 시 성능 보장 불가 |

### 4.2 RabbitMQ Stream (OS Cache Reliance type)
RabbitMQ 프로세스 자체의 메모리는 적게 차지하지만, **OS가 사용할 "여유 메모리(Free RAM)"가 많을수록 성능이 빨라집니다.**

| 환경 | CPU | RAM (App + OS Cache) | 스토리지 (Retention 기준) | 설정 주의사항 |
| :--- | :--- | :--- | :--- | :--- |
| **개발/검증 (Min)** | **1 Core** | **512 MB (App)**<br>+ 여유분 권장 | **10 GB+** | - 힙 메모리는 적게 줘도 되지만, 디스크가 느리면 읽기 속도 저하. |
| **운영 (Prod)** | **2~4 Cores** | **4 GB+**<br>*(OS 캐시용 여유분 포함)* | **SSD** | - App 힙 메모리보다 **Page Cache용 여유 RAM** 확보가 성능의 핵심. |

### 4.3 RabbitMQ Classic/Quorum (Memory Bound type)
메시지가 쌓이면 프로세스 메모리(Heap)가 직접적으로 증가합니다.

| 환경 | CPU | RAM (Heap) | 스토리지 (Backlog 기준) | 설정 주의사항 |
| :--- | :--- | :--- | :--- | :--- |
| **개발/검증 (Min)** | **1 Core** | **256~512 MB** | **5 GB+** | - 큐에 메시지가 안 쌓인다면 매우 저사양 가능. |
| **운영 (Prod)** | **2 Cores** | **4~8 GB+** | **SSD** | - **High Watermark** 모니터링 필수.<br>- 메시지 쌓이면 OOM(Out Of Memory) 위험 가장 높음. |

---

## 6. 핵심 지표 기반 최종 결론 (Performance, Resource, Ops)

사용자의 핵심 요구사항인 **"메시지 내용 확인(Inspection) 및 API 지원"**을 최우선 가치로 두었을 때의 최종 평가입니다.

### 6.1 지표별 평가 (Evaluation)

1.  **운영 편의성 및 데이터 조회 (최우선 중요):** **Redpanda (압도적 승리)**
    *   **Redpanda:** 전용 콘솔(UI)과 표준 Kafka API를 통해 메시지 내용을 실시간으로 확인하고 검색하는 것이 매우 쉽습니다. 솔루션 기능으로 "조회"를 구현하기 가장 좋습니다.
    *   **RabbitMQ Classic:** HTTP API로 조회가 가능하지만, 데이터가 삭제될 위험(`Requeue` 실수)이 있어 조심스럽습니다.
    *   **RabbitMQ Stream:** 기본 UI/API로는 메시지 내용을 **확인할 수 없습니다.** 별도의 조회용 애플리케이션을 직접 개발해야 합니다.
2.  **자원 효율성 (가성비):** RabbitMQ Stream > RabbitMQ Classic > Redpanda
    *   자원 소모량 자체는 RabbitMQ 계열이 적습니다. Redpanda는 편의성을 제공하는 대신 메모리를 많이 점유합니다.
3.  **성능 (Throughput):** Redpanda ≒ RabbitMQ Stream > RabbitMQ Classic

### 6.2 최종 추천 (Final Recommendation)

| 추천 순위 | 선택지 | 추천 이유 |
| :--- | :--- | :--- |
| **1순위 (강력 추천)** | **Redpanda** | **"개발 공수 제로(Zero Development)"**<br>메시지 저장(Log)과 조회(UI) 기능이 완벽하게 내장되어 있습니다. 자원을 많이 쓰지만, 별도 DB 구축이나 UI 개발 없이 즉시 요구사항을 충족합니다. |
| **2순위 (조건부 추천)** | **RabbitMQ Classic + DB** | **"자원 절약형 아키텍처"**<br>브로커 자원은 아낄 수 있으나, **조회를 위해 별도 DB에 로그를 저장하는 로직을 직접 개발**해야 합니다. 인프라 비용 절감이 개발 비용보다 중요할 때 선택합니다. |
| **3순위 (비추천)** | **RabbitMQ Stream** | **"도구 부재"**<br>데이터는 저장되지만 볼 수 있는 도구가 없습니다. 조회 시스템을 바닥부터 만들어야 하므로, 솔루션 기능으로 해결하려는 목적에 부합하지 않습니다. |

> **최종 한 줄 평:**  
> **"메시지 내용을 보고 제어하는 기능"**을 위해 추가 개발을 하고 싶지 않다면 **Redpanda**가 유일한 정답입니다. 만약 개발팀 여력이 충분하고 인프라 자원을 아끼는 게 더 급하다면 **RabbitMQ Classic + 별도 DB(Postgres 등)** 조합이 현실적인 대안입니다.

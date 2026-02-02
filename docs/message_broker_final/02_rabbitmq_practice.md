# RabbitMQ 기능 검증 및 실습 보고서: Classic vs Stream

**작성일:** 2026-01-26  
**작성자:** Gemini Agent  
**환경:** Kubernetes (Kind), RabbitMQ 3.13 (Official Image), rabbitmq-perf-test

---

## 1. 개요 (Overview)
본 보고서는 RabbitMQ의 전통적인 **Classic Queue**와 Kafka 스타일의 **Stream Queue** 기능을 직접 실습하고 비교한 결과를 정리합니다.  
주요 검증 항목은 **메시지 저장 방식, 히스토리 재생(Replay), 그리고 모니터링 지표의 해석**입니다.

---

## 2. 아키텍처 및 철학 비교

실습을 통해 확인된 두 방식의 결정적인 차이는 **"메시지 소비 후 상태"**에 있습니다.

| 구분 | Classic Queue | Stream Queue |
| :--- | :--- | :--- |
| **비유** | **작업 목록 (To-Do List)** | **업무 일지 (Log Book)** |
| **소비 후 데이터** | **삭제됨 (Destructive)** | **보존됨 (Non-destructive)** |
| **Ready의 의미** | 처리해야 할 남은 작업 수 | 현재까지 저장된 전체 누적 데이터 수 |
| **재생 (Replay)** | 불가능 (한 번 읽으면 끝) | **가능** (Offset 조절로 과거 데이터 조회) |
| **파일 저장** | 인덱스 기반, 수시로 삭제됨 | `.osc` 파일(세그먼트)에 Append-Only 저장 |

---

## 3. 실습 시나리오 및 검증 결과

### 3.1. 메시지 적재 (Load)
*   **테스트:** `perf-test` 도구를 사용하여 100,000건의 메시지를 적재.
*   **결과:** 두 방식 모두 빠르게 적재됨. Stream Queue는 디스크(`mnesia/.../stream/`)에 `.segment` 파일 형태로 저장됨을 확인.

### 3.2. 메시지 소비 (Consume)
*   **Classic:** 컨슈머가 메시지를 읽고 Ack를 보내는 순간 **큐에서 사라짐.** `Ready` 숫자가 0으로 줄어듦.
*   **Stream:** 컨슈머가 메시지를 읽어도 **큐에 그대로 남아있음.** `Ready` 숫자는 100,000으로 유지됨.
    *   *핵심 발견:* Stream에서 "소비"란 데이터 삭제가 아니라 **"전송(Deliver)"**을 의미함.

### 3.3. 히스토리 재생 (Time-travel / Replay)
*   **검증 방법:** 이미 소비가 완료된 상태에서 `--stream-consumer-offset first` 옵션으로 재접속 시도.
*   **Classic:** 에러 발생 (`reply-text=PRECONDITION_FAILED`). 과거 데이터가 없으므로 재생 불가.
*   **Stream:** **성공.** 이미 처리했던 100,000건의 메시지를 처음부터 다시 전송받음.

---

## 4. 관측성 및 운영 가이드 (Observability)

실습 중 가장 큰 난관은 **"현재 얼마나 밀려있는가(Lag)?"**를 확인하는 것이었습니다.

### 4.1. Management UI 해석의 차이

**[Classic Queue 화면]**
*   직관적임.
*   `Ready` = 10,000 이라면? 👉 **"아, 처리 안 된 게 1만 개 있구나."** (바로 판단 가능)

**[Stream Queue 화면]**
*   직관적이지 않음.
*   `Ready` = 10,000 이라면? 👉 **"누적된 게 1만 개구나."** (처리 여부는 알 수 없음)
*   `Unacked` = 0 이라면? 👉 **"지금 연결된 컨슈머가 없거나, 배달 중인 게 없구나."** (지연 여부 알 수 없음)

### 4.2. Stream 모드에서의 장애 판단 (Lag Monitoring)
Stream Queue에서 **지연(Lag)**을 확인하려면 UI의 단순 숫자만으로는 불가능하며, 다음 공식을 이해해야 합니다.

> **Lag = (Total Messages) - (Consumer Offset)**

*   **문제점:** RabbitMQ Management UI는 `Total`은 보여주지만, `Consumer Offset`을 한눈에 비교해서 "몇 개 밀림"이라고 보여주는 기능이 약함.
*   **해결책:** 
    1.  `Message rates` 그래프에서 `Deliver` 속도가 `Publish` 속도를 따라가고 있는지 확인.
    2.  CLI (`rabbitmqctl list_stream_consumers`)를 통해 정확한 Lag 수치 조회.
    3.  별도의 모니터링 툴(Grafana 등)에서 커스텀 쿼리로 시각화.

---

## 5. 결론 및 제언

### ✅ RabbitMQ Stream이 적합한 경우
*   **데이터 재사용:** 하나의 데이터를 여러 서비스가 각자의 속도로 읽어야 할 때 (Fan-out).
*   **이벤트 소싱:** 과거의 데이터를 다시 읽어서 상태를 복구하거나 분석해야 할 때.
*   **고성능 로그 수집:** Kafka와 유사한 대용량 처리량이 필요할 때.

### ⚠️ 도입 시 주의사항
*   **모니터링 복잡도 증가:** "큐에 몇 개 쌓였나"만 보던 기존 관제 방식으로는 장애를 감지할 수 없습니다. **"Lag(지연)" 중심의 새로운 모니터링 체계**를 구축해야 합니다.
*   **메시지 내용 조회:** 기본 UI에서는 Stream 메시지 내용을 조회(Browsing)하거나 검색하는 기능이 제공되지 않습니다.

---

**[최종 의견]**
RabbitMQ는 **강력한 라우팅(Classic)**과 **대용량 저장(Stream)**을 동시에 제공하는 하이브리드 솔루션으로 진화했습니다.  
다만, Stream 기능의 **데이터 탐색 및 모니터링 편의성**은 Kafka 생태계(예: Redpanda Console)에 비해 다소 부족하므로, **운영 편의성**이 중요한 대규모 스트리밍 환경에서는 Redpanda와의 비교 검토를 권장합니다.

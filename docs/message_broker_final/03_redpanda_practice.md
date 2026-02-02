# Redpanda 기능 검증 및 실습 보고서: 모니터링과 Lag 관리

**작성일:** 2026-01-26  
**작성자:** Gemini Agent  
**환경:** Kubernetes (Kind), Redpanda 23.x, Redpanda Console, Kafka Lag Exporter

---

## 1. 개요 (Overview)
본 보고서는 Redpanda(Kafka 호환) 환경에서 메시지 내용을 탐색하고 장애의 핵심 지표인 **Lag(지연)**을 모니터링하는 방법을 실습하고 검증한 결과를 정리합니다. 특히 앞서 수행한 RabbitMQ Stream 테스트 결과와의 비교를 통해 **운영 편의성** 측면을 집중 조명합니다.

---

## 2. Redpanda Console (UI) 검증

### 2.1. 메시지 내용 탐색 (Data Browsing)
*   **RabbitMQ Stream:** 기본 UI에서 지원하지 않아 CLI나 별도 도구가 필요했음.
*   **Redpanda:** `Redpanda Console`을 통해 웹 브라우저에서 즉시 확인 가능.
    *   **기능:** 토픽별 메시지 리스트 조회, JSON 포맷팅, 시간대별/오프셋별 이동, 필터링 검색 지원.
    *   **결론:** 데이터 디버깅 및 검증 과정이 압도적으로 빠르고 편리함.

### 2.2. Consumer Lag 확인 (직관성)
*   **RabbitMQ Stream:** "전체 메시지 수"와 "소비된 오프셋"을 따로 보고 사용자가 머릿속으로 뺄셈해야 했음. (직관성 낮음)
*   **Redpanda:** `Consumer Groups` 메뉴에서 **"Lag" 컬럼을 별도로 제공.**
    *   **결과:** "현재 500개 밀림"과 같은 상태를 1초 만에 파악 가능. 장애 판단 속도가 획기적으로 단축됨.

---

## 3. Grafana 모니터링 아키텍처 및 실습

Redpanda 자체(Community Edition)는 Prometheus 포맷의 Lag 메트릭을 기본 제공하지 않음이 확인되었습니다. 따라서 Grafana 통합을 위해 추가 구성요소가 필요했습니다.

### 3.1. Kafka Lag Exporter 도입
*   **목적:** Grafana 대시보드에 Lag 그래프를 그리기 위해 오프셋 정보를 수집하여 Prometheus 메트릭으로 변환.
*   **설치:** Helm Chart (`kafka-lag-exporter`) 사용.
*   **리소스 사용량:**
    *   **CPU:** 매우 낮음 (10m 미만)
    *   **Memory:** JVM 기반이라 약 **300~400Mi** 사용. (안정적인 운영을 위해 Limit 512Mi 권장)

### 3.2. Lag 발생 및 해소 테스트
1.  **상황:** 프로듀서로 10개 메시지 생성, 컨슈머 중지.
2.  **확인:** Grafana에서 `kafka_consumergroup_group_lag` 지표가 **10**으로 치솟음.
3.  **해소:** 컨슈머 실행 후 그래프가 다시 **0**으로 떨어짐.
    *   *결론:* 실시간 장애 감지 및 알림(Alerting) 구현 가능.

---

## 4. RabbitMQ vs Redpanda 비교 요약 (운영자 관점)

| 구분 | RabbitMQ (Stream) | Redpanda (Kafka) |
| :--- | :--- | :--- |
| **핵심 지표** | **Ready 메시지 수** (Classic) / **Lag 계산 필요** (Stream) | **Consumer Lag** (자동 계산됨) |
| **데이터 탐색** | CLI 의존 (불편함) | **전용 웹 Console** (매우 편리함) |
| **Grafana 연동** | 플러그인 내장 (비교적 쉬움) | **별도 Exporter 필요** (추가 리소스 필요) |
| **Lag의 정의** | (Log End) - (Consumer Offset) | **Lag** 그 자체 (단일 숫자) |

---

## 5. 최종 결론

*   **Redpanda의 강점:** **"Redpanda Console"**이라는 강력한 도구 덕분에 데이터의 가시성(Visibility)과 운영 편의성이 매우 뛰어납니다. 특히 "Lag" 중심의 모니터링 체계가 잘 잡혀 있어 대규모 스트리밍 환경에서 장애를 빠르게 인지하고 대응하기 유리합니다.
*   **고려 사항:** Grafana 통합 시 `Kafka Lag Exporter`라는 추가 컨테이너(약 500MB 메모리)를 운영해야 하는 비용이 발생하지만, 얻을 수 있는 관측성 이점에 비하면 미미한 수준입니다.

**[제언]**
단순 메시지 큐 용도가 아니라 **데이터 파이프라인 및 장기 보존**이 목적이라면, **운영 가시성이 확보된 Redpanda**가 관리자에게 훨씬 쾌적한 경험을 제공합니다.

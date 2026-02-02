# Observability 학습 정리: Tempo, Alloy, and Promtail

이 문서는 Grafana Tempo(Tracing)를 도입하기 위해 파생된 질문들인 Promtail, Alloy의 개념과 상관관계, 그리고 전체적인 아키텍처 흐름을 정리한 문서입니다.

## 1. 도구의 역할 및 관계 정의

### Q: Tempo를 공부하려면 Promtail과 Alloy도 알아야 하나요?
**A: 네, 특히 Alloy는 필수입니다.**
Tempo는 데이터를 저장하는 **저장소(Backend)**일 뿐입니다. 애플리케이션에서 발생한 트레이스 데이터를 수집해서 Tempo까지 배달해주는 **에이전트(Agent)**가 필요한데, 그 역할을 **Alloy**가 수행합니다.

### Promtail vs Grafana Alloy 비교

| 구분 | Promtail (구세대) | **Grafana Alloy (신세대)** |
| :--- | :--- | :--- |
| **핵심 역할** | **로그(Log)** 수집 전용 | **로그 + 메트릭 + 트레이스** 통합 수집 |
| **전송 대상** | Loki (로그 저장소) | Loki(로그), Prometheus(메트릭), Tempo(트레이스) |
| **특징** | 기능이 단순하고 명확함 | OpenTelemetry 표준 지원, 프로그래밍 가능한 유연성(River 문법) |

> **결론:** 현대적인 Grafana 스택에서는 **Alloy 하나로 모든 관측성 데이터(Log, Metric, Trace)를 통합 관리**하는 추세입니다. 기존 `promtail` 파드들을 `alloy` 파드로 대체하면 관리 포인트가 획기적으로 줄어듭니다.

---

## 2. 데이터 흐름 아키텍처

Alloy를 도입했을 때 Spring Boot 애플리케이션과 관측성 도구들 간의 데이터 흐름입니다.

```mermaid
graph LR
    subgraph "Kubernetes Pod: Spring Boot App"
        A[Application Logic] -->|Log.info| B(Console / Stdout)
        A -->|Micrometer| C(Metrics)
        A -->|Otel Tracer| D(Traces)
    end

    subgraph "Kubernetes Node / Service"
        E[<b>Grafana Alloy</b><br/>(Telemetry Collector)]
    end

    subgraph "Monitoring Backend"
        L[(Loki<br/>Logs)]
        P[(Prometheus<br/>Metrics)]
        T[(Tempo<br/>Traces)]
    end

    %% Flows
    B -.->|File Scrape (Passive)| E
    C -->|OTLP Push (Active)| E
    D -->|OTLP Push (Active)| E

    E -->|Write| L
    E -->|Remote Write| P
    E -->|Export| T

    style E fill:#f9f,stroke:#333,stroke-width:2px,color:black
    style T fill:#ff9,stroke:#333,stroke-width:2px,color:black
```

### 동작 방식 상세
1.  **로그 (Logs):**
    *   앱은 단순히 콘솔에 출력합니다.
    *   Alloy가 노드에 쌓인 로그 파일을 알아서 읽어갑니다 (Passive).
2.  **메트릭/트레이스 (Metrics/Traces):**
    *   앱이 Alloy의 주소(`http://alloy:4317`)로 데이터를 직접 전송합니다 (Active).

---

## 3. Spring Boot 설정 가이드

Alloy에게 데이터를 보내기 위한 최소한의 설정입니다.

### build.gradle
```groovy
dependencies {
    // Actuator (기본 모니터링)
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    // Micrometer Tracing (트레이싱 기능)
    implementation 'io.micrometer:micrometer-tracing-bridge-otel'
    // OTLP Exporter (Alloy로 데이터 전송)
    implementation 'io.opentelemetry:opentelemetry-exporter-otlp'
}
```

### application.yaml
```yaml
management:
  tracing:
    sampling:
      probability: 1.0 # 모든 요청 기록 (운영 환경에선 조절 필요)
  otlp:
    tracing:
      endpoint: "http://alloy.monitoring.svc.cluster.local:4317" # Alloy 서비스 주소
```

---

## 4. Tempo 리소스 및 운영 정책

Tempo 도입 시 고려해야 할 인프라 리소스 및 저장소 정책입니다.

### 리소스 소비 (CPU/Memory)
*   **CPU:** 평상시 사용량은 낮음. 무거운 조회(TraceQL) 실행 시 순간적으로 사용량 급증.
*   **Memory:** 데이터를 스토리지로 보내기 전 버퍼링(Buffer) 용도로 사용. 트래픽 양에 비례.
*   **Disk (Local PVC):** 장기 저장용 아님. 데이터 유실 방지(WAL)를 위한 임시 공간이므로 고성능 SSD 소량(10GB 내외)이면 충분.

### 데이터 저장 (Object Storage 필수)
Tempo는 로컬 디스크에 트레이스를 저장하지 않습니다.
*   **필수 요건:** S3 호환 Object Storage (AWS S3, GCS, Azure Blob 등).
*   **On-Premise/Local 대안:** **MinIO**를 함께 배포하여 S3처럼 사용.
*   **저장 방식:** Alloy → Tempo(메모리) → (압축/청크) → Object Storage

### 백업 및 보관(Retention)
*   **보관 주기:** 보통 **3일 ~ 14일** (트레이스 데이터는 휘발성이 강해 장기 보관 가치가 낮음).
*   **백업:** 데이터 양이 방대하여 별도 백업은 잘 수행하지 않음. 필요시 스토리지(S3) 레벨의 복제(Replication) 사용.

---

## 5. Alloy 미사용 시 대안

Alloy를 사용하지 않는다면 다음 두 가지 방법이 있습니다.

1.  **Direct Ingest (앱 → Tempo):**
    *   앱이 Tempo로 직접 전송. 구조는 간단하나 앱과 백엔드 간 결합도가 높아짐.
2.  **OpenTelemetry Collector (표준 구현체):**
    *   Alloy 대신 CNCF 표준 Collector 사용. 표준 호환성은 좋으나 Alloy의 로그 통합 편의성은 잃음.

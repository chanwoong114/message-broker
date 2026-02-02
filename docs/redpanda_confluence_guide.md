# [개발 가이드] 차세대 메시지 브로커 Redpanda 도입 가이드

본 문서는 사내 솔루션의 메시징 백본으로 도입되는 **Redpanda**에 대한 개발자용 가이드입니다. 기존 Kafka 환경에 익숙한 개발자라면 별도의 학습 없이 즉시 적용 가능하며, 성능과 운영 편의성 면에서 더 나은 개발 환경을 제공합니다.

---

## 1. Redpanda 소개
**"C++로 다시 설계된, 하드웨어 최적화 Kafka 호환 플랫폼"**

Redpanda는 Apache Kafka API와 100% 호환되는 고성능 스트리밍 플랫폼입니다. 기존 Kafka의 JVM 기반 아키텍처를 C++와 Seastar 프레임워크로 재작성하여, 최신 하드웨어(NVMe, Multi-core) 성능을 극한으로 활용합니다.

### 핵심 특징 (Key Features)
*   **Native Performance:** JVM을 걷어내고 C++로 작성되어 가비지 컬렉션(GC)으로 인한 레이턴시 튐 현상이 없습니다.
*   **Unified Raft:** 메타데이터와 데이터 복제 로직이 Raft 알고리즘 하나로 통합되어 있어 시스템이 견고하고 장애 복구가 매우 빠릅니다.
*   **Single Binary:** 외부 의존성(Zookeeper 등)이 전혀 없는 단일 바이너리 구조로 배포와 관리가 매우 단순합니다.

---

## 2. Kafka (KRaft) vs Redpanda 비교

최신 Kafka도 Zookeeper를 제거(KRaft)했지만, Redpanda는 아키텍처 레벨에서 다음과 같은 근본적인 차이가 있습니다.

| 비교 항목 | Apache Kafka (KRaft) | Redpanda | 개발자 체감 이득 |
| :--- | :--- | :--- | :--- |
| **실행 환경** | **JVM (Java)** | **Native (C++)** | JVM 튜닝 및 메모리 관리 스트레스 해소. |
| **성능 (Latency)** | GC로 인한 Spike 현상 존재 | **매우 낮고 일정함 (Flat)** | 실시간성 서비스의 성능 예측 및 보장 용이. |
| **아키텍처** | Raft(메타데이터) + ISR(복제) 혼용 | **Raft (Unified)** | 구조적 단순함으로 인한 높은 신뢰성과 빠른 복구. |
| **운영 도구** | 별도 설치 필요 (Kafka UI 등) | **Console 내장** | 설치 즉시 웹에서 메시지 열람 및 관리 가능. |
| **리소스 요구량** | 높음 (최소 수 GB) | **매우 낮음 (수백 MB)** | 로컬 개발 환경에서 가볍게 구동 가능. |

---

## 3. Java Spring Boot 연동 방법

Redpanda는 Kafka 프로토콜을 사용하므로, 기존의 **`spring-kafka`** 라이브러리를 코드 수정 없이 그대로 사용합니다.

### 3.1 의존성 추가 (build.gradle)
```gradle
dependencies {
    // Kafka (Redpanda) 연동을 위한 Spring Boot Starter
    implementation 'org.springframework.kafka:spring-kafka'
}
```

### 3.2 설정 (application.yml)
접속 주소(`bootstrap-servers`)를 Redpanda 환경에 맞게 설정합니다.
```yaml
spring:
  kafka:
    bootstrap-servers: ${REDPANDA_HOST}:30092, ${REDPANDA_HOST}:30093, ${REDPANDA_HOST}:30094
    consumer:
      group-id: my-service-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

### 3.3 코드 예시 (Producer)
```java
@Service
public class MessageProducer {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String topic, String message) {
        // Redpanda로 전송되지만 코드는 Kafka와 완벽히 동일합니다.
        kafkaTemplate.send(topic, message);
    }
}
```

---

## 4. 공식 문서 및 API 참조

개발 및 트러블슈팅 시 아래 공식 문서를 참고하세요.

*   **Redpanda 공식 문서:** [https://docs.redpanda.com](https://docs.redpanda.com)
*   **Kafka Protocol Reference:** [https://kafka.apache.org/protocol](https://kafka.apache.org/protocol) (메시지 통신 규격)
*   **Admin API Docs:** [https://docs.redpanda.com/api/admin-api/](https://docs.redpanda.com/api/admin-api/) (클러스터 관리용 REST API)
*   **Redpanda Console Guide:** [https://docs.redpanda.com/docs/manage/console/](https://docs.redpanda.com/docs/manage/console/) (웹 UI 상세 가이드)

---

## 5. 접속 정보 (개발 환경)

현재 구축된 개발 서버의 접속 정보입니다. (로컬 테스트 기준)

*   **Kafka Bootstrap Servers:** `localhost:30092, localhost:30093, localhost:30094`
*   **Redpanda Console (웹 UI):** `http://localhost:8080`
    *   *주요 기능: 토픽 생성/삭제, 실시간 메시지 Payload 열람, Consumer Lag 모니터링*
*   **Schema Registry 주소:** `http://localhost:8081`
*   **Admin API 주소:** `http://localhost:9644`

---
**💡 Tip:** 로컬 개발 시 `Kafka Tool` (Offset Explorer), `kcat(kafkacat)` 등의 외부 도구도 위 접속 정보를 통해 그대로 연결하여 사용 가능합니다.

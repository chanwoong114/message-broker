# Redpanda vs RabbitMQ: 미들웨어 연동 및 성능 심층 비교 (2차)

**작성일:** 2026-01-23
**목적:** DevOps/CI/CD 미들웨어와의 연동성 및 하드웨어 요구 스펙 기반의 도입 적합성 판단.

---

## 1. 미들웨어 연동성 분석 (Integration)

주요 DevOps 도구들이 해당 브로커를 **Native(플러그인 등)**로 지원하는지, 아니면 **Webhook + Adapter(개발 필요)** 방식이어야 하는지 비교합니다.

| 미들웨어 | RabbitMQ (AMQP) | Redpanda (Kafka) | 연동 방식 및 난이도 |
| :--- | :--- | :--- | :--- |
| **Jenkins** | ✅ **지원 (Native)** | ✅ **지원 (Plugin)** | Jenkins는 둘 다 플러그인(`RabbitMQ Build Trigger`, `Kafka Integration`)이 존재하여 연동이 쉽습니다. |
| **GitLab** | ❌ 미지원 (Webhook) | ❌ 미지원 (Webhook) | GitLab은 기본적으로 Webhook만 쏩니다. 브로커로 보내려면 **Webhook을 받아서 큐에 넣어주는 별도 서버(Producer App)**가 필요합니다. |
| **Harbor** | ❌ 미지원 (Webhook) | ❌ 미지원 (Webhook) | Docker 이미지 푸시 이벤트 등을 Webhook으로만 보냅니다. (Adapter 필요) |
| **SonarQube** | ❌ 미지원 (Webhook) | ❌ 미지원 (Webhook) | 분석 완료 알림을 Webhook으로 전송. |
| **Nexus** | ❌ 미지원 (Webhook) | ❌ 미지원 (Webhook) | (Pro 버전의 경우 기능이 다를 수 있으나 기본적으로 Webhook 기반) |
| **MinIO** | ✅ **지원 (Native)** | ✅ **지원 (Native)** | MinIO는 Bucket Notification으로 **Kafka(Redpanda)**와 **RabbitMQ**를 모두 완벽하게 지원합니다. 설정만 하면 파일 업로드 시 이벤트가 발행됩니다. |

### 💡 결론 (연동성)
*   **MinIO, Jenkins:** 둘 다 플러그인/설정으로 바로 연동 가능.
*   **GitLab, Harbor, SonarQube:** 둘 다 **직접 연동 불가.** Webhook을 받아서 브로커로 쏘는 **"Webhook Adaptor"** 애플리케이션을 개발해야 함. (난이도 동일)

---

## 2. 성능 및 하드웨어 스펙 (Performance & Specs)

### 2.1 처리량 및 부하 (Throughput)
*   **Redpanda (Kafka):** **압도적 우위.** 디스크 순차 쓰기(Sequential Write)에 최적화되어 있어, 초당 수십만~수백만 건의 로그를 처리할 수 있습니다.
*   **RabbitMQ:** 메시지마다 라우팅 로직이 돌고 메모리를 많이 사용하여, 대량의 로그성 데이터 처리 시 병목이 올 수 있습니다.

### 2.2 권장 사양 (Hardware Requirements)

| 구분 | RabbitMQ | Redpanda |
| :--- | :--- | :--- |
| **CPU** | 코어 수보다 클럭 속도가 중요함. | 코어 수가 많을수록 좋음 (Thread-per-Core). |
| **Memory** | **많아야 함.** 메시지를 RAM에 큐잉하므로 메모리가 차면 성능이 급격히 저하됨. (최소 4GB, 권장 8GB+) | **적어도 됨.** 디스크를 적극 활용하므로 메모리는 캐시용으로만 씀. (최소 2GB로도 쌩쌩함) |
| **Disk** | 일반 SSD도 무방. | **NVMe SSD 권장.** (XFS 파일시스템 필수). 디스크 속도가 성능을 좌우함. |

---

## 3. 메시지 히스토리 및 보존 (Retention)

### 3.1 Redpanda (Log Storage)
*   **방식:** 디스크에 로그 파일 형태로 영구 저장.
*   **장점:** **"지난주 수요일 배포 로그 다시 보여줘"**가 가능함. (Replay)
*   **활용:** 감사 로그(Audit), 트러블슈팅, 데이터 분석용으로 매우 적합.

### 3.2 RabbitMQ (Queue Storage)
*   **방식:** 컨슈머가 가져가면(Ack) 삭제됨.
*   **한계:** 기본적으로 히스토리가 남지 않음. (Quorum Queue나 Stream Queue 기능이 생겼지만 Kafka만큼 강력하거나 관리하기 쉽지 않음)
*   **활용:** "지금 당장 처리해야 할 작업" 관리에 집중.

---

## 4. 최종 도입 가이드

### 🏆 Redpanda 추천 시나리오
*   **"모든 미들웨어의 로그와 이벤트를 모아서(Log Aggregation) 나중에 분석하고 싶다."**
*   **"MinIO 파일 업로드 이벤트를 놓치지 않고 다 받고 싶다."**
*   **"서버 메모리가 부족하다."** (디스크 기반이라 메모리 효율 좋음)

### 🥈 RabbitMQ 추천 시나리오
*   **"Jenkins 빌드 결과를 복잡하게 라우팅해서 A팀, B팀에게 다르게 알림을 보내고 싶다."** (Routing Key 활용)
*   **"메시지 순서가 생명이고, 하나씩 꺼내서 처리하는 작업 큐가 필요하다."**

**종합 의견:**
미들웨어 통신용으로는 **Webhook Adapter를 어차피 개발해야 하므로**, 개발 편의성은 비슷합니다.
하지만 **"로그 저장 및 이력 관리(History)"**가 중요하다면 **Redpanda**가 훨씬 유리하며, **리소스 효율성(메모리 절약)** 측면에서도 Redpanda가 더 나은 선택이 될 것입니다.

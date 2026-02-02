# Redpanda 도입을 위한 핵심 가이드 (Core Concepts & Operations)

> **작성일:** 2026-01-27  
> **대상:** Redpanda(Kafka API)를 처음 도입하는 개발자 및 엔지니어  
> **목적:** 필수 용어 이해 및 운영/보안 기초 지식 습득

## 1. 핵심 개념 (Core Concepts)

Redpanda는 Kafka API와 호환되므로, Kafka의 용어와 개념을 그대로 사용합니다.

### 1.1 데이터 구조
*   **Topic (토픽):**
    *   데이터가 저장되는 **"주제"** 또는 **"폴더"**입니다. (예: `payment-logs`, `user-signups`)
    *   Producer는 특정 토픽으로 메시지를 보내고, Consumer는 토픽을 구독합니다.
*   **Partition (파티션):**
    *   토픽을 여러 개로 쪼갠 **"물리적인 파일 단위"**입니다.
    *   **병렬 처리의 핵심**입니다. 파티션이 3개면, 최대 3개의 컨슈머가 동시에 데이터를 가져갈 수 있습니다.
    *   *주의:* 파티션 내에서는 순서가 보장되지만, 파티션 간(전체 토픽)에는 순서가 보장되지 않습니다.
*   **Offset (오프셋):**
    *   파티션 내에서 메시지가 저장된 **"고유 번호(위치)"**입니다. (예: 0, 1, 2, ...)
    *   데이터는 지워지지 않고 계속 쌓이며(Log), Consumer는 "나 5번까지 읽었어"라고 오프셋을 기록(Commit)합니다.

### 1.2 행위자 (Actors)
*   **Producer (생산자):** 데이터를 만들어 토픽에 넣는 애플리케이션입니다.
*   **Consumer (소비자):** 토픽에서 데이터를 읽어가는 애플리케이션입니다. **Pull 방식**을 사용하여 자신의 속도에 맞춰 데이터를 가져갑니다.
*   **Consumer Group (컨슈머 그룹):**
    *   **가장 중요한 개념**입니다. 하나의 목적을 위해 협력하는 소비자들의 모임입니다.
    *   **Load Balancing:** 그룹 내 컨슈머들은 토픽의 파티션을 **나눠서** 담당합니다. (N:1 매핑 불가, 1:N 매핑 가능)
    *   **Replay:** 그룹의 오프셋(`committed offset`)을 초기화하면 과거 데이터를 다시 읽을 수 있습니다.

---

## 2. Redpanda 아키텍처 특징

### 2.1 Thread-per-Core 모델
*   Redpanda는 CPU 코어 하나당 하나의 스레드를 고정(Pinning)하여 사용합니다.
*   **장점:** 컨텍스트 스위칭 비용이 없어 지연 시간(Latency)이 매우 낮습니다.
*   **운영 팁:** 컨테이너의 CPU Limit을 정수 단위(1, 2, 4...)로 주는 것이 성능 효율이 가장 좋습니다.

### 2.2 리플리카(Replica)와 리더(Leader)
*   데이터 유실 방지를 위해 파티션을 여러 노드에 복제합니다. (`Replication Factor`)
*   **Leader:** 실제 읽기/쓰기를 담당하는 노드.
*   **Follower:** 리더의 데이터를 복제만 하는 노드. (리더가 죽으면 승격됨)

---

## 3. 보안 및 접근 제어 (Security)

엔터프라이즈 환경에서는 보안 설정이 필수입니다.

### 3.1 인증 (Authentication) - "누구인가?"
*   **SASL/SCRAM:** ID/Password 방식. 가장 보편적입니다.
*   **mTLS:** 인증서를 통한 양방향 인증. 보안성이 높습니다.
*   **OIDC/OAuth:** (Redpanda Enterprise 기능) 구글/Okta 등과 연동.

### 3.2 권한 (Authorization) - "무엇을 할 수 있는가?"
*   **ACL (Access Control List):** Kafka 표준 권한 관리입니다.
    *   *예시:* "User A는 `payment` 토픽에 `Write`만 가능하고, User B는 `Read`만 가능하다."
    *   `rpk acl create` 명령어로 관리합니다.

### 3.3 암호화 (Encryption)
*   **TLS (SSL):** 클라이언트와 브로커 간 통신을 암호화합니다. (HTTPS와 유사)
*   운영 환경(Production)에서는 필수입니다.

---

## 4. 운영 및 관리 포인트 (Management)

### 4.1 CLI 도구: `rpk`
Redpanda 관리는 `rpk` (Redpanda Keeper) 명령어로 99% 해결됩니다.
*   `rpk topic create <name>`: 토픽 생성
*   `rpk topic list`: 토픽 목록 조회
*   `rpk topic consume <name>`: 메시지 내용 확인 (디버깅용)
*   `rpk cluster info`: 클러스터 상태 확인
*   `rpk acl ...`: 권한 관리

### 4.2 모니터링 지표 (Observability)
Prometheus/Grafana 연동 시 다음 지표가 가장 중요합니다.
1.  **Consumer Lag:** (가장 중요) 처리가 얼마나 밀리고 있는지.
2.  **Under Replicated Partitions:** 복제가 제대로 안 되고 있는 파티션 수 (0이어야 정상).
3.  **Disk Usage:** 로그가 쌓여 디스크가 꽉 차지 않는지.

### 4.3 데이터 보관 정책 (Retention)
디스크가 무한하지 않으므로, 오래된 데이터를 언제 지울지 설정해야 합니다.
*   `retention.bytes`: 토픽 용량이 N 바이트를 넘으면 오래된 것부터 삭제.
*   `retention.ms`: N 시간이 지나면 삭제 (예: 7일 = 604800000ms).

---

## 5. 도입 전 최종 체크리스트 (Pre-flight Check)

### ✅ 인프라
- [ ] **스토리지:** 로컬 SSD/NVMe를 확보했는가? (NFS 사용 시 `developer_mode` 필수 확인)
- [ ] **메모리:** 운영용이라면 코어당 2GB 이상 할당했는가?
- [ ] **네트워크:** 클라이언트(Producer/Consumer)와 브로커 간 포트(9092) 통신이 가능한가?

### ✅ 설정
- [ ] **파티션 수:** 예상 처리량에 맞춰 적절히 설정했는가? (보통 브로커 코어 수의 배수로 설정)
- [ ] **Replication Factor:** 3 (운영) 또는 1 (개발)로 설정했는가?
- [ ] **Ack 모드:** 프로듀서의 `acks` 설정 (속도: 0/1, 안전: all)을 결정했는가?

### ✅ 애플리케이션
- [ ] **Consumer Group ID:** 서비스별로 고유한 Group ID를 지정했는가?
- [ ] **에러 처리:** 메시지 처리 실패 시 재시도(Retry) 또는 DLQ 로직이 구현되었는가?

# Apache Kafka Deep Dive & Learning Guide

## 1. Kafka 아키텍처 및 핵심 원리

### 1.1 Log-based Messaging (로그 기반 메시징)
Kafka의 본질은 **분산 커밋 로그(Distributed Commit Log)**입니다.
*   기존 MQ(RabbitMQ 등)는 메시지를 배달하고 나면 큐에서 삭제했습니다.
*   Kafka는 메시지를 디스크에 **순차적으로 기록(Append Only)**하고 지우지 않습니다(설정된 기간/용량까지).
*   이로 인해 컨슈머가 죽었다 살아나도 과거 데이터를 다시 읽을 수 있고(Replay), 여러 컨슈머가 서로 다른 속도로 같은 데이터를 읽을 수 있습니다.

### 1.2 Topic, Partition, Segment
*   **Topic:** 데이터가 들어가는 논리적인 채널명 (예: `user-logs`).
*   **Partition:** 토픽을 물리적으로 쪼갠 단위. 병렬 처리의 핵심입니다. 파티션이 3개면 컨슈머도 최대 3개까지 동시에 붙어서 병렬로 읽을 수 있습니다.
*   **Segment:** 파티션 내의 실제 파일들(.log, .index). 오래된 메시지를 삭제할 때 파티션 전체가 아닌 오래된 세그먼트 파일만 지웁니다.

### 1.3 Consumer Group & Offset
*   **Consumer Group:** 하나의 목적을 위해 협력하는 컨슈머들의 집합. 파티션은 하나의 그룹 내에서 오직 하나의 컨슈머에게만 할당됩니다. (1:1 매핑 원칙).
*   **Offset:** 컨슈머가 어디까지 읽었는지 나타내는 숫자(Bookmark). Kafka는 이 오프셋을 `__consumer_offsets`라는 내부 토픽에 저장하여 관리합니다.

### 1.4 KRaft (Kafka Raft Metadata) Mode
*   과거 Kafka는 클러스터 메타데이터 관리를 위해 Zookeeper에 의존했습니다.
*   최신 버전(3.x)부터는 **KRaft** 모드가 도입되어, Kafka 브로커 내부에서 Raft 알고리즘으로 메타데이터를 직접 관리합니다.
*   **장점:** 아키텍처 단순화, 배포 용이성 증가, 메타데이터 전파 속도 향상.

## 2. 운영 및 설정 가이드 (Best Practices)

### 2.1 OS 레벨 튜닝 (프로덕션 필수)
*   **File Descriptors:** Kafka는 많은 파일을 엽니다. `ulimit -n 100000` 이상 설정 필요.
*   **Page Cache:** Kafka는 별도의 힙 메모리 캐시를 거의 안 쓰고, OS의 페이지 캐시에 전적으로 의존합니다. 따라서 힙 메모리는 적당히(6~8GB) 주고, 남은 RAM을 OS가 캐시로 쓰게 두는 것이 성능에 유리합니다.

### 2.2 중요 설정 파라미터
*   `min.insync.replicas`: 프로듀서가 `acks=all`로 보냈을 때, 최소 몇 개의 복제본이 저장되어야 성공으로 칠 것인가. (보통 2로 설정).
*   `unclean.leader.election.enable=false`: 리더가 죽었을 때, 데이터가 동기화되지 않은 팔로워가 리더가 되는 것을 허용할지 여부. `false`여야 데이터 유실 없음.
*   `auto.create.topics.enable=false`: 실수로 이상한 토픽이 생기는 것을 방지하기 위해 프로덕션에서는 끕니다.

## 3. 트러블슈팅 & 시나리오

### 3.1 Rebalancing Storm (리밸런싱 폭풍)
*   **증상:** 컨슈머들이 일을 안 하고 계속 멈췄다 돌았다 함.
*   **원인:** 컨슈머 하나가 너무 느리거나 GC(Garbage Collection) 때문에 멈춰서 하트비트를 못 보냄 -> 코디네이터는 죽은 줄 알고 내쫓음 -> 리밸런싱 발생 -> 다시 살아나서 들어옴 -> 무한 반복.
*   **해결:** `session.timeout.ms`를 늘리고, `max.poll.records`를 줄여서 한 번에 처리하는 양을 줄임.

### 3.2 Leader Not Available
*   **증상:** 클라이언트 에러 로그에 지속적으로 발생.
*   **원인:** 브로커 간 통신 문제로 컨트롤러가 리더 선출을 못 하거나, 모든 ISR(In-Sync Replicas)이 죽음.

## 4. 납품용 솔루션 관점
Kafka는 무겁습니다. 하지만 데이터 파이프라인의 표준이며, 에코시스템(Kafka Connect, Kafka Streams)이 가장 강력합니다. 고객사가 대용량 처리를 요구하거나, 이미 Kafka를 쓰고 있다면 Kafka를 선택하는 것이 가장 안전합니다.

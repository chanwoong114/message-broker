# 학습 계획 (Learning Plan)

이 프로젝트를 통해 메시지 브로커의 운영 및 아키텍처를 깊이 이해하기 위한 단계별 계획입니다.

## Phase 1: 환경 구성 및 기본 이해 (Week 1)
**목표:** 로컬 K8s에 모든 브로커를 띄우고 "Hello World" 메시지를 주고받는다.

1.  **사전 준비:**
    *   Docker Desktop / Minikube / Kind 중 로컬 K8s 환경 선정.
    *   Helm 설치 및 기본 사용법 숙지.
    *   K9s 설치 (K8s 클러스터 관리를 쉽게 하기 위해).
2.  **배포 실습:**
    *   각 폴더(`kafka`, `nats` 등)에 `Helm` 차트 기반의 `values.yaml` 작성.
    *   `kubectl`로 배포 및 상태 확인.
3.  **기본 통신:**
    *   각 브로커별 CLI 클라이언트 파드를 띄워서 메시지 send/receive 테스트.

## Phase 2: 아키텍처 심화 및 HA 테스트 (Week 2)
**목표:** 브로커가 어떻게 데이터를 분산 저장하고 장애를 견디는지 이해한다.

1.  **이론 학습:**
    *   Kafka: Partition, Replication Factor, ISR, Controller.
    *   RabbitMQ: Exchange Types, Queue Mirroring (Quorum Queues).
    *   NATS: JetStream, Stream vs Consumer, RAFT algorithm.
2.  **Chaos 테스트 (장애 대응):**
    *   클러스터 모드(최소 3 노드)로 배포 변경.
    *   `docs/test_scenarios.md`의 "장애 대응" 항목 수행.
    *   파드가 죽었을 때 로그를 통해 Leader Election 과정을 관찰.

## Phase 3: 모니터링 및 운영 (Week 3)
**목표:** 블랙박스처럼 보이는 브로커의 내부 상태를 시각화한다.

1.  **Prometheus & Grafana:**
    *   K8s 내에 모니터링 스택 배포 (kube-prometheus-stack 추천).
    *   각 브로커의 Exporter 설정 (ServiceMonitor 등).
2.  **대시보드 구성:**
    *   주요 지표(Lag, TPS, Disk Usage)를 포함한 대시보드 임포트 및 커스텀.
3.  **알람 설정 (Optional):**
    *   디스크가 80% 찼을 때 알람이 오도록 설정해보기.

## Phase 4: 성능 튜닝 및 부하 테스트 (Week 4)
**목표:** 한계를 시험하고 최적의 설정을 찾는다.

1.  **부하 테스트 도구 구성:**
    *   k6 또는 브로커별 벤치마크 툴 사용.
2.  **시나리오 수행:**
    *   메시지 크기(1KB vs 1MB)에 따른 성능 변화 확인.
    *   Ack 설정(All vs 1 vs 0)에 따른 속도 차이와 데이터 유실 위험성 비교.
3.  **결과 리포트:**
    *   각 브로커별 장단점 정리 및 "납품 솔루션"으로서의 최종 선정.

## 학습 자료 (참고 링크)
*   **Kafka:** Confluent Blog, "Kafka: The Definitive Guide"
*   **RabbitMQ:** RabbitMQ Official Tutorials
*   **NATS:** NATS by Example (https://natsbyexample.com/)
*   **Kubernetes:** Kubernetes.io Docs

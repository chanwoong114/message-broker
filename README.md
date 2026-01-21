# Message Broker Playground on K8s

다양한 오픈소스 메시지 브로커를 로컬 Kubernetes 환경에서 비교 분석, 테스트, 학습하기 위한 프로젝트입니다.

## 🎯 프로젝트 목적
1.  **기술 검증:** 납품용 솔루션에 탑재할 최적의 오픈소스 메시지 브로커 선정.
2.  **운영 역량 강화:** K8s 환경에서의 배포, HA 구성, 장애 대응, 모니터링 실습.
3.  **성능 비교:** 동일한 환경에서 각 브로커의 처리량과 지연 시간 비교.

## 📂 디렉토리 구조
*   `kafka/`: Apache Kafka 배포 및 테스트 구성
*   `rabbitmq/`: RabbitMQ 배포 및 테스트 구성
*   `nats/`: **[추천]** NATS JetStream 배포 및 테스트 구성
*   `pulsar/`: **[추천]** Apache Pulsar 배포 및 테스트 구성
*   `redpanda/`: Redpanda 배포 및 테스트 구성 (라이선스 검토 필요)
*   `redis/`: Valkey (Redis 오픈소스 포크) 배포 및 테스트 구성
*   `docs/`:
    *   `broker_comparison.md`: 브로커별 특징 및 라이선스 비교
    *   `test_scenarios.md`: 테스트 체크리스트
    *   `learning_plan.md`: 학습 로드맵
*   `common/`: 공통 사용 도구 (Prometheus, Grafana, k6 등)

## 🚀 시작하기
각 폴더의 `README.md` (추후 작성 예정)를 참고하여 브로커를 배포합니다.

## ⚠️ 라이선스 주의사항
상용 솔루션 납품 시 **Redpanda**와 **Redis**의 라이선스 제약 조건을 반드시 확인해야 합니다. 본 프로젝트에서는 대안으로 **Valkey** (Redis 대체)와 **NATS JetStream** (고성능, Apache 2.0)을 강력히 추천하며 테스트 대상에 포함했습니다.

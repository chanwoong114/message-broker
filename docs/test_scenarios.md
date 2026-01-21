# 테스트 시나리오 (Test Scenarios)

각 메시지 브로커에 대해 공통적으로 수행할 테스트 목록입니다.

## 1. 기본 기능 테스트 (Functional Testing)
- [ ] **배포 (Deployment):** Helm Chart 또는 K8s Manifest를 통해 정상적으로 배포되는가?
- [ ] **Pub/Sub:** Producer가 메시지를 보내고 Consumer가 정상적으로 수신하는가?
- [ ] **Persistence:** 브로커 재시작 후에도 데이터가 유실되지 않고 남아있는가?

## 2. 장애 대응 및 고가용성 (Chaos Engineering & HA)
*이 테스트는 메시지 유실이 없어야 합니다.*

- [ ] **Pod Kill:** 3-node 클러스터 구성 후, 리더(Leader/Master) 파드를 강제로 삭제(`kubectl delete pod`)했을 때 자동으로 복구되는가?
- [ ] **Network Partition:** (선택) 파드 간 통신을 차단했을 때 클러스터가 뇌 분리(Split-brain) 현상 없이 동작하는가?
- [ ] **Rolling Update:** 버전 업그레이드 또는 설정 변경으로 인한 롤링 업데이트 시 서비스 중단(Downtime)이 발생하는가?

## 3. 부하 및 강도 테스트 (Load & Stress Testing)
*도구: k6 (xk6-kafka, xk6-amqp), Jmeter, 또는 각 브로커별 벤치마크 도구*

- [ ] **Throughput 측정:** 초당 메시지 처리량(TPS) 측정 (1k, 10k, 100k msg/sec).
- [ ] **Latency 측정:** 메시지 발행부터 소비까지 걸리는 지연 시간 측정 (99th percentile).
- [ ] **Backpressure:** Consumer가 처리를 멈췄을 때 브로커의 메모리/디스크 사용량 변화 및 처리.
- [ ] **Long-run:** 24시간 이상 부하를 지속했을 때 메모리 누수나 성능 저하가 없는가?

## 4. 모니터링 및 관측성 (Observability)
- [ ] **Metrics:** Prometheus endpoint가 노출되는가? (메시지 lag, throughput, storage usage 등)
- [ ] **Dashboard:** Grafana 대시보드 연동이 원활한가?
- [ ] **Log:** 에러 로그나 디버그 로그가 분석 가능한 형태로 출력되는가?

## 5. 메시지 기록 및 디버깅 (Message Tracing)
- [ ] **Rewind/Replay:** 소비한 메시지를 다시 읽을 수 있는가? (Kafka/NATS/Pulsar 해당)
- [ ] **Dead Letter Queue (DLQ):** 처리 실패한 메시지가 별도 큐로 이동하여 보관되는가?

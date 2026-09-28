#!/bin/sh
# Kafka 토픽 생성 스크립트 — auto.create.topics=false 환경에서 최초 1회 실행.
# 사용: docker exec nilm-kafka sh < create-topics.sh  또는 컨테이너에 복사 후 실행.
set -eu

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

create() {
  topic="$1"; partitions="$2"
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$topic" \
    --partitions "$partitions" --replication-factor 1
}

# 원천 전력 측정치 (생산: 브릿지·리플레이 / 소비: 실시간 분석·Bronze 적재기)
create power.raw.v1 8

# 감지 이벤트 (생산: 실시간 분석 / 소비: 모니터링)
create analysis.event.v1 8

# 모니터링 DLQ
create dlq.monitoring 1

# 담당자 가입 (생산: iot-device / 소비: 모니터링)
create device.manager-registered.v1 1

# 위험 정책 변경 요청/결과 (모니터링 <-> 분석)
create risk-policy.change.request.v1 1
create risk-policy.change.result.v1 1

echo "--- 토픽 목록 ---"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list

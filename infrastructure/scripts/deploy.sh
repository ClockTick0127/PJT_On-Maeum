#!/usr/bin/env bash
set -euo pipefail
phase=$1
case "$phase" in
  a) target=a ;;
  b-base|b-analysis|b-bridge|b-loader) target=b ;;
  *) echo "Unknown deployment phase" >&2; exit 2 ;;
esac
python3 infrastructure/scripts/release.py check --manifest /opt/nilm/release.json --target "$target"
cd /opt/nilm
case "$phase" in
  b-base)
    docker compose pull postgres kafka kafka-init namenode datanode hdfs-init node-exporter
    docker compose up -d --wait --wait-timeout 300 postgres kafka namenode datanode node-exporter
    docker compose run --rm --no-deps kafka-init
    docker compose run --rm hdfs-init
    ;;
  a)
    docker compose pull redis mosquitto keycloak prometheus grafana blackbox-exporter
    docker compose up -d --wait --wait-timeout 360 redis mosquitto keycloak
    docker compose up -d --pull never --remove-orphans --wait --wait-timeout 360 api-gateway iot-device-service monitoring-service frontend prometheus grafana blackbox-exporter
    ;;
  b-analysis)
    docker compose up -d --pull never --no-deps --wait --wait-timeout 180 realtime-analysis-service
    docker compose up -d --pull never --no-deps --wait --wait-timeout 180 aggregation-service
    ;;
  b-bridge)
    docker compose up -d --pull never --no-deps --wait --wait-timeout 180 mqtt-kafka-bridge
    docker compose exec -T mqtt-kafka-bridge python smoke.py
    ;;
  b-loader)
    docker compose up -d --pull never --no-deps --wait --wait-timeout 180 bronze-loader
    docker compose up -d --pull never --no-deps --wait --wait-timeout 180 session-lake-loader
    # realtime-analysis-service applies the outbox migration before this phase.
    # The resident publisher resumes every durable PENDING row after restarts.
    docker compose up -d --pull never --no-deps --wait --wait-timeout 180 gold-profile-publisher
    ;;
  *) echo "Unknown deployment phase" >&2; exit 2 ;;
esac

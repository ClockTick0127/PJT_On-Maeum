#!/usr/bin/env bash
set -euo pipefail
phase=$1
cd /opt/nilm
case "$phase" in
  b-base)
    docker compose pull postgres kafka kafka-init mqtt-kafka-bridge
    docker compose up -d --wait --wait-timeout 300 postgres kafka
    docker compose run --rm --no-deps kafka-init
    ;;
  a)
    docker compose pull
    docker compose up -d --wait --wait-timeout 360 redis mosquitto keycloak
    docker compose up -d --wait --wait-timeout 360 api-gateway iot-device-service monitoring-service frontend
    ;;
  b-bridge)
    docker compose up -d --no-deps --wait --wait-timeout 180 mqtt-kafka-bridge
    docker compose exec -T mqtt-kafka-bridge python smoke.py
    ;;
  *) echo "Unknown deployment phase" >&2; exit 2 ;;
esac

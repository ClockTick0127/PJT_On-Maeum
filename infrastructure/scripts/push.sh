#!/usr/bin/env bash
set -euo pipefail
for service in api-gateway iot-device-service monitoring-service mqtt-kafka-bridge frontend; do
  docker push "$IMAGE_PREFIX/nilm-$service:$IMAGE_TAG"
done

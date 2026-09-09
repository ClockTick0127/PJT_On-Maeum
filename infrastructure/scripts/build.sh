#!/usr/bin/env bash
set -euo pipefail
: "${IMAGE_PREFIX:?IMAGE_PREFIX required}" "${IMAGE_TAG:?IMAGE_TAG required}"
for service in api-gateway iot-device-service monitoring-service; do
  docker build -t "$IMAGE_PREFIX/nilm-$service:$IMAGE_TAG" "backend/$service"
done
docker build -t "$IMAGE_PREFIX/nilm-mqtt-kafka-bridge:$IMAGE_TAG" infrastructure/mqtt-kafka-bridge
frontend_args=()
if [[ -n "${FRONTEND_ENV:-}" ]]; then
  frontend_hash=$(sha256sum "$FRONTEND_ENV" | cut -d ' ' -f 1)
  frontend_args=(--secret "id=frontend_env,src=$FRONTEND_ENV" --build-arg "FRONTEND_ENV_HASH=$frontend_hash")
fi
docker build "${frontend_args[@]}" -t "$IMAGE_PREFIX/nilm-frontend:$IMAGE_TAG" frontend

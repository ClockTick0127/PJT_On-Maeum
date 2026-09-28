#!/usr/bin/env bash
set -euo pipefail
: "${IMAGE_REPOSITORY:?IMAGE_REPOSITORY required}" "${IMAGE_TAG:?IMAGE_TAG required}" "${RELEASE_ID:?RELEASE_ID required}"
for service in api-gateway iot-device-service monitoring-service; do
  docker build -t "$IMAGE_REPOSITORY:$service-$RELEASE_ID" "backend/$service"
done
docker build --target test ai/realtime-analysis-service
docker build -f ai/deployment/Dockerfile --target configurable-inference \
  -t "$IMAGE_REPOSITORY:realtime-analysis-service-$RELEASE_ID" ai
docker build --target test -f batch/aggregation_service/Dockerfile .
docker build --target runtime -f batch/aggregation_service/Dockerfile \
  -t "$IMAGE_REPOSITORY:aggregation-service-$RELEASE_ID" .
docker build -t "$IMAGE_REPOSITORY:mqtt-kafka-bridge-$RELEASE_ID" infrastructure/mqtt-kafka-bridge
docker build -t "$IMAGE_REPOSITORY:simulator-$RELEASE_ID" infrastructure/mqtt/simulator
docker build -t "$IMAGE_REPOSITORY:bronze-loader-$RELEASE_ID" collection/bronze-loader
docker build --target test --build-context analysis=ai/realtime-analysis-service \
  collection/session-lake-loader
docker build --target runtime --build-context analysis=ai/realtime-analysis-service \
  -t "$IMAGE_REPOSITORY:session-lake-loader-$RELEASE_ID" collection/session-lake-loader
docker build --target test --build-context analysis=ai/realtime-analysis-service \
  batch/power_silver_service
docker build --target runtime --build-context analysis=ai/realtime-analysis-service \
  -t "$IMAGE_REPOSITORY:power-silver-$RELEASE_ID" batch/power_silver_service
docker build --target test --build-context analysis=ai/realtime-analysis-service \
  --build-context silver=batch/power_silver_service batch/gold_profile_service
docker build --target runtime --build-context analysis=ai/realtime-analysis-service \
  --build-context silver=batch/power_silver_service \
  -t "$IMAGE_REPOSITORY:gold-profile-$RELEASE_ID" batch/gold_profile_service
frontend_args=()
if [[ -n "${FRONTEND_ENV:-}" ]]; then
  frontend_hash=$(sha256sum "$FRONTEND_ENV" | cut -d ' ' -f 1)
  frontend_args=(--secret "id=frontend_env,src=$FRONTEND_ENV" --build-arg "FRONTEND_ENV_HASH=$frontend_hash")
fi
docker build "${frontend_args[@]}" -t "$IMAGE_REPOSITORY:frontend-$RELEASE_ID" frontend

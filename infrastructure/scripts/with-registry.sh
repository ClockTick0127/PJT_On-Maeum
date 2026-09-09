#!/usr/bin/env bash
set -euo pipefail
set +x
: "${REGISTRY:?REGISTRY required}" "${REGISTRY_USER:?}" "${REGISTRY_PASSWORD:?}"
export DOCKER_CONFIG
DOCKER_CONFIG=$(mktemp -d)
trap 'rm -rf -- "$DOCKER_CONFIG"' EXIT
printf '%s' "$REGISTRY_PASSWORD" | docker login "${REGISTRY%%/*}" --username "$REGISTRY_USER" --password-stdin
"$@"

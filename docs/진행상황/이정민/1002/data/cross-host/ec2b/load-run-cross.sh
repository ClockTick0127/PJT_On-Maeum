#!/usr/bin/env bash
# EC2-B 전용. 기존 load-run.sh 와 같되, 드라이버 포트 두 개를 사설 IP 로 공개한다.
#   sudo bash load-run-cross.sh 500 w3xb1a2r1 3
set -uo pipefail
N=$1; CFG=$2; W=${3:-1}
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
source /opt/nilm/experiments/load/bin/common.sh
mapfile -t ENVS < <(grep -v '^#' "$BASE/batch-$N-$CFG.env" | grep . | sed 's/^/-e\n/')
docker compose run --rm --no-deps --name "load-driver-$N-$CFG" "${ENVS[@]}" \
  -p "$B_IP:$DRIVER_PORT:$DRIVER_PORT" -p "$B_IP:$DRIVER_BM_PORT:$DRIVER_BM_PORT" \
  -v "$BASE:/work:ro" gold-profile gold-experiment \
  --experiment-id "load-$N-$CFG" --dataset-id "load-${N}h-10s-${FROM}-${TO}" --households "$N" \
  --sampling-seconds 10 --workers "$W" --run-mode INITIAL_BUILD --image "$GOLD_IMAGE" --keep-going \
  --from "$FROM" --to "$TO"

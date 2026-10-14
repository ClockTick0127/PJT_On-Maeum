#!/usr/bin/env bash
# EC2-B 전용. scenarios.conf 의 시나리오를 순서대로 각 REPEATS 회 실행한다.
#   setsid nohup sudo bash run-all.sh 3 > /opt/nilm/experiments/load/run-all.log 2>&1 &
#   sudo bash run-all.sh 3 my-scenarios.conf
set -uo pipefail
REPEATS=${1:-3}; CONF=${2:-"$(cd "$(dirname "$0")" && pwd)/scenarios.conf"}
HERE=$(cd "$(dirname "$0")" && pwd)
# experiment.sh 안의 ssh 등이 stdin 을 읽어 conf 의 다음 줄을 삼키지 않도록 목록을 먼저 배열로 읽는다.
mapfile -t SCENARIOS < <(grep -vE '^\s*(#|$)' "$CONF")
for LINE in "${SCENARIOS[@]}"; do
  read -r NAME DN BW AW <<< "$LINE"
  echo "$(date +%FT%T) ===== scenario $NAME (datanodes=$DN B=$BW A=$AW) x$REPEATS"
  bash "$HERE/experiment.sh" "$NAME" "$DN" "$BW" "$AW" "$REPEATS" < /dev/null
done
echo "$(date +%FT%T) RUN ALL DONE"
echo "== summary (exit 0/10 only)"
awk -F, 'NR>1 && ($8=="0"||$8=="10") {k=$2" dn="$4" B="$5" A="$6; n[k]++; s[k]+=$10; if(min[k]==""||$10<min[k])min[k]=$10; if($10>max[k])max[k]=$10} END{for(k in n) printf "%-32s runs=%d avg=%7.1fs min=%s max=%s\n", k, n[k], s[k]/n[k], min[k], max[k]}' /opt/nilm/experiments/load/cross-results.csv | sort

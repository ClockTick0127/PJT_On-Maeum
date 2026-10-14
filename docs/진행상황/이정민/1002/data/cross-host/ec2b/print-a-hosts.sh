#!/usr/bin/env bash
# EC2-B 전용. EC2-A 스크립트가 필요로 하는 값을 a.env 형식으로 출력한다.
#   sudo bash print-a-hosts.sh > ec2a/a.env   → EC2-A 의 cross-host/ec2a/a.env 로 복사
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
cd /opt/nilm
GOLD_IMAGE=$(docker compose --profile batch config --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["services"]["gold-profile"]["image"])')
cat <<ENV
# generated $(date -Is) on $(hostname)
B_IP=$B_IP
A_IP=$A_IP
GOLD_IMAGE=$GOLD_IMAGE
DATANODE_IMAGE=$DATANODE_IMAGE
ENV

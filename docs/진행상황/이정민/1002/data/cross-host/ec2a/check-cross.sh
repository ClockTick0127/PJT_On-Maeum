#!/usr/bin/env bash
# EC2-A 전용. EC2-B 로의 연결을 확인한다(읽기 전용). 보안그룹·포트 공개가 됐는지 본다.
#   bash check-cross.sh
# 7077/18080 은 cluster-cross.sh up 뒤에, 7078/7079 는 드라이버가 도는 동안에만 열린다.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
[ -f "$HERE/a.env" ] && source "$HERE/a.env"
probe() { timeout 3 bash -c "</dev/tcp/$1/$2" 2>/dev/null && echo "  $2 ($3): open" || echo "  $2 ($3): closed"; }
echo "EC2-A $(hostname -I | awk '{print $1}') -> EC2-B $B_IP"
probe "$B_IP" "$MASTER_PORT" "spark master; needs cluster-cross.sh up + SG"
probe "$B_IP" "$MASTER_UI_PORT" "master ui"
probe "$B_IP" "$NAMENODE_PORT" "namenode rpc; needs hdfs-ports.sh apply + SG"
probe "$B_IP" 9870 "namenode http (already published)"
probe "$B_IP" 9864 "datanode http; needs hdfs-ports.sh apply + SG"
probe "$B_IP" "$DATANODE_PORT" "datanode transfer; needs hdfs-ports.sh apply + SG"
probe "$B_IP" "$DRIVER_PORT" "driver rpc; only while a run is active"
probe "$B_IP" "$DRIVER_BM_PORT" "driver block manager; only while a run is active"
echo "a.env: GOLD_IMAGE=${GOLD_IMAGE:-<unset>}"
[ -n "${GOLD_IMAGE:-}" ] && { docker image inspect "$GOLD_IMAGE" >/dev/null 2>&1 && echo "gold image: present" || echo "gold image: not pulled yet"; }
docker image inspect "$DATANODE_IMAGE" >/dev/null 2>&1 && echo "datanode image: present" || echo "datanode image: not pulled yet"
echo "memory: $(free -m | awk '/Mem/{print $7}') MB available (worker ~2700 MB each, datanode ~1000 MB)"

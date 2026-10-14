#!/usr/bin/env bash
# EC2-A 전용. EC2-B namenode 에 붙는 두 번째 HDFS datanode 를 host 네트워크로 띄운다.
#
#   sudo bash datanode-cross.sh up
#   sudo bash datanode-cross.sh down
#   sudo bash datanode-cross.sh status
#
# 블록은 도커 볼륨 cross_datanode_data 에 남는다(down 해도 유지). 완전히 지우려면
# `docker volume rm cross_datanode_data` 를 직접 실행한다. 이 스크립트는 지우지 않는다.
# datanode 를 내리면 namenode 는 약 10.5 분 뒤에야 dead 로 본다. 그 사이 이 노드에만 있던
# 블록은 읽을 수 없으므로, datanode 2개 시나리오는 마지막에 몰아서 돌린다.
set -uo pipefail
ACTION=${1:-status}
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
[ -f "$HERE/a.env" ] && source "$HERE/a.env"

case "$ACTION" in
  status)
    docker ps --filter name=datanode-a --format '{{.Names}}\t{{.Status}}'
    curl -s --max-time 5 "http://$B_IP:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" \
      | python3 -c 'import json,sys; b=json.load(sys.stdin)["beans"][0]; print("namenode live datanodes:", b["NumLiveDataNodes"], "dead:", b["NumDeadDataNodes"])' 2>/dev/null || echo "namenode jmx unreachable"
    ;;
  down)
    docker rm -f datanode-a >/dev/null 2>&1; echo "EC2-A datanode removed (volume kept)"
    ;;
  up)
    docker image inspect "$DATANODE_IMAGE" >/dev/null 2>&1 || docker pull "$DATANODE_IMAGE"
    docker rm -f datanode-a >/dev/null 2>&1
    docker run -d --name datanode-a --network host --restart unless-stopped \
      --add-host "namenode:$B_IP" \
      -e CORE_CONF_fs_defaultFS="hdfs://namenode:$NAMENODE_PORT" \
      -e HDFS_CONF_dfs_replication=1 \
      -e HDFS_CONF_dfs_permissions_enabled=false \
      -e HDFS_CONF_dfs_datanode_hostname="$A_IP" \
      -e HDFS_CONF_dfs_datanode_use_datanode_hostname=true \
      -e HDFS_CONF_dfs_client_use_datanode_hostname=true \
      -e SERVICE_PRECONDITION="namenode:9870" \
      -v cross_datanode_data:/hadoop/dfs/data \
      "$DATANODE_IMAGE" >/dev/null
    for _ in $(seq 1 24); do
      sleep 5
      LIVE=$(curl -s --max-time 5 "http://$B_IP:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" | python3 -c 'import json,sys; print(json.load(sys.stdin)["beans"][0]["NumLiveDataNodes"])' 2>/dev/null || echo 0)
      [ "${LIVE:-0}" -ge 2 ] && break
    done
    "$0" status
    ;;
  *) echo "usage: $0 up|down|status" >&2; exit 2 ;;
esac

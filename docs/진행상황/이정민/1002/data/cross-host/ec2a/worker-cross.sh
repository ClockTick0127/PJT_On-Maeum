#!/usr/bin/env bash
# EC2-A 전용. EC2-B 의 Spark master 에 붙는 worker 를 host 네트워크로 띄운다.
#
#   sudo bash worker-cross.sh up 2      # worker 2개 (가용 메모리 10 GB 기준 최대치)
#   sudo bash worker-cross.sh down
#   sudo bash worker-cross.sh status
#
# 같은 폴더의 a.env 를 읽는다(EC2-B 에서 print-a-hosts.sh 로 생성). master 가 재기동돼도
# --restart 로 다시 붙는다.
set -uo pipefail
ACTION=${1:-status}; W=${2:-2}
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
[ -f "$HERE/a.env" ] && source "$HERE/a.env"
: "${GOLD_IMAGE:?GOLD_IMAGE is required (a.env)}"

case "$ACTION" in
  status)
    docker ps --filter name=spark-worker-a --format '{{.Names}}\t{{.Status}}'
    echo "alive workers on master: $(alive_workers)"
    ;;
  down)
    docker rm -f $(docker ps -aq --filter name=spark-worker-a) >/dev/null 2>&1; echo "EC2-A workers removed"
    ;;
  up)
    docker image inspect "$GOLD_IMAGE" >/dev/null 2>&1 || docker pull "$GOLD_IMAGE"
    docker rm -f $(docker ps -aq --filter name=spark-worker-a) >/dev/null 2>&1
    [ "$W" -gt 0 ] || { echo "no EC2-A workers requested"; exit 0; }
    AVAIL=$(free -m | awk '/Mem/{print $7}'); NEED=$(( W * 2700 ))
    [ "$AVAIL" -ge "$NEED" ] || { echo "available ${AVAIL}MB < needed ${NEED}MB for $W workers" >&2; exit 1; }
    for i in $(seq 1 "$W"); do
      docker run -d --name "spark-worker-a$i" --network host --restart unless-stopped \
        --add-host "namenode:$B_IP" --add-host "spark-master:$B_IP" \
        --memory "$WORKER_MEM_LIMIT" --memory-swap "$WORKER_MEM_LIMIT" --oom-score-adj 500 \
        -e SPARK_DAEMON_MEMORY="$DAEMON_MEM" -e SPARK_WORKER_CORES="$WORKER_CORES" -e SPARK_WORKER_MEMORY="$WORKER_MEM" \
        -e SPARK_LOCAL_HOSTNAME="$A_IP" \
        "$GOLD_IMAGE" $SPARK_CLASS org.apache.spark.deploy.worker.Worker \
        --host "$A_IP" --webui-port $((WORKER_UI_BASE + i)) "$(master_url)" >/dev/null
    done
    sleep 10
    "$0" status
    ;;
  *) echo "usage: $0 up|down|status [workers]" >&2; exit 2 ;;
esac

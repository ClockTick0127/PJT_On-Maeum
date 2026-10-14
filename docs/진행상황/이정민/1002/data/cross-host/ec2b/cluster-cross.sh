#!/usr/bin/env bash
# EC2-B 전용. 교차 호스트용 Spark master 와 EC2-B 쪽 worker 를 host 네트워크로 띄운다.
#
#   sudo bash cluster-cross.sh up 1     # master + EC2-B worker 1개
#   sudo bash cluster-cross.sh up 0     # master 만 (worker 는 전부 EC2-A)
#   sudo bash cluster-cross.sh down
#
# 컨테이너 이름은 spark-master, spark-worker-b<i> 라서 기존 cluster.sh down 도 같이 지운다.
set -uo pipefail
ACTION=${1:-up}; W=${2:-1}
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
cd /opt/nilm
GOLD_IMAGE=$(docker compose --profile batch config --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["services"]["gold-profile"]["image"])')

docker rm -f $(docker ps -aq --filter name=spark-) >/dev/null 2>&1
[ "$ACTION" = down ] && { echo "cross cluster down"; exit 0; }

ADD_HOSTS=(--add-host "namenode:$B_IP" --add-host "spark-master:$B_IP")

docker run -d --name spark-master --network host "${ADD_HOSTS[@]}" \
  --memory 768m --oom-score-adj 500 -e SPARK_DAEMON_MEMORY="$DAEMON_MEM" \
  -e SPARK_LOCAL_HOSTNAME="$B_IP" \
  "$GOLD_IMAGE" $SPARK_CLASS org.apache.spark.deploy.master.Master \
  --host "$B_IP" --port "$MASTER_PORT" --webui-port "$MASTER_UI_PORT" >/dev/null
sleep 5
for i in $(seq 1 "$W"); do
  docker run -d --name "spark-worker-b$i" --network host "${ADD_HOSTS[@]}" \
    --memory "$WORKER_MEM_LIMIT" --memory-swap "$WORKER_MEM_LIMIT" --oom-score-adj 500 \
    -e SPARK_DAEMON_MEMORY="$DAEMON_MEM" -e SPARK_WORKER_CORES="$WORKER_CORES" -e SPARK_WORKER_MEMORY="$WORKER_MEM" \
    -e SPARK_LOCAL_HOSTNAME="$B_IP" \
    "$GOLD_IMAGE" $SPARK_CLASS org.apache.spark.deploy.worker.Worker \
    --host "$B_IP" --webui-port $((WORKER_UI_BASE + i)) "$(master_url)" >/dev/null
done
sleep 10
echo "master: $(master_url)  ui: http://$B_IP:$MASTER_UI_PORT  alive workers: $(alive_workers)"

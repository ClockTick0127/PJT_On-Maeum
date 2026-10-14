#!/usr/bin/env bash
# EC2-A 를 EC2-B 클러스터에 붙이는 교차 호스트 실험의 공통 값.
# 두 서버에서 같은 파일을 source 한다. 값이 다르면 실행 전에 환경변수로 덮어쓴다.
#
# 설계
# - Spark master·worker 는 양쪽 모두 --network host 로 띄우고 사설 IP 로 광고한다. 기존
#   cluster.sh 는 한 서버의 도커 브리지 안에서 컨테이너 이름으로 서로를 찾는데, 다른
#   서버에서는 그 이름을 풀 수 없다.
# - 드라이버(load-driver-*)만 compose 브리지에 남고, 실행기가 접속할 두 포트를 호스트로 공개한다.
# - HDFS datanode 는 자기 hostname 을 사설 IP 로 광고한다(dfs.datanode.hostname). 그러면
#   어느 쪽 실행기든 IP 로 바로 접속하고, 데이터 로컬리티(블록 위치 = 실행기 IP)도 맞는다.

B_IP=${B_IP:-172.26.12.162}          # EC2-B 사설 IP (master, namenode, 드라이버)
A_IP=${A_IP:-172.26.8.92}            # EC2-A 사설 IP (원격 worker, 두 번째 datanode)

MASTER_PORT=${MASTER_PORT:-7077}
MASTER_UI_PORT=${MASTER_UI_PORT:-18080}
DRIVER_PORT=${DRIVER_PORT:-7078}
DRIVER_BM_PORT=${DRIVER_BM_PORT:-7079}
WORKER_UI_BASE=${WORKER_UI_BASE:-8081}   # worker i 의 web UI = 8081 + i

# 기존 cluster.sh 와 같은 worker 예산: 코어 1개, 실행기 1.2 GB, 컨테이너 상한 2.5 GB.
WORKER_CORES=${WORKER_CORES:-1}
WORKER_MEM=${WORKER_MEM:-1200m}
WORKER_MEM_LIMIT=${WORKER_MEM_LIMIT:-2560m}
DAEMON_MEM=${DAEMON_MEM:-256m}

SPARK_CLASS=/usr/local/lib/python3.11/site-packages/pyspark/bin/spark-class
CROSS_BASE=${CROSS_BASE:-/opt/nilm/experiments/cross}
A_HOME=${A_HOME:-/home/ubuntu/cross-host}         # EC2-A 에 복사한 스크립트 위치
DATANODE_IMAGE=${DATANODE_IMAGE:-bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8}
NAMENODE_PORT=9000
DATANODE_PORT=9866

# EC2-B 에서 EC2-A 로 명령을 보낼 때 쓰는 ssh. setup-ssh-to-a.sh 로 키를 만든다.
A_SSH=${A_SSH:-ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -i /home/ubuntu/.ssh/cross-a ubuntu@$A_IP}

master_url() { echo "spark://${B_IP}:${MASTER_PORT}"; }
remote_a() { $A_SSH "sudo -n bash $A_HOME/$1" "${@:2}"; }
alive_workers() {
  # master UI 의 JSON 은 /json/ (끝 슬래시 필수). 없으면 리다이렉트만 돌아온다.
  curl -s -L --max-time 5 "http://$B_IP:$MASTER_UI_PORT/json/" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers",0))' 2>/dev/null || echo 0
}

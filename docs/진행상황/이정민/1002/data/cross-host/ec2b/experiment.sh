#!/usr/bin/env bash
# EC2-B 전용. 시나리오 하나를 REPEATS 회 자동 반복한다. EC2-A 쪽은 ssh 로 조작한다.
#
#   sudo bash experiment.sh <이름> <datanode 수:1|2> <B worker 수> <A worker 수> [반복=3]
#
#   sudo bash experiment.sh baseline   1 1 0 3   # B 단독
#   sudo bash experiment.sh spark-dist 1 1 1 3   # Spark 만 두 서버
#   sudo bash experiment.sh hdfs-spark 2 1 1 3   # HDFS 도 두 서버
#
# 회차마다: 이전 컨테이너 정리 → (A datanode) → master·worker 기동 → 모두 ALIVE 확인 →
# 새 DB·출력 경로로 gold-profile daily 실행 → 시간·OOM 기록 → 정리.
# 결과: /opt/nilm/experiments/load/cross-results.csv
# 전제: hdfs-ports.sh apply, setup-ssh-to-a.sh, EC2-A 에 a.env, 보안그룹, 기존 실험 미실행.
set -uo pipefail
SCENARIO=$1; DN=$2; BW=$3; AW=$4; REPEATS=${5:-3}
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
B=/opt/nilm/experiments/load/bin; BASE=/opt/nilm/experiments/load
N=${N:-500}; export LOAD_FROM=${LOAD_FROM:-2026-09-23} LOAD_TO=${LOAD_TO:-2026-09-23}
SHUFFLE=${SHUFFLE:-200}
WINDOW_DAYS=${WINDOW_DAYS:-3}   # Gold 창 일수. 창 안의 날짜마다 Silver 를 다시 집계하므로 입력 크기를 키우는 손잡이다
[[ "$DN" =~ ^[12]$ ]] || { echo "datanodes must be 1 or 2" >&2; exit 2; }
TOTAL=$(( BW + AW )); [ "$TOTAL" -ge 1 ] || { echo "need at least one worker" >&2; exit 2; }
DIST="B"; { [ "$AW" -gt 0 ] || [ "$DN" -eq 2 ]; } && DIST="B+A"
RES=$BASE/cross-results.csv
[ -f "$RES" ] || echo "time,scenario,run,datanodes,b_workers,a_workers,node_distribution,exit,status,time_sec,alive_workers,live_datanodes,container_oom_b,host_oom_b,avail_before_mb,config" > "$RES"
LOG=$BASE/cross-experiment.log
log() { echo "$(date +%FT%T) [$SCENARIO] $*" | tee -a "$LOG" >&2; }
nn() { docker compose -f /opt/nilm/compose.yaml exec -T namenode "$@"; }
live_datanodes() {
  curl -s --max-time 5 "http://$B_IP:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["beans"][0]["NumLiveDataNodes"])' 2>/dev/null || echo "?"
}
wait_for() {  # $1=설명 $2=기대값 $3=최대 초 $4=함수
  local deadline=$(( $(date +%s) + $3 )) got
  while :; do
    got=$($4); [ "$got" = "$2" ] && { echo "$got"; return 0; }
    [ "$(date +%s)" -ge "$deadline" ] && { log "WARN: $1 expected $2, got $got after $3s"; echo "$got"; return 1; }
    sleep 5
  done
}

# 0. 입력 데이터와 ssh
$B/load-setup.sh $N && $B/load-write.sh $N || { log "WRITE FAILED"; exit 1; }
$A_SSH true || { log "cannot ssh to EC2-A; run setup-ssh-to-a.sh first"; exit 1; }
# 드라이버는 compose 브리지에서 host 네트워크의 master 로 붙는다. ufw 가 도커 브리지 대역을
# 막으면 실행기 없이 조용히 실패하므로 먼저 확인한다(ufw allow from 172.18.0.0/16).
"$HERE/cluster-cross.sh" up 0 >/dev/null
docker run --rm --network nilm-b_default alpine sh -c "nc -z -w 3 $B_IP $MASTER_PORT" >/dev/null 2>&1 \
  || { log "driver network (nilm-b_default) cannot reach master $B_IP:$MASTER_PORT; allow docker bridge subnets in ufw"; exit 1; }

# 1. datanode 수 맞추기. 2개면 입력 Bronze 를 두 노드에 복제해 로컬리티 비교가 가능하게 한다.
if [ "$DN" -eq 2 ]; then
  remote_a ec2a/datanode-cross.sh up
  wait_for "live datanodes" 2 180 live_datanodes >/dev/null
  # Spark 가 쓴 실험 출력은 클라이언트 기본 복제계수 3 으로 생성돼, datanode 가 2개가 되는 순간
  # 전부 복제 대기열에 들어가 입력 복제를 수십 분 막는다. 출력(w*)만 1 로 내린 뒤 입력을 복제한다.
  nn hdfs dfs -setrep 1 /nilm/experiments/load-*/w* >/dev/null 2>&1
  log "replicating input to both datanodes (setrep 2)"
  timeout 25m docker compose -f /opt/nilm/compose.yaml exec -T namenode \
    hdfs dfs -setrep -w 2 "/nilm/experiments/load-$N/bronze" "/nilm/experiments/load-$N/manifests" >/dev/null 2>&1 \
    || log "WARN: setrep 2 did not finish in time; continuing"
else
  # 순서가 중요하다. A 가 살아 있을 때 복제계수를 1 로 내리면 namenode 가 B 쪽 복제본을 지울 수
  # 있고, 그 뒤 A 를 내리면 "No active nodes contain this block" 으로 실패한다. A 를 먼저 내리고
  # dead 판정(최대 12 분)을 기다린 뒤 복제계수를 내린다. 그 사이 입력은 B 복제본으로 읽힌다.
  # A 가 아직 살아 있으면, 입력의 모든 블록이 B 에도 있도록 먼저 복제를 채운다. 복제본이 A 에만 있는
  # 블록이 남은 채 A 를 내리면 WebHDFS 가 죽은 A(9864)로 리다이렉트해 usage-daily 가 실패한다.
  if [ "$(live_datanodes)" = "2" ]; then
    nn hdfs dfs -setrep 1 /nilm/experiments/load-*/w* >/dev/null 2>&1
    log "making sure every input block also has a replica on B (setrep -w 2) before taking A down"
    timeout 15m docker compose -f /opt/nilm/compose.yaml exec -T namenode \
      hdfs dfs -setrep -w 2 "/nilm/experiments/load-$N/bronze" "/nilm/experiments/load-$N/manifests" >/dev/null 2>&1 \
      || log "WARN: pre-replication did not finish; some input blocks may be unreadable while A is down"
  fi
  remote_a ec2a/datanode-cross.sh down >/dev/null 2>&1
  wait_for "live datanodes" 1 720 live_datanodes >/dev/null
  nn hdfs dfs -setrep 1 "/nilm/experiments/load-$N/bronze" "/nilm/experiments/load-$N/manifests" >/dev/null 2>&1
fi

# 2. EC2-A worker 는 시나리오당 한 번 띄운다(--restart 로 master 재기동 뒤 다시 붙는다).
remote_a ec2a/worker-cross.sh down >/dev/null 2>&1
"$HERE/cluster-cross.sh" up 0 >/dev/null   # A worker 가 붙을 master 를 먼저 띄운다
[ "$AW" -gt 0 ] && remote_a ec2a/worker-cross.sh up "$AW"

for R in $(seq 1 "$REPEATS"); do
  CFG="w${TOTAL}x${SCENARIO//-/}d${DN}b${BW}a${AW}r${R}"; NEED=$(( 2900 + BW * 1700 ))
  "$HERE/cluster-cross.sh" down >/dev/null; docker rm -f "load-driver-$N-$CFG" >/dev/null 2>&1
  AVAIL=$(free -m | awk '/Mem/{print $7}')
  if [ "$AVAIL" -lt "$NEED" ]; then echo "$(date +%FT%T),$SCENARIO,$R,$DN,$BW,$AW,$DIST,SKIP,LOW_MEMORY,,,,,,$AVAIL,$CFG" >> "$RES"; log "skip run $R avail=$AVAIL"; sleep 60; continue; fi
  docker exec nilm-exp-pg psql -U exp -d analysis_exp -qc "drop database if exists load_${N}_${CFG}"
  nn hdfs dfs -rm -r -f -skipTrash "/nilm/experiments/load-$N/$CFG" >/dev/null 2>&1
  HOST_OOM_BEFORE=$(dmesg | grep -c "oom-kill")
  "$HERE/cluster-cross.sh" up "$BW" >/dev/null
  ALIVE=$(wait_for "alive workers" "$TOTAL" 180 alive_workers) || true
  LIVE=$(live_datanodes)
  log "run $R start cfg=$CFG workers=$ALIVE/$TOTAL datanodes=$LIVE window=${WINDOW_DAYS}d households=$N avail=${AVAIL}MB"
  SHUFFLE=$SHUFFLE "$HERE/load-config-cross.sh" $N "$CFG" "$TOTAL" >/dev/null \
    && sed -i "s/^PROFILE_WINDOW_DAYS=.*/PROFILE_WINDOW_DAYS=$WINDOW_DAYS/" "$BASE/batch-$N-$CFG.env" \
    && timeout "${RUN_TIMEOUT:-120m}" "$HERE/load-run-cross.sh" $N "$CFG" "$TOTAL" >> "$LOG" 2>&1
  RC=$?
  C_OOM=0; for i in $(seq 1 "$BW"); do id=$(docker inspect -f '{{.Id}}' spark-worker-b$i 2>/dev/null) && C_OOM=$(( C_OOM + $(awk '/^oom_kill /{print $2}' /sys/fs/cgroup/system.slice/docker-$id.scope/memory.events 2>/dev/null || echo 0) )); done
  H_OOM=$(( $(dmesg | grep -c "oom-kill") - HOST_OOM_BEFORE ))
  docker rm -f "load-driver-$N-$CFG" >/dev/null 2>&1; "$HERE/cluster-cross.sh" down >/dev/null
  f=$(ls -td /opt/nilm/experiments/load-$N-$CFG/*/ 2>/dev/null | head -1)
  # 날짜가 여럿이면(LOAD_FROM~LOAD_TO) time_sec 은 전체 날짜의 합, exit/status 는 실패가 하나라도 있으면 FAILED,
  # 아니면 마지막 날짜의 값. 날짜별 값은 run-dates.csv 에 그대로 남는다.
  LINE=""; [ -n "$f" ] && LINE=$(awk -F, 'NR>1 && $5!="" {n++; s+=$7; last_exit=$5; last_status=$6; if ($5!="0" && $5!="10" && $5!="12") bad=1}
    END{ if (n==0) exit; if (bad) printf "1,FAILED,%.3f", s; else printf "%s,%s,%.3f", last_exit, last_status, s }' "${f}run-dates.csv")
  echo "$(date +%FT%T),$SCENARIO,$R,$DN,$BW,$AW,$DIST,${LINE:-$RC,NO_RECORD,},$ALIVE,$LIVE,$C_OOM,$H_OOM,$AVAIL,$CFG" >> "$RES"
  log "run $R done rc=$RC ${LINE:-no record} oom_b=$C_OOM/$H_OOM"
  sleep 30
done

# 3. 정리. A datanode 는 남겨 둔다(다음 시나리오가 2개일 수 있고, 내리면 namenode 대기가 생긴다).
[ "$AW" -gt 0 ] && remote_a ec2a/worker-cross.sh down >/dev/null 2>&1
log "SCENARIO DONE ($REPEATS runs)"

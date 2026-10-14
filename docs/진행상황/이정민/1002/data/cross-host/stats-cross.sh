#!/usr/bin/env bash
# 두 서버 공통. 실험 컨테이너(spark-*, datanode*, load-driver-*)의 CPU·메모리를 15초마다 CSV 로 남긴다.
#   sudo bash stats-cross.sh start    # /opt/nilm/experiments/cross/stats-<hostname>.csv
#   sudo bash stats-cross.sh stop
# 형식은 기존 stats.csv 와 같다: time,name,cpu%,mem usage
set -u
ACTION=${1:-start}
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/common-cross.sh"
OUT="$CROSS_BASE/stats-$(hostname).csv"; PID="$CROSS_BASE/stats.pid"
mkdir -p "$CROSS_BASE"
case "$ACTION" in
  start)
    [ -f "$PID" ] && kill -0 "$(cat "$PID")" 2>/dev/null && { echo "already running pid $(cat "$PID")"; exit 0; }
    nohup bash -c "while true; do ts=\$(date +%FT%T); docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null | grep -E '^(spark-|datanode|load-driver-|nilm-b-datanode|nilm-b-namenode)' | sed \"s/^/\$ts,/\" >> '$OUT'; sleep 15; done" >/dev/null 2>&1 &
    echo $! > "$PID"; echo "started pid $(cat "$PID") -> $OUT"
    ;;
  stop)
    [ -f "$PID" ] && kill "$(cat "$PID")" 2>/dev/null && rm -f "$PID" && echo "stopped" || echo "not running"
    ;;
  *) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac

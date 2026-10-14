#!/usr/bin/env bash
# EC2-B 전용. HDFS 를 다른 서버에서 쓸 수 있게 한다.
#   - namenode RPC 9000 을 사설 IP 로 공개
#   - datanode 를 사설 IP(dfs.datanode.hostname)로 광고하고 9864(http), 9866(데이터 전송) 공개
#
#   sudo bash hdfs-ports.sh apply --yes    # compose.yaml 백업 후 수정, namenode/datanode 재생성
#   sudo bash hdfs-ports.sh revert --yes   # 마지막 백업으로 되돌리고 재생성
#   sudo bash hdfs-ports.sh status
#
# 재생성 동안 HDFS 가 수십 초 끊긴다. bronze-loader, session-lake-loader 는 재시도하므로 데이터는
# 잃지 않지만, Gold daily 나 다른 실험이 도는 중에는 하지 않는다. 다음 Jenkins 배포가
# /opt/nilm/compose.yaml 을 덮어쓰면 자동으로 원래대로 돌아간다.
set -euo pipefail
ACTION=${1:-status}; YES=${2:-}
COMPOSE=/opt/nilm/compose.yaml
cd /opt/nilm

status() {
  echo "== published ports"
  docker compose ps --format '{{.Name}}\t{{.Publishers}}' 2>/dev/null | grep -E "namenode|datanode" || true
  echo "== compose.yaml cross-host markers: $(grep -c 'cross-host' "$COMPOSE" || true)"
  echo "== datanodes seen by namenode"
  docker compose exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -E "^Live datanodes|^Name:|^Hostname:" | head -6 || true
}

confirm() {
  [ "$YES" = "--yes" ] || { echo "namenode/datanode 컨테이너를 다시 만듭니다. 계속하려면 두 번째 인자로 --yes 를 주세요." >&2; exit 2; }
}

case "$ACTION" in
  status) status ;;
  apply)
    confirm
    if grep -q 'cross-host' "$COMPOSE"; then echo "already applied"; status; exit 0; fi
    BACKUP="$COMPOSE.bak-cross-$(date +%Y%m%dT%H%M%S)"
    cp -p "$COMPOSE" "$BACKUP"; echo "backup: $BACKUP"
    python3 - "$COMPOSE" <<'PY'
import re, sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
ip = '${EC2_B_PRIVATE_IP:?EC2_B_PRIVATE_IP required}'
# namenode: 기존 ports 목록(9870) 뒤에 9000 추가
nn = re.search(r'(  namenode:\n.*?    ports:\n(?:      - [^\n]*\n)+)', text, re.S)
assert nn, "namenode ports block not found"
text = text.replace(nn.group(1), nn.group(1) + f'      - "{ip}:9000:9000"  # cross-host\n', 1)
# datanode: hostname 을 사설 IP 로 광고하고, ports 를 volumes 앞에 추가
dn = re.search(r'(  datanode:\n.*?      SERVICE_PRECONDITION: "namenode:9870"\n)(.*?)(    volumes:\n      - datanode_data:/hadoop/dfs/data\n)', text, re.S)
assert dn, "datanode block not found"
text = text.replace(
    dn.group(0),
    dn.group(1)
    + f'      HDFS_CONF_dfs_datanode_hostname: "{ip}"  # cross-host\n'
    + dn.group(2)
    + f'    ports:\n      - "{ip}:9864:9864"  # cross-host\n      - "{ip}:9866:9866"  # cross-host\n'
    + dn.group(3),
    1,
)
open(path, "w", encoding="utf-8", newline="\n").write(text)
print("compose.yaml patched")
PY
    docker compose config --quiet
    docker compose up -d namenode datanode
    sleep 20
    status
    ;;
  revert)
    confirm
    BACKUP=$(ls -t "$COMPOSE".bak-cross-* 2>/dev/null | head -1)
    [ -n "$BACKUP" ] || { echo "no backup found" >&2; exit 1; }
    cp -p "$BACKUP" "$COMPOSE"; echo "restored from $BACKUP"
    docker compose config --quiet
    docker compose up -d namenode datanode
    sleep 20
    status
    ;;
  *) echo "usage: $0 apply|revert|status [--yes]" >&2; exit 2 ;;
esac

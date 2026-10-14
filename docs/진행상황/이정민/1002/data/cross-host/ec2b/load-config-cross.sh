#!/usr/bin/env bash
# EC2-B 전용. 기존 load-config.sh 로 구성 env 를 만든 뒤, 드라이버가 두 서버의 실행기와
# 통신할 수 있도록 Spark 설정을 고쳐 쓴다.
#
#   SHUFFLE=200 sudo bash load-config-cross.sh 500 w3xb1a2r1 3
#     N=500 가구, CFG 이름, 총 코어(EC2-B worker + EC2-A worker)
#
# CFG 는 반드시 w<총코어>... 로 시작한다. load-config.sh 가 w 뒤 숫자로 spark.cores.max 를 정한다.
set -euo pipefail
N=$1; CFG=$2; TOTAL=${3:-}
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
B=/opt/nilm/experiments/load/bin; BASE=/opt/nilm/experiments/load
[[ "$CFG" == w* ]] || { echo "CFG must start with w<cores>" >&2; exit 2; }
"$B/load-config.sh" "$N" "$CFG"
ENVFILE="$BASE/batch-$N-$CFG.env"
CORES=$(echo "${CFG#w}" | sed 's/[^0-9].*//')
[ -z "$TOTAL" ] || [ "$TOTAL" = "$CORES" ] || { echo "CFG says $CORES cores but TOTAL=$TOTAL" >&2; exit 2; }

python3 - "$ENVFILE" "$B_IP" "$MASTER_PORT" "$DRIVER_PORT" "$DRIVER_BM_PORT" <<'PY'
import re, sys
path, b_ip, master_port, driver_port, bm_port = sys.argv[1:]
lines = open(path, encoding="utf-8").read().splitlines()
out = []
for line in lines:
    if line.startswith("SPARK_MASTER="):
        line = f"SPARK_MASTER=spark://{b_ip}:{master_port}"
    elif line.startswith("PYSPARK_SUBMIT_ARGS="):
        # 드라이버 위치를 컨테이너 이름 대신 EC2-B 사설 IP 와 고정 포트로 알린다.
        line = re.sub(r"--conf spark\.driver\.host=\S+", f"--conf spark.driver.host={b_ip}", line)
        extra = (
            f" --conf spark.driver.port={driver_port}"
            f" --conf spark.driver.blockManager.port={bm_port}"
            # 실행기가 datanode 를 브리지 IP(172.18.x) 가 아니라 hostname 으로 찾게 한다.
            " --conf spark.hadoop.dfs.client.use.datanode.hostname=true"
            # host 네트워크에서는 같은 서버의 worker 들이 같은 호스트 주소를 갖는다. Spark 는
            # 같은 호스트의 셔플 블록을 네트워크 대신 로컬 디스크에서 직접 읽으려 하는데,
            # worker 가 각자 컨테이너라 /tmp 가 달라 NoSuchFileException 으로 실패한다.
            " --conf spark.shuffle.readHostLocalDisk=false"
        )
        line = line.replace(" pyspark-shell", extra + " pyspark-shell")
    out.append(line)
open(path, "w", encoding="utf-8", newline="\n").write("\n".join(out) + "\n")
print("patched", path)
PY
grep -E "^SPARK_MASTER|^PYSPARK_SUBMIT_ARGS" "$ENVFILE" | fold -w 160

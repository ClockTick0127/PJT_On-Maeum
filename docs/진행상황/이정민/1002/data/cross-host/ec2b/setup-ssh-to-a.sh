#!/usr/bin/env bash
# EC2-B 전용. 실험 오케스트레이터(experiment.sh)가 EC2-A 에 명령을 보낼 ssh 키를 만든다.
#   bash setup-ssh-to-a.sh          # ubuntu 계정으로 실행. 키를 만들고 공개키를 출력한다
# 출력된 한 줄을 EC2-A 의 /home/ubuntu/.ssh/authorized_keys 에 추가한 뒤,
#   bash setup-ssh-to-a.sh test     # 로 연결을 확인한다.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); source "$HERE/../common-cross.sh"
KEY=/home/ubuntu/.ssh/cross-a
if [ "${1:-}" = test ]; then
  $A_SSH 'echo "ssh to EC2-A ok: $(hostname -I | cut -d" " -f1)"; sudo -n true && echo "sudo ok"'
  exit 0
fi
[ -f "$KEY" ] || ssh-keygen -t ed25519 -N "" -C "cross-host-experiment" -f "$KEY" >/dev/null
chown ubuntu:ubuntu "$KEY" "$KEY.pub" 2>/dev/null || true
echo "EC2-A 의 /home/ubuntu/.ssh/authorized_keys 에 아래 한 줄을 추가하세요:"
echo
cat "$KEY.pub"
echo
echo "추가 뒤 확인: bash $0 test"

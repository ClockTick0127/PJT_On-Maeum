#!/usr/bin/env bash
set -euo pipefail

calendar=${1:-"*-*-* 06:30:00 Asia/Seoul"}
source_dir=${NILM_SYSTEMD_SOURCE_DIR:-/opt/nilm/systemd}
unit_dir=${SYSTEMD_UNIT_DIR:-/etc/systemd/system}

systemd-analyze calendar "$calendar" >/dev/null
test -r "$source_dir/gold-profile-daily.service"
test -r "$source_dir/gold-profile-daily.timer.in"

install -m 644 "$source_dir/gold-profile-daily.service" \
  "$unit_dir/gold-profile-daily.service"
escaped=${calendar//&/\\&}
sed "s|@@ON_CALENDAR@@|$escaped|g" \
  "$source_dir/gold-profile-daily.timer.in" >"$unit_dir/gold-profile-daily.timer"
chmod 644 "$unit_dir/gold-profile-daily.timer"
systemctl daemon-reload

echo "Installed gold-profile-daily.service and timer for: $calendar"
echo "Review with: systemctl cat gold-profile-daily.timer"
echo "Enable with: systemctl enable --now gold-profile-daily.timer"

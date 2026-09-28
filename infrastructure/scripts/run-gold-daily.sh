#!/usr/bin/env bash
set -uo pipefail

root=${NILM_ROOT:-/opt/nilm}
compose_file=${GOLD_DAILY_COMPOSE_FILE:-$root/compose.yaml}
env_file=${GOLD_DAILY_ENV_FILE:-$root/.env}
lock_file=${GOLD_DAILY_LOCK_FILE:-/run/lock/nilm-gold-profile-daily.lock}
attempts=${GOLD_DAILY_ATTEMPTS:-3}
retry_seconds=${GOLD_DAILY_RETRY_SECONDS:-30}
catchup_days=${GOLD_DAILY_CATCHUP_DAYS:-3}
business_timezone=${GOLD_DAILY_TIMEZONE:-Asia/Seoul}
report_enabled=${HOUSEHOLD_REPORT_ENABLED:-false}
report_window=${HOUSEHOLD_REPORT_WINDOW_DAYS:-90}
case "$report_enabled" in true|false) ;; *) echo "HOUSEHOLD_REPORT_ENABLED must be true or false" >&2; exit 2;; esac
docker_bin=${GOLD_DAILY_DOCKER_BIN:-docker}
date_bin=${GOLD_DAILY_DATE_BIN:-date}
flock_bin=${GOLD_DAILY_FLOCK_BIN:-flock}

case "$attempts" in ''|*[!0-9]*) echo "GOLD_DAILY_ATTEMPTS must be a positive integer" >&2; exit 2;; esac
case "$catchup_days" in ''|*[!0-9]*) echo "GOLD_DAILY_CATCHUP_DAYS must be a positive integer" >&2; exit 2;; esac
(( attempts >= 1 )) || { echo "GOLD_DAILY_ATTEMPTS must be at least 1" >&2; exit 2; }
(( catchup_days >= 1 )) || { echo "GOLD_DAILY_CATCHUP_DAYS must be at least 1" >&2; exit 2; }
[[ "$retry_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]] || {
  echo "GOLD_DAILY_RETRY_SECONDS must be non-negative" >&2
  exit 2
}
test -r "$compose_file" || { echo "Missing compose file: $compose_file" >&2; exit 1; }
test -r "$env_file" || { echo "Missing environment file: $env_file" >&2; exit 1; }

mkdir -p "$(dirname "$lock_file")"
exec 9>"$lock_file"
if ! "$flock_bin" -n 9; then
  printf '{"status":"SKIPPED","reason":"daily pipeline already running","exit_code":11}\n'
  exit 11
fi

# Oldest first. Completed dates are cheap because every batch stage reuses its
# manifest; this also catches short outages and Persistent timer delays.
#
# Each date aligns its own window, so a date whose inputs are still incomplete
# (exit 10) must not starve the newer dates behind it: a missing Silver date
# weeks ago would otherwise block yesterday's profile on every timer run. The
# incomplete date is still reported through the final exit code.
overall=0
incomplete=0
for ((days_ago=catchup_days; days_ago>=1; days_ago--)); do
  as_of=$(TZ="$business_timezone" "$date_bin" -d "$days_ago days ago" +%F) || exit 1
  "$docker_bin" compose \
    --env-file "$env_file" \
    -f "$compose_file" \
    run --rm --no-deps gold-profile \
    gold-profile daily \
    --as-of "$as_of" \
    --attempts "$attempts" \
    --retry-seconds "$retry_seconds" \
    --no-publish
  status=$?
  case "$status" in
    0) ;;
    10) incomplete=1 ;; # inputs not ready for this date; keep going
    12) overall=12 ;;   # durable outbox is handed to gold-profile-publisher
    *) exit "$status" ;;
  esac
  if [[ "$report_enabled" == true && ( "$status" == 0 || "$status" == 12 ) ]]; then
    "$docker_bin" compose --env-file "$env_file" -f "$compose_file" \
      run --rm --no-deps gold-profile household-report daily \
      --as-of "$as_of" --window-days "$report_window"
    report_status=$?
    (( report_status == 0 )) || exit "$report_status"
  fi
done
if (( incomplete )); then
  exit 10
fi
exit "$overall"

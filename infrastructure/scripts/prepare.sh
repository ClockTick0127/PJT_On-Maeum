#!/usr/bin/env bash
set -euo pipefail
set +x
target=$1
: "${RUNTIME_ENV:?}" "${IMAGE_TAG:?}" "${IMAGE_REPOSITORY:?}" "${RELEASE_ID:?}"
[[ "$target" == a || "$target" == b ]] || exit 2
# Provision this directory once with ownership assigned to the deployment agent.
test -d /opt/nilm || { echo "Missing deployment directory: /opt/nilm" >&2; exit 1; }
test -w /opt/nilm || { echo "Deployment directory is not writable by $(id -un): /opt/nilm" >&2; exit 1; }
python3 infrastructure/scripts/release.py check --target "$target"
# Check required external files before replacing active configuration.
if [[ "$target" == a ]]; then
  for file in mqtt/passwd mqtt/certs/server.crt mqtt/certs/server.key certs/fullchain.pem certs/privkey.pem; do
    test -s "/opt/nilm/$file" || { echo "Missing server file: /opt/nilm/$file" >&2; exit 1; }
  done
else
  test -s /opt/nilm/mqtt/certs/ca.crt || { echo "Missing MQTT CA: /opt/nilm/mqtt/certs/ca.crt" >&2; exit 1; }
fi
test -s "$RUNTIME_ENV" || { echo "Runtime env credential file is missing or empty: $RUNTIME_ENV" >&2; exit 1; }
staged_env=$(mktemp)
trap 'rm -f -- "$staged_env"' EXIT
python3 infrastructure/scripts/release-env.py --input "$RUNTIME_ENV" --output "$staged_env"
docker compose --env-file "$staged_env" -f "infrastructure/ec2-$target/compose.yaml" config --quiet
python3 infrastructure/scripts/release.py snapshot
install -m 600 "$staged_env" /opt/nilm/.env
install -m 644 "infrastructure/ec2-$target/compose.yaml" /opt/nilm/compose.yaml
if [[ "$target" == "b" ]]; then
  install -m 644 infrastructure/ec2-b/hdfs-init.sh /opt/nilm/hdfs-init.sh
fi
install -m 644 release.json /opt/nilm/release.json
if [[ "$target" == a ]]; then
  install -d /opt/nilm/keycloak /opt/nilm/nginx /opt/nilm/nginx/auth /opt/nilm/mqtt /opt/nilm/observability
  install -m 644 infrastructure/keycloak/nilm-realm.json /opt/nilm/keycloak/nilm-realm.json
  install -m 644 infrastructure/nginx/default.conf.template /opt/nilm/nginx/default.conf.template
  install -m 644 infrastructure/mqtt/config/mosquitto.production.conf /opt/nilm/mqtt/mosquitto.conf
  cp -R infrastructure/observability/blackbox /opt/nilm/observability/
  cp -R infrastructure/observability/grafana /opt/nilm/observability/
  cp -R infrastructure/observability/prometheus /opt/nilm/observability/
else
  install -d /opt/nilm/postgres /opt/nilm/bin /opt/nilm/systemd
  install -m 644 infrastructure/postgres/01-create-databases.sql /opt/nilm/postgres/01-create-databases.sql
  install -m 755 infrastructure/scripts/run-gold-daily.sh /opt/nilm/bin/run-gold-daily.sh
  install -m 755 infrastructure/scripts/install-gold-daily-systemd.sh /opt/nilm/bin/install-gold-daily-systemd.sh
  install -m 644 infrastructure/ec2-b/gold-daily.env.example /opt/nilm/gold-daily.env.example
  install -m 644 infrastructure/systemd/gold-profile-daily.service /opt/nilm/systemd/gold-profile-daily.service
  install -m 644 infrastructure/systemd/gold-profile-daily.timer.in /opt/nilm/systemd/gold-profile-daily.timer.in
fi
cd /opt/nilm
docker compose config --quiet

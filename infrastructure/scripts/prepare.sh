#!/usr/bin/env bash
set -euo pipefail
set +x
target=$1
: "${RUNTIME_ENV:?}" "${IMAGE_TAG:?}" "${REGISTRY:?}"
[[ "$target" == a || "$target" == b ]] || exit 2
# Provision this directory once with ownership assigned to the deployment agent.
test -d /opt/nilm
test -w /opt/nilm
install -m 600 "$RUNTIME_ENV" /opt/nilm/.env
install -m 644 "infrastructure/ec2-$target/compose.yaml" /opt/nilm/compose.yaml
if [[ "$target" == a ]]; then
  install -d /opt/nilm/keycloak /opt/nilm/nginx /opt/nilm/mqtt
  install -m 644 infrastructure/keycloak/nilm-realm.json /opt/nilm/keycloak/nilm-realm.json
  install -m 644 infrastructure/nginx/default.conf.template /opt/nilm/nginx/default.conf.template
  install -m 644 infrastructure/mqtt/config/mosquitto.production.conf /opt/nilm/mqtt/mosquitto.conf
  for file in mqtt/passwd mqtt/certs/server.crt mqtt/certs/server.key certs/fullchain.pem certs/privkey.pem; do
    test -s "/opt/nilm/$file" || { echo "Missing server file: /opt/nilm/$file" >&2; exit 1; }
  done
else
  install -d /opt/nilm/postgres
  install -m 644 infrastructure/postgres/01-create-databases.sql /opt/nilm/postgres/01-create-databases.sql
  test -s /opt/nilm/mqtt/certs/ca.crt || { echo "Missing MQTT CA: /opt/nilm/mqtt/certs/ca.crt" >&2; exit 1; }
fi
python3 infrastructure/scripts/release-env.py
cd /opt/nilm
docker compose config --quiet

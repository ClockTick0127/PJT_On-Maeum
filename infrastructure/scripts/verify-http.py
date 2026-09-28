"""Run on EC2-A after deployment; never print credentials or tokens."""
import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request

config = json.loads(subprocess.check_output(
    ["docker", "compose", "--project-directory", "/opt/nilm", "config", "--format", "json"]))
kc = config["services"]["keycloak"]["environment"]
gateway = config["services"]["api-gateway"]["environment"]
device = config["services"]["iot-device-service"]["environment"]
origin = kc["FRONTEND_ORIGIN"].rstrip("/")
issuer = kc["KC_HOSTNAME"].rstrip("/") + "/realms/nilm"
# Same switch the Spring services read. "true" means every /api call needs a Keycloak JWT.
security_enabled = str(gateway.get("APP_SECURITY_ENABLED", "true")).strip().lower() == "true"
ROUTES = ("/api/devices/ping", "/api/monitoring/ping")


def request(url, data=None, headers=None):
    with urllib.request.urlopen(urllib.request.Request(
            url, data=data, headers=headers or {}), timeout=20) as response:
        return response.read()


request(origin + "/")

if security_enabled:
    try:
        request(origin + ROUTES[0])
    except urllib.error.HTTPError as error:
        if error.code != 401:
            raise SystemExit("Unauthenticated API request did not return 401")
    else:
        raise SystemExit("API unexpectedly allowed an unauthenticated request")
else:
    # Demo/test mode: the Gateway and services run with permitAll, so calls succeed without a token.
    for route in ROUTES:
        try:
            request(origin + route)
        except urllib.error.HTTPError as error:
            raise SystemExit(
                f"Security is disabled but {route} returned HTTP {error.code}")

# Keycloak must issue tokens in both modes, and authenticated calls must pass through the Gateway.
body = urllib.parse.urlencode({
    "grant_type": "client_credentials", "client_id": "nilm-smoke",
    "client_secret": kc["NILM_SMOKE_CLIENT_SECRET"]}).encode()
token = json.loads(request(issuer + "/protocol/openid-connect/token", data=body))["access_token"]
for route in ROUTES:
    request(origin + route, headers={"Authorization": "Bearer " + token})

# The backend client carries signup and login. A realm that never received the
# client, or a deploy that forgot its secret, leaves every check above green.
body = urllib.parse.urlencode({
    "grant_type": "client_credentials", "client_id": "nilm-backend",
    "client_secret": device["NILM_BACKEND_CLIENT_SECRET"]}).encode()
request(issuer + "/protocol/openid-connect/token", data=body)

# Rejected credentials must come back as 400 from the domain handler. A 5xx means
# device-service never reached Keycloak — it once pointed at its own localhost.
try:
    request(origin + "/api/auth/login",
            data=json.dumps({"email": "smoke-probe@nilm.invalid",
                             "password": "wrong-on-purpose"}).encode(),
            headers={"Content-Type": "application/json"})
except urllib.error.HTTPError as error:
    if error.code != 400:
        raise SystemExit(f"Login returned HTTP {error.code}, expected 400")
else:
    raise SystemExit("Login with a nonexistent account unexpectedly succeeded")

mode = "JWT required" if security_enabled else "security disabled (APP_SECURITY_ENABLED=false)"
print(f"PASS: frontend, authentication, Gateway -> device/monitoring, account APIs [{mode}]")

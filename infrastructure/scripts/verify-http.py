"""Run on EC2-A after deployment; never print credentials or tokens."""
import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request

config = json.loads(subprocess.check_output(
    ["docker", "compose", "--project-directory", "/opt/nilm", "config", "--format", "json"]))
kc = config["services"]["keycloak"]["environment"]
origin = kc["FRONTEND_ORIGIN"].rstrip("/")
issuer = kc["KC_HOSTNAME"].rstrip("/") + "/realms/nilm"


def request(url, data=None, headers=None):
    with urllib.request.urlopen(urllib.request.Request(
            url, data=data, headers=headers or {}), timeout=20) as response:
        return response.read()


request(origin + "/")
try:
    request(origin + "/api/devices/ping")
except urllib.error.HTTPError as error:
    if error.code != 401:
        raise SystemExit("Unauthenticated API request did not return 401")
else:
    raise SystemExit("API unexpectedly allowed an unauthenticated request")
body = urllib.parse.urlencode({
    "grant_type": "client_credentials", "client_id": "nilm-smoke",
    "client_secret": kc["NILM_SMOKE_CLIENT_SECRET"]}).encode()
token = json.loads(request(issuer + "/protocol/openid-connect/token", data=body))["access_token"]
for route in ("/api/devices/ping", "/api/monitoring/ping"):
    request(origin + route, headers={"Authorization": "Bearer " + token})
print("PASS: frontend, authentication, Gateway -> device/monitoring")

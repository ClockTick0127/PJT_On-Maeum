"""Publish a complete Docker Hub release and deploy its exact image digests."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


SERVICES = {
    "api-gateway": "API_GATEWAY_IMAGE",
    "iot-device-service": "IOT_DEVICE_IMAGE",
    "monitoring-service": "MONITORING_IMAGE",
    "realtime-analysis-service": "REALTIME_ANALYSIS_IMAGE",
    "aggregation-service": "AGGREGATION_IMAGE",
    "frontend": "FRONTEND_IMAGE",
    "mqtt-kafka-bridge": "MQTT_KAFKA_BRIDGE_IMAGE",
    "bronze-loader": "BRONZE_LOADER_IMAGE",
    "session-lake-loader": "SESSION_LAKE_LOADER_IMAGE",
    "power-silver": "POWER_SILVER_IMAGE",
    "gold-profile": "GOLD_PROFILE_IMAGE",
    "simulator": "SIMULATOR_IMAGE",
}
TARGETS = {
    "a": (
        "api-gateway",
        "iot-device-service",
        "monitoring-service",
        "frontend",
        "simulator",
    ),
    "b": (
        "realtime-analysis-service",
        "aggregation-service",
        "mqtt-kafka-bridge",
        "bronze-loader",
        "session-lake-loader",
        "power-silver",
        "gold-profile",
    ),
}
REPOSITORY = re.compile(r"docker\.io/[a-z0-9]+(?:[._-][a-z0-9]+)*/[a-z0-9]+(?:[._-][a-z0-9]+)*")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def identity(repository, sha, release_id):
    if not REPOSITORY.fullmatch(repository):
        raise ValueError("IMAGE_REPOSITORY must be docker.io/<account>/<repository> without a tag")
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("IMAGE_TAG must be the full Git SHA")
    if not re.fullmatch(re.escape(sha) + r"-b[1-9][0-9]*", release_id):
        raise ValueError("RELEASE_ID must be <full Git SHA>-b<build number>")


def validate(data):
    identity(data["repository"], data["git_sha"], data["release_id"])
    if data.get("schema") != 1 or set(data["images"]) != set(SERVICES):
        raise ValueError("Release must contain exactly the application images")
    for service, entry in data["images"].items():
        if not DIGEST.fullmatch(entry["digest"]):
            raise ValueError("Invalid image digest")
        if entry["tag"] != f'{data["repository"]}:{service}-{data["release_id"]}':
            raise ValueError("Image tag does not match release")
        if entry["reference"] != f'{data["repository"]}@{entry["digest"]}':
            raise ValueError("Image must reference the selected repository by digest")
    return data


def load(path):
    return validate(json.loads(Path(path).read_text(encoding="utf-8")))


def read_expected(path):
    data = load(path)
    for key, variable in (("repository", "IMAGE_REPOSITORY"), ("git_sha", "IMAGE_TAG"),
                          ("release_id", "RELEASE_ID")):
        if data[key] != os.environ[variable]:
            raise ValueError(f"Release manifest does not match {variable}")
    return data


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def publish(path):
    repository = os.environ["IMAGE_REPOSITORY"]
    sha, release_id = os.environ["IMAGE_TAG"], os.environ["RELEASE_ID"]
    identity(repository, sha, release_id)
    Path(path).unlink(missing_ok=True)
    data = dict(schema=1, repository=repository, git_sha=sha, release_id=release_id, images={})
    for service in SERVICES:
        tag = f"{repository}:{service}-{release_id}"
        digests = []
        with subprocess.Popen(["docker", "push", tag], stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True) as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                digests.extend(re.findall(r"\bdigest: (sha256:[0-9a-f]{64})\b", line))
            if process.wait() != 0:
                raise RuntimeError(f"Image push failed: {service}; deployment will not start")
        if len(set(digests)) != 1:
            raise ValueError(f"Could not identify the pushed digest for {service}")
        digest = digests[-1]
        data["images"][service] = dict(tag=tag, digest=digest, reference=f"{repository}@{digest}")
    write_json(path, validate(data))


def docker_json(*args):
    return json.loads(subprocess.check_output(["docker", *args], text=True))


def architecture(value):
    return {"x86_64": "amd64", "aarch64": "arm64"}.get(value, value)


def check_images(data, target, pull=False):
    host = docker_json("info", "--format", "{{json .}}")
    for service in TARGETS[target]:
        reference = data["images"][service]["reference"]
        if pull:
            subprocess.run(["docker", "pull", reference], check=True)
        image = docker_json("image", "inspect", reference)[0]
        if (image["Os"] != host["OSType"] or
                architecture(image["Architecture"]) != architecture(host["Architecture"])):
            raise ValueError(f"Image platform does not match Docker host: {service}")
        expected = reference.removeprefix("docker.io/")
        if expected not in [ref.removeprefix("docker.io/") for ref in image.get("RepoDigests", [])]:
            raise ValueError(f"Loaded image digest does not match release: {service}")


def update_env(path, data):
    validate(data)
    path = Path(path)
    controlled = {"REGISTRY", "IMAGE_PREFIX", "IMAGE_TAG", "IMAGE_REPOSITORY", "RELEASE_ID",
                  "CONFIG_ROOT", "OBSERVABILITY_ROOT", *SERVICES.values()}
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    lines = [line for line in lines if line.split("=", 1)[0].strip() not in controlled]
    lines.extend(["IMAGE_TAG=" + data["git_sha"], "RELEASE_ID=" + data["release_id"],
                  "IMAGE_REPOSITORY=" + data["repository"], "CONFIG_ROOT=/opt/nilm",
                  "OBSERVABILITY_ROOT=/opt/nilm/observability"])
    lines.extend(f'{variable}={data["images"][service]["reference"]}'
                 for service, variable in SERVICES.items())
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=path.name + "-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write("\n".join(lines) + "\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def snapshot(root, data):
    """Back up deployment-managed files, never database volumes or TLS keys."""
    destination = Path(root) / "releases" / data["release_id"] / "previous"
    if destination.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    paths = (".env", "compose.yaml", "release.json", "keycloak/nilm-realm.json",
             "nginx/default.conf.template", "mqtt/mosquitto.conf", "postgres/01-create-databases.sql")
    # A partial backup must not be mistaken for a complete snapshot on retry.
    with tempfile.TemporaryDirectory(dir=destination.parent, prefix="snapshot-") as temp:
        staging = Path(temp) / "previous"
        staging.mkdir(mode=0o700)
        for relative in paths:
            source = Path(root) / relative
            if source.is_file():
                output = staging / relative
                output.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, output)
                output.chmod(0o600)
        staging.rename(destination)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("publish", "validate", "pull", "check", "snapshot", "success"))
    parser.add_argument("--manifest", default="release.json")
    parser.add_argument("--target", choices=TARGETS)
    args = parser.parse_args()
    if args.action == "publish":
        publish(args.manifest)
        return
    data = read_expected(args.manifest)
    if args.action in ("pull", "check"):
        if not args.target:
            parser.error("--target is required for pull/check")
        check_images(data, args.target, pull=args.action == "pull")
    elif args.action == "snapshot":
        snapshot("/opt/nilm", data)
    elif args.action == "success":
        write_json(Path("/opt/nilm/releases") / data["release_id"] / "success.json", data)
        write_json("/opt/nilm/last-success.json", data)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(str(error))

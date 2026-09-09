"""Update deployment-controlled keys without evaluating an env file as shell code."""
import os
from pathlib import Path
import re

path = Path("/opt/nilm/.env")
tag = os.environ["IMAGE_TAG"]
registry = os.environ["REGISTRY"]
if not re.fullmatch(r"[0-9a-f]{40}", tag):
    raise SystemExit("IMAGE_TAG must be the full commit SHA")
if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._:/-]*", registry):
    raise SystemExit("Invalid registry namespace")
lines = path.read_text(encoding="utf-8-sig").splitlines()
controlled = {"IMAGE_TAG", "CONFIG_ROOT", "REGISTRY"}
lines = [line for line in lines if line.split("=", 1)[0].strip() not in controlled]
lines.extend(["IMAGE_TAG=" + tag, "REGISTRY=" + registry, "CONFIG_ROOT=/opt/nilm"])
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
path.chmod(0o600)

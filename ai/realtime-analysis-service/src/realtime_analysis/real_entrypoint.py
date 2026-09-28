"""Fail-closed entrypoint for the six-appliance realtime model image."""

import os
import subprocess
import sys

from realtime_analysis.asset_check import verify
from realtime_analysis.config import get_settings


def main() -> None:
    settings = get_settings()
    if settings.model_backend != "real":
        raise ValueError(
            "This image requires MODEL_BACKEND=real; fake fallback is disabled"
        )
    verify(settings.model_asset_root)
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
    )
    os.execv(sys.executable, [sys.executable, "-m", "realtime_analysis"])


if __name__ == "__main__":
    main()

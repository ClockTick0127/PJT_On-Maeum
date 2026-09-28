"""Configurable entrypoint for local Compose model backend switching."""

import os
import subprocess
import sys

from realtime_analysis.asset_check import verify
from realtime_analysis.config import get_settings


def main() -> None:
    settings = get_settings()

    if settings.model_backend == "selected_scene" and not settings.model_household_id:
        raise ValueError(
            "MODEL_HOUSEHOLD_ID is required when MODEL_BACKEND=selected_scene"
        )
    if settings.model_backend in {"real", "selected_scene"}:
        verify(settings.model_asset_root)

    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
    )
    os.execv(sys.executable, [sys.executable, "-m", "realtime_analysis"])


if __name__ == "__main__":
    main()

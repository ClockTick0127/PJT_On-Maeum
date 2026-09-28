"""Fail-closed image entrypoint: configuration, assets, migration, then worker."""
import os
import subprocess
import sys
from realtime_analysis.config import get_settings
from realtime_analysis.asset_check import verify


def main():
    settings=get_settings()
    if settings.model_backend != 'selected_scene' or not settings.model_household_id:
        raise ValueError('This image requires selected_scene and MODEL_HOUSEHOLD_ID; fake fallback is disabled')
    verify(settings.model_asset_root)
    subprocess.run([sys.executable,'-m','alembic','upgrade','head'],check=True)
    os.execv(sys.executable,[sys.executable,'-m','realtime_analysis'])


if __name__ == '__main__': main()

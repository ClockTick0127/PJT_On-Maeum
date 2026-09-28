"""Compatibility import for the standalone validator; runtime owns the contract."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'realtime-analysis-service/src'))
from realtime_analysis.scene_contracts import *  # noqa: F403

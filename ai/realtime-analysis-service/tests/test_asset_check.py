from pathlib import Path
import sys

import pytest
from realtime_analysis.asset_check import verify, verify_forward
from realtime_analysis.predictor import APPLIANCE_ORDER
from realtime_analysis.schemas import AppliancePrediction


def test_minimal_asset_bundles_match_lock():
    root = Path(__file__).resolve().parents[2] / 'assets/nilm_r3'
    if not root.exists(): pytest.skip('Assets are outside service-only build context')
    assert verify(root)['asset_files'] == 7
    assert verify(root, panels=True)['asset_files'] == 6


def test_missing_assets_never_pass(tmp_path):
    with pytest.raises(FileNotFoundError): verify(tmp_path)


def test_forward_check_requires_six_real_model_outputs(monkeypatch):
    import realtime_analysis.asset_check as module

    class StubPredictor:
        model_version = "test-model"

        def __init__(self, root, *, device, dtype):
            self.root = root

        def predict(self, window):
            assert len(window) == 255
            return [
                AppliancePrediction(appliance_type=name, probability=0.5)
                for name in APPLIANCE_ORDER
            ]

    class StubTorch:
        @staticmethod
        def set_num_threads(threads):
            assert threads == 1

    monkeypatch.setattr(module, "RealtimeModelPredictor", StubPredictor)
    monkeypatch.setitem(sys.modules, "torch", StubTorch())

    report = verify_forward("/assets")

    assert report == {
        "forward": "PASS",
        "outputs": 6,
        "model_version": "test-model",
        "device": "cpu",
        "dtype": "float32",
    }

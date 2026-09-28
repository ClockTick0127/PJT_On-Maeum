import hashlib
import os
from pathlib import Path

import pytest

from realtime_analysis.real_predictor import (
    RealtimeModelPredictor, SelectedScenePredictor, load_profile, verified_asset,
)
from realtime_analysis.predictor import APPLIANCE_ORDER
from realtime_analysis.schemas import AppliancePrediction
from realtime_analysis.selected_scene import SelectedSceneDecoder, float32


def test_equal_threshold_keeps_on_at_float32_boundary():
    decoder = SelectedSceneDecoder(.99, .99)
    boundary = float32(.99)
    assert decoder.step(boundary).transition == "SYNC"
    assert decoder.step(boundary).state == "ON"
    assert decoder.step(boundary - 1e-7).transition == "TURNED_OFF"


def test_mid_holds_and_null_recovery_is_sync():
    decoder = SelectedSceneDecoder(.9, .7)
    assert decoder.step(.8).state == "UNKNOWN"
    assert decoder.step(.95).transition == "SYNC"
    assert decoder.step(.8).state == "ON"
    assert decoder.step(float32(.7)).transition == "TURNED_OFF"
    assert decoder.step(None).state == "UNKNOWN"
    assert decoder.step(.95).transition == "SYNC"
    assert decoder.step(float("nan")).state == "UNKNOWN"


def test_asset_hash_and_path_validation(tmp_path):
    asset = tmp_path / "weight.pt"
    asset.write_bytes(b"original")
    sha = hashlib.sha256(b"original").hexdigest()
    assert verified_asset(tmp_path, "weight.pt", sha) == asset
    asset.write_bytes(b"modified")
    with pytest.raises(ValueError, match="SHA-256"):
        verified_asset(tmp_path, "weight.pt", sha)
    with pytest.raises(ValueError, match="escapes"):
        verified_asset(tmp_path, "../outside.pt", sha)


def test_missing_weights_fail_without_fake_fallback(tmp_path):
    with pytest.raises(FileNotFoundError):
        SelectedScenePredictor(tmp_path, "kettle")


def test_frozen_selection():
    assert load_profile("KETTLE")["candidate_id"] == "008b808cf17505534886"
    assert load_profile("microwave")["source_house"] == "H063"
    with pytest.raises(ValueError):
        load_profile("air_fryer")


def test_realtime_predictor_combines_six_models_in_contract_order(monkeypatch):
    import realtime_analysis.real_predictor as module

    class StubPredictor:
        def __init__(self, asset_root, appliance, **kwargs):
            index = APPLIANCE_ORDER.index(appliance)
            self.profile = {
                "appliance_type": appliance.lower(),
                "fixed_window_steps": 255,
                "threshold": {"on": 0.9, "off": 0.7, "confirm": 1},
                "checkpoint_sha256": f"{index:064x}",
            }
            self.probability = index / 10

        def predict(self, window):
            return [
                AppliancePrediction(
                    appliance_type=self.profile["appliance_type"].upper(),
                    probability=self.probability,
                )
            ]

    monkeypatch.setattr(module, "SelectedScenePredictor", StubPredictor)
    predictor = RealtimeModelPredictor("/unused")

    predictions = predictor.predict([(0.0, 0.0, 0.0, 0.0)] * 255)

    assert tuple(item.appliance_type for item in predictions) == APPLIANCE_ORDER
    assert [item.probability for item in predictions] == [
        0.0,
        0.1,
        0.2,
        0.3,
        0.4,
        0.5,
    ]
    assert predictor.confirmation_samples == 1
    assert set(predictor.on_thresholds) == set(APPLIANCE_ORDER)
    assert set(predictor.off_thresholds) == set(APPLIANCE_ORDER)

    with pytest.raises(ValueError, match="255 realtime model input rows"):
        predictor.predict([(0.0, 0.0, 0.0, 0.0)] * 254)


@pytest.fixture
def real_assets():
    root = os.environ.get("R3_TEST_ASSET_ROOT", str(Path(__file__).resolve().parents[2] / 'assets/nilm_r3'))
    pytest.importorskip("torch")
    return Path(root)


@pytest.mark.parametrize("appliance", ["kettle", "induction", "vacuum_cleaner"])
def test_actual_family_forward_is_stateless_and_single_target(real_assets, appliance):
    import torch
    torch.set_num_threads(1)
    predictor = SelectedScenePredictor(real_assets, appliance)
    window = [(400., 100., .8, 2.)] * 255
    first = predictor.predict(window)
    predictor.predict([(900., 30., .9, 4.)] * 255)
    assert predictor.predict(window) == first
    assert len(first) == 1 and first[0].appliance_type == appliance.upper()
    assert 0 <= first[0].probability <= 1
    with pytest.raises(ValueError, match="255 finite"):
        predictor.predict(window[:-1])
    with pytest.raises(ValueError, match="255 finite"):
        predictor.predict([(float("nan"), 0., 0., 0.)] * 255)


def test_kettle_replay_uses_real_forward_and_leaves_others_unknown(real_assets):
    from realtime_analysis.model_replay import replay
    predictor = SelectedScenePredictor(real_assets, "kettle")
    rows = list(replay(predictor, real_assets / predictor.profile["panel_relative"]))
    assert len(rows) == 592
    assert sum(row["ready"] for row in rows) == 338
    assert all(row["on_score"] is None for row in rows[:254])
    assert any(row["state"] == "ON" for row in rows[254:])
    for row in rows:
        for item in row["appliances"][1:]:
            assert item == {"appliance_type": item["appliance_type"],
                            "inferred": False, "probability": None, "state": "UNKNOWN"}


def test_context_mask_and_missing_mean_fill(real_assets, tmp_path, monkeypatch):
    import csv
    from realtime_analysis.model_replay import FEATURES, replay
    predictor = SelectedScenePredictor(real_assets, "kettle")
    panel = tmp_path / "masked.csv"
    with panel.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("source_index", *FEATURES, "valid", "context"))
        for index in range(256):
            writer.writerow((index, 400, 100, .8, 2, int(index != 100), int(index != 254)))
    calls = []
    actual_predict = predictor.predict

    def capture(window):
        calls.append(window)
        return actual_predict(window)

    monkeypatch.setattr(predictor, "predict", capture)
    result = list(replay(predictor, panel))
    assert result[254]["ready"] is False
    assert result[254]["state"] == "UNKNOWN"
    assert result[255]["ready"] is True
    assert result[255]["transition"] == "SYNC"
    assert len(calls) == 1
    assert calls[0][99] == predictor.missing_feature_row

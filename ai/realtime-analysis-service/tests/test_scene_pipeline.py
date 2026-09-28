from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from realtime_analysis.config import Settings
from realtime_analysis.real_predictor import load_profile
from realtime_analysis.scene_pipeline import SceneEvidence, SceneHandler, SceneMeasurement, SceneRepository


class Predictor:
    profile = load_profile("kettle")
    runtime = {"profile_id": profile['profile_id'], "device": "cpu", "dtype": "float32",
        "checkpoint_sha256": profile['checkpoint_sha256'], "model_code_sha256": profile['model_code_sha256'],
        "normalization_sha256": profile['normalization_sha256'], "torch_version": "test",
        "batch_size": 1, "input_shape": [1, 255, 4], "reference_device": "H200",
        "reference_dtype": "BF16", "reference_parity_verified": False}
    missing_feature_row = (10., 20., .8, 1.)
    calls = 0

    def predict(self, window):
        self.calls += 1
        return [SimpleNamespace(probability=1.)]


class Publisher:
    def __init__(self):
        self.rows = []
        self.fail = False

    def publish(self, payload):
        if self.fail:
            raise RuntimeError("broker unavailable")
        self.rows.append(payload)


def measurement(index, **changes):
    data = dict(message_id=uuid4(), household_id="r3-test", device_id="main",
        measured_at=datetime(2026, 9, 21, tzinfo=timezone.utc) + timedelta(seconds=index),
        run_id="run-1", profile_id=Predictor.profile["profile_id"],
        source_index=Predictor.profile["source_prefix_start_index"] + index,
        valid=True, context=True, active_power=400, reactive_power=30,
        power_factor=.9, current=2)
    data.update(changes)
    return SceneMeasurement(**data)


@pytest.fixture
def pipeline():
    engine = create_engine("sqlite://")
    SceneEvidence.__table__.create(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    predictor, publisher = Predictor(), Publisher()
    repo = SceneRepository(sessions)
    return SceneHandler(predictor, repo, publisher, "run-1"), sessions


def test_restart_resume_and_unknown(pipeline):
    handler, sessions = pipeline
    for i in range(254):
        handler(measurement(i))
    assert handler.predictor.calls == 0
    assert all(a["state"] == "UNKNOWN" for a in handler.publisher.rows[-1]["appliances"])
    handler.reset_household("r3-test")
    resumed = SceneHandler(handler.predictor, SceneRepository(sessions), handler.publisher, "run-1")
    resumed(measurement(254))
    result = handler.publisher.rows[-1]
    assert result["ready"] and result["transition"] == "SYNC"
    assert result["appliances"][0]["is_on"] is True
    assert all(a["is_on"] is None for a in result["appliances"][1:])
    assert handler.predictor.calls == 1


def test_publish_failure_retries_committed_payload(pipeline):
    handler, sessions = pipeline
    item = measurement(0)
    handler.publisher.fail = True
    with pytest.raises(RuntimeError):
        handler(item)
    handler.publisher.fail = False
    handler(item)
    handler(item.model_copy(update={"message_id": uuid4()}))
    assert handler.publisher.rows[0] == handler.publisher.rows[1]
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(SceneEvidence)) == 1
    handler(measurement(1))


def test_db_failure_does_not_advance_window(pipeline, monkeypatch):
    handler, _ = pipeline
    save = handler.repository.save
    def fail(*args):
        raise RuntimeError("db unavailable")
    monkeypatch.setattr(handler.repository, "save", fail)
    with pytest.raises(RuntimeError):
        handler(measurement(0))
    assert not handler.publisher.rows
    monkeypatch.setattr(handler.repository, "save", save)
    handler(measurement(0))
    assert len(handler.repository.latest(measurement(0))["window"]) == 1


def test_conflicting_duplicate_order_run_and_profile_rejected(pipeline):
    handler, _ = pipeline
    handler(measurement(0))
    for item in (measurement(0, active_power=999), measurement(2),
                 measurement(1, run_id="wrong"), measurement(1, profile_id="wrong")):
        with pytest.raises(ValueError):
            handler(item)
    handler(measurement(1))
    assert len(handler.publisher.rows) == 2


def test_context_resets_unknown_and_runtime_change_requires_new_run(pipeline):
    handler, _ = pipeline
    for i in range(255):
        handler(measurement(i))
    handler(measurement(255, valid=False, context=False,
                        active_power=None, reactive_power=None, power_factor=None, current=None))
    assert handler.publisher.rows[-1]["measurement"] is None
    assert handler.publisher.rows[-1]["appliances"][0]["state"] == "UNKNOWN"
    handler.predictor.runtime = {"device": "different"}
    with pytest.raises(ValueError, match="Runtime changed"):
        handler(measurement(256))


def test_selected_configuration_requires_dedicated_run_and_topics():
    with pytest.raises(ValueError):
        Settings(_env_file=None, model_backend="selected_scene")
    settings = Settings(_env_file=None, model_backend="selected_scene", model_asset_root="assets",
        analysis_run_id="r3-kettle", kafka_input_topic="power.scene.v2", kafka_group_id="r3-kettle")
    assert settings.kafka_scene_snapshot_topic == "analysis.scene.v2"


def test_main_dispatches_selected_backend_before_legacy_bootstrap(monkeypatch):
    from unittest.mock import Mock
    from realtime_analysis import __main__, scene_pipeline
    settings = Settings(_env_file=None, model_backend="selected_scene", model_asset_root="assets",
        analysis_run_id="r3-kettle", kafka_input_topic="power.scene.v2", kafka_group_id="r3-kettle")
    selected = Mock()
    legacy = Mock(side_effect=AssertionError("Legacy fake bootstrap must not run"))
    monkeypatch.setattr(__main__, "get_settings", lambda: settings)
    monkeypatch.setattr(__main__.signal, "signal", lambda *_: None)
    monkeypatch.setattr(__main__, "DailyActivityTracker", legacy)
    monkeypatch.setattr(scene_pipeline, "run_selected_scene", selected)
    __main__.main()
    selected.assert_called_once()
    assert selected.call_args.args[0] is settings
    legacy.assert_not_called()

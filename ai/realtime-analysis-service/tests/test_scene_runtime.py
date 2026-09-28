import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from realtime_analysis.scene_pipeline import SceneMeasurement, SceneHandler
from realtime_analysis.scene_lease import household_lease
from tests.test_scene_pipeline import Predictor, measurement


def test_strict_wire_contract_rejects_string_boolean():
    row = measurement(0).model_dump(mode='json')
    row['valid'] = 'false'
    with pytest.raises(ValueError):
        SceneMeasurement.model_validate_json(json.dumps(row))


def test_unconfigured_household_never_touches_storage():
    repo = MagicMock()
    handler = SceneHandler(Predictor(), repo, MagicMock(), 'run-1', 'other-house')
    with pytest.raises(ValueError, match='household'):
        handler(measurement(0))
    repo.get.assert_not_called()


def test_lost_lease_stops_before_storage():
    repo = MagicMock()
    handler = SceneHandler(Predictor(), repo, MagicMock(), 'run-1')
    handler.lease_check = MagicMock(side_effect=RuntimeError('lease lost'))
    with pytest.raises(RuntimeError, match='lease lost'):
        handler(measurement(0))
    repo.get.assert_not_called()


def test_lease_denial_and_exception_release():
    factory = MagicMock()
    connection = factory.return_value.__enter__.return_value.connection.return_value
    connection.scalar.return_value = False
    with pytest.raises(RuntimeError, match='already owns'):
        with household_lease(factory, 'test'): pass
    connection.scalar.return_value = True
    with pytest.raises(ValueError):
        with household_lease(factory, 'test') as check:
            check()
            raise ValueError('worker failed')
    assert 'pg_advisory_unlock' in str(connection.execute.call_args.args[0])

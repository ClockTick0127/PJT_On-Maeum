from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import pytest

from realtime_analysis.scene_acceptance import local_run, read_jsonl, verify

ASSETS = Path(__file__).resolve().parents[2] / 'assets/nilm_r3'
START = datetime(2026, 9, 21, tzinfo=timezone.utc)


@pytest.fixture(scope='module')
def complete(tmp_path_factory):
    pytest.importorskip('torch')
    if not ASSETS.exists():
        pytest.skip('Full AI asset context required')
    path = tmp_path_factory.mktemp('acceptance') / 'kettle'
    report = local_run(ASSETS, 'kettle', path, 'acceptance-test', 'test-run', START)
    return report, [read_jsonl(path / (name + '.jsonl')) for name in ('snapshots', 'sessions', 'risks')]


def check(rows, **kwargs):
    return verify(ASSETS, 'kettle', 'acceptance-test', 'test-run', START, *rows, **kwargs)


def test_real_model_restart_and_full_duplicate_replay(complete):
    report, rows = complete
    assert report['snapshot_count'] == 592 and report['ready_count'] == 338
    assert report['session_count'] == report['risk_count'] == 1
    assert report['restart_and_full_replay'] == 'PASS'
    assert report['deployed_backend'] == report['notification_creation'] == 'NOT_RUN'


@pytest.mark.parametrize('corruption', ['missing_snapshot', 'raw_drift', 'wrong_run', 'missing_risk', 'missing_session'])
def test_incomplete_or_corrupted_readback_never_passes(complete, corruption):
    rows = deepcopy(complete[1])
    if corruption == 'missing_snapshot':
        index = rows[0][0]['source_index']
        rows[0] = [r for r in rows[0] if r['source_index'] != index]
    elif corruption == 'raw_drift':
        next(r for r in rows[0] if r['measurement'])['measurement']['active_power'] += 1
    elif corruption == 'wrong_run':
        rows[0][0]['run_id'] = 'wrong'
    elif corruption == 'missing_risk':
        rows[2] = []
    else:
        rows[1] = []
    with pytest.raises(ValueError):
        check(rows)


def test_local_results_cannot_pass_broker_readback(complete):
    with pytest.raises(ValueError, match='provenance'):
        check(complete[1], require_source=True)

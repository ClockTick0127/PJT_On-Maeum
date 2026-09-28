from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from realtime_analysis.scene_pipeline import SceneEvidence, SceneRepository, SceneHandler
from realtime_analysis.scene_events import SceneProjection, SceneUsage, SceneOutbox, SceneProjector, flush_outbox, RISK_TOPIC
from tests.test_scene_pipeline import Predictor, Publisher, measurement


@pytest.fixture
def db():
    engine = create_engine('sqlite://')
    for cls in (SceneEvidence, SceneProjection, SceneUsage, SceneOutbox):
        cls.__table__.create(engine)
    yield sessionmaker(engine)
    engine.dispose()


def snapshot(index, state, transition=None):
    return {'household_id':'r3-test','run_id':'run-1','profile_id':'profile',
            'source_index':index,'observed_at':(datetime(2026,9,21,tzinfo=timezone.utc)+timedelta(seconds=index)).isoformat(),
            'target_appliance':'KETTLE','transition':transition,
            'appliances':[{'appliance_type':'KETTLE','state':state,'inferred':state!='UNKNOWN'}]}


def apply(db, projector, row, last=9999):
    with db.begin() as session: projector.apply(session,row,last)


def records(db, cls):
    with db() as session: return [r.payload for r in session.scalars(select(cls)).all()]


def test_known_on_one_risk_and_final_off(db):
    projector=SceneProjector(10,'r3-test')
    apply(db,projector,snapshot(0,'ON','TURNED_ON'))
    for i in range(1,13): apply(db,projector,snapshot(i,'ON'))
    apply(db,projector,snapshot(13,'OFF','TURNED_OFF'))
    usage=records(db,SceneUsage)[0]
    assert usage['status']=='CLOSED' and usage['observed_on_seconds']==13
    risks=[e for e in records(db,SceneOutbox) if 'reason' in e]
    assert len(risks)==1 and risks[0]['reason']['continuous_on_seconds']==10


def test_sync_gap_recovery_and_eof_never_fabricate_events(db):
    projector=SceneProjector(1,'r3-test')
    apply(db,projector,snapshot(0,'ON','SYNC'))
    apply(db,projector,snapshot(1,'ON'))
    apply(db,projector,snapshot(2,'UNKNOWN'))
    apply(db,projector,snapshot(3,'ON','SYNC'),last=3)
    usage=records(db,SceneUsage)
    assert len(usage)==2
    assert {r['end_reason'] for r in usage}=={'GAP','EOF'}
    assert all(r['ended_at'] is None and r['started_at'] is None for r in usage)
    assert not [r for r in records(db,SceneOutbox) if 'reason' in r]


def test_other_house_cannot_generate_test_policy_risk(db):
    projector=SceneProjector(1,'different-test-house')
    apply(db,projector,snapshot(0,'ON','TURNED_ON'))
    apply(db,projector,snapshot(1,'ON'))
    assert not [r for r in records(db,SceneOutbox) if 'reason' in r]


def test_cancel_is_idempotent_and_terminal(db):
    projector=SceneProjector()
    apply(db,projector,snapshot(0,'ON','TURNED_ON'))
    scope=dict(household_id='r3-test',run_id='run-1',profile_id='profile')
    for _ in range(2):
        with db.begin() as session: projector.cancel(session,scope)
    assert records(db,SceneUsage)[0]['end_reason']=='CANCELLED'
    assert len(records(db,SceneOutbox))==2
    with pytest.raises(ValueError,match='finalized'): apply(db,projector,snapshot(1,'ON'))


def test_outbox_failure_keeps_pending_and_retries_same_id(db):
    projector=SceneProjector()
    apply(db,projector,snapshot(0,'ON','TURNED_ON'))
    publisher=MagicMock()
    publisher.publish.side_effect=RuntimeError('lost ACK')
    with pytest.raises(RuntimeError): flush_outbox(db,publisher,'r3-test','run-1','profile')
    first=publisher.publish.call_args.args
    publisher.publish.side_effect=None
    flush_outbox(db,publisher,'r3-test','run-1','profile')
    assert publisher.publish.call_args.args==first
    flush_outbox(db,publisher,'r3-test','run-1','profile')
    assert publisher.publish.call_count==2


def test_snapshot_projection_and_outbox_rollback_together(db,monkeypatch):
    projector=SceneProjector()
    repo=SceneRepository(db,projector,MagicMock(),Predictor.profile['source_clip_end_exclusive']-1)
    handler=SceneHandler(Predictor(),repo,Publisher(),'run-1')
    for i in range(254): handler(measurement(i))
    original=projector.queue
    monkeypatch.setattr(projector,'queue',MagicMock(side_effect=RuntimeError('outbox failed')))
    with pytest.raises(RuntimeError): handler(measurement(254))
    with db() as session:
        assert session.scalar(select(func.count()).select_from(SceneEvidence))==254
        assert session.scalar(select(func.count()).select_from(SceneUsage))==0
    monkeypatch.setattr(projector,'queue',original)
    handler(measurement(254))
    handler(measurement(254))
    assert len(records(db,SceneOutbox))==1


def test_policy_change_within_run_rejected(db):
    predictor=Predictor()
    handler=SceneHandler(predictor,SceneRepository(db,SceneProjector(),MagicMock(),99999),Publisher(),'run-1')
    handler(measurement(0))
    changed=SceneHandler(predictor,SceneRepository(db,SceneProjector(10,'r3-test'),MagicMock(),99999),Publisher(),'run-1')
    with pytest.raises(ValueError,match='policy changed'): changed(measurement(1))
    with pytest.raises(ValueError,match='policy changed'): changed(measurement(0))


@pytest.mark.parametrize('appliance,sessions_expected,risks_expected', [
    ('kettle',1,1),('induction',3,2),('iron',1,1),('microwave',1,1),('hair_dryer',1,1),('vacuum_cleaner',1,1)])
def test_actual_model_fixture_generates_only_target_sessions(db,appliance,sessions_expected,risks_expected):
    torch = pytest.importorskip('torch')
    from realtime_analysis.real_predictor import SelectedScenePredictor
    from realtime_analysis.scene_replay import measurements
    torch.set_num_threads(1)
    assets=Path(__file__).resolve().parents[2]/'assets/nilm_r3'
    predictor=SelectedScenePredictor(assets,appliance)
    event_publisher=MagicMock()
    repo=SceneRepository(db,SceneProjector(10,'r3-test'),event_publisher,
                         predictor.profile['source_clip_end_exclusive']-1)
    handler=SceneHandler(predictor,repo,Publisher(),'actual-test','r3-test')
    rows=list(measurements(assets,appliance,'r3-test','actual-test',datetime(2026,9,21,tzinfo=timezone.utc)))
    for row in rows: handler(row)
    usage=records(db,SceneUsage)
    risks=[r for r in records(db,SceneOutbox) if 'reason' in r]
    assert len(usage)==sessions_expected and len(risks)==risks_expected
    assert all(r['status']=='CLOSED' and r['appliance_type']==appliance.upper() for r in usage)
    delivered=event_publisher.publish.call_count
    handler(rows[-1])
    assert event_publisher.publish.call_count==delivered

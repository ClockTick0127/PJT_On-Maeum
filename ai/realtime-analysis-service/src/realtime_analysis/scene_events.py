"""Transactional partial-observation sessions and test-household risk outbox."""
from copy import deepcopy
from datetime import datetime
import json
from uuid import NAMESPACE_URL, uuid5
from sqlalchemy import JSON, String, Integer, Boolean, select
from sqlalchemy.orm import Mapped, mapped_column
from realtime_analysis.database import Base
from realtime_analysis.scene_contracts import Session, RiskEvent

SESSION_TOPIC = 'analysis.scene-session.v1'
RISK_TOPIC = 'analysis.scene-event.v1'


def identity(*parts):
    return str(uuid5(NAMESPACE_URL, json.dumps(parts, separators=(',', ':'))))


class SceneProjection(Base):
    __tablename__ = 'selected_scene_projection'
    household_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    profile_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    state: Mapped[dict] = mapped_column(JSON)


class SceneUsage(Base):
    __tablename__ = 'selected_scene_usage'
    session_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    household_id: Mapped[str] = mapped_column(String(50))
    run_id: Mapped[str] = mapped_column(String(100))
    profile_id: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict] = mapped_column(JSON)


class SceneOutbox(Base):
    __tablename__ = 'selected_scene_outbox'
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), unique=True)
    household_id: Mapped[str] = mapped_column(String(50))
    run_id: Mapped[str] = mapped_column(String(100))
    profile_id: Mapped[str] = mapped_column(String(100))
    topic: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict] = mapped_column(JSON)
    published: Mapped[bool] = mapped_column(Boolean, default=False)


class SceneProjector:
    def __init__(self, threshold_seconds=None, test_household=None, policy_id='selected-test-v1'):
        if threshold_seconds is not None and (threshold_seconds < 1 or not test_household):
            raise ValueError('Risk requires a positive threshold and explicit test household')
        self.config = dict(threshold_seconds=threshold_seconds, test_household=test_household, policy_id=policy_id)

    @staticmethod
    def queue(db, topic, payload, scope):
        db.add(SceneOutbox(event_id=payload['event_id'], topic=topic, payload=payload, **scope))

    def save_usage(self, db, usage, scope):
        usage = deepcopy(usage)
        usage['event_id'] = identity('scene-session-v1', usage['session_id'], usage['revision'])
        Session.model_validate_json(json.dumps(usage))
        saved = db.get(SceneUsage, usage['session_id'])
        if saved is None:
            db.add(SceneUsage(session_id=usage['session_id'], payload=usage, **scope))
        else:
            saved.payload = usage
        self.queue(db, SESSION_TOPIC, usage, scope)
        return usage

    def apply(self, db, snapshot, final_index):
        scope = {key: snapshot[key] for key in ('household_id', 'run_id', 'profile_id')}
        record = db.get(SceneProjection, tuple(scope.values()))
        state = deepcopy(record.state) if record else {'active': None, 'risk_sent': False, 'terminal': False}
        if state['terminal']:
            raise ValueError('Scene was finalized; use a new run_id')
        target = next(a for a in snapshot['appliances'] if a['appliance_type'] == snapshot['target_appliance'])
        usage = state['active']
        now, index = snapshot['observed_at'], snapshot['source_index']
        on = target['state'] == 'ON' and target['inferred']
        known = target['state'] != 'UNKNOWN' and target['inferred']
        if usage is None and on:
            usage = {'schema_version': 1, 'scope': 'selected_scene', **scope,
                'event_id': identity('temporary'), 'session_id': identity('scene-session', *scope.values(), index),
                'revision': 1, 'appliance_type': snapshot['target_appliance'], 'source_index': index,
                'status': 'OPEN', 'start_known': snapshot['transition'] == 'TURNED_ON',
                'started_at': now if snapshot['transition'] == 'TURNED_ON' else None,
                'observed_start_at': now, 'observed_until_at': now, 'ended_at': None,
                'end_reason': None, 'observed_on_seconds': 0}
            state['risk_sent'] = False
        elif usage is not None:
            usage['revision'] += 1
            usage['source_index'] = index
            if known:
                usage['observed_until_at'] = now
                usage['observed_on_seconds'] = int((datetime.fromisoformat(now) - datetime.fromisoformat(usage['observed_start_at'])).total_seconds())
                if not on:
                    usage.update(status='CLOSED', ended_at=now, end_reason='TURNED_OFF')
            else:
                usage.update(status='INTERRUPTED', ended_at=None, end_reason='GAP')
        if usage is not None:
            threshold = self.config['threshold_seconds']
            if (on and usage['start_known'] and not state['risk_sent'] and threshold is not None
                    and scope['household_id'] == self.config['test_household']
                    and usage['observed_on_seconds'] >= threshold):
                event = {'event_id': identity('scene-risk-v1', usage['session_id'], self.config['policy_id']),
                    'household_id': scope['household_id'], 'event_type': 'PROLONGED_APPLIANCE_USE',
                    'occurred_at': now, 'reason': {'scope': 'selected_scene', 'run_id': scope['run_id'],
                        'profile_id': scope['profile_id'], 'appliance_type': usage['appliance_type'],
                        'session_id': usage['session_id'], 'source_index': index,
                        'policy_id': self.config['policy_id'], 'policy_scope': 'test_household',
                        'session_started_at': usage['started_at'], 'continuous_on_seconds': usage['observed_on_seconds'],
                        'threshold_seconds': threshold}}
                RiskEvent.model_validate_json(json.dumps(event))
                self.queue(db, RISK_TOPIC, event, scope)
                state['risk_sent'] = True
            if index == final_index and usage['status'] == 'OPEN':
                usage.update(status='INTERRUPTED', end_reason='EOF')
            usage = self.save_usage(db, usage, scope)
        state['active'] = usage if usage and usage['status'] == 'OPEN' else None
        state['terminal'] = index == final_index
        if record is None:
            db.add(SceneProjection(**scope, state=state))
        else:
            record.state = state

    def cancel(self, db, scope):
        record = db.get(SceneProjection, tuple(scope[k] for k in ('household_id','run_id','profile_id')))
        if record is None: raise ValueError('No scene to cancel')
        state = deepcopy(record.state)
        if state['terminal']: return
        if state['active']:
            usage = state['active']
            usage.update(status='INTERRUPTED', end_reason='CANCELLED', revision=usage['revision'] + 1)
            self.save_usage(db, usage, scope)
        state.update(active=None, terminal=True)
        record.state = state


def flush_outbox(sessions, publisher, household, run_id, profile_id):
    """ACK before marking published; repeated delivery retains immutable event_id."""
    while True:
        with sessions() as db:
            row = db.scalar(select(SceneOutbox).where(SceneOutbox.household_id == household,
                SceneOutbox.run_id == run_id, SceneOutbox.profile_id == profile_id,
                SceneOutbox.published.is_(False)).order_by(SceneOutbox.sequence).limit(1))
            if row is None: return
            sequence, topic, payload = row.sequence, row.topic, deepcopy(row.payload)
        publisher.publish(topic, payload)
        with sessions.begin() as db:
            db.get(SceneOutbox, sequence).published = True


class SceneEventPublisher:
    def __init__(self, settings):
        from confluent_kafka import Producer
        from realtime_analysis.snapshot_publisher import AnalysisSnapshotPublisher
        producer = Producer(settings.producer_config())
        self.publishers = {topic: AnalysisSnapshotPublisher(settings, producer, topic=topic)
                           for topic in (SESSION_TOPIC, RISK_TOPIC)}

    def publish(self, topic, payload):
        self.publishers[topic].publish(payload)

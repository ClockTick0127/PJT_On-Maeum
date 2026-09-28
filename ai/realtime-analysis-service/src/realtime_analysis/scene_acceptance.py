"""Verify exported scene readback; local execution is explicitly not deployed E2E."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from realtime_analysis.scene_contracts import Snapshot, Session, RiskEvent
from realtime_analysis.scene_pipeline import SceneEvidence, SceneHandler, SceneRepository, FEATURES
from realtime_analysis.scene_events import SceneProjection, SceneUsage, SceneOutbox, SceneProjector, RISK_TOPIC
from realtime_analysis.scene_replay import measurements
from realtime_analysis.real_predictor import load_profile


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_jsonl(path):
    with Path(path).open(encoding='utf-8') as stream:
        return [json.loads(line) for line in stream if line.strip()]


def write_jsonl(path, rows):
    with Path(path).open('x', encoding='utf-8') as stream:
        for row in rows:
            stream.write(json.dumps(row, allow_nan=False) + '\n')


def unique(rows, model, key):
    result = {}
    for row in rows:
        model.model_validate_json(json.dumps(row, allow_nan=False))
        identity = row[key]
        require(identity not in result or result[identity] == row, 'Conflicting duplicate ' + key)
        result[identity] = row
    return result


def verify(root, appliance, household, run_id, start, snapshots, sessions, risks,
           threshold_seconds=10, require_source=False):
    """All records must belong to this exact run; no filtering hides wrong-scope rows."""
    profile = load_profile(appliance)
    inputs = list(measurements(root, appliance, household, run_id, start))
    actual = unique(snapshots, Snapshot, 'source_index')
    require(set(actual) == {m.source_index for m in inputs}, 'Incomplete or extra snapshot indices')
    runtime = None
    engine = create_engine('sqlite://')
    for cls in (SceneProjection, SceneUsage, SceneOutbox):
        cls.__table__.create(engine)
    factory = sessionmaker(engine)
    projector = SceneProjector(threshold_seconds, household if threshold_seconds else None)
    try:
        for offset, measurement in enumerate(inputs):
            row = actual[measurement.source_index]
            require((row['household_id'], row['run_id'], row['profile_id'], row['target_appliance']) ==
                    (household, run_id, profile['profile_id'], appliance.upper()), 'Wrong snapshot scope')
            require(datetime.fromisoformat(row['observed_at']) == measurement.measured_at, 'Event time drift')
            expected = {k: getattr(measurement, k) for k in FEATURES} if measurement.valid else None
            require(row['measurement'] == expected, 'Raw features changed in transit')
            require(row['ready'] == (offset >= 254 and measurement.context), 'Window readiness mismatch')
            if require_source:
                require(row['source'] is not None, 'Missing Kafka input provenance')
            for field in ('checkpoint_sha256', 'model_code_sha256', 'normalization_sha256'):
                require(row['runtime'][field] == profile[field], 'Frozen asset identity mismatch: ' + field)
            if runtime is None:
                runtime = row['runtime']
            require(row['runtime'] == runtime, 'Runtime changed within run')
            with factory.begin() as db:
                projector.apply(db, row, inputs[-1].source_index)
        with factory() as db:
            expected_sessions = {r.payload['session_id']: r.payload for r in db.scalars(select(SceneUsage))}
            expected_risks = {r.payload['event_id']: r.payload for r in db.scalars(
                select(SceneOutbox).where(SceneOutbox.topic == RISK_TOPIC))}
        # Session exports are current persisted rows, not all revision messages.
        actual_sessions = unique(sessions, Session, 'session_id')
        actual_risks = unique(risks, RiskEvent, 'event_id')
        require(actual_sessions == expected_sessions, 'Persisted sessions differ from observed transitions')
        require(actual_risks == expected_risks, 'Missing, extra, or incorrect risk events')
        return dict(status='PASS', snapshot_count=len(actual), ready_count=sum(r['ready'] for r in actual.values()),
                    session_count=len(actual_sessions), risk_count=len(actual_risks), runtime=runtime,
                    model_numerical_parity='NOT_CHECKED', notification_creation='NOT_RUN', ui='NOT_RUN')
    finally:
        engine.dispose()


def local_run(root, appliance, output, household, run_id, start):
    import torch
    from realtime_analysis.real_predictor import SelectedScenePredictor
    torch.set_num_threads(1)
    output.mkdir(parents=True, exist_ok=False)
    engine = create_engine('sqlite:///' + (output / 'analysis.sqlite').resolve().as_posix())
    for cls in (SceneEvidence, SceneProjection, SceneUsage, SceneOutbox):
        cls.__table__.create(engine)
    factory = sessionmaker(engine)
    predictor = SelectedScenePredictor(root, appliance)
    snapshots, events = [], []
    class SnapshotSink:
        def publish(self, payload):
            snapshots.append(payload)
    class EventSink:
        def publish(self, topic, payload):
            events.append(dict(topic=topic, payload=payload))
    def handler():
        return SceneHandler(predictor, SceneRepository(factory, SceneProjector(10, household),
            EventSink(), predictor.profile['source_clip_end_exclusive'] - 1), SnapshotSink(), run_id, household)
    worker = handler()
    inputs = list(measurements(root, appliance, household, run_id, start))
    try:
        for index, row in enumerate(inputs):
            if index in (254, 300):
                worker = handler()  # Restore durable window/decoder/projection at readiness and mid-scene.
            worker(row)
        event_count = len(events)
        worker = handler()
        for row in inputs:
            worker(row)  # Entire replay retains immutable IDs and no second session/risk.
        require(len(events) == event_count, 'Replay duplicated service events')
        with factory() as db:
            sessions = [r.payload for r in db.scalars(select(SceneUsage))]
            risks = [r.payload for r in db.scalars(select(SceneOutbox).where(SceneOutbox.topic == RISK_TOPIC))]
            require(not db.scalars(select(SceneOutbox).where(SceneOutbox.published.is_(False))).first(),
                    'Unpublished local outbox entries')
        report = verify(root, appliance, household, run_id, start, snapshots, sessions, risks)
        report.update(mode='local_actual_model_sqlite', mqtt_kafka='NOT_RUN', deployed_backend='NOT_RUN',
                      restart_and_full_replay='PASS', event_delivery_count=len(events))
        for name, rows in [('snapshots', snapshots), ('sessions', sessions), ('risks', risks), ('events', events)]:
            write_jsonl(output / (name + '.jsonl'), rows)
        (output / 'summary.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        return report
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['local', 'readback'])
    parser.add_argument('--asset-root', type=Path, required=True)
    parser.add_argument('--appliance', required=True)
    parser.add_argument('--household', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--start-time', type=datetime.fromisoformat, required=True)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--risk-threshold', type=int, default=10)
    args = parser.parse_args()
    require(args.risk_threshold >= 0, 'Risk threshold must be nonnegative; 0 disables risk')
    if args.mode == 'local':
        require(args.risk_threshold == 10, 'Local test policy uses 10 seconds')
        report = local_run(args.asset_root, args.appliance, args.directory, args.household, args.run_id, args.start_time)
    else:
        report = verify(args.asset_root, args.appliance, args.household, args.run_id, args.start_time,
            *[read_jsonl(args.directory / (name + '.jsonl')) for name in ('snapshots', 'sessions', 'risks')],
            threshold_seconds=args.risk_threshold or None, require_source=True)
        report.update(mode='exported_readback', transport_delivery='NOT_OBSERVED_BY_THIS_TOOL')
        (args.directory / 'acceptance.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))


if __name__ == '__main__':
    main()

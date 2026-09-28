"""Read actual Kafka output plus PostgreSQL evidence; never claim backend acceptance."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import time
from uuid import uuid4

from confluent_kafka import Consumer
from sqlalchemy import select
from realtime_analysis.config import Settings
from realtime_analysis.database import create_session_factory
from realtime_analysis.scene_pipeline import SceneEvidence
from realtime_analysis.scene_events import SceneUsage, SceneOutbox, SESSION_TOPIC, RISK_TOPIC
from realtime_analysis.scene_acceptance import require, verify, write_jsonl
from realtime_analysis.real_predictor import load_profile


def collect(settings, root, appliance, household, run_id, start, timeout=180):
    sessions = create_session_factory(settings)
    profile = load_profile(appliance)
    consumer = Consumer({'bootstrap.servers': settings.kafka_bootstrap_servers,
        'group.id': 'scene-check-' + str(uuid4()), 'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    consumer.subscribe([settings.kafka_scene_snapshot_topic, SESSION_TOPIC, RISK_TOPIC])
    captured = {topic: {} for topic in (settings.kafka_scene_snapshot_topic, SESSION_TOPIC, RISK_TOPIC)}
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(.2)
            if message is not None:
                require(not message.error(), 'Kafka readback error')
                row = json.loads(message.value())
                scope = row.get('reason', row)
                if row['household_id'] != household or scope['run_id'] != run_id:
                    continue
                identity = row.get('snapshot_id', row.get('event_id'))
                previous = captured[message.topic()].get(identity)
                require(previous is None or previous == row, 'Conflicting Kafka duplicate')
                captured[message.topic()][identity] = row
            if len(captured[settings.kafka_scene_snapshot_topic]) != profile['source_clip_end_exclusive'] - profile['source_prefix_start_index']:
                continue
            with sessions() as db:
                def scoped(cls):
                    return select(cls).where(cls.household_id == household, cls.run_id == run_id,
                                             cls.profile_id == profile['profile_id'])
                evidence = [r.payload for r in db.scalars(scoped(SceneEvidence))]
                usage = [r.payload for r in db.scalars(scoped(SceneUsage))]
                outbox = list(db.scalars(scoped(SceneOutbox)))
            if any(not r.published or captured[r.topic].get(r.event_id) != r.payload for r in outbox):
                continue
            snapshots = list(captured[settings.kafka_scene_snapshot_topic].values())
            require({r['snapshot_id']: r for r in evidence} == captured[settings.kafka_scene_snapshot_topic],
                    'Kafka output differs from committed PostgreSQL evidence')
            require(sum(len(captured[t]) for t in (SESSION_TOPIC, RISK_TOPIC)) == len(outbox), 'Extra Kafka events')
            risks = list(captured[RISK_TOPIC].values())
            report = verify(root, appliance, household, run_id, start, snapshots, usage, risks, require_source=True)
            report.update(mode='kafka_postgresql_readback', kafka_outbox_delivery='PASS', deployed_backend='NOT_RUN')
            return report, snapshots, usage, risks
        raise TimeoutError('Incomplete broker/database readback before deadline')
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('appliance', 'household', 'run-id'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--asset-root', type=Path, required=True)
    parser.add_argument('--start-time', type=datetime.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    report, snapshots, sessions, risks = collect(Settings(), args.asset_root, args.appliance,
                                               args.household, args.run_id, args.start_time)
    for name, rows in [('snapshots', snapshots), ('sessions', sessions), ('risks', risks)]:
        write_jsonl(args.output / (name + '.jsonl'), rows)
    (args.output / 'summary.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))


if __name__ == '__main__':
    main()

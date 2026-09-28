"""TLS MQTT publisher for locked raw fixtures. Never reads expected outputs."""
import argparse
import asyncio
from datetime import datetime
import json
import math
import os
from pathlib import Path
import ssl
import sys
import time

from realtime_analysis.real_predictor import load_profile
from realtime_analysis.scene_replay import measurements


def mqtt_tls(allow_plaintext=False, ca_file=None):
    if allow_plaintext:
        if ca_file:
            raise ValueError('CA file cannot be used with plaintext')
        return None
    context = ssl.create_default_context(cafile=str(ca_file) if ca_file else None)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


def prepare(root, appliance, household, run_id, start_time, topic_template):
    if any(c in household for c in '/+#') or not household.strip():
        raise ValueError('household must be one MQTT topic segment')
    if topic_template.count('{household_id}') != 1:
        raise ValueError('topic must contain exactly one {household_id} placeholder')
    topic = topic_template.format(household_id=household)
    if any(c in topic for c in '+#') or not topic or topic.startswith('$'):
        raise ValueError('publish topic must be a concrete non-system topic')
    # Validate every row and hash before making the first external write.
    rows = list(measurements(root, appliance, household, run_id, start_time))
    profile = load_profile(appliance)
    if len(rows) != profile['expected_source_rows']:
        raise ValueError('fixture row count mismatch')
    for index, row in enumerate(rows):
        if row.source_index != profile['source_prefix_start_index'] + index:
            raise ValueError('fixture must be chronological and contiguous')
    return topic, rows, profile


async def replay_mqtt(rows, topic, *, hostname, port, tls_context, interval=1.,
                      retries=3, username=None, password=None, on_progress=lambda _: None,
                      client_factory=None):
    import aiomqtt
    if not math.isfinite(interval) or interval < 0 or retries < 0:
        raise ValueError('interval/retries must be non-negative')
    factory = client_factory or aiomqtt.Client
    started = time.monotonic()
    acknowledged = 0
    for attempt in range(retries + 1):
        try:
            async with factory(hostname=hostname, port=port, tls_context=tls_context,
                               username=username, password=password) as client:
                # Re-send all input on reconnect. MQTT ACK is not a backend
                # checkpoint; stable message/run IDs make backend retries safe.
                for position, row in enumerate(rows):
                    await client.publish(topic, row.model_dump_json(), qos=1)
                    acknowledged += 1
                    on_progress({'status': 'publishing', 'attempt': attempt + 1,
                        'mqtt_acknowledged_total': acknowledged, 'source_index': row.source_index})
                    if interval and position + 1 < len(rows):
                        await asyncio.sleep(interval)
            return {'status': 'published', 'source_rows': len(rows), 'attempts': attempt + 1,
                'mqtt_acknowledged_total': acknowledged, 'elapsed_seconds': time.monotonic() - started,
                'backend_completion_verified': False, 'model_forward_executed_by_publisher': False}
        except aiomqtt.MqttError:
            if attempt == retries:
                raise
            await asyncio.sleep(min(2 ** attempt, 5))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--asset-root', type=Path, default=os.getenv('MODEL_ASSET_ROOT', '/assets'))
    parser.add_argument('--appliance', default='kettle')
    parser.add_argument('--household', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--start-time', required=True, type=datetime.fromisoformat)
    parser.add_argument('--mqtt-host', required=True)
    parser.add_argument('--mqtt-port', type=int, default=8883)
    parser.add_argument('--ca-file', type=Path)
    parser.add_argument('--allow-plaintext', action='store_true', help='Explicit local/test transport only')
    parser.add_argument('--topic-template', default='v1/power/demo/{household_id}/main')
    parser.add_argument('--interval', type=float, default=1.)
    parser.add_argument('--retries', type=int, default=3)
    parser.add_argument('--output', required=True, type=Path, help='New report directory')
    args = parser.parse_args()
    if not math.isfinite(args.interval) or args.interval < 0 or args.retries < 0 or not 1 <= args.mqtt_port <= 65535:
        parser.error('invalid interval, retries or port')
    topic, rows, profile = prepare(args.asset_root, args.appliance, args.household,
                                  args.run_id, args.start_time, args.topic_template)
    tls = mqtt_tls(args.allow_plaintext, args.ca_file)
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {'run_id': args.run_id, 'household_id': args.household, 'profile_id': profile['profile_id'],
        'panel_sha256': profile['panel_sha256'], 'start_time': args.start_time.isoformat(),
        'topic': topic, 'tls': tls is not None, 'interval': args.interval,
        'expected_rows': len(rows), 'expected_last_index': rows[-1].source_index}
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    def progress(value):
        temp = args.output / 'progress.tmp'
        temp.write_text(json.dumps(value), encoding='utf-8')
        temp.replace(args.output / 'progress.json')
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        result = asyncio.run(replay_mqtt(rows, topic, hostname=args.mqtt_host, port=args.mqtt_port,
            tls_context=tls, interval=args.interval, retries=args.retries,
            username=os.getenv('MQTT_USER') or None, password=os.getenv('MQTT_PASS') or None,
            on_progress=progress))
    except (Exception, KeyboardInterrupt) as error:
        progress({'status': 'failed', 'error_type': type(error).__name__, 'backend_completion_verified': False})
        print(json.dumps({'status': 'failed', 'error_type': type(error).__name__}), file=sys.stderr)
        raise SystemExit(1)
    progress(result)
    print(json.dumps(result))


if __name__ == '__main__': main()

"""Populate the isolated demo using actual MQTT -> Kafka -> model -> PostgreSQL.

Run with the analysis-service venv after docker compose up. Only the demo analysis
container is recreated; volumes, other services and projects are preserved.
"""
import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlencode
from urllib.request import urlopen

from evaluate_scenes import AI, ASSETS, compare, read_jsonl

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--tag', default=datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S'))
    args = parser.parse_args()
    if not args.tag or not all(c.isascii() and (c.isalnum() or c in '-_') for c in args.tag):
        parser.error('--tag must contain only ASCII letters, numbers, hyphens or underscores')
    args.output.mkdir(parents=True, exist_ok=False)
    profiles = json.loads((ASSETS / 'profiles/selected_profiles.lock.json').read_text())['profiles']
    compose = AI.parent / 'infrastructure/r3-demo'
    catalog, results = [], []
    def docker(*command, env=None):
        result = subprocess.run([args.docker, 'compose', *command], cwd=compose,
            env=env, check=True, capture_output=True, encoding='utf-8')
        return result.stdout.strip()
    for p in profiles:
        name = p['appliance_type']
        run = f'r3-{name}-{args.tag}'
        household = f'r3-{name}'
        env = {**os.environ, 'R3_APPLIANCE': name, 'R3_RUN_ID': run, 'R3_HOUSEHOLD_ID': household}
        docker('up', '-d', '--no-build', '--no-deps', '--force-recreate', 'analysis', env=env)
        # Kafka retains input; startup and processing are verified by API, not a sleep.
        subprocess.run([sys.executable, '-m', 'realtime_analysis.scene_replay', 'publish',
            '--asset-root', str(ASSETS), '--appliance', name, '--household', household,
            '--run-id', run, '--start-time', '2026-09-21T12:00:00+09:00', '--interval', '0.005'], check=True)
        query = urlencode({'householdId': household, 'runId': run, 'profileId': p['profile_id']})
        url = 'http://127.0.0.1:18084/api/monitoring/admin/selected-scene?' + query
        deadline = time.monotonic() + 180
        last_error = None
        while time.monotonic() < deadline:
            try:
                with urlopen(url, timeout=5) as response: latest = json.load(response)
                if latest['source_index'] == p['source_clip_end_exclusive'] - 1: break
            except Exception as error: last_error = type(error).__name__
            time.sleep(1)
        else: raise RuntimeError(f'{name}: API did not reach EOF ({last_error})')
        # run is generated internally from validated tag; never accept SQL punctuation.
        assert all(c.isalnum() or c in '-_' for c in run)
        payloads = docker('exec', '-T', 'postgres', 'psql', '-U', 'nilm_demo', '-d', 'monitoring_db',
            '-At', '-c', f"SELECT payload FROM selected_scene_snapshots WHERE run_id='{run}' ORDER BY source_index")
        path = args.output / f'{name}.jsonl'
        path.write_text(payloads + '\n', encoding='utf-8')
        rows = read_jsonl(path)
        result = compare(p, rows)
        with (ASSETS / p['panel_relative']).open() as handle:
            raw = {int(r['source_index']): r for r in csv.DictReader(handle)}
        assert all(r['source']['topic'] == 'power.scene.v2' and
            r['measurement'] == {k: float(raw[r['source_index']][k]) for k in
                ('active_power', 'reactive_power', 'power_factor', 'current')} for r in rows)
        assert latest == rows[-1]
        result.update(actual_mqtt_kafka=True, raw_roundtrip_exact=True, api_latest_exact=True,
                      run_id=run, runtime=latest['runtime'])
        results.append(result)
        catalog.append({'appliance': name, 'house': p['source_house'], 'family': p['family'],
            'householdId': household, 'runId': run, 'profileId': p['profile_id'],
            'firstIndex': p['source_prefix_start_index'], 'clipIndex': p['source_clip_start_index'],
            'lastIndex': p['source_clip_end_exclusive'] - 1, 'threshold': p['threshold'],
            'stateParity': result['runtime_state_parity'], 'maxScoreDifference': result['max_score_difference_vs_h200']})
        print(json.dumps(result), flush=True)
        (args.output / 'summary.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
        (AI.parent / 'frontend/public/scene-demo-catalog.json').write_text(json.dumps(catalog, indent=2), encoding='utf-8')

if __name__ == '__main__': main()

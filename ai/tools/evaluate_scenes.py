"""Post-hoc evaluation only: never imported by the inference service."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

AI = Path(__file__).resolve().parents[1]
ASSETS = AI / 'assets/nilm_r3'
ORDER = ['kettle', 'induction', 'iron', 'microwave', 'hair_dryer', 'vacuum_cleaner']

def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding='utf-8-sig').splitlines() if line.strip()]

def compare(profile, rows):
    folder = ASSETS / 'fixtures' / profile['candidate_id'] / 'evaluation_only'
    ref = {r['source_index']: r for r in read_jsonl(folder / 'cold_1/live_scores.jsonl')}
    states = {r['source_index']: r['state'] for r in read_jsonl(folder / 'cold_1/live_states.jsonl')}
    label_index = json.loads((folder.parent / 'source_candidate_manifest.json').read_text())['model']['label_index']
    truth = {r['source_index']: r['labels'][label_index] for r in read_jsonl(folder / 'truth_sidecar.jsonl')}
    decode = {'UNKNOWN': -1, 'OFF': 0, 'ON': 1}
    counts = dict.fromkeys(['tp', 'fp', 'fn', 'tn', 'u_on', 'u_off'], 0)
    keys = {(1,1):'tp', (0,1):'fp', (1,0):'fn', (0,0):'tn', (1,-1):'u_on', (0,-1):'u_off'}
    diffs, mismatch, transitions = [], [], []
    decoded = {}
    previous = -1
    for row in rows:
        i = row['source_index']
        if 'on_score' in row:
            score, state = row['on_score'], row['state']
        else:
            target = next(a for a in row['appliances'] if a['appliance_type'].lower() == profile['appliance_type'])
            score, state = target['probability'], target['state']
            assert all(a['state'] == 'UNKNOWN' and a['probability'] is None and a['is_on'] is None and not a['inferred']
                       for a in row['appliances'] if a is not target)
        assert row['ready'] == ref[i]['ready']
        value = decode[state]
        decoded[i] = value
        if value != states[i]: mismatch.append(i)
        if value != previous: transitions.append({'source_index': i, 'state': state})
        previous = value
        if score is not None: diffs.append(abs(score - ref[i]['on_score']))
        if profile['source_clip_start_index'] <= i < profile['source_clip_end_exclusive']:
            counts[keys[truth[i], value]] += 1
    assert len(rows) == profile['expected_source_rows']
    assert len({r['source_index'] for r in rows}) == len(rows)
    assert set(decoded) == set(ref)
    # Measure OFF only at/after true event end, never treat an internal dip as completion.
    events = []
    start = None
    for i in range(profile['source_clip_start_index'], profile['source_clip_end_exclusive']):
        if truth[i] == 1 and start is None: start = i
        if truth[i] != 1 and start is not None:
            off = next((j for j in range(i, profile['source_clip_end_exclusive']) if decoded[j] == 0), None)
            events.append({'true_start_index': start, 'true_end_exclusive': i,
                'first_off_at_or_after_true_end': off,
                'end_delay_seconds': off - i if off is not None else None,
                'off_seconds_inside_true_on': sum(decoded[j] == 0 for j in range(start, i))})
            start = None
    return {'appliance': profile['appliance_type'], 'rows': len(rows), 'ready': len(diffs),
            'max_score_difference_vs_h200': max(diffs), 'state_mismatch_indices': mismatch,
            'counts': counts, 'locked_expected_counts': profile['expected_selected_scene_counts'],
            'transitions': transitions, 'true_event_end_checks': events, 'runtime_state_parity': not mismatch}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    profiles = json.loads((ASSETS / 'profiles/selected_profiles.lock.json').read_text())['profiles']
    results = []
    for p in profiles:
        name = p['appliance_type']
        runs = []
        for n in (1, 2):
            dest = args.output / f'{name}-cold-{n}'
            subprocess.run([sys.executable, '-m', 'realtime_analysis.model_replay', '--asset-root', str(ASSETS),
                            '--appliance', name, '--output', str(dest)], check=True, stdout=subprocess.DEVNULL)
            runs.append(read_jsonl(dest / 'scores.jsonl'))
        result = compare(p, runs[0])
        result['fresh_process_repeat_exact'] = runs[0] == runs[1]
        result['runtime'] = json.loads((args.output / f'{name}-cold-1/runtime.json').read_text())
        results.append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
    (args.output / 'summary.json').write_text(json.dumps(results, indent=2), encoding='utf-8')

if __name__ == '__main__': main()

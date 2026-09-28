"""Actual CPU inference on frozen long-prefix, OFF and other-day raw panels."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from evaluate_scenes import AI, ASSETS, read_jsonl

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--worker', choices=['long_prefix', 'off_control', 'other_day'])
    parser.add_argument('--appliance')
    parser.add_argument('--cold-output', type=Path, default=AI / 'evidence/local-six-scenes-v2')
    args = parser.parse_args()
    profiles = json.loads((ASSETS / 'profiles/selected_profiles.lock.json').read_text())['profiles']
    if args.worker:
        # This process sees input config/panel/model only, never expected scores or labels.
        import torch
        from realtime_analysis.model_replay import replay
        from realtime_analysis.real_predictor import SelectedScenePredictor
        torch.set_num_threads(1)
        p = next(p for p in profiles if p['appliance_type'] == args.appliance)
        folder = ASSETS / 'fixtures' / p['candidate_id'] / 'evaluation_only'
        config = json.loads((folder / f'{args.worker}_config.json').read_text())
        panel = folder / f'{args.worker}_input/panel.csv'
        assert hashlib.sha256(panel.read_bytes()).hexdigest() == config['panel_sha256']
        predictor = SelectedScenePredictor(ASSETS, args.appliance)
        assert config['threshold'] == p['threshold']
        with args.output.open('x', encoding='utf-8') as handle:
            for row in replay(predictor, panel): handle.write(json.dumps(row) + '\n')
        return
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    for p in profiles:
        folder = ASSETS / 'fixtures' / p['candidate_id'] / 'evaluation_only'
        label = json.loads((folder.parent / 'source_candidate_manifest.json').read_text())['model']['label_index']
        for control in ('long_prefix', 'off_control', 'other_day'):
            dest = args.output / f"{p['appliance_type']}-{control}.jsonl"
            subprocess.run([sys.executable, __file__, '--worker', control, '--appliance', p['appliance_type'], '--output', str(dest)], check=True)
            config = json.loads((folder / f'{control}_config.json').read_text())
            a, b = config['evaluation_interval']
            truth = {r['source_index']: r['labels'][label] for r in read_jsonl(folder / f'{control}_input/truth_sidecar.jsonl')}
            counts = dict.fromkeys(['tp','fp','fn','tn','u_on','u_off'], 0)
            keys = {(1,'ON'):'tp', (0,'ON'):'fp', (1,'OFF'):'fn', (0,'OFF'):'tn', (1,'UNKNOWN'):'u_on', (0,'UNKNOWN'):'u_off'}
            rows = read_jsonl(dest)
            reference_name = {'off_control':'off'}.get(control, control)
            reference_path = folder / reference_name / 'live_states.jsonl'
            refs = {r['source_index']: r['state'] for r in read_jsonl(reference_path)}
            mismatches = []
            for r in rows:
                i = r['source_index']
                if a <= i < b: counts[keys[truth[i], r['state']]] += 1
                if {'ON':1,'OFF':0,'UNKNOWN':-1}[r['state']] != refs[i]: mismatches.append(i)
            result = {'appliance': p['appliance_type'], 'control': control, 'counts': counts,
                      'rows': len(rows), 'model_forward_executed': True, 'threshold_unchanged': True,
                      'state_mismatch_indices_vs_h200': mismatches,
                      'recall': counts['tp'] / (counts['tp']+counts['fn']+counts['u_on']) if counts['tp']+counts['fn']+counts['u_on'] else None,
                      'false_positive_seconds': counts['fp']}
            if control == 'long_prefix':
                cold = read_jsonl(args.cold_output / f"{p['appliance_type']}-cold-1/scores.jsonl")
                values = {r['source_index']:r for r in rows}
                selected = [r for r in cold if r['in_evaluation_clip']]
                result['selected_clip_scores_identical_to_cold'] = all(r['on_score'] == values[r['source_index']]['on_score'] for r in selected)
                result['selected_clip_states_identical_to_cold'] = all(r['state'] == values[r['source_index']]['state'] for r in selected)
            results.append(result)
            print(json.dumps(result), flush=True)
            (args.output / 'summary.json').write_text(json.dumps(results, indent=2), encoding='utf-8')

if __name__ == '__main__': main()

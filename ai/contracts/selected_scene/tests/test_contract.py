import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from models import MODELS
from validate import validate


def example(name):
    return json.loads((ROOT / 'examples' / f'{name}.json').read_text())


class ContractTests(unittest.TestCase):
    def test_documented_examples(self):
        cases = json.loads((ROOT / 'examples/cases.json').read_text())
        for case in cases:
            with self.subTest(file=case['file'], rule=case['rule']):
                content = (ROOT / 'examples' / case['file']).read_text()
                if case['valid']:
                    validate(case['kind'], content)
                else:
                    with self.assertRaises(ValueError):
                        validate(case['kind'], content)

    def test_legacy_input_metadata_does_not_replace_features(self):
        payload = example('measurement-valid')
        payload.update(power_w=9999, house='ignored-house', active_devices=['IRON'])
        model = validate('measurement', json.dumps(payload))
        self.assertEqual(model.active_power, payload['active_power'])
        self.assertEqual(model.household_id, payload['household_id'])

    def test_invalid_row_accepts_null_features(self):
        payload = example('measurement-valid')
        payload.update(valid=False, context=False, active_power=None, reactive_power=None, power_factor=None, current=None)
        validate('measurement', json.dumps(payload))

    def test_context_does_not_mean_prefix_or_current_validity(self):
        payload = example('measurement-valid')
        payload['context'] = False
        validate('measurement', json.dumps(payload))
        payload.update(valid=False, context=True)
        validate('measurement', json.dumps(payload))

    def test_numeric_coercion_and_nonfinite_rejected(self):
        for value in ('0.5', True, float('nan'), float('inf')):
            with self.subTest(value=value):
                payload = example('snapshot-on')
                payload['appliances'][0]['probability'] = value
                with self.assertRaises(ValueError):
                    validate('snapshot', json.dumps(payload))

    def test_mid_band_may_be_inferred_but_unknown(self):
        payload = example('snapshot-on')
        payload['transition'] = None
        payload['appliances'][0].update(state='UNKNOWN', probability=.8, is_on=None)
        validate('snapshot', json.dumps(payload))

    def test_inconsistent_transition_rejected(self):
        payload = example('snapshot-on')
        payload['transition'] = 'TURNED_OFF'
        with self.assertRaises(ValueError):
            validate('snapshot', json.dumps(payload))

    def test_broker_provenance_required_only_in_broker_mode(self):
        payload = example('snapshot-on')
        payload['source'] = None
        validate('snapshot', json.dumps(payload))
        with self.assertRaises(ValueError):
            validate('snapshot', json.dumps(payload), broker=True)

    def test_session_duration_and_end_semantics(self):
        for changes in ({'observed_on_seconds': 39}, {'ended_at': None}, {'status': 'OPEN'}):
            payload = example('session-closed')
            payload.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate('session', json.dumps(payload))

    def test_risk_clock_and_policy_scope(self):
        for changes in ({'continuous_on_seconds': 11}, {'policy_scope': 'production'}, {'session_started_at': None}):
            payload = example('risk-threshold')
            payload['reason'].update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate('risk', json.dumps(payload))

    def test_new_risk_preserves_legacy_envelope(self):
        # Real legacy model from develop, without importing an inference runtime.
        sys.path.insert(0, str(ROOT.parents[1] / 'realtime-analysis-service/src'))
        from realtime_analysis.schemas import AnalysisEvent
        content = json.dumps(example('risk-threshold'))
        validate('risk', content)
        AnalysisEvent.model_validate_json(content)

    def test_schema_export_is_available_for_every_kind(self):
        for kind in MODELS:
            result = subprocess.run([sys.executable, str(ROOT / 'validate.py'), kind, '--schema'], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            schema = json.loads(result.stdout)
            self.assertEqual(schema['type'], 'object')

    def test_jsonl_cli_failure_exit_and_line(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'payloads.jsonl'
            good = example('snapshot-on')
            bad = copy.deepcopy(good)
            bad['runtime']['checkpoint_sha256'] = 'not-a-hash'
            path.write_text(json.dumps(good) + '\n' + json.dumps(bad), encoding='utf-8')
            result = subprocess.run([sys.executable, str(ROOT / 'validate.py'), 'snapshot', str(path), '--jsonl', '--broker'], capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stderr)['line'], 2)
            self.assertNotIn('not-a-hash', result.stderr)

    def test_empty_input_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'empty.jsonl'
            path.write_text('\n', encoding='utf-8')
            result = subprocess.run([sys.executable, str(ROOT / 'validate.py'), 'measurement', str(path), '--jsonl'], capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)


if __name__ == '__main__':
    unittest.main()

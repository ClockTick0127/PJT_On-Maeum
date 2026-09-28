import csv
import json
import sys
from types import SimpleNamespace

import pytest

from gold_profile.experiment import main, parse_report


def options(tmp_path):
    return ['--root', str(tmp_path), '--experiment-id', 'baseline-3h',
            '--execution-id', 'repeat-1', '--from', '2026-09-21', '--to', '2026-09-22',
            '--dataset-id', 'demo-v1', '--households', '3', '--sampling-seconds', '1',
            '--workers', '1', '--run-mode', 'INITIAL_BUILD']


def test_nested_report_and_trailing_logs():
    report = {'status': 'PUBLISH_PENDING', 'stages': [{'stage': 'publish', 'status': 'SKIPPED'}]}
    assert parse_report('log\n' + json.dumps(report, indent=2) + '\nshutdown\n') == report


def test_outputs_preserve_incomplete_reused_and_secrets(tmp_path, monkeypatch):
    codes = iter([10, 12])
    def run(command, stdout, **kwargs):
        code = next(codes)
        stdout.write(json.dumps({'status': 'INPUT_INCOMPLETE' if code == 10 else 'PUBLISH_PENDING',
                                 'stages': [{'stage': 'gold-profile', 'status': 'SUCCEEDED',
                                            'detail': {'reused_run_id': 'old-run'}}]}, indent=2))
        return SimpleNamespace(returncode=code)
    monkeypatch.setattr('gold_profile.experiment.subprocess.run', run)
    monkeypatch.setenv('DATABASE_PASSWORD', 'must-not-be-recorded')
    assert main(options(tmp_path)) == 10
    directory = tmp_path / 'baseline-3h' / 'repeat-1'
    assert {p.name for p in directory.iterdir()} == {'config.json', 'result.json', 'execution.log', 'run-dates.csv'}
    result = json.loads((directory / 'result.json').read_text())
    assert result['completed_dates'] == 2 and result['status'] == 'INPUT_INCOMPLETE'
    assert result['correctness_verified'] is False
    with (directory / 'run-dates.csv').open(encoding='utf-8', newline='') as file:
        rows = list(csv.DictReader(file))
    assert len(rows) == 2 and json.loads(rows[0]['reused_run_ids_json']) == ['old-run']
    assert 'must-not-be-recorded' not in (directory / 'config.json').read_text()
    with pytest.raises(FileExistsError):
        main(options(tmp_path))


def test_failure_and_launch_error_are_persisted(tmp_path, monkeypatch):
    monkeypatch.setattr('gold_profile.experiment.subprocess.run',
                        lambda *a, **k: SimpleNamespace(returncode=1))
    assert main(options(tmp_path)) == 1
    result_path = tmp_path / 'baseline-3h' / 'repeat-1' / 'result.json'
    assert json.loads(result_path.read_text())['completed_dates'] == 1
    def missing(*a, **k):
        raise FileNotFoundError('missing command')
    monkeypatch.setattr('gold_profile.experiment.subprocess.run', missing)
    second = options(tmp_path)
    second[second.index('repeat-1')] = 'repeat-2'
    with pytest.raises(FileNotFoundError):
        main(second)
    result = json.loads(result_path.parent.with_name('repeat-2').joinpath('result.json').read_text())
    assert result['status'] == 'FAILED' and 'missing command' in result['error']


def test_report_command_and_auto_ids(tmp_path, monkeypatch):
    seen = []
    def run(command, stdout, **kwargs):
        seen.append(command)
        stdout.write('{"status":"REUSED","report_id":"report-1"}\n')
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr('gold_profile.experiment.subprocess.run', run)
    args = options(tmp_path)
    index = args.index('--execution-id')
    del args[index:index+2]
    args += ['--command', 'household-report', 'daily', '--as-of', '{date}']
    assert main(args) == 0
    assert main(args) == 0
    assert len(list((tmp_path / 'baseline-3h').iterdir())) == 2
    assert seen[0][-1] == '2026-09-21'


def test_reject_path_traversal_before_writes(tmp_path):
    args = options(tmp_path)
    args[args.index('baseline-3h')] = '../escape'
    with pytest.raises(SystemExit):
        main(args)
    assert list(tmp_path.iterdir()) == []


def test_real_subprocess_output_and_stderr(tmp_path):
    args = options(tmp_path) + ['--command', sys.executable, '-c',
        'import sys,json; print("diagnostic", file=sys.stderr); '
        'print(json.dumps({"status":"SUCCEEDED", "stages":[]}, indent=2))']
    assert main(args) == 0
    directory = tmp_path / 'baseline-3h' / 'repeat-1'
    assert 'diagnostic' in (directory / 'execution.log').read_text()
    assert json.loads((directory / 'result.json').read_text())['completed_dates'] == 2


def test_zero_exit_without_report_is_not_verified(tmp_path, monkeypatch):
    monkeypatch.setattr('gold_profile.experiment.subprocess.run',
                        lambda *a, **k: SimpleNamespace(returncode=0))
    assert main(options(tmp_path)) == 1
    result = json.loads((tmp_path / 'baseline-3h' / 'repeat-1' / 'result.json').read_text())
    assert result['status'] == 'UNVERIFIED'

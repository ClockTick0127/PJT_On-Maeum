"""Persistent, container-local date-range experiment recorder (no Docker socket)."""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time
from uuid import uuid4


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_suffix('.pending')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(path)


def parse_report(output):
    decoder = json.JSONDecoder()
    candidate = None
    consumed = 0
    for match in re.finditer(r'^\s*\{', output, re.MULTILINE):
        if match.start() < consumed:
            continue
        try:
            tail = output[match.start():]
            stripped = tail.lstrip()
            value, end = decoder.raw_decode(stripped)
            consumed = match.start() + len(tail) - len(stripped) + end
            if isinstance(value, dict) and 'status' in value:
                candidate = value
        except ValueError:
            pass
    return candidate


def identifier(value):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', value):
        raise argparse.ArgumentTypeError('ID must be 1..80 ASCII letters, digits, hyphens or underscores')
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--from', dest='start', type=date.fromisoformat, required=True)
    parser.add_argument('--to', dest='end', type=date.fromisoformat, required=True)
    parser.add_argument('--experiment-id', type=identifier, required=True)
    parser.add_argument('--execution-id', type=identifier)
    parser.add_argument('--root', type=Path, default=Path(os.getenv('EXPERIMENTS_ROOT', '/app/experiments')))
    parser.add_argument('--dataset-id', required=True)
    parser.add_argument('--households', type=int, required=True)
    parser.add_argument('--sampling-seconds', type=float, required=True)
    parser.add_argument('--workers', type=int, required=True, help='declared worker count, not automatically verified')
    parser.add_argument('--run-mode', choices=['INITIAL_BUILD', 'DAILY_UPDATE', 'RECOMPUTE', 'REUSE'], required=True)
    parser.add_argument('--git-commit', default=os.getenv('EXPERIMENT_GIT_COMMIT'))
    parser.add_argument('--image', default=os.getenv('EXPERIMENT_IMAGE'))
    parser.add_argument('--keep-going', action='store_true')
    parser.add_argument('--command', nargs=argparse.REMAINDER,
                        help='argument array with {date}; default: gold-profile daily --as-of {date} --no-publish')
    args = parser.parse_args(argv)
    if args.end < args.start or min(args.households, args.sampling_seconds, args.workers) <= 0:
        parser.error('date range and positive resource/input counts are required')
    command = args.command or ['gold-profile', 'daily', '--as-of', '{date}', '--no-publish']
    execution = args.execution_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + uuid4().hex[:12]
    directory = args.root / args.experiment_id / execution
    directory.mkdir(parents=True, exist_ok=False)  # Never overwrite a previous measurement.
    config = {
        'schema_version': 1, 'experiment_id': args.experiment_id, 'execution_id': execution,
        'created_at': utc_now(), 'period_start': args.start.isoformat(), 'period_end': args.end.isoformat(),
        'period_days': (args.end - args.start).days + 1, 'dataset_id': args.dataset_id,
        'households': args.households, 'sampling_interval_seconds': args.sampling_seconds,
        'declared_workers': args.workers, 'requested_run_mode': args.run_mode,
        'git_commit': args.git_commit, 'image': args.image, 'command_template': command,
        # Only non-secret execution settings; never dump the process environment.
        'spark_settings': {key: os.getenv(key) for key in (
            'SPARK_MASTER', 'SPARK_SHUFFLE_PARTITIONS', 'SPARK_EXECUTOR_CORES',
            'SPARK_EXECUTOR_MEMORY', 'SPARK_DRIVER_MEMORY')},
        'keep_going': args.keep_going,
    }
    write_json(directory / 'config.json', config)
    result = {'experiment_id': args.experiment_id, 'execution_id': execution,
              'started_at': utc_now(), 'status': 'RUNNING', 'runs': [],
              'correctness_verified': False}
    write_json(directory / 'result.json', result)
    started = time.perf_counter()
    print(f'experiment output: {directory}', flush=True)
    try:
        with (directory / 'run-dates.csv').open('w', newline='', encoding='utf-8') as csv_file, \
                (directory / 'execution.log').open('w+', encoding='utf-8') as log:
            writer = csv.DictWriter(csv_file, fieldnames=[
                'experiment_id', 'execution_id', 'order', 'date', 'exit_code', 'status',
                'seconds', 'stages_json', 'report_id', 'reused_run_ids_json'])
            writer.writeheader()
            csv_file.flush()
            for offset in range(config['period_days']):
                day = (args.start + timedelta(days=offset)).isoformat()
                actual = [part.replace('{date}', day) for part in command]
                log.write(f'\n[{utc_now()}] date={day}\n')
                log.flush()
                position = log.tell()
                tick = time.perf_counter()
                completed = subprocess.run(actual, stdout=log, stderr=subprocess.STDOUT, check=False)
                seconds = time.perf_counter() - tick
                log.flush()
                log.seek(position)
                report = parse_report(log.read())
                log.seek(0, 2)
                status = (report or {}).get('status', 'NO_REPORT')
                stages = (report or {}).get('stages', [])
                reused = [stage.get('detail', {}).get('reused_run_id') for stage in stages
                          if stage.get('detail', {}).get('reused_run_id')]
                run = {'date': day, 'exit_code': completed.returncode, 'status': status,
                       'seconds': round(seconds, 3), 'report': report}
                result['runs'].append(run)
                writer.writerow({
                    'experiment_id': args.experiment_id, 'execution_id': execution,
                    'order': offset + 1, 'date': day, 'exit_code': completed.returncode,
                    'status': status, 'seconds': run['seconds'],
                    'stages_json': json.dumps(stages, ensure_ascii=False),
                    'report_id': (report or {}).get('report_id'),
                    'reused_run_ids_json': json.dumps(reused),
                })
                csv_file.flush()
                write_json(directory / 'result.json', result)
                print(f'{day} exit={completed.returncode} status={status} seconds={seconds:.3f}', flush=True)
                if completed.returncode not in (0, 10, 11, 12) and not args.keep_going:
                    break
        codes = [run['exit_code'] for run in result['runs']]
        if any(code not in (0, 10, 11, 12) for code in codes):
            result['status'], exit_code = 'FAILED', 1
        elif any(run['report'] is None for run in result['runs']):
            result['status'], exit_code = 'UNVERIFIED', 1
        elif 10 in codes:
            result['status'], exit_code = 'INPUT_INCOMPLETE', 10
        elif 11 in codes:
            result['status'], exit_code = 'SKIPPED', 11
        elif 12 in codes:
            result['status'], exit_code = 'PUBLISH_PENDING', 12
        else:
            result['status'], exit_code = 'SUCCEEDED', 0
        result['exit_code'] = exit_code
        return exit_code
    except BaseException as error:
        result['status'] = 'INTERRUPTED' if isinstance(error, KeyboardInterrupt) else 'FAILED'
        result['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['finished_at'] = utc_now()
        result['total_seconds'] = round(time.perf_counter() - started, 3)
        result['completed_dates'] = len(result['runs'])
        result['exit_counts'] = {str(code): sum(run['exit_code'] == code for run in result['runs'])
                                 for code in sorted({run['exit_code'] for run in result['runs']})}
        write_json(directory / 'result.json', result)


if __name__ == '__main__':
    raise SystemExit(main())

"""Offline JSON/JSONL contract validator. Does not connect to any service."""
import argparse
import json
from pathlib import Path
import sys

from pydantic import ValidationError
from models import MODELS, Snapshot


def validate(kind, value, *, broker=False):
    # Reject JSON non-finite literals even for ignored legacy input fields.
    def reject(value):
        raise ValueError(f'Non-standard JSON constant: {value}')
    payload = json.loads(value, parse_constant=reject)
    model = MODELS[kind].model_validate_json(json.dumps(payload, allow_nan=False))
    if broker and isinstance(model, Snapshot) and model.source is None:
        raise ValueError('broker snapshot requires source topic/partition/offset')
    return model


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=MODELS)
    parser.add_argument('file', type=Path, nargs='?')
    parser.add_argument('--jsonl', action='store_true')
    parser.add_argument('--broker', action='store_true')
    parser.add_argument('--schema', action='store_true', help='Print structural JSON Schema; semantic rules still require this validator')
    args = parser.parse_args()
    if args.schema:
        print(json.dumps(MODELS[args.kind].model_json_schema(), indent=2))
        return 0
    if args.file is None:
        parser.error('file is required unless --schema is used')
    count = 0
    try:
        with args.file.open(encoding='utf-8-sig') as handle:
            records = enumerate(handle, 1) if args.jsonl else [(1, handle.read())]
            for line, value in records:
                if args.jsonl and not value.strip():
                    continue
                try:
                    validate(args.kind, value, broker=args.broker)
                except (ValidationError, ValueError) as error:
                    # Do not echo full measurement or credential-bearing legacy input.
                    if isinstance(error, ValidationError):
                        detail = [{'location': e['loc'], 'type': e['type']} for e in error.errors()]
                    else:
                        detail = type(error).__name__
                    print(json.dumps({'valid': False, 'line': line, 'errors': detail}), file=sys.stderr)
                    return 1
                count += 1
        if count == 0:
            raise ValueError('empty input')
    except (OSError, UnicodeError, ValueError) as error:
        print(json.dumps({'valid': False, 'error': type(error).__name__}), file=sys.stderr)
        return 1
    print(json.dumps({'valid': True, 'kind': args.kind, 'records': count}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

"""Verify the minimal runtime or publisher image assets against the frozen lock."""
import argparse
import json
from pathlib import Path
from realtime_analysis.real_predictor import (
    MODEL_DIRECTORY,
    RealtimeModelPredictor,
    load_profile,
    verified_asset,
)
from realtime_analysis.predictor import APPLIANCE_ORDER


def verify(root, panels=False):
    files = set()
    for name in APPLIANCE_ORDER:
        p = load_profile(name)
        fields = [('panel_relative', 'panel_sha256')] if panels else [
            ('local_checkpoint_relative', 'checkpoint_sha256'), ('normalization_relative', 'normalization_sha256')]
        for path, digest in fields:
            verified_asset(Path(root), p[path], p[digest]); files.add(p[path])
        verified_asset(MODEL_DIRECTORY, 'models.py', p['model_code_sha256'])
    return {'status': 'MATCH', 'profiles': 6, 'asset_files': len(files), 'panels': panels}


def verify_forward(root, device="cpu", dtype="float32"):
    """Load every checkpoint and execute one finite six-appliance forward."""

    import torch

    torch.set_num_threads(1)
    predictor = RealtimeModelPredictor(root, device=device, dtype=dtype)
    predictions = predictor.predict([(400.0, 100.0, 0.8, 2.0)] * 255)
    if len(predictions) != 6:
        raise ValueError("Expected six predictions from realtime model smoke test")
    return {
        "forward": "PASS",
        "outputs": len(predictions),
        "model_version": predictor.model_version,
        "device": device,
        "dtype": dtype,
    }


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--asset-root', type=Path, required=True)
    parser.add_argument('--panels', action='store_true')
    parser.add_argument('--forward', action='store_true')
    parser.add_argument('--device', default='cpu')
    parser.add_argument('--dtype', choices=('float32', 'bfloat16'), default='float32')
    args=parser.parse_args()
    if args.panels and args.forward:
        parser.error('--panels and --forward cannot be used together')
    report = verify(args.asset_root, args.panels)
    if args.forward:
        report.update(verify_forward(args.asset_root, args.device, args.dtype))
    print(json.dumps(report))


if __name__ == '__main__': main()

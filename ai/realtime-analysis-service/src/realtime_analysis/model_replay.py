"""Run a frozen panel through an actual local model. No broker or reference scores."""

import argparse
from collections import deque
import csv
from dataclasses import asdict
import json
from pathlib import Path
import time

from realtime_analysis.predictor import APPLIANCE_ORDER
from realtime_analysis.real_predictor import SelectedScenePredictor, verified_asset
from realtime_analysis.selected_scene import SelectedSceneDecoder

FEATURES = ("active_power", "reactive_power", "power_factor", "current")


def replay(predictor: SelectedScenePredictor, panel: Path):
    """Yield actual scores; context is the reference inference-validity mask."""
    window = deque(maxlen=255)
    decoder = SelectedSceneDecoder(**{key: predictor.profile["threshold"][key]
                                     for key in ("on", "off", "confirm")})
    previous_index = None
    target = predictor.profile["appliance_type"].upper()
    with panel.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            index = int(row["source_index"])
            if previous_index is not None and index <= previous_index:
                raise ValueError("Duplicate or out-of-order source_index")
            if previous_index is not None and index != previous_index + 1:
                raise ValueError("Panel must include missing seconds explicitly")
            previous_index = index
            if row["valid"] not in {"0", "1"} or row["context"] not in {"0", "1"}:
                raise ValueError("Expected binary valid/context flags")
            if row["valid"] == "0":
                window.append(predictor.missing_feature_row)
            else:
                window.append(tuple(float(row[key]) for key in FEATURES))
            ready = len(window) == 255 and row["context"] == "1"
            score = predictor.predict(list(window))[0].probability if ready else None
            decision = decoder.step(score)
            yield {
                "source_index": index, "profile_id": predictor.profile["profile_id"],
                "context": row["context"] == "1", "ready": ready,
                "in_evaluation_clip": (predictor.profile["source_clip_start_index"] <= index
                                       < predictor.profile["source_clip_end_exclusive"]),
                "on_score": score, **asdict(decision),
                "appliances": [{"appliance_type": name,
                                "inferred": name == target and ready,
                                "probability": score if name == target else None,
                                "state": decision.state if name == target else "UNKNOWN"}
                               for name in APPLIANCE_ORDER],
            }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset-root", type=Path, required=True)
    parser.add_argument("--appliance", default="kettle")
    parser.add_argument("--device", default="cpu")
    parser.add_argument("--dtype", choices=("float32", "bfloat16"), default="float32")
    parser.add_argument("--output", type=Path, required=True, help="New evidence directory")
    parser.add_argument("--threads", type=int, default=1)
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("--threads must be positive")
    import torch
    torch.set_num_threads(args.threads)
    predictor = SelectedScenePredictor(args.asset_root, args.appliance,
                                       device=args.device, dtype=args.dtype)
    panel = verified_asset(args.asset_root, predictor.profile["panel_relative"],
                           predictor.profile["panel_sha256"])
    args.output.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    rows = ready = 0
    with (args.output / "scores.jsonl").open("x", encoding="utf-8") as handle:
        for result in replay(predictor, panel):
            handle.write(json.dumps(result, allow_nan=False) + "\n")
            rows += 1
            ready += result["ready"]
    report = {**predictor.runtime, "source_rows": rows, "ready_scores": ready,
              "elapsed_seconds": time.perf_counter() - started,
              "model_forward_executed": True, "reference_scores_read": False,
              "backend_started": False}
    if (rows != predictor.profile["expected_source_rows"]
            or ready != predictor.profile["expected_ready_scores"]):
        raise ValueError(f"Frozen panel row count mismatch: {rows}/{ready}")
    (args.output / "runtime.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

"""Replay raw frozen features via MQTT, or verify the durable pipeline locally."""
import argparse
import asyncio
import csv
from datetime import datetime, timedelta
import json
import os
import sys
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from realtime_analysis.real_predictor import load_profile, verified_asset
from realtime_analysis.scene_pipeline import FEATURES, SceneMeasurement


def measurements(root, appliance, household, run_id, start):
    if start.tzinfo is None or start.utcoffset() is None:
        raise ValueError("--start-time must include timezone; reuse it on retries")
    profile = load_profile(appliance)
    panel = verified_asset(root, profile["panel_relative"], profile["panel_sha256"])
    with panel.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            index = int(row["source_index"])
            if row["valid"] not in {"0", "1"} or row["context"] not in {"0", "1"}:
                raise ValueError("Expected boolean panel masks")
            valid = row["valid"] == "1"
            yield SceneMeasurement(message_id=uuid5(NAMESPACE_URL, json.dumps([household, run_id, index])),
                household_id=household, device_id="main", run_id=run_id,
                profile_id=profile["profile_id"], source_index=index, valid=valid,
                context=row["context"] == "1",
                measured_at=start + timedelta(seconds=index - profile["source_prefix_start_index"]),
                **{key: float(row[key]) if valid else None for key in FEATURES})


async def publish(args):
    import aiomqtt
    topic = f"v1/power/demo/{args.household}/main"
    async with aiomqtt.Client(hostname=args.mqtt_host, port=args.mqtt_port,
            username=os.getenv("MQTT_USER") or None, password=os.getenv("MQTT_PASS") or None) as client:
        count = 0
        for measurement in measurements(args.asset_root, args.appliance, args.household, args.run_id, args.start_time):
            await client.publish(topic, measurement.model_dump_json(), qos=1)
            count += 1
            await asyncio.sleep(args.interval)
    print(json.dumps({"mqtt_published": count, "topic": topic, "model_forward_executed": False}))


def storage_smoke(args):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from realtime_analysis.real_predictor import SelectedScenePredictor
    from realtime_analysis.scene_pipeline import SceneEvidence, SceneHandler, SceneRepository
    import torch
    torch.set_num_threads(1)
    args.output.mkdir(parents=True, exist_ok=False)
    engine = create_engine("sqlite:///" + (args.output / "analysis.sqlite").resolve().as_posix())
    SceneEvidence.__table__.create(engine)
    predictor = SelectedScenePredictor(args.asset_root, args.appliance)
    with (args.output / "snapshots.jsonl").open("x", encoding="utf-8") as handle:
        class FileSink:
            def publish(self, payload):
                handle.write(json.dumps(payload, allow_nan=False) + "\n")
        handler = SceneHandler(predictor, SceneRepository(sessionmaker(engine)), FileSink(), args.run_id)
        for measurement in measurements(args.asset_root, args.appliance, args.household, args.run_id, args.start_time):
            handler(measurement)
    (args.output / "runtime.json").write_text(json.dumps({**predictor.runtime,
        "model_forward_executed": True, "analysis_storage": "sqlite",
        "actual_mqtt_kafka_executed": False}, indent=2), encoding="utf-8")
    engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("publish", "storage-smoke"))
    parser.add_argument("--asset-root", required=True, type=Path)
    parser.add_argument("--appliance", default="kettle")
    parser.add_argument("--household", default="r3-kettle")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--start-time", required=True, type=datetime.fromisoformat)
    parser.add_argument("--mqtt-host", default="127.0.0.1")
    parser.add_argument("--mqtt-port", default=18884, type=int)
    parser.add_argument("--interval", default=1., type=float)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.interval < 0:
        parser.error("--interval must be non-negative")
    if any(character in args.household for character in "/+#"):
        parser.error("--household must be one MQTT topic segment")
    if args.mode == "publish":
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(publish(args))
    else:
        if args.output is None:
            parser.error("storage-smoke requires --output")
        storage_smoke(args)


if __name__ == "__main__":
    main()

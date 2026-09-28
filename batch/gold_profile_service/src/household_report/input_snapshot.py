from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json


REQUIRED_SOURCES = {"targets", "usage", "baseline", "statistics", "assessments"}


@dataclass(frozen=True)
class ReportInput:
    document: dict
    snapshot_id: str

    @property
    def report_id(self) -> str:
        return self.snapshot_id

    @property
    def start(self) -> date:
        return date.fromisoformat(self.document["period_start"])

    @property
    def end(self) -> date:
        return date.fromisoformat(self.document["period_end"])

    @property
    def gold_run_id(self) -> str:
        return self.document["gold_run_id"]

    @property
    def rule_version(self) -> str:
        return self.document["report_rule_version"]

    def paths(self, name: str) -> list[str]:
        return list(self.document["sources"][name]["paths"])


def load_report_input(storage, path: str) -> ReportInput:
    document = deepcopy(json.loads(storage.read_bytes(path)))
    if document.get("schema_version") != 1:
        raise ValueError("unsupported report input schema")

    start = date.fromisoformat(document["period_start"])
    end = date.fromisoformat(document["period_end"])
    if end < start:
        raise ValueError("period_end must not precede period_start")

    cutoff = datetime.fromisoformat(document["assessment_cutoff"])
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("assessment_cutoff must include timezone")
    if document.get("assessment_delivery_complete") is not True:
        raise RuntimeError("assessment history delivery is not complete")
    if not document.get("gold_run_id"):
        raise ValueError("gold_run_id is required")
    if not document.get("report_rule_version"):
        raise ValueError("report_rule_version is required")

    sources = document.get("sources")
    if not isinstance(sources, dict) or set(sources) != REQUIRED_SOURCES:
        raise ValueError("unexpected report source set")
    for name, source in sources.items():
        if not isinstance(source, dict) or not source.get("version_id"):
            raise ValueError(f"missing source identity: {name}")
        paths = source.get("paths")
        if not isinstance(paths, list) or not paths or not all(
            isinstance(item, str) and item for item in paths
        ):
            raise ValueError(f"missing source identity: {name}")
        source["paths"] = sorted(set(paths))
        for source_path in source["paths"]:
            if not storage.exists(source_path):
                raise FileNotFoundError(source_path)

    canonical = json.dumps(
        document, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode()
    return ReportInput(document, hashlib.sha256(canonical).hexdigest())


def read_source(spark, storage, inputs: ReportInput, name: str):
    if name not in REQUIRED_SOURCES:
        raise ValueError(f"unknown report source: {name}")
    return spark.read.parquet(*[storage.uri(path) for path in inputs.paths(name)])

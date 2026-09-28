from __future__ import annotations

from datetime import date, datetime, timezone
from io import StringIO
import json
from unittest.mock import MagicMock, Mock

from aggregation_service.retention import BronzeRetentionService, RetentionConfig


class FakeHdfsClient:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.directories = {
            "/nilm/bronze/power",
            "/nilm/bronze/power/ingest_date=2026-08-31",
            "/nilm/manifests/job=bronze-loader/date=2026-08-31",
            "/nilm/manifests/job=compaction/date=2026-08-31/_SUCCESS",
        }
        manifest = {"business_dates": ["2026-08-31", "2026-09-01"]}
        self.files[
            "/nilm/manifests/job=bronze-loader/date=2026-08-31/manifest-0.json"
        ] = json.dumps(manifest).encode()
        self.deleted: list[tuple[str, bool]] = []

    def list(self, path: str):
        prefix = path.rstrip("/") + "/"
        names = set()
        for item in self.directories | set(self.files):
            if item.startswith(prefix):
                remainder = item.removeprefix(prefix)
                if remainder:
                    names.add(remainder.split("/", 1)[0])
        return sorted(names)

    def content(self, path: str):
        return {"spaceConsumed": 1024, "fileCount": 2}

    def status(self, path: str, strict: bool = True):
        if path in self.files:
            return {"length": len(self.files[path])}
        if path in self.directories:
            return {"length": 0}
        if strict:
            raise FileNotFoundError(path)
        return None

    def read(self, path: str, encoding: str):
        return StringIO(self.files[path].decode(encoding))

    def makedirs(self, path: str):
        self.directories.add(path)

    def write(self, path: str, data: bytes, overwrite: bool):
        self.files[path] = data

    def rename(self, source: str, destination: str):
        if source in self.files:
            self.files[destination] = self.files.pop(source)
            return None
        if source in self.directories:
            self.directories.remove(source)
            self.directories.add(destination)
            return None
        raise FileNotFoundError(source)

    def delete(self, path: str, recursive: bool = False):
        self.deleted.append((path, recursive))
        removed = self.files.pop(path, None) is not None
        if path in self.directories:
            self.directories.remove(path)
            removed = True
        return removed


def config(*, apply: bool = False) -> RetentionConfig:
    return RetentionConfig(
        bronze_base="/nilm/bronze/power",
        bronze_manifest_base="/nilm/manifests/job=bronze-loader",
        manifest_base="/nilm/manifests/job=retention",
        retention_days=14,
        grace_days=3,
        max_delete_bytes=5 * 1024**3,
        max_delete_dates=1,
        apply=apply,
    )


def run_repository() -> Mock:
    repository = MagicMock()
    repository.has_succeeded.side_effect = (
        lambda pipeline, target: pipeline == "daily-aggregation"
    )
    repository.run.side_effect = (
        lambda pipeline, target, operation, dry_run=False: operation()
    )
    return repository


def test_dry_run_writes_plan_without_deleting_bronze() -> None:
    client = FakeHdfsClient()
    runs = run_repository()
    service = BronzeRetentionService(client, runs, config())

    processed = service.run(datetime(2026, 9, 18, 12, tzinfo=timezone.utc))

    assert processed == 1
    assert "/nilm/bronze/power/ingest_date=2026-08-31" in client.directories
    assert not any(recursive for _, recursive in client.deleted)
    plan_paths = [path for path in client.files if path.endswith("/plan.json")]
    assert len(plan_paths) == 1
    plan = json.loads(client.files[plan_paths[0]])
    assert plan["planned_bytes"] == 1024
    assert plan["related_business_dates"] == ["2026-08-31", "2026-09-01"]


def test_missing_compaction_marker_is_skipped() -> None:
    client = FakeHdfsClient()
    client.directories.remove(
        "/nilm/manifests/job=compaction/date=2026-08-31/_SUCCESS"
    )
    runs = run_repository()
    service = BronzeRetentionService(client, runs, config())

    processed = service.run(datetime(2026, 9, 18, 12, tzinfo=timezone.utc))

    assert processed == 0
    details = runs.record_skipped.call_args.args[2]
    assert "COMPACTION_NOT_SUCCEEDED" in details["failed"]


def test_apply_stages_and_recursively_deletes_target() -> None:
    client = FakeHdfsClient()
    runs = run_repository()
    service = BronzeRetentionService(client, runs, config(apply=True))

    processed = service.run(datetime(2026, 9, 18, 12, tzinfo=timezone.utc))

    assert processed == 1
    assert "/nilm/bronze/power/ingest_date=2026-08-31" not in client.directories
    assert any(recursive for _, recursive in client.deleted)

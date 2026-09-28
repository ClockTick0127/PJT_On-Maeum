"""대상 날짜의 입력을 확정한다.

수집일 폴더만 보고 전날 파일을 고르면 늦게 도착한 데이터를 놓친다. Bronze 적재기가
남긴 manifest의 ``business_dates``에 대상 날짜가 들어 있는 파일만 고른다.

manifest는 저장 완료의 증거이지 그날의 모든 센서 데이터가 도착했다는 증거가 아니다.
그래서 "무엇을 읽을지"(스냅샷)와 "지금 확정해도 되는지"(준비 상태)를 따로 계산한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import logging
import re

from power_silver.constants import SECONDS_PER_DAY
from power_silver.storage import LakeStorage
from power_silver.targets import day_start_utc


logger = logging.getLogger(__name__)

DATE_DIR = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")


class InputChanged(RuntimeError):
    """확정한 입력 파일이 실행 도중 바뀌었다."""


@dataclass(frozen=True)
class InputFile:
    path: str
    length: int
    modification_time: int


@dataclass(frozen=True)
class BronzeManifest:
    path: str
    topic: str
    partition: int
    start_offset: int | None
    end_offset: int | None
    ok_count: int
    quarantine_count: int
    min_measured_at: datetime | None
    max_measured_at: datetime | None
    business_dates: tuple[str, ...]
    committed_at: datetime | None
    files: tuple[str, ...]


@dataclass(frozen=True)
class InputSnapshot:
    target_date: date
    snapshot_id: str
    files: tuple[InputFile, ...]
    manifest_paths: tuple[str, ...]
    partitions: tuple[int, ...]
    bronze_ok_count: int
    bronze_quarantine_count: int
    ready: bool
    wait_reason: str | None
    scanned_manifest_count: int

    @property
    def total_bytes(self) -> int:
        return sum(item.length for item in self.files)

    def as_dict(self) -> dict:
        return {
            "target_date": self.target_date.isoformat(),
            "input_snapshot_id": self.snapshot_id,
            "file_count": len(self.files),
            "total_bytes": self.total_bytes,
            "files": [
                {
                    "path": item.path,
                    "length": item.length,
                    "modification_time": item.modification_time,
                }
                for item in self.files
            ],
            "manifest_paths": list(self.manifest_paths),
            "partitions": list(self.partitions),
            "bronze_ok_count": self.bronze_ok_count,
            "bronze_quarantine_count": self.bronze_quarantine_count,
            "scanned_manifest_count": self.scanned_manifest_count,
            "ready": self.ready,
            "wait_reason": self.wait_reason,
        }


def _parse_instant(value) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_manifest(storage: LakeStorage, path: str) -> BronzeManifest | None:
    try:
        payload = json.loads(storage.read_bytes(path))
    except (ValueError, OSError):
        logger.warning("Unreadable bronze manifest skipped: %s", path)
        return None
    if not isinstance(payload, dict):
        logger.warning("Bronze manifest is not an object: %s", path)
        return None
    files = payload.get("files") or []
    return BronzeManifest(
        path=path,
        topic=str(payload.get("topic", "")),
        partition=int(payload.get("partition", -1)),
        start_offset=payload.get("start_offset"),
        end_offset=payload.get("end_offset"),
        ok_count=int(payload.get("ok_count", 0)),
        quarantine_count=int(payload.get("quarantine_count", 0)),
        min_measured_at=_parse_instant(payload.get("min_measured_at")),
        max_measured_at=_parse_instant(payload.get("max_measured_at")),
        business_dates=tuple(str(value) for value in payload.get("business_dates", [])),
        committed_at=_parse_instant(payload.get("committed_at")),
        files=tuple(str(value) for value in files if isinstance(value, str)),
    )


def scan_manifests(
    storage: LakeStorage,
    manifest_base: str,
    target_date: date,
    *,
    scan_back_days: int,
    now: datetime,
) -> list[BronzeManifest]:
    """대상 날짜 이후(및 시계 오차 대비 직전 며칠)의 수집일 폴더를 모두 읽는다."""

    oldest = target_date - timedelta(days=scan_back_days)
    newest = now.date() + timedelta(days=1)
    manifests: list[BronzeManifest] = []
    for name in storage.list(manifest_base):
        matched = DATE_DIR.match(name)
        if not matched:
            continue
        try:
            ingest_date = date.fromisoformat(matched.group(1))
        except ValueError:
            continue
        if not oldest <= ingest_date <= newest:
            continue
        directory = f"{manifest_base}/{name}"
        for child in storage.list(directory):
            if not child.endswith(".json"):
                continue
            manifest = read_manifest(storage, f"{directory}/{child}")
            if manifest is not None:
                manifests.append(manifest)
    return manifests


def _readiness(
    manifests: list[BronzeManifest],
    *,
    target_date: date,
    business_utc_offset_seconds: int,
    now: datetime,
    grace_seconds: int,
    deadline_seconds: int,
) -> tuple[bool, str | None]:
    """지금 이 날짜를 확정해도 되는지 판단한다.

    적재기는 Kafka 파티션마다 따로 플러시한다. 한 파티션이 밀려 있으면 그 파티션의
    데이터가 아직 오지 않은 것이므로, 모든 파티션이 날짜 경계를 넘겼다는 증거가 있어야
    한다. 증거는 두 가지다: 대상일 종료 이후 시각의 측정값을 이미 실었거나(측정 진행),
    종료 후 유예시간이 지난 뒤에도 계속 플러시하고 있거나(적재기 생존).
    """

    day_end = day_start_utc(target_date, business_utc_offset_seconds) + timedelta(
        seconds=SECONDS_PER_DAY
    )
    if now >= day_end + timedelta(seconds=deadline_seconds):
        return True, None
    if now < day_end:
        return False, "TARGET_DATE_NOT_FINISHED"
    if not manifests:
        return False, "NO_BRONZE_MANIFEST"

    lagging: list[int] = []
    for partition in sorted({manifest.partition for manifest in manifests}):
        partition_manifests = [
            manifest for manifest in manifests if manifest.partition == partition
        ]
        measured = [
            manifest.max_measured_at
            for manifest in partition_manifests
            if manifest.max_measured_at is not None
        ]
        committed = [
            manifest.committed_at
            for manifest in partition_manifests
            if manifest.committed_at is not None
        ]
        crossed = bool(measured) and max(measured) >= day_end
        alive = bool(committed) and max(committed) >= day_end + timedelta(
            seconds=grace_seconds
        )
        if not (crossed or alive):
            lagging.append(partition)
    if lagging:
        return False, f"PARTITIONS_BEHIND:{','.join(str(item) for item in lagging)}"
    # 모든 파티션이 경계를 넘겼는데 대상 날짜 파일이 하나도 없으면, 그날 유효 측정이
    # 없었다는 뜻이다. 기다리지 않고 관측 없음으로 확정한다.
    return True, None


def build_snapshot(
    storage: LakeStorage,
    settings,
    target_date: date,
    *,
    now: datetime | None = None,
) -> InputSnapshot:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    manifests = scan_manifests(
        storage,
        settings.bronze_manifest_base,
        target_date,
        scan_back_days=settings.manifest_scan_back_days,
        now=now,
    )
    wanted = target_date.isoformat()
    selected = [
        manifest for manifest in manifests if wanted in manifest.business_dates
    ]

    prefix = settings.bronze_base.rstrip("/") + "/"
    paths: set[str] = set()
    for manifest in selected:
        paths.update(path for path in manifest.files if path.startswith(prefix))

    files: list[InputFile] = []
    for path in sorted(paths):
        if not storage.exists(path):
            raise InputChanged(f"manifest lists a missing file: {path}")
        status = storage.status(path)
        files.append(InputFile(path, status.length, status.modification_time))

    ready, wait_reason = _readiness(
        manifests,
        target_date=target_date,
        business_utc_offset_seconds=settings.business_utc_offset_seconds,
        now=now,
        grace_seconds=settings.input_ready_grace_seconds,
        deadline_seconds=settings.input_wait_deadline_seconds,
    )

    return InputSnapshot(
        target_date=target_date,
        snapshot_id=snapshot_id(target_date, files),
        files=tuple(files),
        manifest_paths=tuple(sorted(manifest.path for manifest in selected)),
        partitions=tuple(sorted({manifest.partition for manifest in selected})),
        bronze_ok_count=sum(manifest.ok_count for manifest in selected),
        bronze_quarantine_count=sum(
            manifest.quarantine_count for manifest in selected
        ),
        ready=ready,
        wait_reason=wait_reason,
        scanned_manifest_count=len(manifests),
    )


def snapshot_id(target_date: date, files: list[InputFile]) -> str:
    """입력 목록의 지문.

    경로만 담으면 적재기가 같은 경로를 교체했을 때 달라진 입력을 같은 입력으로 본다.
    크기와 수정 시각을 함께 넣어 교체를 감지한다.
    """

    digest = hashlib.sha256()
    digest.update(target_date.isoformat().encode())
    for item in sorted(files, key=lambda value: value.path):
        digest.update(
            f"\n{item.path}|{item.length}|{item.modification_time}".encode()
        )
    return digest.hexdigest()[:32]


def verify_unchanged(storage: LakeStorage, snapshot: InputSnapshot) -> None:
    """실행 전후로 입력 파일이 그대로인지 확인한다."""

    for item in snapshot.files:
        if not storage.exists(item.path):
            raise InputChanged(f"input file disappeared: {item.path}")
        status = storage.status(item.path)
        if status.length != item.length or status.modification_time != item.modification_time:
            raise InputChanged(
                f"input file changed during the run: {item.path} "
                f"({item.length}@{item.modification_time} -> "
                f"{status.length}@{status.modification_time})"
            )

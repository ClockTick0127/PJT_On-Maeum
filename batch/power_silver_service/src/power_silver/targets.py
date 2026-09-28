"""관측 대상 가구·계측기 설정 스냅샷.

원본에 등장한 가구만 집계하면 하루 종일 데이터가 없는 가구를 발견할 수 없다.
관측일 결과는 이 설정을 기준으로 만들고, 측정값을 왼쪽 조인으로 붙인다.

초기 정책은 가구별 대표 계측기 1개다. 여러 계측기의 건수를 합산하면 수집률이
부풀려지므로, 같은 가구의 적용 구간이 겹치면 설정 자체를 오류로 본다. 계측기 교체는
구간을 나눠 표현한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

SECONDS_PER_DAY = 86_400


class InvalidObservationTargets(ValueError):
    pass


@dataclass(frozen=True)
class ObservationTarget:
    household_id: str
    device_id: str
    effective_from: datetime | None
    effective_to: datetime | None
    sampling_interval_seconds: int
    observation_enabled: bool

    def overlaps(self, start: datetime, end: datetime) -> bool:
        if self.effective_from is not None and self.effective_from >= end:
            return False
        if self.effective_to is not None and self.effective_to <= start:
            return False
        return True


@dataclass(frozen=True)
class DaySegment:
    """대상 날짜 안에서 한 계측기가 관측 대상인 구간(로컬 하루 초 좌표)."""

    household_id: str
    device_id: str
    segment_index: int
    start_second: int
    end_second: int
    sampling_interval_seconds: int

    @property
    def expected_sample_count(self) -> int:
        span = self.end_second - self.start_second
        interval = self.sampling_interval_seconds
        return (span + interval - 1) // interval


@dataclass(frozen=True)
class ObservationTargets:
    config_version: str
    targets: tuple[ObservationTarget, ...]

    def as_dict(self) -> dict:
        """manifest에 그대로 실을 설정 스냅샷.

        설정 파일은 배포와 함께 바뀐다. 레이크만 보고 과거 실행을 재현하려면 그때 쓴
        가구·기기 매핑과 적용 기간이 결과 옆에 남아 있어야 한다.
        """

        return {
            "config_version": self.config_version,
            "fingerprint": self.fingerprint,
            "targets": [
                {
                    "household_id": target.household_id,
                    "device_id": target.device_id,
                    "effective_from": target.effective_from.isoformat()
                    if target.effective_from
                    else None,
                    "effective_to": target.effective_to.isoformat()
                    if target.effective_to
                    else None,
                    "sampling_interval_seconds": target.sampling_interval_seconds,
                    "observation_enabled": target.observation_enabled,
                }
                for target in self.targets
            ],
        }

    @property
    def fingerprint(self) -> str:
        """설정 내용 해시. 같은 버전 이름으로 내용만 바뀐 경우를 잡는다."""

        payload = [
            [
                target.household_id,
                target.device_id,
                target.effective_from.isoformat() if target.effective_from else None,
                target.effective_to.isoformat() if target.effective_to else None,
                target.sampling_interval_seconds,
                target.observation_enabled,
            ]
            for target in self.targets
        ]
        encoded = json.dumps(
            {"config_version": self.config_version, "targets": payload},
            ensure_ascii=False,
            sort_keys=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()[:32]

    def segments_for_date(
        self,
        target_date: date,
        business_utc_offset_seconds: int,
    ) -> tuple[DaySegment, ...]:
        """대상 날짜와 겹치는 활성 구간을 로컬 하루 초 좌표로 자른다."""

        day_start = day_start_utc(target_date, business_utc_offset_seconds)
        day_end = day_start + timedelta(seconds=SECONDS_PER_DAY)
        segments: list[DaySegment] = []
        for target in sorted(
            self.targets,
            key=lambda item: (
                item.household_id,
                item.effective_from or datetime.min.replace(tzinfo=timezone.utc),
            ),
        ):
            if not target.observation_enabled or not target.overlaps(day_start, day_end):
                continue
            start = max(target.effective_from or day_start, day_start)
            end = min(target.effective_to or day_end, day_end)
            if end <= start:
                continue
            segments.append(
                DaySegment(
                    household_id=target.household_id,
                    device_id=target.device_id,
                    segment_index=len(segments),
                    start_second=int((start - day_start).total_seconds()),
                    end_second=int((end - day_start).total_seconds()),
                    sampling_interval_seconds=target.sampling_interval_seconds,
                )
            )
        return tuple(segments)

    def household_ids_for_date(
        self,
        target_date: date,
        business_utc_offset_seconds: int,
    ) -> tuple[str, ...]:
        """대상 날짜와 겹치는 설정이 하나라도 있는 가구.

        비활성 구간만 있는 가구도 포함한다. 기대 관측량이 0이 되어 NOT_APPLICABLE로
        남고, 설정에서 아예 빠진 가구와 구분된다.
        """

        day_start = day_start_utc(target_date, business_utc_offset_seconds)
        day_end = day_start + timedelta(seconds=SECONDS_PER_DAY)
        return tuple(
            sorted(
                {
                    target.household_id
                    for target in self.targets
                    if target.overlaps(day_start, day_end)
                }
            )
        )


def day_start_utc(target_date: date, business_utc_offset_seconds: int) -> datetime:
    """업무 날짜가 시작하는 UTC 시각."""

    midnight = datetime(
        target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc
    )
    return midnight - timedelta(seconds=business_utc_offset_seconds)


def _parse_instant(value, field_name: str) -> datetime | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise InvalidObservationTargets(f"{field_name} must be an ISO 8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise InvalidObservationTargets(f"{field_name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def load_targets(path: str | Path) -> ObservationTargets:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise InvalidObservationTargets("observation targets file must be a JSON object")

    config_version = payload.get("config_version")
    if not isinstance(config_version, str) or not config_version:
        raise InvalidObservationTargets("config_version is required")

    entries = payload.get("targets")
    if not isinstance(entries, list) or not entries:
        raise InvalidObservationTargets("targets must be a non-empty list")

    targets: list[ObservationTarget] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise InvalidObservationTargets(f"targets[{index}] must be an object")
        household_id = str(entry.get("household_id", "")).strip()
        device_id = str(entry.get("device_id", "")).strip()
        if not 1 <= len(household_id) <= 50 or not 1 <= len(device_id) <= 50:
            raise InvalidObservationTargets(
                f"targets[{index}] household_id/device_id length must be between 1 and 50"
            )
        interval = entry.get("sampling_interval_seconds", 1)
        if not isinstance(interval, int) or isinstance(interval, bool) or interval < 1:
            raise InvalidObservationTargets(
                f"targets[{index}] sampling_interval_seconds must be a positive integer"
            )
        if SECONDS_PER_DAY % interval:
            raise InvalidObservationTargets(
                f"targets[{index}] sampling_interval_seconds must divide 86400"
            )
        effective_from = _parse_instant(
            entry.get("effective_from"), f"targets[{index}].effective_from"
        )
        effective_to = _parse_instant(
            entry.get("effective_to"), f"targets[{index}].effective_to"
        )
        if effective_from and effective_to and effective_to <= effective_from:
            raise InvalidObservationTargets(
                f"targets[{index}] effective_to must be after effective_from"
            )
        enabled = entry.get("observation_enabled", True)
        if not isinstance(enabled, bool):
            raise InvalidObservationTargets(
                f"targets[{index}] observation_enabled must be a boolean"
            )
        targets.append(
            ObservationTarget(
                household_id=household_id,
                device_id=device_id,
                effective_from=effective_from,
                effective_to=effective_to,
                sampling_interval_seconds=interval,
                observation_enabled=enabled,
            )
        )

    _reject_overlaps(targets)
    return ObservationTargets(config_version=config_version, targets=tuple(targets))


def _reject_overlaps(targets: list[ObservationTarget]) -> None:
    """같은 가구의 활성 구간이 겹치면 대표 계측기가 둘이 되므로 설정 오류다."""

    far_past = datetime(1, 1, 1, tzinfo=timezone.utc)
    far_future = datetime(9999, 12, 31, tzinfo=timezone.utc)
    by_household: dict[str, list[ObservationTarget]] = {}
    for target in targets:
        if target.observation_enabled:
            by_household.setdefault(target.household_id, []).append(target)
    for household_id, household_targets in by_household.items():
        ordered = sorted(
            household_targets, key=lambda item: item.effective_from or far_past
        )
        for previous, current in zip(ordered, ordered[1:]):
            previous_end = previous.effective_to or far_future
            if previous_end > (current.effective_from or far_past):
                raise InvalidObservationTargets(
                    f"household {household_id} has overlapping effective ranges"
                )
